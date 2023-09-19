"""
Base classes for event sinks
"""
import csv
import datetime
import io
import json
from collections import namedtuple

import requests
from django.conf import settings
from edx_toggles.toggles import WaffleFlag

from event_sink_clickhouse.utils import get_model
from event_sink_clickhouse.waffle import WAFFLE_FLAG_NAMESPACE

ClickHouseAuth = namedtuple("ClickHouseAuth", ["username", "password"])


class BaseSink:
    """
    Base class for ClickHouse event sink, allows overwriting of default settings
    """

    CLICKHOUSE_BULK_INSERT_PARAMS = {
        "input_format_allow_errors_num": 1,
        "input_format_allow_errors_ratio": 0.1,
    }

    def __init__(self, connection_overrides, log):
        self.connection_overrides = connection_overrides
        self.log = log
        self.ch_url = settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG["url"]
        self.ch_auth = ClickHouseAuth(
            settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG["username"],
            settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG["password"],
        )
        self.ch_database = settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG["database"]
        self.ch_timeout_secs = settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG[
            "timeout_secs"
        ]

        # If any overrides to the ClickHouse connection
        if connection_overrides:
            self.ch_url = connection_overrides.get("url", self.ch_url)
            self.ch_auth = ClickHouseAuth(
                connection_overrides.get("username", self.ch_auth.username),
                connection_overrides.get("password", self.ch_auth.password),
            )
            self.ch_database = connection_overrides.get("database", self.ch_database)
            self.ch_timeout_secs = connection_overrides.get(
                "timeout_secs", self.ch_timeout_secs
            )

    def _send_clickhouse_request(self, request, expected_insert_rows=None):
        """
        Perform the actual HTTP requests to ClickHouse.
        """
        session = requests.Session()
        prepared_request = request.prepare()
        response = None

        try:
            response = session.send(prepared_request, timeout=self.ch_timeout_secs)
            response.raise_for_status()

            if expected_insert_rows:
                summary = response.headers["X-ClickHouse-Summary"]
                written_rows = json.loads(summary)["written_rows"]
                if expected_insert_rows != int(written_rows):
                    self.log.error(
                        f"Clickhouse query {prepared_request.url} expected {expected_insert_rows} "
                        f"rows to be inserted, but only got {written_rows}!"
                    )

            return response
        except requests.exceptions.HTTPError as e:
            self.log.error(str(e))
            self.log.error(e.response.headers)
            self.log.error(e.response)
            self.log.error(e.response.text)
            raise
        except (requests.exceptions.InvalidJSONError, KeyError):
            # ClickHouse can be configured not to return the metadata / summary we check above for
            # performance reasons. It's not critical, so we eat those here.
            return response


class ModelBaseSink(BaseSink):
    """
    Base class for ClickHouse event sink, allows overwriting of default settings

    This class is used for the model based event sink, which uses the Django ORM to write
    events to ClickHouse.
    """

    unique_key = None
    """
    str: A unique identifier key used to distinguish between different instances of the sink.
    It can be used to specify the uniqueness constraint when writing events to ClickHouse.
    """

    clickhouse_table_name = None
    """
    str: The name of the ClickHouse table where the events will be written.
    This should be set to the desired table name for the specific event type.
    """

    queryset = None
    """
    QuerySet: A Django QuerySet that represents the initial set of data to be processed by the sink.
    It can be used to filter and select specific data for writing to ClickHouse.
    """

    name = None
    """
    str: A human-readable name for the sink instance. This can be used for logging and identification purposes.
    """

    timestamp_field = None
    """
    str: The name of the field in the model representing the timestamp of the event.
    It is used to extract the timestamp from the event data for writing to ClickHouse.
    """

    serializer_class = None
    """
    Serializer: The serializer class responsible for converting event data into a format suitable for storage.
    This serializer should be compatible with Django's serialization framework.
    """

    model = None
    """
    Model: The Django model class representing the structure of the event data.
    This is used to validate and organize the data before writing it to ClickHouse.
    """

    nested_sinks = []
    """
    list: A list of nested sink instances that can be used to further process or route the event data.
    Nested sinks allow chaining multiple sinks together for more complex event processing pipelines.
    """

    def __init__(self, connection_overrides, log):
        super().__init__(connection_overrides, log)

        required_fields = [
            self.clickhouse_table_name,
            self.timestamp_field,
            self.unique_key,
            self.name,
        ]

        if not all(required_fields):
            raise NotImplementedError(
                "ModelBaseSink needs to be subclassed with clickhouse_table_name,"
                "timestamp_field, unique_key, and name"
            )

        self._nested_sinks = [
            sink(connection_overrides, log) for sink in self.nested_sinks
        ]

    def get_model(self):
        """
        Return the model to be used for the insert
        """
        return get_model(self.model)

    def get_queryset(self):
        """
        Return the queryset to be used for the insert
        """
        return self.get_model().objects.all()

    def dump(self, item_id, many=False, initial=None):
        """
        Do the serialization and send to ClickHouse
        """
        if many:
            # If we're dumping many items, we expect to get a list of items
            serialized_item = self.serialize_item(item_id, many=many, initial=initial)
            self.log.info(
                f"Now dumping {len(serialized_item)} {self.name} to ClickHouse",
            )
            self.send_item_and_log(item_id, serialized_item, many)
            self.log.info(
                f"Completed dumping {len(serialized_item)} {self.name} to ClickHouse"
            )

            for item in serialized_item:
                for nested_sink in self._nested_sinks:
                    nested_sink.dump_related(
                        item, item["dump_id"], item["time_last_dumped"]
                    )
        else:
            item = self.get_object(item_id)
            serialized_item = self.serialize_item(item, many=many, initial=initial)
            self.log.info(
                f"Now dumping {self.name} {item_id} to ClickHouse",
            )
            self.send_item_and_log(item_id, serialized_item, many)
            self.log.info(f"Completed dumping {self.name} {item_id} to ClickHouse")

            for nested_sink in self._nested_sinks:
                nested_sink.dump_related(
                    serialized_item,
                    serialized_item["dump_id"],
                    serialized_item["time_last_dumped"],
                )

    def send_item_and_log(
        self,
        item_id,
        serialized_item,
        many,
    ):
        """Send the item to clickhouse and log any errors"""
        try:
            self.send_item(serialized_item, many=many)
        except Exception:
            self.log.exception(
                f"Error trying to dump {self.name} {str(item_id)} to ClickHouse!",
            )
            raise

    def get_object(self, item_id):
        """
        Return the object to be dumped to ClickHouse
        """
        return self.get_model().objects.get(id=item_id)

    def dump_related(self, serialized_item, dump_id, time_last_dumped):
        """
        Dump related items to ClickHouse
        """
        raise NotImplementedError(
            "dump_related needs to be implemented in the subclass"
            f"{self.__class__.__name__}!"
        )

    def serialize_item(self, item, many=False, initial=None):
        """
        Serialize the data to be sent to ClickHouse
        """
        Serializer = self.get_serializer()
        serializer = Serializer(  # pylint: disable=not-callable
            item, many=many, initial=initial
        )
        return serializer.data

    def get_serializer(self):
        """
        Return the serializer to be used for the insert
        """
        return self.serializer_class

    def send_item(self, serialized_item, many=False):
        """
        Create the insert query and CSV to send the serialized CourseOverview to ClickHouse.

        We still use a CSV here even though there's only 1 row because it affords handles
        type serialization for us and keeps the pattern consistent.
        """
        params = self.CLICKHOUSE_BULK_INSERT_PARAMS.copy()

        # "query" is a special param for the query, it's the best way to get the FORMAT CSV in there.
        params[
            "query"
        ] = f"INSERT INTO {self.ch_database}.{self.clickhouse_table_name} FORMAT CSV"

        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)

        if many:
            for node in serialized_item:
                writer.writerow(node.values())
        else:
            writer.writerow(serialized_item.values())

        request = requests.Request(
            "POST",
            self.ch_url,
            data=output.getvalue().encode("utf-8"),
            params=params,
            auth=self.ch_auth,
        )

        self._send_clickhouse_request(
            request, expected_insert_rows=len(serialized_item) if many else 1
        )

    def fetch_target_items(self, ids=None, skip_ids=None, force_dump=False):
        """
        Fetch the items that should be dumped to ClickHouse
        """
        if ids:
            item_keys = [self.convert_id(item_id) for item_id in ids]
        else:
            item_keys = [item.id for item in self.get_queryset()]

        skip_ids = (
            [str(item_id) for item_id in skip_ids] if skip_ids else []
        )

        for item_key in item_keys:
            if str(item_key) in skip_ids:
                yield item_key, False, f"{self.name} is explicitly skipped"
            elif force_dump:
                yield item_key, True, "Force is set"
            else:
                should_be_dumped, reason = self.should_dump_item(item_key)
                yield item_key, should_be_dumped, reason

    def convert_id(self, item_id):
        """
        Convert the id to the correct type for the model
        """
        return item_id

    def should_dump_item(self, unique_key):  # pylint: disable=unused-argument
        """
        Return True if the item should be dumped to ClickHouse, False otherwise
        """
        return True, "No reason"

    def get_last_dumped_timestamp(self, item_id):
        """
        Return the last timestamp that was dumped to ClickHouse
        """
        params = {
            "query": f"SELECT max({self.timestamp_field}) as time_last_dumped "
            f"FROM {self.ch_database}.{self.clickhouse_table_name} "
            f"WHERE {self.unique_key} = '{item_id}'"
        }

        request = requests.Request("GET", self.ch_url, params=params, auth=self.ch_auth)

        response = self._send_clickhouse_request(request)
        response.raise_for_status()
        if response.text.strip():
            # ClickHouse returns timestamps in the format: "2023-05-03 15:47:39.331024+00:00"
            # Our internal comparisons use the str() of a datetime object, this handles that
            # transformation so that downstream comparisons will work.
            return str(datetime.datetime.fromisoformat(response.text.strip()))

        # Item has never been dumped, return None
        return None

    @classmethod
    def is_enabled(cls):
        """
        Return True if the sink is enabled, False otherwise
        """
        enabled = getattr(
            settings,
            f"{WAFFLE_FLAG_NAMESPACE.upper()}_{cls.model.upper()}_ENABLED",
            False,
        )
        # .. toggle_name: event_sink_clickhouse.model.enabled
        # .. toggle_implementation: WaffleFlag
        # .. toggle_default: False
        # .. toggle_description: Waffle flag to enable sink
        # .. toggle_use_cases: open_edx
        # .. toggle_creation_date: 2022-08-17
        waffle_flag = WaffleFlag(
            f"{WAFFLE_FLAG_NAMESPACE}.{cls.model}.enabled",
            __name__,
        )

        return enabled or waffle_flag.is_enabled()
