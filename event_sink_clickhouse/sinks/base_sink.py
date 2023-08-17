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

from event_sink_clickhouse.utils import get_model

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
    clickhouse_table_name = None
    queryset = None
    name = None
    timestamp_field = None
    serializer_class = None
    model = None

    def __init__(self, connection_overrides, log):
        super().__init__(connection_overrides, log)

        required_fields = [
            self.model,
            self.clickhouse_table_name,
            self.timestamp_field,
            self.unique_key,
            self.name,
        ]

        if not all(required_fields):
            raise NotImplementedError(
                "ModelBaseSink needs to be subclassed with model, clickhouse_table_name,"
                "timestamp_field, unique_key, and name"
            )

    def get_model(self):
        """
        Return the model to be used for the insert
        """
        return get_model("user_profile")

    def get_queryset(self):
        """
        Return the queryset to be used for the insert
        """
        return self.get_model().objects.all()

    def dump(self, item_id):
        """
        Do the serialization and send to ClickHouse
        """
        item = self.get_model().objects.get(id=item_id)
        serialized_items = self.serialize_item(item)

        self.log.info(
            f"Now dumping {self.name} {item_id} to ClickHouse",
        )

        try:
            self.send_item(serialized_items)
            self.log.info("Completed dumping %s to ClickHouse", item_id)
        except Exception:
            self.log.exception(
                f"Error trying to dump {self.name} f{str(item_id)} to ClickHouse!",
            )
            raise

    def serialize_item(self, item):
        """
        Serialize the data to be sent to ClickHouse
        """
        Serializer = self.get_serializer()
        serializer = Serializer(item)  # pylint: disable=not-callable
        return serializer.data

    def get_serializer(self):
        """
        Return the serializer to be used for the insert
        """
        return self.serializer_class

    def send_item(self, serialized_item):
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
        writer.writerow(serialized_item.values())

        request = requests.Request(
            "POST",
            self.ch_url,
            data=output.getvalue().encode("utf-8"),
            params=params,
            auth=self.ch_auth,
        )

        self._send_clickhouse_request(request, expected_insert_rows=1)

    def fetch_target_items(self, ids=None, skip_ids=None, force_dump=False):
        """
        Fetch the items that should be dumped to ClickHouse
        """
        if ids:
            item_keys = [self.convert_id(id) for ids in ids]
        else:
            item_keys = [item.id for item in self.get_queryset()]

        for item_key in item_keys:
            if item_key in skip_ids:
                yield item_key, False, f"{self.name} is explicitly skipped"
            elif force_dump:
                yield item_key, True, "Force is set"
            else:
                should_be_dumped, reason = self.should_dump_item(item_key)
                yield item_key, should_be_dumped, reason

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

    def get_fields(self):
        """
        Return the fields to be used for the insert
        """
        return self.fields

    def fetch_model_data(self):
        """
        Fetch the data from the model queryset
        """
        return self.get_queryset().values(*self.get_fields())
