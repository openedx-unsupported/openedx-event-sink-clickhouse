"""
Tests for the base sinks.
"""
import ddt
import logging
from django.test import TestCase
from django.test.utils import override_settings

from unittest.mock import patch, Mock

from event_sink_clickhouse.sinks.base_sink import ModelBaseSink


class ChildSink(ModelBaseSink):

    model = "child_model"
    unique_key = "id"
    clickhouse_table_name = "child_model_table"
    timestamp_field = "time_last_dumped"
    name = "Child Model"
    serializer_class = Mock()


@override_settings(
    EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG = {
        # URL to a running ClickHouse server's HTTP interface. ex: https://foo.openedx.org:8443/ or
        # http://foo.openedx.org:8123/ . Note that we only support the ClickHouse HTTP interface
        # to avoid pulling in more dependencies to the platform than necessary.
        "url": "http://clickhouse:8123",
        "username": "ch_cms",
        "password": "TYreGozgtDG3vkoWPUHVVM6q",
        "database": "event_sink",
        "timeout_secs": 5,
    },
    EVENT_SINK_CLICKHOUSE_MODEL_CONFIG = {}
)
@ddt.ddt
class TestModelBaseSink(TestCase):

    def setUp(self):
        """
        Set up the test suite.
        """
        self.child_sink = ChildSink(connection_overrides={}, log=logging.getLogger())

    @ddt.data(
        (1, {"dump_id": 1, "time_last_dumped": "2020-01-01 00:00:00"}, False),
        (
            [1, 2],
            [
                {"dump_id": 1, "time_last_dumped": "2020-01-01 00:00:00"},
                {"dump_id": 2, "time_last_dumped": "2020-01-01 00:00:00"},
            ],
            True
        ),
    )
    @ddt.unpack
    def test_dump(self, items_id, serialized_items, many):
        """
        Test that the serialization/send logic is called correctly with many=True and many=False.
        """
        self.child_sink.send_item_and_log = Mock()
        self.child_sink.serialize_item = Mock(return_value=serialized_items)
        self.child_sink.get_object = Mock(return_value=items_id)

        self.child_sink.dump(items_id, many=many)

        self.child_sink.serialize_item.assert_called_once_with(items_id, many=many, initial=None)
        self.child_sink.send_item_and_log.assert_called_once_with(
            items_id,
            self.child_sink.serialize_item.return_value,
            many
        )

    def test_send_item_and_log(self):
        """
        Test that send_item is called correctly.
        """
        item = Mock(id=1)
        self.child_sink.send_item = Mock()
        serialized_item = {"dump_id": 1, "time_last_dumped": "2020-01-01 00:00:00"}

        self.child_sink.send_item_and_log(item.id, serialized_item, many=False)

        self.child_sink.send_item.assert_called_once_with(serialized_item, many=False)

    def test_serialize_item(self):
        """
        Test that serialize_item() returns the correct serialized data.
        """
        item = Mock(id=1)
        serialized_item = {"dump_id": 1, "time_last_dumped": "2020-01-01 00:00:00"}
        self.child_sink.get_serializer = Mock(data=serialized_item)
        self.child_sink.send_item_and_log = Mock()

        serialized_item = self.child_sink.serialize_item(item, many=False, initial=None)

        self.child_sink.get_serializer.return_value.assert_called_once_with(
            item,
            many=False,
            initial=None,
        )
        self.assertEqual(serialized_item, self.child_sink.get_serializer.return_value.return_value.data)

    @patch("event_sink_clickhouse.sinks.base_sink.csv")
    @patch("event_sink_clickhouse.sinks.base_sink.io")
    @patch("event_sink_clickhouse.sinks.base_sink.requests")
    @ddt.data(
        ({"dump_id": 1, "time_last_dumped": "2020-01-01 00:00:00"}, False),
        (
            [
                {"dump_id": 1, "time_last_dumped": "2020-01-01 00:00:00"},
                {"dump_id": 2, "time_last_dumped": "2020-01-01 00:00:00"},
            ],
            True
        ),
    )
    @ddt.unpack
    def test_send_items(self, serialized_items, many, mock_requests, mock_io, mock_csv):
        """
        Test that send_item() calls the correct requests.
        """
        params = self.child_sink.CLICKHOUSE_BULK_INSERT_PARAMS.copy()
        params["query"] = "INSERT INTO event_sink.child_model_table FORMAT CSV"
        self.child_sink._send_clickhouse_request = Mock()
        data = "1,2020-01-01 00:00:00\n2,2020-01-01 00:00:00\n"
        mock_io.StringIO.return_value.getvalue.return_value.encode.return_value = data

        self.child_sink.send_item(serialized_items, many=many)

        mock_requests.Request.assert_called_once_with(
            "POST",
            self.child_sink.ch_url,
            data=data,
            params=params,
            auth=self.child_sink.ch_auth,
        )
        self.child_sink._send_clickhouse_request(
            mock_requests.Request.return_value,
            expected_insert_rows=len(serialized_items),
        )

    def fetch_target_items(self):
        """
        Test that fetch_target_items() returns the correct data.
        """

    def test_get_last_dumped_timestamp(self):
        """
        Test that get_last_dumped_timestamp() returns the correct data.
        """
        pass
