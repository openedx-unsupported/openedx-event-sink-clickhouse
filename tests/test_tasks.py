"""
Tests for the tasks module.
"""
import unittest
from unittest.mock import MagicMock, patch

from event_sink_clickhouse.tasks import dump_data_to_clickhouse, dump_user_profile_to_clickhouse


class TestTasks(unittest.TestCase):
    """
    Test cases for tasks.
    """

    @patch("event_sink_clickhouse.tasks.UserProfileSink.is_enabled", return_value=True)
    @patch("event_sink_clickhouse.tasks.UserProfileSink")
    @patch("event_sink_clickhouse.tasks.celery_log")
    def test_dump_user_profile_to_clickhouse(
        self, mock_celery_log, mock_UserProfileSink, mock_is_enabled
    ):
        # Mock the required objects and methods
        mock_sink_instance = mock_UserProfileSink.return_value
        mock_sink_instance.dump.return_value = None

        # Call the function
        dump_user_profile_to_clickhouse(
            "user_profile_id", connection_overrides={"param": "value"}
        )

        # Assertions
        mock_is_enabled.assert_called_once()
        mock_UserProfileSink.assert_called_once_with(
            connection_overrides={"param": "value"}, log=mock_celery_log
        )
        mock_sink_instance.dump.assert_called_once_with("user_profile_id")

    @patch("event_sink_clickhouse.tasks.import_module")
    @patch("event_sink_clickhouse.tasks.celery_log")
    def test_dump_data_to_clickhouse(self, mock_celery_log, mock_import_module):
        # Mock the required objects and methods
        mock_Sink_class = MagicMock()
        mock_Sink_instance = mock_Sink_class.return_value
        mock_Sink_instance.dump.return_value = None
        mock_import_module.return_value = MagicMock(**{"sink_name": mock_Sink_class})

        # Call the function
        dump_data_to_clickhouse(
            "sink_module",
            "sink_name",
            "object_id",
            connection_overrides={"param": "value"},
        )

        # Assertions
        mock_import_module.assert_called_once_with("sink_module")
        mock_Sink_class.assert_called_once_with(
            connection_overrides={"param": "value"}, log=mock_celery_log
        )
        mock_Sink_instance.dump.assert_called_once_with("object_id")

    @patch("event_sink_clickhouse.tasks.import_module")
    def test_dump_data_to_clickhouse_disabled_sink(
        self, mock_import_module
    ):
        # Mock the required objects and methods
        mock_Sink_class = MagicMock()
        mock_Sink_class.is_enabled.return_value = False
        mock_Sink_instance = mock_Sink_class.return_value
        mock_Sink_instance.dump.return_value = None
        mock_import_module.return_value = MagicMock(**{"sink_name": mock_Sink_class})

        dump_data_to_clickhouse(
            "sink_module",
            "sink_name",
            "object_id",
            connection_overrides={"param": "value"},
        )

        # Assertions
        mock_import_module.assert_called_once_with("sink_module")
        mock_Sink_class.assert_not_called()
        mock_Sink_instance.dump.assert_not_called()
