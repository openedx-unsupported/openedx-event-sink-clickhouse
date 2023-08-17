"""
Tests for signal handlers.
"""
from unittest.mock import Mock, patch

from django.test import TestCase

from event_sink_clickhouse.signals import on_user_profile_updated, receive_course_publish


class SignalHandlersTestCase(TestCase):
    """
    Test cases for signal handlers.
    """

    @patch("event_sink_clickhouse.tasks.dump_course_to_clickhouse")
    def test_receive_course_publish(self, mock_dump_task):
        """
        Test that receive_course_publish calls dump_course_to_clickhouse.
        """
        sender = Mock()
        course_key = "sample_key"
        receive_course_publish(sender, course_key)

        mock_dump_task.delay.assert_called_once_with(course_key)

    @patch("event_sink_clickhouse.tasks.dump_user_profile_to_clickhouse")
    def test_on_user_profile_updated(self, mock_dump_task):
        """
        Test that on_user_profile_updated calls dump_user_profile_to_clickhouse.
        """
        instance = Mock()
        sender = Mock()
        on_user_profile_updated(sender, instance)

        mock_dump_task.delay.assert_called_once_with(instance.id)
