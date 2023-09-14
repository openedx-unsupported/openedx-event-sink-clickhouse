"""
This file contains a management command for exporting course modulestore data to ClickHouse.
"""

import logging
from importlib import import_module

from celery import shared_task
from edx_django_utils.monitoring import set_code_owner_attribute
from opaque_keys.edx.keys import CourseKey

from event_sink_clickhouse.sinks.course_published import CourseOverviewSink
from event_sink_clickhouse.sinks.user_profile_sink import UserProfileSink

log = logging.getLogger(__name__)
celery_log = logging.getLogger("edx.celery.task")


@shared_task
@set_code_owner_attribute
def dump_course_to_clickhouse(course_key_string, connection_overrides=None):
    """
    Serialize a course and writes it to ClickHouse.

    Arguments:
        course_key_string: course key for the course to be exported
        connection_overrides (dict):  overrides to ClickHouse connection
            parameters specified in `settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG`.
    """
    if CourseOverviewSink.is_enabled():  # pragma: no cover
        course_key = CourseKey.from_string(course_key_string)
        sink = CourseOverviewSink(connection_overrides=connection_overrides, log=celery_log)
        sink.dump(course_key)


@shared_task
@set_code_owner_attribute
def dump_user_profile_to_clickhouse(user_profile_id, connection_overrides=None):
    """
    Serialize a user profile and writes it to ClickHouse.

    Arguments:
        user_profile_id: user profile id for the user profile to be exported
        connection_overrides (dict):  overrides to ClickHouse connection
            parameters specified in `settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG`.
    """
    if UserProfileSink.is_enabled():  # pragma: no cover
        sink = UserProfileSink(
            connection_overrides=connection_overrides, log=celery_log
        )
        sink.dump(user_profile_id)


@shared_task
@set_code_owner_attribute
def dump_data_to_clickhouse(
    sink_module, sink_name, object_id, connection_overrides=None
):
    """
    Serialize a data and writes it to ClickHouse.

    Arguments:
        sink_module: module path of sink
        sink_name: name of sink class
        object_id: id of object
        connection_overrides (dict):  overrides to ClickHouse connection
    """
    Sink = getattr(import_module(sink_module), sink_name)

    sink = Sink(connection_overrides=connection_overrides, log=celery_log)
    sink.dump(object_id)
