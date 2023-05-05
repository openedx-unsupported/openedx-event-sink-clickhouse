"""
This file contains a management command for exporting course modulestore data to ClickHouse.
"""

import logging

from celery import shared_task
from edx_django_utils.monitoring import set_code_owner_attribute
from opaque_keys.edx.keys import CourseKey

from event_sink_clickhouse.sinks.course_published import CoursePublishedSink

log = logging.getLogger(__name__)
celery_log = logging.getLogger('edx.celery.task')


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
    course_key = CourseKey.from_string(course_key_string)
    sink = CoursePublishedSink(connection_overrides=connection_overrides, log=celery_log)
    sink.dump(course_key)
