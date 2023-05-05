"""
Signal handler functions, mapped to specific signals in apps.py.
"""


def receive_course_publish(sender, course_key, **kwargs):  # pylint: disable=unused-argument  # pragma: no cover
    """
    Receives COURSE_PUBLISHED signal and queues the dump job.
    """
    # import here, because signal is registered at startup, but items in tasks are not yet able to be loaded
    from event_sink_clickhouse.tasks import dump_course_to_clickhouse  # pylint: disable=import-outside-toplevel

    dump_course_to_clickhouse.delay(str(course_key))
