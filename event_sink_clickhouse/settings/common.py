"""
Default settings for the openedx_event_sink_clickhouse app.
"""


def plugin_settings(settings):
    """
    Adds default settings
    """
    settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG = {
        # URL to a running ClickHouse server's HTTP interface. ex: https://foo.openedx.org:8443/ or
        # http://foo.openedx.org:8123/ . Note that we only support the ClickHouse HTTP interface
        # to avoid pulling in more dependencies to the platform than necessary.
        "url": "http://clickhouse:8123",
        "username": "ch_cms",
        "password": "TYreGozgtDG3vkoWPUHVVM6q",
        "database": "event_sink",
        "timeout_secs": 5,
    }

    settings.EVENT_SINK_CLICKHOUSE_MODEL_CONFIG = {
        "user_profile": {
            "module": "common.djangoapps.student.models",
            "model": "UserProfile",
        },
        "course_overviews": {
            "module": "openedx.core.djangoapps.content.course_overviews.models",
            "model": "CourseOverview",
        },
    }
