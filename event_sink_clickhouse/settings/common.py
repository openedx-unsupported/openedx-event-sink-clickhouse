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
        "username": "changeme",
        "password": "changeme",
        "database": "event_sink",
        "timeout_secs": 3,
    }
