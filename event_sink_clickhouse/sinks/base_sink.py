"""
Base classes for event sinks
"""

from django.conf import settings


class BaseSink:
    """
    Base class for ClickHouse event sink, allows overwriting of default settings
    """
    def __init__(self, connection_overrides, log):
        self.log = log
        self.ch_url = settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG["url"]
        self.ch_auth = (settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG["username"],
                        settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG["password"])
        self.ch_database = settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG["database"]
        self.ch_timeout_secs = settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG["timeout_secs"]

        # If any overrides to the ClickHouse connection
        if connection_overrides:
            self.ch_url = connection_overrides.get("url", self.ch_url)
            self.ch_auth = (connection_overrides.get("username", self.ch_auth[0]),
                            connection_overrides.get("password", self.ch_auth[1]))
            self.ch_database = connection_overrides.get("database", self.ch_database)
            self.ch_timeout_secs = connection_overrides.get("timeout_secs", self.ch_timeout_secs)
