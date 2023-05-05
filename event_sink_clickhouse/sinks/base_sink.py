"""
Base classes for event sinks
"""
from collections import namedtuple

import requests
from django.conf import settings

ClickHouseAuth = namedtuple("ClickHouseAuth", ["username", "password"])


class BaseSink:
    """
    Base class for ClickHouse event sink, allows overwriting of default settings
    """
    def __init__(self, connection_overrides, log):
        self.log = log
        self.ch_url = settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG["url"]
        self.ch_auth = ClickHouseAuth(settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG["username"],
                                      settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG["password"])
        self.ch_database = settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG["database"]
        self.ch_timeout_secs = settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG["timeout_secs"]

        # If any overrides to the ClickHouse connection
        if connection_overrides:
            self.ch_url = connection_overrides.get("url", self.ch_url)
            self.ch_auth = ClickHouseAuth(connection_overrides.get("username", self.ch_auth.username),
                                          connection_overrides.get("password", self.ch_auth.password))
            self.ch_database = connection_overrides.get("database", self.ch_database)
            self.ch_timeout_secs = connection_overrides.get("timeout_secs", self.ch_timeout_secs)

    def _send_clickhouse_request(self, request):
        """
        Perform the actual HTTP requests to ClickHouse.
        """
        session = requests.Session()
        prepared_request = request.prepare()

        try:
            response = session.send(prepared_request, timeout=self.ch_timeout_secs)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self.log.error(str(e))
            self.log.error(e.response.headers)
            self.log.error(e.response)
            self.log.error(e.response.text)
            raise
