"""
Production settings for the openedx_event_sink_clickhouse app.
"""


def plugin_settings(settings):
    """
    Override the default app settings with production settings.
    """
    settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG = settings.ENV_TOKENS.get(
        "EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG",
        settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG,
    )
