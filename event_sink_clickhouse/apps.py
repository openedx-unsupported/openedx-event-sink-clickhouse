"""
event_sink_clickhouse Django application initialization.
"""

from django.apps import AppConfig
from edx_django_utils.plugins import PluginSettings, PluginSignals


class EventSinkClickhouseConfig(AppConfig):
    """
    Configuration for the event_sink_clickhouse Django application.
    """

    name = "event_sink_clickhouse"
    verbose_name = "Event Sink ClickHouse"

    plugin_app = {
        PluginSettings.CONFIG: {
            "lms.djangoapp": {
                "production": {PluginSettings.RELATIVE_PATH: "settings.production"},
                "common": {PluginSettings.RELATIVE_PATH: "settings.common"},
            },
            "cms.djangoapp": {
                "production": {PluginSettings.RELATIVE_PATH: "settings.production"},
                "common": {PluginSettings.RELATIVE_PATH: "settings.common"},
            },
        },
        # Configuration setting for Plugin Signals for this app.
        PluginSignals.CONFIG: {
            # Configure the Plugin Signals for each Project Type, as needed.
            "cms.djangoapp": {
                # List of all plugin Signal receivers for this app and project type.
                PluginSignals.RECEIVERS: [
                    {
                        # The name of the app's signal receiver function.
                        PluginSignals.RECEIVER_FUNC_NAME: "receive_course_publish",
                        # The full path to the module where the signal is defined.
                        PluginSignals.SIGNAL_PATH: "xmodule.modulestore.django.COURSE_PUBLISHED",
                    }
                ],
            }
        },
    }

    def ready(self):
        """
        Import our Celery tasks for initialization.
        """
        super().ready()

        from event_sink_clickhouse import (  # pylint: disable=import-outside-toplevel, unused-import
            signals,
            sinks,
            tasks,
        )
