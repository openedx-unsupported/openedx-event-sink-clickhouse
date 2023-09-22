"""User profile sink"""
from event_sink_clickhouse.serializers import UserExternalIDSerializer
from event_sink_clickhouse.sinks.base_sink import ModelBaseSink


class ExternalIDSInk(ModelBaseSink):  # pylint: disable=abstract-method
    """
    Sink for user external ID serializer
    """

    model = "external_id"
    unique_key = "id"
    clickhouse_table_name = "external_id"
    timestamp_field = "time_last_dumped"
    name = "External ID"
    serializer_class = UserExternalIDSerializer
