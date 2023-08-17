"""User profile sink"""
from event_sink_clickhouse.serializers import UserProfileSerializer
from event_sink_clickhouse.sinks.base_sink import ModelBaseSink


class UserProfileSink(ModelBaseSink):  # pylint: disable=abstract-method
    """
    Sink for user profile events
    """

    model = "user_profile"
    unique_key = "id"
    clickhouse_table_name = "user_profile"
    timestamp_field = "time_last_dumped"
    name = "User Profile"
    serializer_class = UserProfileSerializer
