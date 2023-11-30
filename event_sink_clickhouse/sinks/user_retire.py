"""User retirement sink"""
import requests

from event_sink_clickhouse.serializers import UserRetirementSerializer
from event_sink_clickhouse.sinks.base_sink import ModelBaseSink


class UserRetirementSink(ModelBaseSink):  # pylint: disable=abstract-method
    """
    Sink for user retirement events
    """

    model = "auth_user"
    unique_key = "id"
    clickhouse_table_name = ["user_profile", "external_id"]
    timestamp_field = "modified"
    name = "User Retirement"
    serializer_class = UserRetirementSerializer

    def send_item(self, serialized_item, many=False):
        """
        Unlike the other data sinks, the User Retirement sink deletes records from the user PII tables in Clickhouse.

        Send delete queries to remove the serialized User from ClickHouse.
        """
        users = serialized_item if many else [serialized_item]
        user_ids = {str(user["user_id"]) for user in users}
        user_ids_str = ",".join(user_ids)

        for table in self.clickhouse_table_name:
            params = {
                "query": f"ALTER TABLE {self.ch_database}.{table} DELETE WHERE user_id in ({user_ids_str})",
            }
            request = requests.Request(
                "POST",
                self.ch_url,
                params=params,
                auth=self.ch_auth,
            )
            self._send_clickhouse_request(
                request,
                expected_insert_rows=0,  # DELETE requests don't return a row count
            )
