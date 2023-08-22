"""
Handler for the CMS COURSE_PUBLISHED event

Does the following:
- Pulls the course structure from modulestore
- Serialize the xblocks and their parent/child relationships
- Sends them to ClickHouse in CSV format

Note that the serialization format does not include all fields as there may be things like
LTI passwords and other secrets. We just take the fields necessary for reporting at this time.
"""
import json

from opaque_keys.edx.keys import CourseKey

from event_sink_clickhouse.serializers import CourseOverviewSerializer
from event_sink_clickhouse.sinks.base_sink import ModelBaseSink

# Defaults we want to ensure we fail early on bulk inserts
CLICKHOUSE_BULK_INSERT_PARAMS = {
    "input_format_allow_errors_num": 1,
    "input_format_allow_errors_ratio": 0.1,
}


class XBlockRelationshipSink(ModelBaseSink):
    """
    Sink for XBlock relationships
    """

    # unique_key = "dump_id"
    clickhouse_table_name = "course_relationships"
    name = "XBlock Relationships"
    timestamp_field = "time_last_dumped"
    unique_key = "parent_location"

    def dump_related(self, serialized_item, dump_id, time_last_dumped):
        self.dump(
            serialized_item,
            many=True,
            initial={"dump_id": dump_id, "time_last_dumped": time_last_dumped},
        )

    def serialize_item(self, item, many=False, initial=None):
        return item


class XBlockSink(ModelBaseSink):
    """
    Sink for XBlock model
    """

    unique_key = "location"
    clickhouse_table_name = "course_blocks"
    timestamp_field = "time_last_dumped"
    name = "XBlock"
    nested_sinks = []

    def dump_related(self, serialized_item, dump_id, time_last_dumped):
        """Dump all XBlocks for a course"""
        self.dump(
            serialized_item,
            many=True,
            initial={"dump_id": dump_id, "time_last_dumped": time_last_dumped},
        )

    def serialize_item(self, item, many=False, initial=None):
        """
        Serialize an XBlock into a dict
        """
        course_key = CourseKey.from_string(item["course_key"])
        modulestore = XBlockSink.get_modulestore()
        detached_xblock_types = XBlockSink.get_detached_xblock_types()

        location_to_node = {}
        items = modulestore.get_items(course_key)

        # Serialize the XBlocks to dicts and map them with their location as keys the
        # whole map needs to be completed before we can define relationships
        index = 0
        for block in items:
            index += 1
            fields = self.serialize_xblock(
                block,
                index,
                detached_xblock_types,
                initial["dump_id"],
                initial["time_last_dumped"],
            )
            location_to_node[
                XBlockSink.strip_branch_and_version(block.location)
            ] = fields

        nodes = list(location_to_node.values())

        self.serialize_relationships(
            items,
            location_to_node,
            course_key,
            initial["dump_id"],
            initial["time_last_dumped"],
        )

        return nodes

    def serialize_relationships(
        self, items, location_to_node, course_id, dump_id, dump_timestamp
    ):
        """Serialize the relationships between XBlocks"""
        relationships = []
        for item in items:
            for index, child in enumerate(item.get_children()):
                parent_node = location_to_node.get(
                    XBlockSink.strip_branch_and_version(item.location)
                )
                child_node = location_to_node.get(
                    XBlockSink.strip_branch_and_version(child.location)
                )

                if parent_node is not None and child_node is not None:
                    relationship = {
                        "course_key": str(course_id),
                        "parent_location": str(parent_node["location"]),
                        "child_location": str(child_node["location"]),
                        "order": index,
                        "dump_id": dump_id,
                        "time_last_dumped": dump_timestamp,
                    }
                    relationships.append(relationship)
        XBlockRelationshipSink(self.connection_overrides, self.log).dump_related(
            relationships, dump_id, dump_timestamp
        )

    def serialize_xblock(
        self, item, index, detached_xblock_types, dump_id, time_last_dumped
    ):
        """Serialize an XBlock instance into a dict"""
        course_key = item.scope_ids.usage_id.course_key
        block_type = item.scope_ids.block_type

        # Extra data not needed for the table to function, things can be
        # added here without needing to rebuild the whole table.
        json_data = {
            "course": course_key.course,
            "run": course_key.run,
            "block_type": block_type,
            "detached": 1 if block_type in detached_xblock_types else 0,
            "graded": 1 if getattr(item, "graded", False) else 0,
        }

        # Core table data, if things change here it's a big deal.
        serialized_block = {
            "org": course_key.org,
            "course_key": str(course_key),
            "location": str(item.location),
            "display_name": item.display_name_with_default.replace("'", "'"),
            "xblock_data_json": json.dumps(json_data),
            "order": index,
            "edited_on": str(getattr(item, "edited_on", "")),
            "dump_id": dump_id,
            "time_last_dumped": time_last_dumped,
        }

        return serialized_block

    @classmethod
    def get_modulestore(cls):
        """
        Import and return modulestore.
        Placed here to avoid model import at startup and to facilitate mocking them in testing.
        """
        # pylint: disable=import-outside-toplevel,import-error
        from xmodule.modulestore.django import modulestore

        return modulestore()

    @classmethod
    def get_detached_xblock_types(cls):
        """
        Import and return DETACHED_XBLOCK_TYPES.
        Placed here to avoid model import at startup and to facilitate mocking them in testing.
        """
        # pylint: disable=import-outside-toplevel,import-error
        from xmodule.modulestore.store_utilities import DETACHED_XBLOCK_TYPES

        return DETACHED_XBLOCK_TYPES

    @staticmethod
    def strip_branch_and_version(location):
        """
        Removes the branch and version information from a location.
        Args:
            location: an xblock's location.
        Returns: that xblock's location without branch and version information.
        """
        return location.for_branch(None)


class CourseOverviewSink(ModelBaseSink):  # pylint: disable=abstract-method
    """
    Sink for CourseOverview model
    """

    model = "course_overviews"
    unique_key = "course_key"
    clickhouse_table_name = "course_overviews"
    timestamp_field = "time_last_dumped"
    name = "Course Overview"
    serializer_class = CourseOverviewSerializer
    nested_sinks = [XBlockSink]
