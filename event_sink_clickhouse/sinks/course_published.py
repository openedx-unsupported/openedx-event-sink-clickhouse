"""
Handler for the CMS COURSE_PUBLISHED event

Does the following:
- Pulls the course structure from modulestore
- Serialize the xblocks and their parent/child relationships
- Sends them to ClickHouse in CSV format

Note that the serialization format does not include all fields as there may be things like
LTI passwords and other secrets. We just take the fields necessary for reporting at this time.
"""

import csv
import io

import requests
from django.utils import timezone

from .base_sink import BaseSink


class CoursePublishedSink(BaseSink):
    """
    Event sink for the COURSE_PUBLISHED signal
    """
    @staticmethod
    def _get_detached_xblock_types():
        """
        Import and return DETACHED_XBLOCK_TYPES.
        Placed here to avoid model import at startup and to facilitate mocking them in testing.
        """
        # pylint: disable=import-outside-toplevel,import-error
        from xmodule.modulestore.store_utilities import DETACHED_XBLOCK_TYPES
        return DETACHED_XBLOCK_TYPES

    @staticmethod
    def _get_modulestore():
        """
        Import and return modulestore.
        Placed here to avoid model import at startup and to facilitate mocking them in testing.
        """
        # pylint: disable=import-outside-toplevel,import-error
        from xmodule.modulestore.django import modulestore
        return modulestore()

    @staticmethod
    def _get_course_overview_model():
        """
        Import and return CourseOverview.
        Placed here to avoid model import at startup and to facilitate mocking them in testing.
        """
        # pylint: disable=import-outside-toplevel,import-error
        from openedx.core.djangoapps.content.course_overviews.models import CourseOverview
        return CourseOverview

    @staticmethod
    def strip_branch_and_version(location):
        """
        Removes the branch and version information from a location.
        Args:
            location: an xblock's location.
        Returns: that xblock's location without branch and version information.
        """
        return location.for_branch(None)

    @staticmethod
    def get_course_last_published(course_key):
        """
        Get approximate last publish date for the given course.

        We use the 'modified' column in the CourseOverview table as a quick and easy
        (although perhaps inexact) way of determining when a course was last
        published. This works because CourseOverview rows are re-written upon
        course publish.

        Args:
            course_key: a CourseKey

        Returns: The datetime the course was last published at, stringified.
            Uses Python's default str(...) implementation for datetimes, which
            is sortable and similar to ISO 8601:
            https://docs.python.org/3/library/datetime.html#datetime.date.__str__
        """
        CourseOverview = CoursePublishedSink._get_course_overview_model()
        approx_last_published = CourseOverview.get_from_id(course_key).modified
        return str(approx_last_published)

    @staticmethod
    def serialize_item(item, index, detached_xblock_types):
        """
        Args:
            item: an XBlock
            index: a number indicating where the item falls in the course hierarchy

        Returns:
            fields: a *limited* dictionary of an XBlock's field names and values
            block_type: the name of the XBlock's type (i.e. 'course'
            or 'problem')
        """
        course_key = item.scope_ids.usage_id.course_key
        block_type = item.scope_ids.block_type

        rtn_fields = {
            'org': course_key.org,
            'course_key': str(course_key),
            'course': course_key.course,
            'run': course_key.run,
            'location': str(item.location),
            'display_name': item.display_name_with_default.replace("'", "\'"),
            'block_type': block_type,
            'detached': 1 if block_type in detached_xblock_types else 0,
            'edited_on': str(getattr(item, 'edited_on', '')),
            'time_last_dumped': str(timezone.now()),
            'order': index,
        }

        return rtn_fields

    def serialize_course(self, course_id):
        """
        Serializes a course into a CSV of nodes and relationships.

        Args:
            course_id: CourseKey of the course we want to serialize

        Returns:
            nodes: a list of dicts representing xblocks for the course
            relationships: a list of dicts representing relationships between nodes
        """
        modulestore = CoursePublishedSink._get_modulestore()
        detached_xblock_types = CoursePublishedSink._get_detached_xblock_types()

        # create a location to node mapping we'll need later for
        # writing relationships
        location_to_node = {}
        items = modulestore.get_items(course_id)

        # create nodes
        i = 0
        for item in items:
            i += 1
            fields = self.serialize_item(item, i, detached_xblock_types)
            location_to_node[self.strip_branch_and_version(item.location)] = fields

        # create relationships
        relationships = []
        for item in items:
            for index, child in enumerate(item.get_children()):
                parent_node = location_to_node.get(self.strip_branch_and_version(item.location))
                child_node = location_to_node.get(self.strip_branch_and_version(child.location))

                if parent_node is not None and child_node is not None:
                    relationship = {
                        'course_key': str(course_id),
                        'parent_location': str(parent_node["location"]),
                        'child_location': str(child_node["location"]),
                        'order': index
                    }
                    relationships.append(relationship)

        nodes = list(location_to_node.values())
        return nodes, relationships

    def dump(self, course_key):
        """
        Do the serialization and send to ClickHouse
        """
        nodes, relationships = self.serialize_course(course_key)

        self.log.info(
            "Now dumping %s to ClickHouse: %d nodes and %d relationships",
            course_key,
            len(nodes),
            len(relationships),
        )

        course_string = str(course_key)

        try:
            # Params that begin with "param_" will be used in the query replacement
            # all others are ClickHouse settings.
            params = {
                # Fail early on bulk inserts
                "input_format_allow_errors_num": 1,
                "input_format_allow_errors_ratio": 0.1,
            }

            # "query" is a special param for the query, it's the best way to get the FORMAT CSV in there.
            params["query"] = f"INSERT INTO {self.ch_database}.course_blocks FORMAT CSV"

            output = io.StringIO()
            writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)

            for node in nodes:
                writer.writerow(node.values())

            response = requests.post(self.ch_url, data=output.getvalue(), params=params, auth=self.ch_auth,
                                     timeout=self.ch_timeout_secs)
            self.log.info(response.headers)
            self.log.info(response)
            self.log.info(response.text)
            response.raise_for_status()

            # Just overwriting the previous query
            params["query"] = f"INSERT INTO {self.ch_database}.course_relationships FORMAT CSV"
            output = io.StringIO()
            writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)

            for relationship in relationships:
                writer.writerow(relationship.values())

            response = requests.post(self.ch_url, data=output.getvalue(), params=params, auth=self.ch_auth,
                                     timeout=self.ch_timeout_secs)
            self.log.info(response.headers)
            self.log.info(response)
            self.log.info(response.text)
            response.raise_for_status()

            self.log.info("Completed dumping %s to ClickHouse", course_key)

        except Exception:
            self.log.exception(
                "Error trying to dump course %s to ClickHouse!",
                course_string
            )
            raise
