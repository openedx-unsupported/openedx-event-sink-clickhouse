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
import datetime
import io
import json
import uuid

import requests
from django.utils import timezone
from opaque_keys.edx.keys import CourseKey

from event_sink_clickhouse.sinks.base_sink import BaseSink

# Defaults we want to ensure we fail early on bulk inserts
CLICKHOUSE_BULK_INSERT_PARAMS = {
    "input_format_allow_errors_num": 1,
    "input_format_allow_errors_ratio": 0.1,
}


class CoursePublishedSink(BaseSink):
    """
    Event sink for the COURSE_PUBLISHED signal
    """
    @staticmethod
    def _get_detached_xblock_types():  # pragma: no cover
        """
        Import and return DETACHED_XBLOCK_TYPES.
        Placed here to avoid model import at startup and to facilitate mocking them in testing.
        """
        # pylint: disable=import-outside-toplevel,import-error
        from xmodule.modulestore.store_utilities import DETACHED_XBLOCK_TYPES
        return DETACHED_XBLOCK_TYPES

    @staticmethod
    def _get_modulestore():  # pragma: no cover
        """
        Import and return modulestore.
        Placed here to avoid model import at startup and to facilitate mocking them in testing.
        """
        # pylint: disable=import-outside-toplevel,import-error
        from xmodule.modulestore.django import modulestore
        return modulestore()

    @staticmethod
    def _get_course_overview_model():  # pragma: no cover
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
    def serialize_xblock(item, index, detached_xblock_types, dump_id, dump_timestamp):
        """
        Args:
            item: an XBlock
            index: a number indicating where the item falls in the course hierarchy

        Returns:
            fields: a *limited* dictionary of an XBlock's field names and values
            block_type: the name of the XBlock's type (i.e. 'course'
            or 'problem')

        Schema of the destination table, as defined in tutor-contrib-oars:
            org String NOT NULL,
            course_key String NOT NULL,
            location String NOT NULL,
            display_name String NOT NULL,
            xblock_data_json String NOT NULL,
            order Int32 default 0,
            edited_on String NOT NULL,
            dump_id UUID NOT NULL,
            time_last_dumped String NOT NULL
        """
        course_key = item.scope_ids.usage_id.course_key
        block_type = item.scope_ids.block_type

        # Extra data not needed for the table to function, things can be
        # added here without needing to rebuild the whole table.
        json_data = {
            'course': course_key.course,
            'run': course_key.run,
            'block_type': block_type,
            'detached': 1 if block_type in detached_xblock_types else 0,
        }

        # Core table data, if things change here it's a big deal.
        serialized_block = {
            'org': course_key.org,
            'course_key': str(course_key),
            'location': str(item.location),
            'display_name': item.display_name_with_default.replace("'", "\'"),
            'xblock_data_json': json.dumps(json_data),
            'order': index,
            'edited_on': str(getattr(item, 'edited_on', '')),
            'dump_id': dump_id,
            'time_last_dumped': dump_timestamp,
        }

        return serialized_block

    @staticmethod
    def serialize_course_overview(overview, dump_id, time_last_dumped):
        """
        Return a dict representing a subset of CourseOverview fields.

        Schema of the downstream table as defined in tutor-contrib-oars:
            org String NOT NULL,
            course_key String NOT NULL,
            display_name String NOT NULL,
            course_start String NOT NULL,
            course_end String NOT NULL,
            enrollment_start String NOT NULL,
            enrollment_end String NOT NULL,
            self_paced BOOL NOT NULL,
            course_data_json String NOT NULL,
            created String NOT NULL,
            modified String NOT NULL
            dump_id UUID NOT NULL,
            time_last_dumped String NOT NULL
        """
        json_fields = {
            "advertised_start": str(overview.advertised_start),
            "announcement": str(overview.announcement),
            "lowest_passing_grade": str(overview.lowest_passing_grade),
            "invitation_only": overview.invitation_only,
            "max_student_enrollments_allowed": overview.max_student_enrollments_allowed,
            "effort": overview.effort,
            "enable_proctored_exams": overview.enable_proctored_exams,
            "entrance_exam_enabled": overview.entrance_exam_enabled,
            "external_id": overview.external_id,
            "language": overview.language,
        }

        return {
            "org": overview.org,
            "course_key": str(overview.id),
            "display_name": overview.display_name,
            "course_start": overview.start,
            "course_end": overview.end,
            "enrollment_start": overview.enrollment_start,
            "enrollment_end": overview.enrollment_end,
            "self_paced": overview.self_paced,
            "course_data_json": json.dumps(json_fields),
            "created": overview.created,
            "modified": overview.modified,
            "dump_id": dump_id,
            "time_last_dumped": time_last_dumped
        }

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

        dump_id = str(uuid.uuid4())
        dump_timestamp = str(timezone.now())

        courseoverview_model = self._get_course_overview_model()
        course_overview = courseoverview_model.get_from_id(course_id)
        serialized_course_overview = self.serialize_course_overview(course_overview, dump_id, dump_timestamp)

        # Create a location to node mapping as a lookup for writing relationships later
        location_to_node = {}
        items = modulestore.get_items(course_id)

        # Serialize the XBlocks to dicts and map them with their location as keys the
        # whole map needs to be completed before we can define relationships
        index = 0
        for item in items:
            index += 1
            fields = self.serialize_xblock(item, index, detached_xblock_types, dump_id, dump_timestamp)
            location_to_node[self.strip_branch_and_version(item.location)] = fields

        # Create a list of relationships between blocks, using their locations as identifiers
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
                        'order': index,
                        'dump_id': dump_id,
                        'time_last_dumped': dump_timestamp,
                    }
                    relationships.append(relationship)

        nodes = list(location_to_node.values())
        return serialized_course_overview, nodes, relationships

    def _send_course_overview(self, serialized_overview):
        """
        Create the insert query and CSV to send the serialized CourseOverview to ClickHouse.

        We still use a CSV here even though there's only 1 row because it affords handles
        type serialization for us and keeps the pattern consistent.
        """
        params = CLICKHOUSE_BULK_INSERT_PARAMS.copy()

        # "query" is a special param for the query, it's the best way to get the FORMAT CSV in there.
        params["query"] = f"INSERT INTO {self.ch_database}.course_overviews FORMAT CSV"

        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(serialized_overview.values())

        request = requests.Request(
            'POST',
            self.ch_url,
            data=output.getvalue(),
            params=params,
            auth=self.ch_auth
        )

        self._send_clickhouse_request(request, expected_insert_rows=1)

    def _send_xblocks(self, serialized_xblocks):
        """
        Create the insert query and CSV to send the serialized XBlocks to ClickHouse.
        """
        params = CLICKHOUSE_BULK_INSERT_PARAMS.copy()

        # "query" is a special param for the query, it's the best way to get the FORMAT CSV in there.
        params["query"] = f"INSERT INTO {self.ch_database}.course_blocks FORMAT CSV"

        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)

        for node in serialized_xblocks:
            writer.writerow(node.values())

        request = requests.Request(
            'POST',
            self.ch_url,
            data=output.getvalue(),
            params=params,
            auth=self.ch_auth
        )

        self._send_clickhouse_request(request, expected_insert_rows=len(serialized_xblocks))

    def _send_relationships(self, relationships):
        """
        Create the insert query and CSV to send the serialized relationships to ClickHouse.
        """
        params = CLICKHOUSE_BULK_INSERT_PARAMS.copy()

        # "query" is a special param for the query, it's the best way to get the FORMAT CSV in there.
        params["query"] = f"INSERT INTO {self.ch_database}.course_relationships FORMAT CSV"
        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)

        for relationship in relationships:
            writer.writerow(relationship.values())

        request = requests.Request(
            'POST',
            self.ch_url,
            data=output.getvalue(),
            params=params,
            auth=self.ch_auth
        )

        self._send_clickhouse_request(request, expected_insert_rows=len(relationships))

    def dump(self, course_key):
        """
        Do the serialization and send to ClickHouse
        """
        serialized_courseoverview, serialized_blocks, relationships = self.serialize_course(course_key)

        self.log.info(
            "Now dumping %s to ClickHouse: %d serialized_blocks and %d relationships",
            course_key,
            len(serialized_blocks),
            len(relationships),
        )

        course_string = str(course_key)

        try:
            self._send_course_overview(serialized_courseoverview)
            self._send_xblocks(serialized_blocks)
            self._send_relationships(relationships)
            self.log.info("Completed dumping %s to ClickHouse", course_key)
        except Exception:
            self.log.exception(
                "Error trying to dump course %s to ClickHouse!",
                course_string
            )
            raise

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
        if approx_last_published:
            return str(approx_last_published)

        return None

    def fetch_target_courses(self, courses=None, skipped_courses=None, force=None):
        """
        Yield a set of courses meeting the given criteria.

        If no parameters are given, loads all course_keys from the
        modulestore. Filters out course_keys in the `skip` parameter,
        if provided.

        Args:
            courses: A list of string serializations of course keys.
                For example, ["course-v1:org+course+run"].
            skipped_courses: A list of string serializations of course keys to
                be ignored.
            force: Include all courses except those explicitly skipped via
                skipped_courses
        """
        modulestore = CoursePublishedSink._get_modulestore()

        if courses:
            course_keys = [CourseKey.from_string(course) for course in courses]
        else:
            course_keys = [
                course.id for course in modulestore.get_course_summaries()
            ]

        for course_key in course_keys:
            if course_key in skipped_courses:
                yield course_key, False, "Course is explicitly skipped"
            elif force:
                yield course_key, True, "Force is set"
            else:
                should_be_dumped, reason = self.should_dump_course(course_key)
                yield course_key, should_be_dumped, reason

    def get_course_last_dump_time(self, course_key):
        """
        Get the most recent dump time for this course from ClickHouse

        Args:
            course_key: a CourseKey

        Returns: The datetime that the command was last run, converted into
            text, or None, if there's no record of this command last being run.
        """
        params = {
            "query": f"SELECT max(time_last_dumped) as time_last_dumped "
                     f"FROM {self.ch_database}.course_blocks "
                     f"WHERE course_key = '{course_key}'"
        }

        request = requests.Request(
            'GET',
            self.ch_url,
            params=params,
            auth=self.ch_auth
        )

        response = self._send_clickhouse_request(request)
        response.raise_for_status()
        if response.text.strip():
            # ClickHouse returns timestamps in the format: "2023-05-03 15:47:39.331024+00:00"
            # Our internal comparisons use the str() of a datetime object, this handles that
            # transformation so that downstream comparisons will work.
            return str(datetime.datetime.fromisoformat(response.text.strip()))

        # Course has never been dumped, return None
        return None

    def should_dump_course(self, course_key):
        """
        Only dump the course if it's been changed since the last time it's been
        dumped.

        Args:
            course_key: a CourseKey object.

        Returns:
            - whether this course should be dumped (bool)
            - reason why course needs, or does not need, to be dumped (string)
        """

        course_last_dump_time = self.get_course_last_dump_time(course_key)

        # If we don't have a record of the last time this command was run,
        # we should serialize the course and dump it
        if course_last_dump_time is None:
            return True, "Course is not present in ClickHouse"

        course_last_published_date = self.get_course_last_published(course_key)

        # If we've somehow dumped this course but there is no publish date
        # skip it
        if course_last_dump_time and course_last_published_date is None:
            return False, "No last modified date in CourseOverview"

        # Otherwise, dump it if it is newer
        course_last_dump_time = datetime.datetime.strptime(course_last_dump_time, "%Y-%m-%d %H:%M:%S.%f+00:00")
        course_last_published_date = datetime.datetime.strptime(
            course_last_published_date,
            "%Y-%m-%d %H:%M:%S.%f+00:00"
        )
        needs_dump = course_last_dump_time < course_last_published_date

        if needs_dump:
            reason = (
                "Course has been published since last dump time - "
                f"last dumped {course_last_dump_time} < last published {str(course_last_published_date)}"
            )
        else:
            reason = (
                f"Course has NOT been published since last dump time - "
                f"last dumped {course_last_dump_time} >= last published {str(course_last_published_date)}"
            )
        return needs_dump, reason
