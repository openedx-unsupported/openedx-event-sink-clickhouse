"""
Helper functions for tests
"""

import csv
import random
import string
from datetime import datetime
from io import StringIO
from unittest.mock import MagicMock, Mock

from opaque_keys.edx.keys import CourseKey
from opaque_keys.edx.locator import BlockUsageLocator

from event_sink_clickhouse.sinks.course_published import CoursePublishedSink

ORIG_IMPORT = __import__
ORG = "testorg"
COURSE = "testcourse"
COURSE_RUN = "2023_Fall"


class FakeXBlock:
    """
    Fakes the parameters of an XBlock that we care about.
    """
    def __init__(self, identifier, detached_block=False):
        self.block_type = "course_info" if detached_block else "vertical"
        self.scope_ids = Mock()
        self.scope_ids.usage_id.course_key = course_key_factory()
        self.scope_ids.block_type = self.block_type
        self.location = block_usage_locator_factory()
        self.display_name_with_default = f"Display name {identifier}"
        self.edited_on = datetime.now()
        self.children = []

    def get_children(self):
        """
        Fakes the method of the same name from an XBlock.
        """
        return self.children


def course_str_factory():
    """
    Return a valid course key string.
    """
    course_str = f"course-v1:{ORG}+{COURSE}+{COURSE_RUN}"
    return course_str


def course_key_factory():
    """
    Return a CourseKey object from our course key string.
    """
    return CourseKey.from_string(course_str_factory())


def block_usage_locator_factory():
    """
    Create a BlockUsageLocator with a random id.
    """
    block_id = ''.join(random.choices(string.ascii_letters, k=10))
    return BlockUsageLocator(course_key_factory(), block_type="category", block_id=block_id, deprecated=True)


def mock_course_overview():
    """
    Create a fake CourseOverview object that supports just the things we care about.
    """
    mock_overview = MagicMock()
    mock_overview.get_from_id = MagicMock()
    mock_overview.get_from_id.return_value.modified = datetime.now()
    return mock_overview


def mock_detached_xblock_types():
    """
    Mock the return results of xmodule.modulestore.store_utilities.DETACHED_XBLOCK_TYPES
    """
    # Current values as of 2023-05-01
    return {'static_tab', 'about', 'course_info'}


def get_clickhouse_http_params():
    """
    Get the params used in ClickHouse queries.
    """
    blocks_params = {
        "input_format_allow_errors_num": 1,
        "input_format_allow_errors_ratio": 0.1,
        "query": "INSERT INTO cool_data.course_blocks FORMAT CSV"
    }
    relationships_params = {
        "input_format_allow_errors_num": 1,
        "input_format_allow_errors_ratio": 0.1,
        "query": "INSERT INTO cool_data.course_relationships FORMAT CSV"
    }

    return blocks_params, relationships_params


def course_factory():
    """
    Return a fake course structure that exercises most of the serialization features.
    """
    # Create a base block
    top_block = FakeXBlock("top")
    course = [top_block, ]

    # Create a few children
    for i in range(3):
        block = FakeXBlock(f"Child {i}")
        course.append(block)
        top_block.children.append(block)

        # Create grandchildren on some children
        if i > 0:
            sub_block = FakeXBlock(f"Grandchild {i}")
            course.append(sub_block)
            block.children.append(sub_block)

    # Create some detached blocks at the top level
    for i in range(3):
        course.append(FakeXBlock(f"Detached {i}", detached_block=True))

    return course


def check_block_csv_matcher(course):
    """
    Match the course structure CSV against the test course.

    This is a matcher for the "responses" library. It returns a function
    that actually does the matching.
    """
    def match(request):
        body = request.body
        lines = body.split("\n")[:-1]

        # There should be one CSV line for each block in the test course
        if len(lines) != len(course):
            return False, f"Body has {len(lines)} lines, course has {len(course)}"

        f = StringIO(body)
        reader = csv.reader(f)

        i = 0
        try:
            # The CSV should be in the same order as our course, make sure
            # everything matches
            for row in reader:
                block = course[i]
                assert row[0] == block.location.org
                assert row[1] == str(block.location.course_key)
                assert row[2] == block.location.course
                assert row[3] == block.location.run
                assert row[4] == str(course[i].location)
                assert row[5] == block.display_name_with_default
                assert row[6] == str(block.block_type)
                i += 1
        except AssertionError as e:
            return False, f"Mismatch in row {i}: {e}"
        return True, ""
    return match


def check_relationship_csv_matcher(course):
    """
    Match the relationship CSV against the test course.

    This is a matcher for the "responses" library. It returns a function
    that actually does the matching.
    """
    # Build our own copy of the test relationships first
    relationships = []
    for block in course:
        course_key = str(block.location.course_key)
        for _, child in enumerate(block.get_children()):
            parent_node = str(CoursePublishedSink.strip_branch_and_version(block.location))
            child_node = str(CoursePublishedSink.strip_branch_and_version(child.location))
            relationships.append((course_key, parent_node, child_node))

    def match(request):
        body = request.body
        lines = body.split("\n")[:-1]

        # The relationships CSV should have the same number of relationships as our test
        if len(lines) != len(relationships):
            return False, f"Body has {len(lines)} lines but there are {len(relationships)} relationships"

        f = StringIO(body)
        reader = csv.reader(f)

        i = 0
        try:
            # The CSV should be in the same order as our relationships, make sure
            # everything matches
            for row in reader:
                print(row)
                print(relationships[i])
                relation = relationships[i]
                assert row[0] == relation[0]
                assert row[1] == relation[1]
                assert row[2] == relation[2]
                i += 1
        except AssertionError as e:
            return False, f"Mismatch in row {i}: {e}"
        return True, ""
    return match
