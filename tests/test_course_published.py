"""
Tests for the course_published sinks.
"""
import json
import logging
from datetime import datetime
from unittest.mock import patch

import pytest
import requests
import responses
from django.test.utils import override_settings
from responses import matchers
from responses.registries import OrderedRegistry

from event_sink_clickhouse.sinks.course_published import CourseOverviewSink
from event_sink_clickhouse.tasks import dump_course_to_clickhouse
from test_utils.helpers import (
    check_block_csv_matcher,
    check_overview_csv_matcher,
    check_relationship_csv_matcher,
    course_factory,
    course_str_factory,
    fake_course_overview_factory,
    get_clickhouse_http_params,
    mock_course_overview,
    mock_detached_xblock_types,
)


@responses.activate(registry=OrderedRegistry)  # pylint: disable=unexpected-keyword-arg,no-value-for-parameter
@override_settings(EVENT_SINK_CLICKHOUSE_COURSE_OVERVIEW_ENABLED=True)
@patch("event_sink_clickhouse.sinks.course_published.CourseOverviewSink.serialize_item")
@patch("event_sink_clickhouse.sinks.course_published.CourseOverviewSink.get_model")
@patch("event_sink_clickhouse.sinks.course_published.get_detached_xblock_types")
@patch("event_sink_clickhouse.sinks.course_published.get_modulestore")
def test_course_publish_success(mock_modulestore, mock_detached, mock_overview, mock_serialize_item):
    """
    Test of a successful end-to-end run.
    """
    # Create a fake course structure with a few fake XBlocks
    course = course_factory()
    course_overview = fake_course_overview_factory(modified=datetime.now())
    mock_modulestore.return_value.get_items.return_value = course

    json_fields = {
        "advertised_start": str(course_overview.advertised_start),
        "announcement": str(course_overview.announcement),
        "lowest_passing_grade": float(course_overview.lowest_passing_grade),
        "invitation_only": course_overview.invitation_only,
        "max_student_enrollments_allowed": course_overview.max_student_enrollments_allowed,
        "effort": course_overview.effort,
        "enable_proctored_exams": course_overview.enable_proctored_exams,
        "entrance_exam_enabled": course_overview.entrance_exam_enabled,
        "external_id": course_overview.external_id,
        "language": course_overview.language,
    }

    mock_serialize_item.return_value = {
        "org": course_overview.org,
        "course_key": str(course_overview.id),
        "display_name": course_overview.display_name,
        "course_start": course_overview.start,
        "course_end": course_overview.end,
        "enrollment_start": course_overview.enrollment_start,
        "enrollment_end": course_overview.enrollment_end,
        "self_paced": course_overview.self_paced,
        "course_data_json": json.dumps(json_fields),
        "created": course_overview.created,
        "modified": course_overview.modified,
        "dump_id": "",
        "time_last_dumped": "",
    }

    # Fake the "detached types" list since we can't import it here
    mock_detached.return_value = mock_detached_xblock_types()

    mock_overview.return_value.get_from_id.return_value = course_overview

    # Use the responses library to catch the POSTs to ClickHouse
    # and match them against the expected values, including CSV
    # content
    course_overview_params, blocks_params, relationships_params = get_clickhouse_http_params()

    responses.post(
        "https://foo.bar/",
        match=[
            matchers.query_param_matcher(course_overview_params),
            check_overview_csv_matcher(course_overview)
        ],
    )
    responses.post(
        "https://foo.bar/",
        match=[
            matchers.query_param_matcher(relationships_params),
            check_relationship_csv_matcher(course)
        ],
    )
    responses.post(
        "https://foo.bar/",
        match=[
            matchers.query_param_matcher(blocks_params),
            check_block_csv_matcher(course)
        ],
    )

    course = course_str_factory()
    dump_course_to_clickhouse(course)

    # Just to make sure we're not calling things more than we need to
    assert mock_modulestore.call_count == 1
    assert mock_detached.call_count == 1


@responses.activate(registry=OrderedRegistry)  # pylint: disable=unexpected-keyword-arg,no-value-for-parameter
@patch("event_sink_clickhouse.sinks.course_published.CourseOverviewSink.serialize_item")
@patch("event_sink_clickhouse.sinks.course_published.CourseOverviewSink.get_model")
@patch("event_sink_clickhouse.sinks.course_published.get_detached_xblock_types")
@patch("event_sink_clickhouse.sinks.course_published.get_modulestore")
# pytest:disable=unused-argument
def test_course_publish_clickhouse_error(mock_modulestore, mock_detached, mock_overview, mock_serialize_item, caplog):
    """
    Test the case where a ClickHouse POST fails.
    """
    course = course_factory()
    mock_modulestore.return_value.get_items.return_value = course
    mock_detached.return_value = mock_detached_xblock_types()

    course_overview = fake_course_overview_factory(modified=datetime.now())
    mock_overview.return_value.get_from_id.return_value = course_overview

    json_fields = {
        "advertised_start": str(course_overview.advertised_start),
        "announcement": str(course_overview.announcement),
        "lowest_passing_grade": float(course_overview.lowest_passing_grade),
        "invitation_only": course_overview.invitation_only,
        "max_student_enrollments_allowed": course_overview.max_student_enrollments_allowed,
        "effort": course_overview.effort,
        "enable_proctored_exams": course_overview.enable_proctored_exams,
        "entrance_exam_enabled": course_overview.entrance_exam_enabled,
        "external_id": course_overview.external_id,
        "language": course_overview.language,
    }

    mock_serialize_item.return_value = {
        "org": course_overview.org,
        "course_key": str(course_overview.id),
        "display_name": course_overview.display_name,
        "course_start": course_overview.start,
        "course_end": course_overview.end,
        "enrollment_start": course_overview.enrollment_start,
        "enrollment_end": course_overview.enrollment_end,
        "self_paced": course_overview.self_paced,
        "course_data_json": json.dumps(json_fields),
        "created": course_overview.created,
        "modified": course_overview.modified,
        "dump_id": "",
        "time_last_dumped": "",
    }

    # This will raise an exception when we try to post to ClickHouse
    responses.post(
        "https://foo.bar/",
        body="Test Bad Request error",
        status=400
    )

    course = course_str_factory()

    with pytest.raises(requests.exceptions.RequestException):
        dump_course_to_clickhouse(course)

    # Make sure our log messages went through.
    assert "Test Bad Request error" in caplog.text
    assert f"Error trying to dump Course Overview {course} to ClickHouse!" in caplog.text


@patch("event_sink_clickhouse.sinks.course_published.CourseOverviewSink.get_model")
def test_get_course_last_published(mock_overview):
    """
    Make sure we get a valid date back from this in the expected format.
    """
    # Create a fake course overview, which will return a datetime object
    course = mock_course_overview()
    mock_overview.return_value = course

    # Request our course last published date
    course_key = course_str_factory()

    # Confirm that the string date we get back is a valid date
    last_published_date = CourseOverviewSink(None, None).get_course_last_published(course_key)
    dt = datetime.strptime(last_published_date, "%Y-%m-%d %H:%M:%S.%f")
    assert dt


@responses.activate(registry=OrderedRegistry)  # pylint: disable=unexpected-keyword-arg,no-value-for-parameter
@patch("event_sink_clickhouse.sinks.course_published.CourseOverviewSink.get_model")
def test_no_last_published_date(mock_overview):
    """
    Test that we get a None value back for courses that don't have a modified date.

    In some cases there is not modified date on a course. In coursegraph we
    skipped these if they are already in the database, so we're continuing this trend here.
    """
    # Fake a course with no modified date
    course = mock_course_overview()
    mock_overview.return_value = course
    mock_overview.return_value.get_from_id.return_value = fake_course_overview_factory(modified=None)

    # Request our course last published date
    course_key = course_str_factory()

    # should_dump_course will reach out to ClickHouse for the last dump date
    # we'll fake the response here to have any date, such that we'll exercise
    # all the "no modified date" code.
    responses.get(
        "https://foo.bar/",
        body="2023-05-03 15:47:39.331024+00:00"
    )

    # Confirm that the string date we get back is a valid date
    sink = CourseOverviewSink(connection_overrides={}, log=logging.getLogger())
    should_dump_course, reason = sink.should_dump_item(course_key)

    assert should_dump_course is False
    assert reason == "No last modified date in CourseOverview"


@responses.activate(registry=OrderedRegistry)  # pylint: disable=unexpected-keyword-arg,no-value-for-parameter
def test_course_not_present_in_clickhouse():
    """
    Test that a course gets dumped if it's never been dumped before
    """
    # Request our course last published date
    course_key = course_str_factory()

    responses.get(
        "https://foo.bar/",
        body=""
    )

    # Confirm that the string date we get back is a valid date
    sink = CourseOverviewSink(connection_overrides={}, log=logging.getLogger())
    last_published_date = sink.get_last_dumped_timestamp(course_key)
    assert last_published_date is None


@responses.activate(registry=OrderedRegistry)  # pylint: disable=unexpected-keyword-arg,no-value-for-parameter
def test_get_last_dump_time():
    """
    Test that we return the expected thing from last dump time.
    """
    # Request our course last published date
    course_key = course_str_factory()

    # Mock out the response we expect to get from ClickHouse, just a random
    # datetime in the correct format.
    responses.get(
        "https://foo.bar/",
        body="2023-05-03 15:47:39.331024+00:00"
    )

    # Confirm that the string date we get back is a valid date
    sink = CourseOverviewSink(connection_overrides={}, log=logging.getLogger())
    last_published_date = sink.get_last_dumped_timestamp(course_key)
    dt = datetime.strptime(last_published_date, "%Y-%m-%d %H:%M:%S.%f+00:00")
    assert dt
