"""
Tests for the course_published sinks.
"""
import logging
from datetime import datetime
from unittest.mock import patch

import pytest
import responses
from responses import matchers
from responses.registries import OrderedRegistry

from event_sink_clickhouse.sinks.course_published import CoursePublishedSink
from event_sink_clickhouse.tasks import dump_course_to_clickhouse
from test_utils.helpers import (
    check_block_csv_matcher,
    check_relationship_csv_matcher,
    course_factory,
    course_str_factory,
    get_clickhouse_http_params,
    mock_course_overview,
    mock_detached_xblock_types,
)


@responses.activate(registry=OrderedRegistry)  # pylint: disable=unexpected-keyword-arg,no-value-for-parameter
@patch("event_sink_clickhouse.sinks.course_published.CoursePublishedSink._get_detached_xblock_types")
@patch("event_sink_clickhouse.sinks.course_published.CoursePublishedSink._get_modulestore")
def test_course_publish_success(mock_modulestore, mock_detached, caplog):
    """
    Test of a successful end-to-end run.
    """
    # Necessary to get logs from the task
    caplog.set_level(logging.INFO, logger="edx.celery.task")

    # Create a fake course structure with a few fake XBlocks
    course = course_factory()
    mock_modulestore.return_value.get_items.return_value = course

    # Fake the "detached types" list since we can't import it here
    mock_detached.return_value = mock_detached_xblock_types()

    # Use the responses library to catch the POSTs to ClickHouse
    # and match them against the expected values, including CSV
    # content
    blocks_params, relationships_params = get_clickhouse_http_params()

    responses.post(
        "https://foo.bar/",
        match=[
            matchers.query_param_matcher(blocks_params),
            check_block_csv_matcher(course)
        ],
    )
    responses.post(
        "https://foo.bar/",
        match=[
            matchers.query_param_matcher(relationships_params),
            check_relationship_csv_matcher(course)
        ],
    )

    course = course_str_factory()
    dump_course_to_clickhouse(course)

    # Just to make sure we're not calling things more than we need to
    assert mock_modulestore.call_count == 1
    assert mock_detached.call_count == 1


@responses.activate(registry=OrderedRegistry)  # pylint: disable=unexpected-keyword-arg,no-value-for-parameter
@patch("event_sink_clickhouse.sinks.course_published.CoursePublishedSink._get_detached_xblock_types")
@patch("event_sink_clickhouse.sinks.course_published.CoursePublishedSink._get_modulestore")
def test_course_publish_clickhouse_error(mock_modulestore, mock_detached, caplog):
    """
    Test the case where a ClickHouse POST fails.
    """
    caplog.set_level(logging.INFO, logger="edx.celery.task")
    course = course_factory()
    mock_modulestore.return_value.get_items.return_value = course
    mock_detached.return_value = mock_detached_xblock_types()

    # This will raise an exception when we try to post to ClickHouse
    responses.post(
        "https://foo.bar/",
        body=Exception("Bogus test exception in ClickHouse call")
    )

    course = course_str_factory()

    with pytest.raises(Exception):
        dump_course_to_clickhouse(course)

    # Make sure everything was called as we expect
    assert mock_modulestore.call_count == 1
    assert mock_detached.call_count == 1

    # Make sure our log message went through.
    assert f"Error trying to dump course {course} to ClickHouse!" in caplog.text


@patch("event_sink_clickhouse.sinks.course_published.CoursePublishedSink._get_course_overview_model")
def test_get_course_last_published(mock_overview):
    """
    This function isn't in use yet, but we'll need it for the management command
    """
    # Create a fake course overview, which will return a datetime object
    course = mock_course_overview()
    mock_overview.return_value = course

    # Request our course last published date
    course_key = course_str_factory()

    # Confirm that the string date we get back is a valid date
    last_published_date = CoursePublishedSink.get_course_last_published(course_key)
    dt = datetime.strptime(last_published_date, "%Y-%m-%d %H:%M:%S.%f")
    assert dt
