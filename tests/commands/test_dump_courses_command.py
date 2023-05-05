"""
Tests for the dump_courses_to_clickhouse management command.
"""
from collections import namedtuple
from datetime import datetime, timedelta
from unittest.mock import patch

import django.core.management.base
import pytest
from django.core.management import call_command

from test_utils.helpers import FakeCourse, FakeCourseOverview, course_str_factory


@pytest.fixture
def mock_common_calls():
    """
    Mock out calls that we test elsewhere and aren't relevant to the command tests.
    """
    command_path = "event_sink_clickhouse.management.commands.dump_courses_to_clickhouse"
    with patch(command_path+".dump_course_to_clickhouse") as mock_dump_course:
        with patch(command_path+".CoursePublishedSink._get_course_overview_model") as mock_get_course_overview_model:
            with patch(command_path+".CoursePublishedSink._get_modulestore") as mock_modulestore:
                with patch(command_path+".CoursePublishedSink.get_course_last_dump_time") as mock_last_dump_time:
                    # Set a reasonable default last dump time a year in the past
                    mock_last_dump_time.return_value = \
                        (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S.%f+00:00")
                    yield mock_dump_course, mock_get_course_overview_model, mock_modulestore, mock_last_dump_time


def dump_command_clickhouse_options():
    """
    Pytest params for all the different ClickHouse options.

    Just making sure every option gets passed through correctly.
    """
    options = [
        {},
        {'url': "https://foo/"},
        {'username': "Foo"},
        {'password': "F00"},
        {'database': "foo"},
        {'timeout_secs': 60},
        {'url': "https://foo/", 'username': "Foo", 'password': "F00", 'database': "foo", 'timeout_secs': 60},
    ]

    for option in options:
        yield option


@pytest.mark.parametrize("option_combination", dump_command_clickhouse_options())
def test_dump_courses_to_clickhouse_db_options(
    option_combination,
    mock_common_calls,
    caplog
):
    mock_dump_course, mock_get_course_overview_model, mock_modulestore, mock_last_dump_time = mock_common_calls

    course_id = course_str_factory()

    fake_modulestore_courses = [FakeCourse(course_id)]
    mock_modulestore.return_value.get_course_summaries.return_value = fake_modulestore_courses
    mock_get_course_overview_model.return_value.get_from_id.return_value = FakeCourseOverview(
        modified=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f+00:00")
    )

    call_command(
        'dump_courses_to_clickhouse',
        **option_combination
    )

    # Make sure that our mocks were called as expected
    assert mock_modulestore.call_count == 1
    assert mock_dump_course.apply_async.call_count == 1
    mock_dump_course.apply_async.assert_called_once_with(kwargs=dict(
        course_key_string=course_id,
        connection_overrides=option_combination
    ))
    assert "Course has been published since last dump time" in caplog.text
    assert "These courses were submitted for dump to ClickHouse successfully" in caplog.text


CommandOptions = namedtuple("TestCommandOptions", ["options", "expected_num_submitted", "expected_logs"])


def dump_command_basic_options():
    """
    Pytest params for all the different non-ClickHouse command options.
    """
    options = [
        CommandOptions(
            options={"courses_to_skip": [course_str_factory()]},
            expected_num_submitted=0,
            expected_logs=[
                "0 courses submitted for export to ClickHouse. 1 courses skipped.",
                "Course is explicitly skipped"
            ]
        ),
        CommandOptions(
            options={"limit": 1},
            expected_num_submitted=1,
            expected_logs=["Limit of 1 eligible course has been reached, quitting!"]
        ),
        CommandOptions(
            options={"courses": [course_str_factory()]},
            expected_num_submitted=1,
            expected_logs=[
                "Course has been published since last dump time",
                "These courses were submitted for dump to ClickHouse successfully"
            ]
        ),
        CommandOptions(
            options={"force": True},
            expected_num_submitted=1,
            expected_logs=["Force is set"]
        ),
    ]

    for option in options:
        yield option


@pytest.mark.parametrize("test_command_option", dump_command_basic_options())
def test_dump_courses_options(
    test_command_option,
    mock_common_calls,
    caplog
):
    mock_dump_course, mock_get_course_overview_model, mock_modulestore, mock_last_dump_time = mock_common_calls

    option_combination, expected_num_submitted, expected_outputs = test_command_option
    course_id = course_str_factory()

    fake_modulestore_courses = [FakeCourse(course_id), ]
    mock_modulestore.return_value.get_course_summaries.return_value = fake_modulestore_courses
    mock_get_course_overview_model.return_value.get_from_id.return_value = FakeCourseOverview(
        modified=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f+00:00")
    )

    call_command(
        'dump_courses_to_clickhouse',
        **option_combination
    )

    # Make sure that our mocks were called as expected
    assert mock_modulestore.call_count == 1
    assert mock_dump_course.apply_async.call_count == expected_num_submitted
    for expected_output in expected_outputs:
        assert expected_output in caplog.text


def dump_command_invalid_options():
    """
    Pytest params for all the different non-ClickHouse command options.
    """
    options = [
        CommandOptions(
            options={"force": True, "limit": 100},
            expected_num_submitted=0,
            expected_logs=[
                "The 'limit' option cannot be used with 'force'",
            ]
        ),
        CommandOptions(
            options={"limit": -1},
            expected_num_submitted=0,
            expected_logs=["'limit' must be greater than 0!"]
        ),
    ]

    for option in options:
        yield option


@pytest.mark.parametrize("test_command_option", dump_command_invalid_options())
def test_invalid_dump_command_options(
    test_command_option,
    mock_common_calls,
    caplog
):
    mock_dump_course, mock_get_course_overview_model, mock_modulestore, mock_last_dump_time = mock_common_calls
    option_combination, expected_num_submitted, expected_outputs = test_command_option

    with pytest.raises(django.core.management.base.CommandError):
        call_command(
            'dump_courses_to_clickhouse',
            **option_combination
        )

    # Just to make sure we're not calling things more than we need to
    assert mock_modulestore.call_count == 0
    assert mock_dump_course.apply_async.call_count == 0
    for expected_output in expected_outputs:
        assert expected_output in caplog.text


def test_multiple_courses_different_times(
    mock_common_calls,
    caplog
):
    mock_dump_course, mock_get_course_overview_model, mock_modulestore, mock_last_dump_time = mock_common_calls

    test_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f+00:00")

    course_id_1 = course_str_factory("course_1")
    course_id_2 = course_str_factory("course_2")
    course_id_3 = course_str_factory("course_3")

    fake_modulestore_courses = [FakeCourse(course_id_1), FakeCourse(course_id_2), FakeCourse(course_id_3)]
    mock_modulestore.return_value.get_course_summaries.return_value = fake_modulestore_courses
    mock_get_course_overview_model.return_value.get_from_id.return_value = FakeCourseOverview(modified=test_timestamp)

    # Each time last_dump_time is called it will get a different date so we can test
    # them all together
    mock_last_dump_time.side_effect = [
        # One year ago
        (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S.%f+00:00"),
        # A magic date that matches the last published date of our test course
        test_timestamp,
        # Course not dumped to ClickHouse yet
        None,
    ]

    call_command(
        'dump_courses_to_clickhouse'
    )

    assert mock_modulestore.call_count == 1
    assert mock_last_dump_time.call_count == 3
    assert "Course has been published since last dump time" in caplog.text
    assert "Course has NOT been published since last dump time" in caplog.text
    assert "Course is not present in ClickHouse" in caplog.text
    assert "2 courses submitted for export to ClickHouse. 1 courses skipped." in caplog.text
