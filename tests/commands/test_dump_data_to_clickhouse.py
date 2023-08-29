"""
Tests for the dump_data_to_clickhouse management command.
"""

from collections import namedtuple
from datetime import datetime, timedelta
from unittest.mock import patch

import django.core.management.base
import pytest
from django.core.management import call_command

from event_sink_clickhouse.sinks.base_sink import ModelBaseSink

CommandOptions = namedtuple(
    "TestCommandOptions", ["options", "expected_num_submitted", "expected_logs"]
)


def dummy_model_factory():
    """
    Create a dummy model for testing.
    """

    class DummyModel:
        """
        Dummy model for testing.
        """

        def __init__(self, id):
            self.id = id
            self.created = datetime.now()

    return DummyModel


def dummy_serializer_factory():
    """
    Create a dummy serializer for testing.
    """

    class DummySerializer:
        """
        Dummy serializer for testing.
        """

        def __init__(self, model):
            self.model = model

        @property
        def data(self):
            return {"id": self.model.id, "created": self.model.created}

    return DummySerializer


class DummySink(ModelBaseSink):
    """
    Dummy sink for testing.
    """

    name = "Dummy"
    model = "dummy"
    unique_key = "id"
    serializer_class = dummy_serializer_factory()
    timestamp_field = "created"
    clickhouse_table_name = "dummy_table"

    def get_queryset(self):
        return [dummy_model_factory()(id) for id in range(1, 5)]

    def convert_id(self, item_id):
        return int(item_id)

    def should_dump_item(self, unique_key):
        if unique_key % 2 == 0:
            return True, "Even number"
        else:
            return False, "Odd number"


def dump_command_basic_options():
    """
    Pytest params for all the different non-ClickHouse command options.
    """
    options = [
        CommandOptions(
            options={"object": "dummy", "ids_to_skip": ["1", "2", "3", "4"]},
            expected_num_submitted=0,
            expected_logs=[
                "submitted for export to ClickHouse",
            ],
        ),
        CommandOptions(
            options={"object": "dummy", "limit": 1},
            expected_num_submitted=1,
            expected_logs=["Limit of 1 eligible objects has been reached, quitting!"],
        ),
        CommandOptions(
            options={"object": "dummy", "ids": ["1", "2", "3", "4"]},
            expected_num_submitted=2,
            expected_logs=[
                "These objects were submitted for dump to ClickHouse successfully",
            ],
        ),
        CommandOptions(
            options={"object": "dummy", "force": True},
            expected_num_submitted=4,
            expected_logs=["Force is set"],
        ),
    ]

    for option in options:
        yield option


@pytest.mark.parametrize("test_command_option", dump_command_basic_options())
@patch(
    "event_sink_clickhouse.management.commands.dump_data_to_clickhouse.dump_data_to_clickhouse"
)
def test_dump_courses_options(mock_dump_data, test_command_option, caplog):
    option_combination, expected_num_submitted, expected_outputs = test_command_option

    assert DummySink.model in [cls.model for cls in ModelBaseSink.__subclasses__()]

    call_command("dump_data_to_clickhouse", **option_combination)

    assert mock_dump_data.apply_async.call_count == expected_num_submitted
    for expected_output in expected_outputs:
        assert expected_output in caplog.text


def dump_basic_invalid_options():
    """
    Pytest params for all the different non-ClickHouse command options.
    """
    options = [
        CommandOptions(
            options={"object": "dummy", "limit": 1, "force": True},
            expected_num_submitted=1,
            expected_logs=[],
        ),
        CommandOptions(
            options={"object": "dummy", "limit": 1, "force": True},
            expected_num_submitted=1,
            expected_logs=[],
        ),
        CommandOptions(
            options={"object": "dummy", "limit": 0, "force": True},
            expected_num_submitted=1,
            expected_logs=[],
        ),
        CommandOptions(
            options={},
            expected_num_submitted=1,
            expected_logs=[],
        ),
    ]

    for option in options:
        yield option


@pytest.mark.parametrize("test_command_option", dump_basic_invalid_options())
@patch(
    "event_sink_clickhouse.management.commands.dump_data_to_clickhouse.dump_data_to_clickhouse"
)
def test_dump_courses_options_invalid(mock_dump_data, test_command_option, caplog):
    option_combination, expected_num_submitted, expected_outputs = test_command_option
    assert DummySink.model in [cls.model for cls in ModelBaseSink.__subclasses__()]

    with pytest.raises(django.core.management.base.CommandError):
        call_command("dump_data_to_clickhouse", **option_combination)
    # assert mock_dump_data.apply_async.call_count == expected_num_submitted
    for expected_output in expected_outputs:
        assert expected_output in caplog.text
