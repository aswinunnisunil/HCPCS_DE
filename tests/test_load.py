
from unittest.mock import MagicMock

import load


def sample_record():
    return {
        "hcpcs_code": "A0021",
        "group_code": "A",
        "category_name": "Medical Services",
        "long_description": "Test description",
        "description_hash": "hash_new",
    }


def setup_mock_connection():
    engine = MagicMock()
    connection = MagicMock()

    engine.begin.return_value.__enter__.return_value = connection

    first_result = (
        connection.execute.return_value.mappings.return_value.first
    )

    return engine, connection, first_result


def test_new_record_inserted():
    engine, connection, first_result = setup_mock_connection()
    first_result.return_value = None

    result = load.load_records(engine, [sample_record()])

    assert result == (1, 0, 0)
    assert connection.execute.call_count == 2

    insert_params = connection.execute.call_args_list[1].args[1]

    assert insert_params["version"] == 1
    assert insert_params["hcpcs_code"] == "A0021"


def test_unchanged_record_skipped():
    engine, connection, first_result = setup_mock_connection()

    first_result.return_value = {
        "id": 10,
        "description_hash": "hash_new",
        "version": 1,
    }

    result = load.load_records(engine, [sample_record()])

    assert result == (0, 0, 1)
    assert connection.execute.call_count == 1


def test_changed_record_creates_new_version():
    engine, connection, first_result = setup_mock_connection()

    first_result.return_value = {
        "id": 10,
        "description_hash": "hash_old",
        "version": 1,
    }

    result = load.load_records(engine, [sample_record()])

    assert result == (0, 1, 0)
    assert connection.execute.call_count == 3

    close_params = connection.execute.call_args_list[1].args[1]
    insert_params = connection.execute.call_args_list[2].args[1]

    assert close_params["record_id"] == 10
    assert insert_params["version"] == 2
