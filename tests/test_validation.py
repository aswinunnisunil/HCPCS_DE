
from unittest.mock import MagicMock

import validation


def test_all_validation_checks_pass():
    engine = MagicMock()
    connection = MagicMock()

    engine.connect.return_value.__enter__.return_value = connection
    connection.execute.return_value.scalar_one.return_value = 0

    results = validation.run_validation(engine)

    assert len(results) == 7
    assert all(count == 0 for count in results.values())
    assert connection.execute.call_count == 7


def test_validation_returns_failed_record_counts():
    engine = MagicMock()
    connection = MagicMock()

    engine.connect.return_value.__enter__.return_value = connection

    connection.execute.return_value.scalar_one.side_effect = [
        0, 2, 0, 0, 0, 0, 0
    ]

    results = validation.run_validation(engine)

    assert results["Duplicate current HCPCS codes"] == 0
    assert results["Codes without exactly one current version"] == 2
    assert sum(results.values()) == 2
