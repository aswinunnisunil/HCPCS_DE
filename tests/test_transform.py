
import hashlib

import pytest

import transform


def test_clean_text():
    assert transform.clean_text("  HCPCS   CODE  A  ") == "HCPCS CODE A"


def test_generate_hash():
    expected = hashlib.md5("Test description".encode("utf-8")).hexdigest()

    assert transform.generate_hash("Test description") == expected


def test_transform_records_normalizes_values():
    records = [
        {
            "hcpcs_code": " a0021 ",
            "group_code": " a ",
            "category_name": "  Medical   Services ",
            "long_description": "  Test   description "
        }
    ]

    result = transform.transform_records(records)

    assert len(result) == 1
    assert result[0]["hcpcs_code"] == "A0021"
    assert result[0]["group_code"] == "A"
    assert result[0]["category_name"] == "Medical Services"
    assert result[0]["long_description"] == "Test description"


def test_transform_records_removes_invalid_records():
    records = [
        {"hcpcs_code": "", "long_description": "Description"},
        {"hcpcs_code": "A0021", "long_description": ""},
        {"hcpcs_code": "A0022", "long_description": "Valid description"}
    ]

    result = transform.transform_records(records)

    assert len(result) == 1
    assert result[0]["hcpcs_code"] == "A0022"


def test_transform_records_removes_duplicate_codes():
    records = [
        {"hcpcs_code": "A0021", "long_description": "First description"},
        {"hcpcs_code": "A0021", "long_description": "Second description"}
    ]

    result = transform.transform_records(records)

    assert len(result) == 1
    assert result[0]["long_description"] == "First description"


def test_get_latest_raw_file_not_found(monkeypatch, tmp_path):
    monkeypatch.setattr(transform, "RAW_DIR", tmp_path)

    with pytest.raises(FileNotFoundError):
        transform.get_latest_raw_file()
