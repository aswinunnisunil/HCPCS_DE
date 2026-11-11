
"""Transform raw HCPCS JSON into a cleaned processed dataset."""

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path

RAW_DIR = Path("raw")
PROCESSED_DIR = Path("data/processed")


def get_latest_raw_file() -> Path:
    files = sorted(RAW_DIR.glob("hcpcs_*.json"))

    if not files:
        raise FileNotFoundError("No raw HCPCS JSON file found.")

    return files[-1]


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def generate_hash(description: str) -> str:
    return hashlib.md5(
        description.encode("utf-8")
    ).hexdigest()


def transform_records(records: list[dict]) -> list[dict]:
    transformed = []
    seen_codes = set()

    for record in records:
        code = clean_text(record.get("hcpcs_code", "")).upper()
        description = clean_text(record.get("long_description", ""))
        group = clean_text(record.get("group_code", "")).upper()
        category = clean_text(record.get("category_name", ""))

        if not code or not description:
            continue

        if code in seen_codes:
            continue

        seen_codes.add(code)

        transformed.append({
            "hcpcs_code": code,
            "group_code": group,
            "category_name": category,
            "long_description": description,
            "description_hash": generate_hash(description)
        })

    return transformed


def main():
    source_file = get_latest_raw_file()

    with source_file.open("r", encoding="utf-8") as file:
        raw_records = json.load(file)

    clean_records = transform_records(raw_records)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_file = PROCESSED_DIR / f"hcpcs_transformed_{timestamp}.json"

    with output_file.open("x", encoding="utf-8") as file:
        json.dump(clean_records, file, ensure_ascii=False, indent=2)

    print("Transformation completed!")
    print("Input records:", len(raw_records))
    print("Output records:", len(clean_records))
    print("Records removed:", len(raw_records) - len(clean_records))
    print("Output file:", output_file)


if __name__ == "__main__":
    main()
