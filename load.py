
"""Load HCPCS records into PostgreSQL using SCD Type 2."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv()

PROCESSED_DIR = Path("data/processed")


def get_latest_file():
    files = sorted(PROCESSED_DIR.glob("hcpcs_transformed_*.json"))

    if not files:
        raise FileNotFoundError("No transformed JSON file found.")

    return files[-1]


def get_database_engine():
    connection_url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME"),
    )

    return create_engine(connection_url)


def load_records(engine, records):
    select_query = text("""
        SELECT id, description_hash, version
        FROM public.hcpcs_codes
        WHERE hcpcs_code = :hcpcs_code
          AND is_current = TRUE
        FOR UPDATE
    """)

    close_query = text("""
        UPDATE public.hcpcs_codes
        SET valid_to = :change_time,
            is_current = FALSE
        WHERE id = :record_id
    """)

    insert_query = text("""
        INSERT INTO public.hcpcs_codes (
            hcpcs_code,
            group_code,
            category_name,
            long_description,
            description_hash,
            valid_from,
            valid_to,
            is_current,
            version
        )
        VALUES (
            :hcpcs_code,
            :group_code,
            :category_name,
            :long_description,
            :description_hash,
            :change_time,
            NULL,
            TRUE,
            :version
        )
    """)

    inserted = 0
    updated = 0
    unchanged = 0

    with engine.begin() as connection:
        for record in records:
            current = connection.execute(
                select_query,
                {"hcpcs_code": record["hcpcs_code"]}
            ).mappings().first()

            change_time = datetime.now(timezone.utc).replace(tzinfo=None)

            if current is None:
                connection.execute(
                    insert_query,
                    {**record, "change_time": change_time, "version": 1}
                )
                inserted += 1

            elif current["description_hash"] == record["description_hash"]:
                unchanged += 1

            else:
                connection.execute(
                    close_query,
                    {
                        "record_id": current["id"],
                        "change_time": change_time
                    }
                )

                connection.execute(
                    insert_query,
                    {
                        **record,
                        "change_time": change_time,
                        "version": current["version"] + 1
                    }
                )
                updated += 1

    return inserted, updated, unchanged


def main():
    input_file = get_latest_file()

    with input_file.open("r", encoding="utf-8") as file:
        records = json.load(file)

    engine = get_database_engine()

    try:
        inserted, updated, unchanged = load_records(engine, records)

        print("SCD Type 2 loading completed!")
        print("Input records:", len(records))
        print("New records inserted:", inserted)
        print("Existing records updated:", updated)
        print("Unchanged records:", unchanged)

    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
