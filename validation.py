
"""Validate HCPCS warehouse data quality."""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv()


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


CHECKS = {
    "Duplicate current HCPCS codes": """
        SELECT COUNT(*)
        FROM (
            SELECT hcpcs_code
            FROM public.hcpcs_codes
            WHERE is_current = TRUE
            GROUP BY hcpcs_code
            HAVING COUNT(*) > 1
        ) AS duplicates;
    """,

    "Codes without exactly one current version": """
        SELECT COUNT(*)
        FROM (
            SELECT hcpcs_code
            FROM public.hcpcs_codes
            GROUP BY hcpcs_code
            HAVING COUNT(*) FILTER (WHERE is_current = TRUE) <> 1
        ) AS invalid_codes;
    """,

    "Missing codes or descriptions": """
        SELECT COUNT(*)
        FROM public.hcpcs_codes
        WHERE hcpcs_code IS NULL
           OR TRIM(hcpcs_code) = ''
           OR long_description IS NULL
           OR TRIM(long_description) = '';
    """,

    "Invalid version numbers": """
        SELECT COUNT(*)
        FROM public.hcpcs_codes
        WHERE version < 1;
    """,

    "Invalid historical dates": """
        SELECT COUNT(*)
        FROM public.hcpcs_codes
        WHERE valid_to IS NOT NULL
          AND valid_to < valid_from;
    """,

    "Current records with an end date": """
        SELECT COUNT(*)
        FROM public.hcpcs_codes
        WHERE is_current = TRUE
          AND valid_to IS NOT NULL;
    """,

    "Historical records without an end date": """
        SELECT COUNT(*)
        FROM public.hcpcs_codes
        WHERE is_current = FALSE
          AND valid_to IS NULL;
    """
}


def run_validation(engine):
    results = {}

    with engine.connect() as connection:
        for check_name, query in CHECKS.items():
            failed_records = connection.execute(text(query)).scalar_one()
            results[check_name] = failed_records

    return results


def main():
    engine = get_database_engine()

    try:
        results = run_validation(engine)

        print("\nHCPCS DATA QUALITY REPORT")
        print("-" * 45)

        failed_checks = 0

        for check_name, failed_records in results.items():
            status = "PASS" if failed_records == 0 else "FAIL"

            print(
                f"{check_name}: {status} "
                f"(Invalid records: {failed_records})"
            )

            if failed_records > 0:
                failed_checks += 1

        print("-" * 45)

        if failed_checks:
            raise RuntimeError(
                f"Data quality validation failed: {failed_checks} check(s)."
            )

        print("Overall result: ALL CHECKS PASSED")

    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
