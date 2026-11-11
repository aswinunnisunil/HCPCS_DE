
"""Airflow DAG for the HCPCS data pipeline with observability metrics."""

import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import dag, task, get_current_context
from airflow.task.trigger_rule import TriggerRule
from sqlalchemy import text

PROJECT_DIR = Path("/opt/airflow/project")
sys.path.insert(0, str(PROJECT_DIR))

from load import get_database_engine  # noqa: E402
from monitoring import notify_failure  # noqa: E402


def run_script(script_name):
    """Execute a project script and return its console output."""

    result = subprocess.run(
        [sys.executable, script_name],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
    )

    if result.stdout:
        print(result.stdout)

    if result.stderr:
        print(result.stderr, file=sys.stderr)

    result.check_returncode()
    return result.stdout


def count_latest_json(directory, pattern):
    """Count records in the latest generated JSON file."""

    files = sorted(directory.glob(pattern))

    if not files:
        raise FileNotFoundError(
            f"No output file found matching {pattern}"
        )

    with files[-1].open("r", encoding="utf-8") as file:
        records = json.load(file)

    return len(records)


@dag(
    dag_id="hcpcs_pipeline",
    description="Extract, transform, load, validate and measure HCPCS data",
    schedule=None,
    start_date=datetime(2026, 11, 11),
    catchup=False,
    max_active_runs=1,
    tags=["hcpcs", "data-engineering", "observability"],
    on_failure_callback=notify_failure,
)
def hcpcs_pipeline():

    @task
    def scrape_hcpcs():
        run_script("scrapper.py")

        count = count_latest_json(
            PROJECT_DIR / "raw",
            "hcpcs_*.json",
        )

        print("Records extracted:", count)
        return {"records_extracted": count}

    @task
    def transform_hcpcs():
        run_script("transform.py")

        count = count_latest_json(
            PROJECT_DIR / "data" / "processed",
            "hcpcs_transformed_*.json",
        )

        print("Records transformed:", count)
        return {"records_transformed": count}

    @task
    def load_to_postgres():
        output = run_script("load.py")

        patterns = {
            "records_inserted": r"New records inserted:\s*(\d+)",
            "records_updated": r"Existing records updated:\s*(\d+)",
            "records_unchanged": r"Unchanged records:\s*(\d+)",
        }

        metrics = {}

        for key, pattern in patterns.items():
            match = re.search(pattern, output)

            if not match:
                raise ValueError(
                    f"Could not capture loader metric: {key}"
                )

            metrics[key] = int(match.group(1))

        print("Loader metrics:", metrics)
        return metrics

    @task
    def validate_data():
        run_script("validation.py")
        print("Data validation completed successfully.")

        return {"validation_status": "success"}

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def record_pipeline_metrics():
        """Store one observability record for this DAG run."""

        context = get_current_context()

        dag_run = context["dag_run"]
        task_instance = context["ti"]

        # Retrieve successful task outputs from XCom.
        scrape_metrics = (
            task_instance.xcom_pull(task_ids="scrape_hcpcs") or {}
        )

        transform_metrics = (
            task_instance.xcom_pull(task_ids="transform_hcpcs") or {}
        )

        load_metrics = (
            task_instance.xcom_pull(task_ids="load_to_postgres") or {}
        )

        validation_metrics = (
            task_instance.xcom_pull(task_ids="validate_data") or {}
        )

        # Determine pipeline status from task outputs.
        states = {
            "scrape_hcpcs": (
                "success" if scrape_metrics else "not_successful"
            ),
            "transform_hcpcs": (
                "success" if transform_metrics else "not_successful"
            ),
            "load_to_postgres": (
                "success" if load_metrics else "not_successful"
            ),
            "validate_data": (
                "success"
                if validation_metrics.get("validation_status") == "success"
                else "not_successful"
            ),
        }

        unsuccessful = [
            f"{task_id}={state}"
            for task_id, state in states.items()
            if state != "success"
        ]

        run_status = "success" if not unsuccessful else "failed"

        error_message = (
            "Unsuccessful task states: " + ", ".join(unsuccessful)
            if unsuccessful
            else None
        )

        # Capture pipeline execution timestamps.
        start_time = dag_run.start_date
        end_time = datetime.now(timezone.utc)

        if start_time and start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)

        duration = (
            (end_time - start_time).total_seconds()
            if start_time
            else None
        )

        # Insert or update the observability record.
        insert_query = text("""
            INSERT INTO public.pipeline_metrics (
                dag_id,
                run_id,
                run_status,
                start_time,
                end_time,
                duration_seconds,
                records_extracted,
                records_transformed,
                records_inserted,
                records_updated,
                records_unchanged,
                error_message
            )
            VALUES (
                :dag_id,
                :run_id,
                :run_status,
                :start_time,
                :end_time,
                :duration_seconds,
                :records_extracted,
                :records_transformed,
                :records_inserted,
                :records_updated,
                :records_unchanged,
                :error_message
            )
            ON CONFLICT (run_id)
            DO UPDATE SET
                run_status = EXCLUDED.run_status,
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time,
                duration_seconds = EXCLUDED.duration_seconds,
                records_extracted = EXCLUDED.records_extracted,
                records_transformed = EXCLUDED.records_transformed,
                records_inserted = EXCLUDED.records_inserted,
                records_updated = EXCLUDED.records_updated,
                records_unchanged = EXCLUDED.records_unchanged,
                error_message = EXCLUDED.error_message
        """)

        parameters = {
            "dag_id": dag_run.dag_id,
            "run_id": dag_run.run_id,
            "run_status": run_status,
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": duration,
            "records_extracted": scrape_metrics.get(
                "records_extracted", 0
            ),
            "records_transformed": transform_metrics.get(
                "records_transformed", 0
            ),
            "records_inserted": load_metrics.get(
                "records_inserted", 0
            ),
            "records_updated": load_metrics.get(
                "records_updated", 0
            ),
            "records_unchanged": load_metrics.get(
                "records_unchanged", 0
            ),
            "error_message": error_message,
        }

        engine = get_database_engine()

        try:
            with engine.begin() as connection:
                connection.execute(insert_query, parameters)
        finally:
            engine.dispose()

        print("Pipeline metrics successfully recorded.")
        print("Run status:", run_status)
        print("Duration (seconds):", duration)
        print("Metrics:", parameters)

    # Define pipeline task dependencies.
    scrape = scrape_hcpcs()
    transform = transform_hcpcs()
    load = load_to_postgres()
    validate = validate_data()
    metrics = record_pipeline_metrics()

    scrape >> transform >> load >> validate >> metrics


hcpcs_pipeline()
