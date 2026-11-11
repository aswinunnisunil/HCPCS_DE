"""Monitoring and failure notification utilities."""

import logging

logger = logging.getLogger(__name__)


def notify_failure(context):
    """Log details when an Airflow task fails."""

    task_instance = context.get("task_instance")
    exception = context.get("exception")

    if task_instance:
        logger.error(
            "HCPCS PIPELINE FAILURE | "
            "DAG: %s | Task: %s | Run: %s",
            task_instance.dag_id,
            task_instance.task_id,
            task_instance.run_id,
        )

    logger.error("Failure reason: %s", exception)
