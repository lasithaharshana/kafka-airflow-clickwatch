"""Airflow DAG for daily batch processing."""

import os
from datetime import datetime, timedelta

from airflow.sdk import DAG, dag
from airflow.providers.standard.operators.python import PythonOperator

# Use environment variables or default paths (mounted volumes in Docker)
RAW_LOGS_PATH = os.getenv("RAW_LOGS_PATH", "/opt/airflow/logs/clickstream")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/opt/airflow/logs/output")

# PostgreSQL configuration for analytics storage
POSTGRES_ENABLED = os.getenv("POSTGRES_ANALYTICS_ENABLED", "true").lower() == "true"


def segment_users_task(**context):
    """Task to segment users and generate summary."""
    import sys
    from datetime import datetime, timedelta
    sys.path.insert(0, '/opt/airflow')
    from src.batch.user_segmentation import UserSegmentation

    # Get execution date - in Airflow 3.x, use logical_date
    # For manual triggers, logical_date may be None, so default to today
    logical_date = context.get("logical_date")
    
    if logical_date and hasattr(logical_date, 'strftime'):
        execution_date = logical_date.strftime("%Y-%m-%d")
    elif logical_date:
        execution_date = str(logical_date)[:10]  # Extract YYYY-MM-DD
    else:
        # Default to today's date for manual triggers
        execution_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"\n[AIRFLOW TASK] Processing clickstream data for date: {execution_date}")
    
    segmentation = UserSegmentation(RAW_LOGS_PATH)

    # Generate summary
    summary = segmentation.generate_summary(
        date=execution_date, output_path=OUTPUT_PATH
    )

    # Store in PostgreSQL if enabled
    if POSTGRES_ENABLED and summary:
        try:
            from src.batch.postgres_storage import AnalyticsStorage
            storage = AnalyticsStorage()
            storage.connect()
            storage.initialize_schema()
            storage.store_summary(summary)
            print("[AIRFLOW TASK] Summary stored in PostgreSQL successfully")
            storage.close()
        except Exception as e:
            print(f"[AIRFLOW TASK] Warning: Failed to store in PostgreSQL: {e}")
            print("[AIRFLOW TASK] Continuing with file-based storage only")

    # Generate email text
    email_text = segmentation.generate_email_text(summary)

    # Log the email text (in production, this would send an actual email)
    print("\n" + "=" * 60)
    print("EMAIL/TEXT SUMMARY")
    print("=" * 60)
    print(email_text)
    print("=" * 60 + "\n")

    # Push summary to XCom for downstream tasks
    return summary


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG using Airflow 3.x syntax
with DAG(
    dag_id="clickstream_daily_batch",
    default_args=default_args,
    description="Daily batch processing for e-commerce clickstream data",
    schedule="0 2 * * *",  # Run daily at 2 AM (Airflow 3.x uses 'schedule' instead of 'schedule_interval')
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["clickstream", "batch", "analytics"],
) as clickstream_dag:

    # Define tasks
    segment_users = PythonOperator(
        task_id="segment_users_and_generate_summary",
        python_callable=segment_users_task,
    )

    # Task dependencies (single task for now, can be extended)
    segment_users
