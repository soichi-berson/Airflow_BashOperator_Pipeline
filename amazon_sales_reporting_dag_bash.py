from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging
import os

# -------------------------------------------------
# Logging
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)


# -------------------------------------------------
# Config
# -------------------------------------------------
DEMO_MODE   = os.environ["DEMO_MODE"].lower() == "true"
FORCED_DATE = os.environ["FORCED_DATE"]
SCRIPT_PATH = os.environ["SCRIPT_PATH"]

# Date passed to steps that need it
DATE_ARG = f"--date {FORCED_DATE}" if DEMO_MODE else "--date {{ ds }}"


# -------------------------------------------------
# Default DAG Arguments
# -------------------------------------------------
default_args = {
    "owner":           os.environ["DAG_OWNER"],
    "depends_on_past": False,
    "retries":         1,
    "retry_delay":     timedelta(minutes=5),
}


# -------------------------------------------------
# DAG Definition
# -------------------------------------------------
with DAG(
    dag_id="amazon_sales_report_bash_AWS",
    default_args=default_args,
    description="Weekly Amazon Sales Reporting ETL Pipeline (BashOperator)",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["portfolio", "reporting"],
) as dag:

    # -------------------------------------------------
    # Task 1: Load data from S3 → /tmp/raw_data.csv
    # -------------------------------------------------
    task_loading = BashOperator(
        task_id="task_loading",
        bash_command=f"python {SCRIPT_PATH} --step loading",
    )

    # -------------------------------------------------
    # Task 2: Clean data → /tmp/cleaned_data.csv
    # -------------------------------------------------
    task_cleaning = BashOperator(
        task_id="task_cleaning",
        bash_command=f"python {SCRIPT_PATH} --step clean",
    )

    # -------------------------------------------------
    # Task 3a: Prepare weekly window → /tmp/one_week.csv
    # Task 3b: Prepare monthly window → /tmp/one_month.csv
    #   (both run from the same step; split is done inside the class)
    # -------------------------------------------------
    task_prepare = BashOperator(
        task_id="task_prepare",
        bash_command=f"python {SCRIPT_PATH} --step prepare {DATE_ARG}",
    )

    # -------------------------------------------------
    # Task 4: One-week KPI analysis → /tmp/one_week_results.json
    # -------------------------------------------------
    task_week_analysis = BashOperator(
        task_id="task_week_analysis",
        bash_command=f"python {SCRIPT_PATH} --step week",
    )

    # -------------------------------------------------
    # Task 5: One-month analysis → /tmp/one_month_results.json
    # -------------------------------------------------
    task_month_analysis = BashOperator(
        task_id="task_month_analysis",
        bash_command=f"python {SCRIPT_PATH} --step month {DATE_ARG}",
    )

    # -------------------------------------------------
    # Task 6: Generate PDF → /tmp/*.pdf + charts
    #         Saves file paths to /tmp/file_paths.json
    # -------------------------------------------------
    task_generate_pdf = BashOperator(
        task_id="task_generate_pdf",
        bash_command=f"python {SCRIPT_PATH} --step pdf",
    )

    # -------------------------------------------------
    # Task 7: Upload all files to S3
    # -------------------------------------------------
    task_upload = BashOperator(
        task_id="task_upload_to_s3",
        bash_command=f"python {SCRIPT_PATH} --step upload",
    )

    # -------------------------------------------------
    # Task Dependencies
    # -------------------------------------------------
    task_loading >> task_cleaning >> task_prepare >> [task_week_analysis, task_month_analysis] >> task_generate_pdf >> task_upload


