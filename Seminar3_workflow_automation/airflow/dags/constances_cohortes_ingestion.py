import json
import textwrap
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from pathlib import Path

# workflow config
CONFIG_PATH = "/home/pliu/git/ConstanceDataPlatform/Seminar3_workflow_automation/airflow/conf/constances_cohortes_ingestion_config.json"

def load_config():
    with open(CONFIG_PATH, "r") as file:
        return json.load(file)

config = load_config()

# some system config
dag_id = "constances_cohortes_ingestion"
dag_tags = ["pengfei","constance"]

default_args={
        "owner":"pengfei",
        "depends_on_past": False,
        "email": ["pengfei.liu@casd.eu"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }


def validate_constances_cohortes(raw_data_path:str):
    print("Checking file existence...")
    file = Path(raw_data_path)
    if not file.exists():
        print("Can't find a valid data file with the given path. Please check the path")
        return "generate_anomaly_report"
    else:
        print("Checking required columns...")
        print("Contains all required columns.")
        print("Checking required volunteer id...")
        print("Contains all required required volunteer id.")
        return "clean_constances_cohortes"


def generate_anomaly_report(raw_data_path:str,output_dir:str):
    print(f"Generating anomaly report for {raw_data_path}")
    report_path = f"{output_dir}/anomaly_report.txt"
    with open(report_path, "w") as file:
        file.write(f"This is the anomaly report of {raw_data_path}\n.")


def clean_constances_cohortes(raw_data_path:str,output_dir:str):
    print(f"Cleaning constances cohortes at {raw_data_path}")
    print("Removing useless columns.")
    print(f"Removing duplicated rows.")
    clean_path = f"{output_dir}/clean_constances_cohortes.csv"
    # copy the raw file to clean file
    Path(raw_data_path).rename(clean_path)
    print(f"Cleaning finished successfully.")




with DAG(
    dag_id=dag_id,
    default_args=default_args,
    description="This workflow checks the validity of the raw constance cohort",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=dag_tags,
) as dag:
    t1 = BranchPythonOperator(
        task_id="validate_constances_cohortes",
        python_callable=validate_constances_cohortes,
        op_args=[config.get("raw_data_path")],
    )

    t2 = PythonOperator(
        task_id="generate_anomaly_report",
        python_callable=generate_anomaly_report,
        op_args=[config.get("raw_data_path"),config.get("output_dir")],
    )

    t3 = PythonOperator(
        task_id="clean_constances_cohortes",
        python_callable=clean_constances_cohortes,
        op_args=[config.get("raw_data_path"), config.get("output_dir")],
    )

# the main workflow :
    # t1: data validation donnee air
    # t2: if valid: clean data, if not: generate anomalies report
    # t3: data validation cohort data
    # t4: if valid: clean data, if not: generate anomalies report
    # t5: appariements of cohort and air

    # join to continue DAG execution
    end_task = EmptyOperator(task_id="join", trigger_rule="none_failed_or_skipped")

    t1 >> [t2, t3]  # Conditional branching
    [t2, t3] >> end_task
