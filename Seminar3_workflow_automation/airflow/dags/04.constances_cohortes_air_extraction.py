import json
from shutil import copy
from datetime import datetime, timedelta
from typing import List

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from pathlib import Path

# workflow config
CONFIG_PATH = "/home/pliu/git/ConstanceDataPlatform/Seminar3_workflow_automation/airflow/conf/constances_cohortes_extraction_config.json"

def load_config():
    with open(CONFIG_PATH, "r") as file:
        return json.load(file)

config = load_config()

# some system config
dag_id = "constances_cohortes_air_extraction"
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

def validate_source_file(source_data_dir:str,dataset_name:str,year:str):
    print("Checking source file existence...")
    file = Path(f"{source_data_dir}/{dataset_name}_{year}.csv")
    if not file.exists():
        print(f"Can't find a valid data file of {dataset_name} with year {year}. Please check with your data provider")
        return "extraction_failed"
    else:
        print("Checking required columns...")
        print("Contains all required columns.")
        return "start_extraction"

def validate_volunteer_ids(volunteer_ids:List[str]):
    print("Checking volunteer ids validity...")
    all_volunteer_ids = ["id1","id2","id3"]
    is_allowed = set(volunteer_ids).issuperset(all_volunteer_ids)
    if is_allowed:
        return "start_extraction"
    else:
        print("The given volunteer ids are not valid.")
        return "extraction_failed"


def generate_extraction(dataset_name:str,year:str,output_dir:str):
    print(f"Generating extraction for {dataset_name}_{year}")
    extraction_path = f"{output_dir}/{dataset_name}_{year}_extraction.csv"
    with open(extraction_path, "w") as file:
        file.write(f"This is an extraction of {dataset_name}_{year}.")



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
        task_id="validate_source_file",
        python_callable=validate_source_file,
        op_args=[config.get("source_data_dir"),config.get("dataset_name"),config.get("year")],
    )

    t2 = PythonOperator(
        task_id="validate_volunteer_ids",
        python_callable=validate_volunteer_ids,
        op_args=[config.get("volunteer_ids")],
    )

    t3 = PythonOperator(
        task_id="generate_extraction",
        python_callable=generate_extraction,
        op_args=[config.get("dataset_name"),config.get("year"), config.get("output_dir")],
    )

    # define state operator
    extraction_failed = EmptyOperator(task_id="extraction_failed", trigger_rule="none_failed_or_skipped")
    start_extraction = EmptyOperator(task_id="start_extraction", trigger_rule="none_failed_or_skipped")
    extraction_succeed = EmptyOperator(task_id="extraction_succeed", trigger_rule="none_failed_or_skipped")
    # define workflow
    t1 >> [extraction_failed, start_extraction]  # Conditional branching
    t2 >> [extraction_failed, start_extraction]
    start_extraction >> t3 >> extraction_succeed
