import json
from shutil import copy
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


def validate_constances_cohortes(source_data_dir:str,dataset_name:str,year:str):
    print("Checking file existence...")
    file = Path(f"{source_data_dir}/{dataset_name}_{year}.csv")
    if not file.exists():
        print(f"Can't find a valid data file of {dataset_name} with year {year}. Please check with your data provider")
        return "generate_anomaly_report_of_constances_cohortes"
    else:
        print("Checking required columns...")
        print("Contains all required columns.")
        print("Checking required volunteer id...")
        print("Contains all required required volunteer id.")
        return "clean_constances_cohortes"


def generate_anomaly_report(dataset_name:str,year:str,output_dir:str):
    print(f"Generating anomaly report for {dataset_name}_{year}")
    report_path = f"{output_dir}/{dataset_name}_{year}_anomaly_report.txt"
    with open(report_path, "w") as file:
        file.write(f"This is the anomaly report of {dataset_name}_{year}.")


def clean_constances_cohortes(source_data_dir:str,dataset_name:str,year:str,output_dir:str):
    source_file = f"{source_data_dir}/{dataset_name}_{year}.csv"
    print(f"Cleaning constances cohortes {source_file}")
    print("Removing useless columns.")
    print(f"Removing duplicated rows.")
    clean_file = f"{output_dir}/clean_{dataset_name}_{year}.csv"
    # copy the raw file to clean file
    copy(source_file, clean_file)
    print(f"Cleaning finished successfully.")


def validate_constances_air(source_data_dir:str,dataset_name:str,year:str):
    print("Checking file existence...")
    file = Path(f"{source_data_dir}/{dataset_name}_{year}.csv")
    if not file.exists():
        print(f"Can't find a valid data file of {dataset_name} with year {year}. Please check with your data provider")
        return "generate_anomaly_report_of_constances_air"
    else:
        print("Checking required columns...")
        print("Contains all required columns.")
        return "clean_constances_air"


def clean_constances_air(source_data_dir:str,dataset_name:str,year:str,output_dir:str):
    source_file = f"{source_data_dir}/{dataset_name}_{year}.csv"
    print(f"Cleaning constances air {source_file}")
    print("Removing useless columns.")
    print(f"Removing duplicated rows.")
    clean_file = f"{output_dir}/clean_{dataset_name}_{year}.csv"
    # copy the raw file to clean file
    copy(source_file, clean_file)
    print(f"Cleaning finished successfully.")

def space_join_cohortes_with_air(cohortes_dataset_name:str, air_dataset_name:str, year:str, out_dir:str):
    clean_cohortes_name = f"{out_dir}/{cohortes_dataset_name}_{year}.csv"
    clean_air_name = f"{out_dir}/{air_dataset_name}_{year}.csv"
    join_resu_file_name = f"{out_dir}/{cohortes_dataset_name}_air_{year}.csv"
    print(f"Executing space join with ST_CONTAIN(distance=5000) between {clean_cohortes_name} and {clean_air_name}")
    with open(join_resu_file_name, "w") as file:
        file.write(f"This is the result of space join with ST_CONTAIN(distance=5000) between {clean_cohortes_name} and {clean_air_name}.")
    print(f"Space join finished successfully.")



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
        op_args=[config.get("constances_cohortes_data_dir"),config.get("cohortes_dataset_name"),config.get("year")],
    )

    t2 = PythonOperator(
        task_id="generate_anomaly_report_of_constances_cohortes",
        python_callable=generate_anomaly_report,
        op_args=[config.get("cohortes_dataset_name"),config.get("year"),config.get("output_dir")],
    )

    t3 = PythonOperator(
        task_id="clean_constances_cohortes",
        python_callable=clean_constances_cohortes,
        op_args=[config.get("constances_cohortes_data_dir"),config.get("cohortes_dataset_name"),config.get("year"), config.get("output_dir")],
    )

    t4 = BranchPythonOperator(
        task_id="validate_constances_air",
        python_callable=validate_constances_air,
        op_args=[config.get("constances_air_data_dir"),config.get("air_dataset_name"),config.get("year")],
    )

    t5 = PythonOperator(
        task_id="generate_anomaly_report_of_constances_air",
        python_callable=generate_anomaly_report,
        op_args=[config.get("air_dataset_name"),config.get("year"),config.get("output_dir")],
    )

    t6 = PythonOperator(
        task_id="clean_constances_air",
        python_callable=clean_constances_cohortes,
        op_args=[config.get("constances_air_data_dir"),config.get("air_dataset_name"),config.get("year"), config.get("output_dir")],
    )

    t7 = PythonOperator(
        task_id="space_join_cohortes_with_air",
        python_callable=space_join_cohortes_with_air,
        op_args=[config.get("cohortes_dataset_name"),config.get("air_dataset_name"),config.get("year"), config.get("output_dir")],
    )

# the main workflow :
    # t1: data validation donnee air
    # t2: if valid: clean data, if not: generate anomalies report
    # t3: data validation cohort data
    # t4: if valid: clean data, if not: generate anomalies report
    # t5: appariements of cohort and air

    # join to continue DAG execution
    ingestion_task_failed = EmptyOperator(task_id="ingestion_task_failed", trigger_rule="none_failed_or_skipped")
    ingestion_task_succeed = EmptyOperator(task_id="ingestion_task_succeed", trigger_rule="none_failed_or_skipped")
    t1 >> [t2, t3]  # Conditional branching
    [t2, t5] >> ingestion_task_failed
    t4 >> [t5, t6]
    [t3, t6] >> t7 >> ingestion_task_succeed

