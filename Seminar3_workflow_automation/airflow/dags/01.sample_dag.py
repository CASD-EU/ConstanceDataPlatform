import textwrap
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# some config
dag_id = "hospital_count_dag"
dag_tags = ["pengfei"]

default_args={
        "owner":"pengfei",
        "depends_on_past": False,
        "email": ["pengfei.liu@casd.eu"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }




with DAG(
    dag_id=dag_id,
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=dag_tags,
) as dag:
    t1 = PythonOperator(
        task_id="task_1",
        python_callable=run_task,
        op_args=['task_1'],
    )

    t2 = PythonOperator(
        task_id="task_2",
        python_callable=run_task,
        op_args=['task_2'],
        retries=3,
    )

    templated_command = textwrap.dedent(
        """
    {% for i in range(5) %}
        echo "This is task 3 running"
    {% endfor %}
    """
    )

    t3 = BashOperator(
        task_id="task_3",
        depends_on_past=False,
        bash_command=templated_command,
        retries=3,
    )

    t1 >> t2 >> t3
