#!/bin/bash

# set up the home folder for airflow application, it will store all the required files of the airflow
# if not define, when you run the server, it will create the files in ~/airflow

# set up python and airflow version
AIRFLOW_VERSION=2.10.4
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

# add this to your bashrc
echo 'AIRFLOW_HOME=$air_root_path' >> ~/.bashrc

export AIRFLOW_HOME=$air_root_path

# auto determine the constraint url
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-no-providers-3.8.txt
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"