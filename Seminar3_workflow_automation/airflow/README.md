# Airflow 

`Apache Airflow` is an **open-source platform for developing, scheduling, and monitoring batch-oriented workflows**. 
Airflow’s extensible Python framework enables you to build workflows connecting with virtually any technology. 
A web interface helps manage the state of your workflows. Airflow is deployable in many ways, varying from a single 
process on your laptop to a distributed setup to support even the biggest workflows

## run airflow locally


The below scripts are tested with Debian 11

```shell
# start a web server (web gui)
airflow webserver --port 8080 &

# start a scheduler
airflow scheduler &

```