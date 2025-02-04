# Prefect

Prefect is an open-source orchestration engine that turns your Python functions into production-grade data pipelines 
with minimal friction. You can build and schedule workflows in pure Python—no DSLs or complex config files—and 
run them anywhere. Prefect handles the heavy lifting for you out of the box: automatic state tracking, failure 
handling, real-time monitoring, and more.


## 1. Quickstart

Prefect makes it easy to deploy Python scripts, run them on a schedule, make them robust to failure, and 
observe them in a UI.

To do this, you need to perform the following tasks:

1. Install Prefect
2. Start to a Prefect API server (self-hosted)
3. Transform your python script to prefect workflow(Add prefect decorators to the functions in the script)

```shell
# install the prefect package
pip install prefect

# after installation, you can start a local prefect API server
prefect server start

# This will open the Prefect dashboard in your browser at http://localhost:4200.

# if you have other prefect service which need to connect to the server api, you may need to setup 
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api 
```

### 1.1 Transform your python script to prefect workflow

Decorators are the easiest way to convert a Python script into a workflow.

There are two major decorators:
- **@flow decorator**: It defines how the tasks are connected in your workflow.
- **@task decorators**: It defines the real logic of each task which is called by the workflow.

Below code is an example, the function which starts a workflow is called `show_chat`. It contains
two tasks: `build_welcome_msg` and `answer_welcome_msg`

```shell
import httpx

from prefect import flow, task # Prefect flow and task decorators


@flow(log_prints=True)
def show_chat(guest_list: list[str]):
    """Flow: Show the number of stars that GitHub repos have"""
    for i, guest in enumerate(guest_list):
        # Call Task 1
        welcome_msg = build_welcome_msg(guest)

        # Call Task 2
        answer_msg = answer_welcome_msg(i)

        # Print the result
        print(f"CASD: {welcome_msg}")
        print(f"GUEST: {answer_msg}")


@task
def build_welcome_msg(guest_name: str):
    """Task 1: Fetch the statistics for a GitHub repo"""
    return f"Hello, {guest_name}! Welcome to CASD"


@task
def answer_welcome_msg(index: dict):
    """Task 2: Get the number of stars from GitHub repo statistics"""
    return f"Hello, CASD. thank you very much! I'm guest {index}!"


# Run the flow
if __name__ == "__main__":
    show_chat([
        "pengfei",
        "Thibaut"
    ])


```

### 1.2 Run the workflow 

After run the below code, you can check the workflow status via web UI (http://localhost:4200) 


```shell
python sample_workflow.py
```

### 1.3 Schedule a flow

In 1.1 and 1.2, you learned how to convert a Python script to a Prefect workflow.

In this chapter, you’ll learn how to get that flow off of your local machine and run it on 
a schedule with Prefect worker.

#### 1.3.1 Creates a work pool

**Work pools** are a bridge between the Prefect orchestration layer and the infrastructure where 
flows are run.

The primary reason to use work pools is for `dynamic infrastructure provisioning and configuration`. 
For example, you might have a workflow that has expensive infrastructure requirements and runs infrequently. 
In this case, you don’t want an idle process running within that infrastructure.

Work pools have different operational modes, each designed to work with specific infrastructures and work 
delivery methods:

- **Pull work pools**: These require workers to actively poll for flow runs to execute.
- **Push work pools**: These submit runs directly to serverless infrastructure providers.
- **Managed work pools**: These are administered by Prefect and handle both submission and execution of code.


```shell
# Create a Process work pool:
prefect work-pool create --type process casd-work-pool

# Verify that the work pool exists:
prefect work-pool ls

# Start a worker to poll the work pool:
prefect worker start --pool casd-work-pool
```

prefect supports many `work pool types`. You can get the full list in [here](https://docs.prefect.io/v3/deploy/infrastructure-concepts/work-pools#work-pool-types)

#### 1.3.2 Deploy and schedule your flow

A **deployment** is used to determine `when, where, and how a flow should run`. Deployments elevate flows to 
remotely configurable entities that have their own API. To set a flow to run on a schedule, you need to create a deployment.

Below `sample_deployment.py` is an example of the deployment of the previous sample_workflow.

```python
from prefect import flow, task
from pathlib import Path

@flow(log_prints=True)
def show_chat(guest_list: list[str]):
    """Flow: Show the number of stars that GitHub repos have"""
    for i, guest in enumerate(guest_list):
        # Call Task 1
        welcome_msg = build_welcome_msg(guest)

        # Call Task 2
        answer_msg = answer_welcome_msg(i)

        # Print the result
        print(f"CASD: {welcome_msg}")
        print(f"GUEST: {answer_msg}")


@task
def build_welcome_msg(guest_name: str):
    """Task 1: CASD says hello to guest"""
    return f"Hello, {guest_name}! Welcome to CASD"


@task
def answer_welcome_msg(index: dict):
    """Task 2: guest say hello to CASD"""
    return f"Hello, CASD. thank you very much! I'm guest {index}!"

if __name__ == "__main__":
    flow.from_source(
        source=str(Path(__file__).parent),
        entrypoint="sample_deployment.py:show_chat", # Specific flow to run
    ).deploy(
        name="sample-deployment",
        parameters={
            "guest_names": [
                 "pengfei",
                 "Thibaut"
            ]
        },
        work_pool_name="casd-work-pool",
        cron="0 * * * *",  # Run every hour
    )
```

You can notice the main difference between a workflow and a deployment is the main.

For a deployment, the main contains two parts:
- source spec: define which is the source code of the workflow
- deployement spec: define when (cron), where(work_pool_name), how(the input parameter) the workflow will run.

> In our example, we use a local source, prefect supports various remote source(e.g. github, s3, etc.)
> 

#### 1.3.3 Activate a deployment

Before activate a deployment, you need to check:
1. the prefect server is running (In our case http://localhost:4200/)
2. Check the work_pool_name exist
```shell
# if no prefect server, start one
prefect server start

# This will open the Prefect dashboard in your browser at http://localhost:4200.

# if you have other prefect service which need to connect to the server api, you may need to setup 
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api 

# check work pool
prefect work-pool ls

# Create a Process work pool if the desired work pool does not exist:
prefect work-pool create --type process casd-work-pool

# Start a worker to poll the work pool:
prefect worker start --pool casd-work-pool

# activate a deployment
python sample_deployment.py
```

> If all executed correctly, you should see a activate deployment in the WebUI.
