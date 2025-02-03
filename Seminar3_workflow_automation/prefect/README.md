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
```

### 1.1 Transform your python script to prefect workflow

Decorators are the easiest way to convert a Python script into a workflow.

There are two major decorators:
- **@flow decorator**: It defines how the tasks are connected in your workflow.
- **@task decorators**: It defines the real logic of each task which is called by the workflow.

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