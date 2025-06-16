from prefect import flow, task
from pathlib import Path

@flow(log_prints=True, name="show_chat")
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
            "guest_list": [
                 "pengfei",
                 "Thibaut"
            ]
        },
        work_pool_name="casd-work-pool",
        cron="0 * * * *",  # Run every hour
    )
