from asyncio import wait_for

from prefect import flow, task
from pathlib import Path

# if you don't specify the flow name, the method name will be used as the flow name
@flow(log_prints=True, name="advance_show_chat")
def show_chat(guest_list: list[str]):
    """Flow: Show the number of stars that GitHub repos have"""
    for i, guest in enumerate(guest_list):
        # Call Task 1
        task1 = build_welcome_msg.submit(guest)
        welcome_msg = task1.result()
        # Call Task 2
        task2 = answer_welcome_msg.submit(i)
        answer_msg = task2.result()

        # call task 3 when task1 and 2 are finished
        task3 = close_conversation.submit(wait_for=[task1, task2])
        sys_msg = task3.result()
        # Print the result
        print(f"CASD: {welcome_msg}")
        print(f"GUEST: {answer_msg}")
        print(f"SYS: {sys_msg}")


@task
def build_welcome_msg(guest_name: str):
    """Task 1: CASD says hello to guest"""
    return f"Hello, {guest_name}! Welcome to CASD"


@task
def answer_welcome_msg(index: dict):
    """Task 2: guest say hello to CASD"""
    return f"Hello, CASD. thank you very much! I'm guest {index}!"

@task
def close_conversation():
    """ Task 3: close conversation"""
    return f"The conversation closed successfully."

if __name__ == "__main__":
    flow.from_source(
        source=str(Path(__file__).parent),
        entrypoint="advance_sample_deployment.py:show_chat", # Specific flow to run
    ).deploy(
        name="advance_sample-deployment",
        parameters={
            "guest_list": [
                 "pengfei",
                 "Thibaut"
            ]
        },
        work_pool_name="casd-work-pool",
        cron="0 * * * *",  # Run every hour
    )
