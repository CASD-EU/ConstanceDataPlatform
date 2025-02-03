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

