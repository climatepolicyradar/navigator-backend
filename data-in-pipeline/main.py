import random

from prefect import flow, task
from prefect.futures import PrefectFutureList


@task(log_prints=True)
def get_customer_ids() -> list[str]:
    # Fetch customer IDs from a database or API
    return [f"customer{n}" for n in random.choices(range(100), k=10)]  # nosec B311


@task(log_prints=True)
def process_customer(customer_id: str) -> str:
    # Process a single customer
    print(f"Hello customer #{customer_id}!")
    return f"Hello customer #{customer_id}!"


@flow(log_prints=True)
def main() -> PrefectFutureList[str]:
    customer_ids = get_customer_ids()

    # Map the process_customer task across all customer IDs
    results = process_customer.map(customer_ids=customer_ids)
    return results


if __name__ == "__main__":
    main()
