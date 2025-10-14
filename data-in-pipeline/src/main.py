from prefect import flow, task


@task(log_prints=True)
def add(a: int, b: int) -> int:
    print(f"{a} plus {b} equals {a + b}!")
    return a + b


@flow(log_prints=True)
def main() -> list[int]:
    x = [1, 2, 3]
    y = [10, 20, 30]

    results = add.map(a=x, b=y)
    return [result.result() for result in results]


if __name__ == "__main__":
    main()
