from prefect import flow, task

from app.extract.connectors import NavigatorFamily


@task
def transform_families(families: list[NavigatorFamily]): ...


@task
def transform_family(family: NavigatorFamily): ...


@task
def extract_all_families() -> list[NavigatorFamily]: ...


@task
def extract_family_page(page: int) -> list[NavigatorFamily]: ...


@task
def get_family_api_page_count() -> int: ...


@flow
def navigator_family_etl_pipeline():
    # All / fan out per family
    families = extract_all_families()
    transformed_families = transform_family.map(families)

    # All / 1 task for all transformations
    families = extract_all_families()
    transformed_families = transform_families(families)

    # Batch / fan out per family
    page_count = get_family_api_page_count()
    for page in range(1, page_count + 1):
        families = extract_family_page(page)
        transformed_families = transform_family.map(families)

    # Batch / fan out per family
    page_count = get_family_api_page_count()
    for page in range(1, page_count + 1):
        families = extract_family_page(page)
        transformed_families = transform_families(families)

    return transformed_families
