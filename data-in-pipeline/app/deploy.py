import os

from prefect import Flow
from prefect.blocks.system import JSON
from prefect.docker.docker_image import DockerImage

from app.logging_config import get_logger
from app.navigator_document_etl_pipeline import process_updates

_LOGGER = get_logger()

MEGABYTES_PER_GIGABYTE = 1024
DEFAULT_FLOW_VARIABLES = {
    "cpu": MEGABYTES_PER_GIGABYTE * 1,
    "memory": MEGABYTES_PER_GIGABYTE * 2,
}


def create_deployment(flow: Flow) -> None:
    """Create a deployment for the specified flow"""

    if os.environ.get("DOCKER_REGISTRY") is None:
        raise RuntimeError("DOCKER_REGISTRY environment variable is not set")

    aws_env = os.environ.get("AWS_ENV", "prod")
    docker_registry = os.environ["DOCKER_REGISTRY"]
    _LOGGER.info(
        f"Creating deployment for flow `{flow}` in `{aws_env}` with docker registry `{docker_registry}`"
    )

    # Work pool will provide its base job variables automatically.
    # Job variables - our custom defaults will override the work pool's CPU and memory
    # whilst keeping the work pool's other variables.
    default_job_variables = JSON.load(
        f"default-job-variables-prefect-mvp-{aws_env}"
    ).value  # type: ignore
    job_variables = {**DEFAULT_FLOW_VARIABLES, **default_job_variables}

    # Deploy our flow to Prefect cloud.
    _ = flow.deploy(
        "data-in-pipeline-deployment",
        work_pool_name=f"mvp-{aws_env}-ecs",
        image=DockerImage(
            name=f"{docker_registry}/data-in-pipeline",
            tag="latest",
            dockerfile="Dockerfile",
        ),
        job_variables=job_variables,
        build=False,
        push=False,
    )


if __name__ == "__main__":
    create_deployment(process_updates)
