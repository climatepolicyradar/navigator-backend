import json
import logging
import os
from typing import Any

from prefect import Flow
from prefect.docker.docker_image import DockerImage
from prefect.variables import Variable

from app.flow import pipeline as test_flow

_LOGGER = logging.getLogger(__name__)

MEGABYTES_PER_GIGABYTE = 1024
DEFAULT_FLOW_VARIABLES = {
    "cpu": MEGABYTES_PER_GIGABYTE * 1,
    "memory": MEGABYTES_PER_GIGABYTE * 2,
}


def load_default_job_variables(name: str) -> dict[str, Any]:
    """
    Load default job variables from a Prefect Variable by name.
    The Variable value should be a JSON string (e.g. '{"cpu": 1024, "memory": 2048}').
    Raises a clear error if the Variable is missing or malformed.
    """
    raw = Variable.get(name, default=None)
    if raw is None:
        raise RuntimeError(
            f"Prefect Variable '{name}' not found. "
            "Create it in the Prefect UI or via CLI with a JSON string value."
        )
    try:
        return json.loads(str(raw))
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Prefect Variable '{name}' does not contain valid JSON. "
            f"Value was: {raw}"
        ) from exc


def create_deployment(flow: Flow) -> None:
    """Create a deployment for the specified flow"""

    if os.environ.get("AWS_ENV") is None:
        raise RuntimeError("AWS_ENV environment variable is not set")
    if os.environ.get("DOCKER_REGISTRY") is None:
        raise RuntimeError("DOCKER_REGISTRY environment variable is not set")

    aws_env = os.environ["AWS_ENV"]
    docker_registry = os.environ["DOCKER_REGISTRY"]
    _LOGGER.info(
        f"Creating deployment for flow `{flow}` in `{aws_env}` with docker registry `{docker_registry}`"
    )

    # Our Prefect default job variables are stored in our environment specific AWS
    # accounts and we need to load these variables for them to be used in the deployment.
    variable_name = f"default-job-variables-prefect-mvp-{aws_env}"
    default_variables = load_default_job_variables(variable_name)
    job_variables = {**default_variables, **DEFAULT_FLOW_VARIABLES}

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
    create_deployment(test_flow)
