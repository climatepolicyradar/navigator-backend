import os
from typing import Any

from prefect import Flow
from prefect.blocks.core import Block
from prefect.docker.docker_image import DockerImage
from prefect_aws.workers.ecs_worker import ECSVariables

from app.bootstrap_telemetry import get_logger
from app.navigator_family_etl_pipeline import etl_pipeline

MEGABYTES_PER_GIGABYTE = 1024
DEFAULT_FLOW_VARIABLES = {
    "cpu": MEGABYTES_PER_GIGABYTE * 1,
    "memory": MEGABYTES_PER_GIGABYTE * 2,
}
REQUIRED_RUNTIME_ENVIRONMENT_VARIABLES = (
    ("API_BASE_URL", "https://api.climatepolicyradar.org"),
    ("DISABLE_OTEL_LOGGING", "false"),
    ("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"),
    ("OTEL_EXPORTER_OTLP_ENDPOINT", "https://otel.prod.climatepolicyradar.org"),
    ("OTEL_PYTHON_LOGGER_PROVIDER", "sdk"),
    ("OTEL_PYTHON_LOG_CORRELATION", True),
    ("OTEL_PYTHON_LOG_LEVEL", "INFO"),
    (
        "OTEL_RESOURCE_ATTRIBUTES",
        "deployment.environment=production,service.namespace=data-fetching",
    ),
    ("OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED", True),
    (
        # https://docs.prefect.io/v3/api-ref/settings-ref#enable-orchestration-telemetry
        "PREFECT_CLOUD_ENABLE_ORCHESTRATION_TELEMETRY",
        True,
    ),
    (
        # https://docs.prefect.io/v3/api-ref/settings-ref#enabled-2
        "PREFECT_LOGGING_TO_API_ENABLED",
        True,
    ),
    (
        # https://docs.prefect.io/v3/api-ref/settings-ref#extra-loggers
        "PREFECT_LOGGING_EXTRA_LOGGERS",
        "app",
    ),
)


def _ensure_environment_variables() -> dict[str, Any]:
    """Collect environment variables required by Prefect runtimes.

    :return: Mapping of environment variables to forward to Prefect jobs.
    :rtype: dict[str, Any]
    """
    logger = get_logger()
    resolved = {}

    for variable, default_value in REQUIRED_RUNTIME_ENVIRONMENT_VARIABLES:
        value = os.getenv(variable)

        # If the environment variable is not set, use the default value
        if value is None:
            value = default_value
        resolved[variable] = value

    logger.debug(
        "Forwarding %s runtime environment variables to Prefect worker.",
        len(resolved),
    )
    return resolved


def _merge_job_environments(
    base_job_variables: dict[str, Any],
    runtime_environment: dict[str, Any],
) -> dict[str, Any]:
    """Merge runtime environment variables into job configuration.

    :param base_job_variables: Existing job configuration from Prefect blocks.
    :type base_job_variables: dict[str, Any]
    :param runtime_environment: Environment variables collected locally.
    :type runtime_environment: dict[str, Any]
    :return: Job variables with the ``env`` section overwritten.
    :rtype: dict[str, Any]
    """
    logger = get_logger()
    merged = {**base_job_variables}
    existing_env = {
        key: str(value)
        for key, value in merged.get("env", {}).items()
        if value is not None
    }
    merged["env"] = {**existing_env, **runtime_environment}

    logger.debug(
        "Job environment now contains %s variables.",
        len(merged["env"]),
    )
    return merged


class ECSVariablesBlock(Block, ECSVariables):  # type: ignore
    pass


def create_deployment(flow: Flow) -> None:
    """Create a deployment for the specified flow.

    :param flow: Prefect flow that needs deploying.
    :type flow: Flow
    :return: The function does not return anything.
    :rtype: None
    """
    logger = get_logger()

    if os.environ.get("DOCKER_REGISTRY") is None:
        raise RuntimeError("DOCKER_REGISTRY environment variable is not set.")

    aws_env = os.environ.get("AWS_ENV", "prod")
    docker_registry = os.environ["DOCKER_REGISTRY"]
    runtime_environment = _ensure_environment_variables()
    logger.info(
        "Creating deployment for flow `%s` in Prefect's production workspace with "
        "docker registry `%s`.",
        flow.name,
        docker_registry,
    )

    default_job_variables = ECSVariablesBlock.load(
        "ecs-default-job-variables-prefect-mvp-prod"
    ).model_dump(  # type: ignore
        # We have to exclude None for now as sending over values like container_name=None vs the key missing affects functionality
        exclude_none=True
    )

    job_variables = _merge_job_environments(
        {**DEFAULT_FLOW_VARIABLES, **default_job_variables},
        runtime_environment,
    )
    logger.info("Job variables: %s", job_variables)

    _ = flow.deploy(
        f"data-in-pipeline-{aws_env}",
        work_pool_name="mvp-prod-ecs",
        image=DockerImage(
            name=f"{docker_registry}/data-in-pipeline",
            tag="latest",
            dockerfile="Dockerfile",
        ),
        job_variables=job_variables,
        build=False,
        push=False,
    )
    logger.info("Deployment registration completed.")


if __name__ == "__main__":
    create_deployment(etl_pipeline)
