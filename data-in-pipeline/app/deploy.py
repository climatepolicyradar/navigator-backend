import os
from pathlib import Path
from typing import Any

import pulumi.automation as auto
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


def _get_pulumi_stack_outputs(aws_env: str) -> dict[str, Any]:
    """Read environment variables and secrets from Pulumi stack outputs.

    :param aws_env: AWS environment name (e.g., 'production', 'staging').
    :type aws_env: str
    :return: Dictionary containing 'env_vars' and 'secrets_arns' keys.
    :rtype: dict[str, Any]
        Could be any type of value, but currently we only use strings
        and booleans.
    """
    logger = get_logger()
    infra_dir = Path(__file__).parent.parent / "infra"

    try:
        stack = auto.select_stack(
            stack_name=f"climatepolicyradar/data-in-pipeline/{aws_env}",
            project_name="data-in-pipeline",
            work_dir=str(infra_dir),
        )
        outputs = stack.outputs()

        env_vars_output = outputs.get("prefect_runtime_environment_variables")
        if env_vars_output and hasattr(env_vars_output, "value"):
            # The exported value is the dict itself, not nested under "value"
            env_vars = (
                env_vars_output.value if isinstance(env_vars_output.value, dict) else {}
            )
        else:
            env_vars = {}

        secrets_arns_output = outputs.get("prefect_secrets_arns")
        if secrets_arns_output and hasattr(secrets_arns_output, "value"):
            secrets_arns = (
                secrets_arns_output.value
                if isinstance(secrets_arns_output.value, dict)
                else {}
            )
        else:
            secrets_arns = {}

        ssm_secrets_output = outputs.get("prefect_ssm_secrets")
        if ssm_secrets_output and hasattr(ssm_secrets_output, "value"):
            ssm_secrets = (
                ssm_secrets_output.value
                if isinstance(ssm_secrets_output.value, dict)
                else {}
            )
        else:
            ssm_secrets = {}

        logger.info(
            "Retrieved %s environment variables and %s secrets from Pulumi stack.",
            len(env_vars),
            len(secrets_arns) + len(ssm_secrets),
        )

        return {
            "env_vars": env_vars,
            "secrets_arns": secrets_arns,
            "ssm_secrets": ssm_secrets,
        }
    except Exception as e:
        logger.warning(
            "Failed to read Pulumi stack outputs: %s. Falling back to defaults.",
            e,
        )
        return {
            "env_vars": {},
            "secrets_arns": {},
            "ssm_secrets": {},
        }


def _ensure_environment_variables(
    aws_env: str, pulumi_env_vars: dict[str, Any] | None = None
) -> dict[str, str]:
    """Collect environment variables required by Prefect runtimes.

    Reads from Pulumi stack outputs first, then falls back to environment
    variables or defaults.

    :param aws_env: AWS environment name (e.g., 'production', 'staging').
    :type aws_env: str
    :param pulumi_env_vars: Optional pre-fetched Pulumi environment variables.
    :type pulumi_env_vars: dict[str, Any] | None
    :return: Mapping of environment variables to forward to Prefect jobs.
    :rtype: dict[str, str]
    """
    logger = get_logger()

    # Use provided Pulumi env vars or fetch them
    if pulumi_env_vars is None:
        pulumi_outputs = _get_pulumi_stack_outputs(aws_env)
        pulumi_env_vars = pulumi_outputs.get("env_vars", {})

    # Fallback values if Pulumi stack outputs are not available
    # NOTE: Should we define the OTEL variables here or in the Pulumi stack?
    FALLBACK_ENVIRONMENT_VARIABLES = {}

    # Merge with environment variables (env vars take precedence)
    if not isinstance(pulumi_env_vars, dict):
        raise TypeError(f"pulumi_env_vars must be a dict, got {type(pulumi_env_vars)}")
    resolved = {**FALLBACK_ENVIRONMENT_VARIABLES, **pulumi_env_vars}

    # Override with any explicitly set environment variables
    for variable in FALLBACK_ENVIRONMENT_VARIABLES.keys():
        env_value = os.getenv(variable)
        if env_value is not None:
            resolved[variable] = env_value

    # Convert all values to strings (required for ECS)
    resolved = {k: str(v) for k, v in resolved.items()}

    logger.debug(
        "Forwarding %s runtime environment variables to Prefect worker.",
        len(resolved),
    )
    return resolved


def _merge_job_environments(
    base_job_variables: dict[str, Any],
    runtime_environment: dict[str, Any],
    secrets_arns: dict[str, str],
    ssm_secrets: dict[str, str],
) -> dict[str, Any]:
    """Merge runtime environment variables and secrets into job configuration.

    Configures secrets for ECS task definition. Prefect's ECS worker will inject
    these secrets into the container definition when creating ECS tasks.

    The secrets are configured in the job_variables, which Prefect uses to build
    the ECS task definition. The ECS task execution role must have permissions
    to access these secrets.

    :param base_job_variables: Existing job configuration from Prefect blocks.
    :type base_job_variables: dict[str, Any]
    :param runtime_environment: Environment variables collected from Pulumi or defaults.
    :type runtime_environment: dict[str, Any]
    :param secrets_arns: Mapping of environment variable names to Secrets Manager ARNs.
    :type secrets_arns: dict[str, str]
    :param ssm_secrets: Mapping of environment variable names to SSM Parameter ARNs.
    :type ssm_secrets: dict[str, str]
    :return: Job variables with the ``env`` and container secrets configured.
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

    # Configure secrets for ECS task definition
    # Format: [{"name": "ENV_VAR_NAME", "valueFrom": "arn:aws:secretsmanager:..."}]
    secrets_list = []
    for env_var_name, secret_arn in secrets_arns.items():
        secrets_list.append({"name": env_var_name, "valueFrom": secret_arn})

    # SSM Parameter Store secrets (for SecureString parameters)
    for env_var_name, ssm_arn in ssm_secrets.items():
        # SSM ARN format: arn:aws:ssm:region:account:parameter/name
        secrets_list.append({"name": env_var_name, "valueFrom": ssm_arn})

    if secrets_list:
        # Prefect's ECS worker uses task_definition_override to merge custom
        # container definitions. We need to provide the container definitions
        # with secrets that will be merged into Prefect's generated definition.
        #
        # The structure should match ECS container definition format:
        # {
        #   "containerDefinitions": [
        #     {
        #       "secrets": [{"name": "...", "valueFrom": "..."}]
        #     }
        #   ]
        # }
        #
        # Prefect will merge this with its own container definition.
        #
        # At runtime, ECS will:
        # 1. Use the task execution role to fetch secrets from Secrets Manager/SSM
        # 2. Inject them as environment variables into the container
        # 3. Our Prefect flows/tasks can access them via os.getenv("SECRET_NAME")
        if "task_definition_override" not in merged:
            merged["task_definition_override"] = {}

        # Initialise containerDefinitions if not present
        if "containerDefinitions" not in merged["task_definition_override"]:
            merged["task_definition_override"]["containerDefinitions"] = [{}]

        # Get the first container (Prefect will merge secrets into its container)
        first_container = merged["task_definition_override"]["containerDefinitions"][0]

        # Merge secrets (avoid duplicates by name)
        existing_secrets = first_container.get("secrets", [])
        existing_secret_names = {
            secret.get("name")
            for secret in existing_secrets
            if isinstance(secret, dict) and "name" in secret
        }

        # Add new secrets that don't already exist
        new_secrets = [
            secret
            for secret in secrets_list
            if secret.get("name") not in existing_secret_names
        ]

        if new_secrets:
            first_container["secrets"] = existing_secrets + new_secrets
            logger.debug(
                "Configured %s secrets for ECS task definition (total: %s).",
                len(new_secrets),
                len(existing_secrets) + len(new_secrets),
            )
        else:
            logger.debug(
                "All %s secrets already configured in ECS task definition.",
                len(secrets_list),
            )

    logger.debug(
        "Job environment now contains %s variables and %s secrets.",
        len(merged["env"]),
        len(secrets_list),
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

    aws_env = os.environ.get("AWS_ENV", "staging")
    docker_registry = os.environ["DOCKER_REGISTRY"]

    # Get environment variables and secrets from Pulumi
    pulumi_outputs = _get_pulumi_stack_outputs(aws_env)
    runtime_environment = _ensure_environment_variables(
        aws_env, pulumi_outputs.get("env_vars")
    )
    secrets_arns = pulumi_outputs.get("secrets_arns", {})
    ssm_secrets = pulumi_outputs.get("ssm_secrets", {})

    logger.info(
        "Creating deployment for flow `%s` in Prefect's production workspace with "
        "docker registry `%s`.",
        flow.name,
        docker_registry,
    )

    default_job_variables = ECSVariablesBlock.load(
        "ecs-default-job-variables-prefect-mvp-prod"
    ).model_dump(  # type: ignore
        # We have to exclude None for now as sending over values like
        # container_name=None vs the key missing affects functionality
        exclude_none=True
    )

    job_variables = _merge_job_environments(
        {**DEFAULT_FLOW_VARIABLES, **default_job_variables},
        runtime_environment,
        secrets_arns,
        ssm_secrets,
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
