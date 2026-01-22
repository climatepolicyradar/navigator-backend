"""
Deploy the data-in-load-api connectivity test flow.

Usage:
    DOCKER_REGISTRY=xxx.dkr.ecr.eu-west-1.amazonaws.com python -m app.data_in_load_api_deploy
"""

import os
from pathlib import Path

import pulumi.automation as auto
from prefect.blocks.core import Block
from prefect.docker import DockerImage
from prefect_aws.workers.ecs_worker import ECSVariables
from pulumi.automation._output import OutputMap
from pydantic import BaseModel, model_validator

from app.data_in_load_api_flow import test_data_in_load_api


class ECSVariablesBlock(Block, ECSVariables):  # type: ignore
    pass


class PulumiStackOutputs(BaseModel):
    prefect_vpc_id: str
    prefect_security_group_id: str
    prefect_subnet_id: str

    @model_validator(mode="before")
    @classmethod
    def from_pulumi_outputs(cls, output_map: OutputMap):
        output_dict = {k: v.value for k, v in output_map.items()}
        return output_dict


def create_deployment() -> None:
    """Create deployment for the connectivity test flow."""
    docker_registry = os.environ.get("DOCKER_REGISTRY")
    if not docker_registry:
        raise RuntimeError("DOCKER_REGISTRY environment variable is not set")

    # Load default job variables (contains cluster, subnets, etc.)
    default_job_variables = ECSVariablesBlock.load(
        "ecs-default-job-variables-prefect-mvp-prod"
    ).model_dump(  # type: ignore
        exclude_none=True
    )

    # Get network configuration from Pulumi stack outputs
    infra_dir = Path(__file__).parent.parent / "load-api-infra"
    stack = auto.select_stack(
        stack_name="climatepolicyradar/data-in-load-api/production",
        project_name="data-in-pipeline",
        work_dir=str(infra_dir),
    )

    pulumi_stack_outputs = PulumiStackOutputs.model_validate(stack.outputs())

    # Override with our minimal resources and add network configuration
    # The default job variables have empty network_configuration, so we need to specify it
    job_variables = {
        **default_job_variables,
        "vpc_id": pulumi_stack_outputs.prefect_vpc_id,
        "network_configuration": {
            "subnets": [pulumi_stack_outputs.prefect_subnet_id],
            "securityGroups": [pulumi_stack_outputs.prefect_security_group_id],
            "assignPublicIp": "ENABLED",  # Required for Fargate in public subnet
        },
    }

    # Build and push the Docker image separately
    docker_image = DockerImage(
        name=f"{docker_registry}/data-in-load-api",
        tag="latest",
        dockerfile="Dockerfile.data-in-load-api",
    )

    # Deploy with image as string and build=False to avoid git source inference
    test_data_in_load_api.deploy(
        name="data-in-load-api-connectivity-test",
        work_pool_name="mvp-prod-ecs",
        job_variables=job_variables,
        image=docker_image,
        build=False,
        push=False,
    )


if __name__ == "__main__":
    create_deployment()
