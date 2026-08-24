import json

import pulumi
import pulumi_aws as aws
from pulumi_aws.ecs.express_gateway_service import (
    ExpressGatewayService,
    ExpressGatewayServiceNetworkConfigurationArgs,
    ExpressGatewayServicePrimaryContainerArgs,
    ExpressGatewayServicePrimaryContainerEnvironmentArgs,
    ExpressGatewayServicePrimaryContainerSecretArgs,
    ExpressGatewayServiceScalingTargetArgs,
)

account_id = aws.get_caller_identity().account_id

pulumi_config = pulumi.Config()
stack = pulumi.get_stack()
NAME_PREFIX = f"families-api-{stack}"


# TODO: https://linear.app/climate-policy-radar/issue/APP-584/standardise-naming-in-infra
def generate_secret_key(project: str, aws_service: str, name: str):
    return f"/{project}/{aws_service}/{name}"


# TODO: once we get VPS info from the aws_env in navigator-infra, we should use that once it is ready

apprunner_vpc_connector_arn = pulumi_config.require("apprunner_vpc_connector_arn")


########################################################################
# Reference to shared infra stacks
########################################################################

ecs_infra = pulumi.StackReference(f"climatepolicyradar/ecs-infra/{stack}")
aws_env_stack = pulumi.StackReference(f"climatepolicyradar/aws_env/{stack}")
eu_west_1a_public_subnet_id = aws_env_stack.get_output("eu_west_1a_public_subnet_id")
eu_west_1b_public_subnet_id = aws_env_stack.get_output("eu_west_1b_public_subnet_id")
eu_west_1c_public_subnet_id = aws_env_stack.get_output("eu_west_1c_public_subnet_id")
rds_vpc_security_group_id = aws_env_stack.get_output("rds_security_group_id")
ecs_shared_task_execution_role_name = ecs_infra.get_output("task_execution_role_name")


families_api_apprunner_navigator_database_url = aws.ssm.Parameter(
    "families-api-apprunner-navigator-database-url",
    name=generate_secret_key("families-api", "apprunner", "NAVIGATOR_DATABASE_URL"),
    description="The URL string to connect to the navigator database",
    type=aws.ssm.ParameterType.SECURE_STRING,
    # This value is managed directly in SSM
    value="PLACEHOLDER",
    opts=pulumi.ResourceOptions(
        # This value is managed directly in SSM
        ignore_changes=["value"],
        # For a new environment, you need to import a manually created parameter
        # import_=f"arn:aws:ssm:eu-west-1:{account_id}:parameter{generate_secret_key('families-api', 'apprunner', 'NAVIGATOR_DATABASE_URL')}",
    ),
)


families_api_ecr_repository = aws.ecr.Repository(
    "families-api-ecr-repository",
    encryption_configurations=[
        aws.ecr.RepositoryEncryptionConfigurationArgs(
            encryption_type="AES256",
        )
    ],
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=False,
    ),
    image_tag_mutability="MUTABLE",
    name="families-api",
    opts=pulumi.ResourceOptions(protect=True),
)


families_api_ecr_lifecycle_policy = aws.ecr.LifecyclePolicy(
    "families-api-ecr-repository-lifecycle",
    repository=families_api_ecr_repository.name,
    policy=json.dumps(
        {
            "rules": [
                {
                    "rulePriority": 1,
                    "description": "Keep last 50 images",
                    "selection": {
                        "tagStatus": "any",
                        "countType": "imageCountMoreThan",
                        "countNumber": 50,
                    },
                    "action": {"type": "expire"},
                }
            ]
        }
    ),
)

families_api_apprunner_autoscaling_configuration = aws.apprunner.AutoScalingConfigurationVersion(
    # len(name) < 32
    "families-api-autoscaling-config",
    auto_scaling_configuration_name="families-api-autoscaling-config",
    max_concurrency=10,
    max_size=10,
    min_size=2 if stack == "production" else 1,
)


########################################################################
# ECS Express Gateway service
########################################################################

# Task role: the IAM role the *running container* assumes.
ecs_task_role = aws.iam.Role(
    f"{NAME_PREFIX}-ecs-task-role",
    name=f"{NAME_PREFIX}-ecs-task-role",
    assume_role_policy=aws.iam.get_policy_document(
        statements=[
            aws.iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                principals=[
                    aws.iam.GetPolicyDocumentStatementPrincipalArgs(
                        type="Service",
                        identifiers=["ecs-tasks.amazonaws.com"],
                    )
                ],
                actions=["sts:AssumeRole"],
            )
        ]
    ).json,
)

# SSM access for secrets the container reads at runtime — scoped to the
# full families-api/* prefix so both apprunner/ and any future ECS-specific
# secrets are accessible.
aws.iam.RolePolicy(
    f"{NAME_PREFIX}-ecs-task-role-ssm-policy",
    role=ecs_task_role.id,
    policy=aws.iam.get_policy_document(
        statements=[
            aws.iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                actions=["ssm:GetParameters"],
                resources=[
                    f"arn:aws:ssm:eu-west-1:{account_id}:parameter/families-api/*"
                ],
            )
        ]
    ).json,
)


aws.iam.RolePolicy(
    f"{NAME_PREFIX}-execution-role-ssm-policy",
    role=ecs_shared_task_execution_role_name,
    policy=aws.iam.get_policy_document(
        statements=[
            aws.iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                actions=["ssm:GetParameters"],
                resources=[
                    f"arn:aws:ssm:eu-west-1:{account_id}:parameter/families-api/*"
                ],
            )
        ]
    ).json,
)

# Allow the ECS tasks to reach the RDS instance — adds an ingress rule on
# the existing RDS security group rather than modifying it in place.
aws.ec2.SecurityGroupRule(
    f"{NAME_PREFIX}-rds-ingress-from-ecs",
    type="ingress",
    from_port=5432,
    to_port=5432,
    protocol="tcp",
    security_group_id=rds_vpc_security_group_id,
    source_security_group_id=ecs_infra.get_output("alb_security_group_id"),
    description="Allow ECS Express tasks to reach Postgres",
)


# Container config — NAVIGATOR_DATABASE_URL and CDN_URL are read from SSM
# at runtime via the task role rather than injected as plaintext env vars.
primary_container = ExpressGatewayServicePrimaryContainerArgs(
    image=families_api_ecr_repository.repository_url.apply(lambda url: f"{url}:latest"),
    container_port=8080,  # @related: PORT_NUMBER
    environments=[
        ExpressGatewayServicePrimaryContainerEnvironmentArgs(
            name="CDN_URL",
            value=(
                "https://cdn.climatepolicyradar.org"
                if stack == "production"
                else "https://cdn.dev.climatepolicyradar.org"
            ),
        ),
    ],
    secrets=[
        # Pulled from SSM at container startup by the execution role
        ExpressGatewayServicePrimaryContainerSecretArgs(
            name="NAVIGATOR_DATABASE_URL",
            value_from=families_api_apprunner_navigator_database_url.arn,
        ),
    ],
)

ecs_express_service = ExpressGatewayService(
    f"{NAME_PREFIX}-ecs-express-service",
    service_name=NAME_PREFIX,
    cluster=ecs_infra.get_output("cluster_arn"),
    execution_role_arn=ecs_infra.get_output("task_execution_role_arn"),
    infrastructure_role_arn=ecs_infra.get_output("infrastructure_role_arn"),
    task_role_arn=ecs_task_role.arn,
    primary_container=primary_container,
    health_check_path="/health",
    cpu="2048",
    memory="4096",
    scaling_targets=[
        ExpressGatewayServiceScalingTargetArgs(
            auto_scaling_metric="AVERAGE_CPU",
            auto_scaling_target_value=70,
            min_task_count=2,
            max_task_count=4,
        ),
    ],
    network_configurations=[
        ExpressGatewayServiceNetworkConfigurationArgs(
            security_groups=[ecs_infra.get_output("alb_security_group_id")],
            subnets=[
                eu_west_1a_public_subnet_id,
                eu_west_1b_public_subnet_id,
                eu_west_1c_public_subnet_id,
            ],
        ),
    ],
)


########################################################################
# Exports
########################################################################

pulumi.export(
    "ecs_express_service_url",
    ecs_express_service.ingress_paths.apply(
        lambda paths: paths[0].endpoint.removeprefix("https://") if paths else None
    ),
)
