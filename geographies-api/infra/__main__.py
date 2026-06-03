import pulumi
import pulumi_aws as aws
from pulumi_aws.ecs.express_gateway_service import (
    ExpressGatewayService,
    ExpressGatewayServiceNetworkConfigurationArgs,
    ExpressGatewayServicePrimaryContainerArgs,
    ExpressGatewayServicePrimaryContainerEnvironmentArgs,
    ExpressGatewayServiceScalingTargetArgs,
)

account_id = aws.get_caller_identity().account_id


# TODO: https://linear.app/climate-policy-radar/issue/APP-584/standardise-naming-in-infra
def generate_secret_key(project: str, aws_service: str, name: str):
    return f"/{project}/{aws_service}/{name}"


config = pulumi.Config()
stack = pulumi.get_stack()
NAME_PREFIX = f"geographies-api-{stack}"

########################################################################
# Reference to shared API services infra
########################################################################

shared = pulumi.StackReference(f"climatepolicyradar/ecs-infra/{stack}")


# This stuff is being encapsulated in navigator-infra and we should use that once it is ready
# IAM role trusted by App Runner
geographies_api_role = aws.iam.Role(
    "geographies-api-role",
    assume_role_policy=aws.iam.get_policy_document(
        statements=[
            aws.iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                principals=[
                    aws.iam.GetPolicyDocumentStatementPrincipalArgs(
                        type="Service",
                        identifiers=["build.apprunner.amazonaws.com"],
                    )
                ],
                actions=["sts:AssumeRole"],
            )
        ]
    ).json,
)


# Attach ECR access policy to the role
geographies_api_role_policy = aws.iam.RolePolicy(
    "geographies-api-role-ecr-policy",
    role=geographies_api_role.id,
    policy=aws.iam.get_policy_document(
        statements=[
            aws.iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                actions=[
                    "ecr:GetDownloadUrlForLayer",
                    "ecr:BatchGetImage",
                    "ecr:DescribeImages",
                    "ecr:GetAuthorizationToken",
                    "ecr:BatchCheckLayerAvailability",
                ],
                resources=["*"],
            )
        ]
    ).json,
)

geographies_api_instance_role = aws.iam.Role(
    "geographies-api-instance-role",
    assume_role_policy=aws.iam.get_policy_document(
        statements=[
            aws.iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                principals=[
                    aws.iam.GetPolicyDocumentStatementPrincipalArgs(
                        type="Service",
                        identifiers=["tasks.apprunner.amazonaws.com"],
                    )
                ],
                actions=["sts:AssumeRole"],
            )
        ]
    ).json,
)

# Allow access to specific SSM Parameter Store secrets
geographies_api_ssm_policy = aws.iam.RolePolicy(
    "geographies-api-instance-role-ssm-policy",
    role=geographies_api_instance_role.id,
    policy=aws.iam.get_policy_document(
        statements=[
            aws.iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                actions=["ssm:GetParameters"],
                resources=[
                    f"arn:aws:ssm:eu-west-1:{account_id}:parameter/geographies-api/apprunner/*"
                ],
            )
        ]
    ).json,
)


geographies_api_ecr_repository = aws.ecr.Repository(
    "geographies-api-ecr-repository",
    encryption_configurations=[
        aws.ecr.RepositoryEncryptionConfigurationArgs(
            encryption_type="AES256",
        )
    ],
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=False,
    ),
    image_tag_mutability="MUTABLE",
    name="geographies-api",
    opts=pulumi.ResourceOptions(protect=True),
)


# Define the S3 access policy
s3_access_policy = aws.iam.Policy(
    "geographies-api-s3-access-policy",
    policy=aws.iam.get_policy_document(
        statements=[
            aws.iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                actions=[
                    "s3:PutObject",
                    "s3:PutObjectAcl",
                    "s3:GetObject",
                    "s3:DeleteObject",
                ],
                resources=[f"arn:aws:s3:::{config.require('geographies_bucket')}/*"],
            ),
            aws.iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                actions=["s3:ListBucket"],
                resources=[f"arn:aws:s3:::{config.require('geographies_bucket')}"],
            ),
        ]
    ).json,
    description="Policy to allow geographies API to access S3 bucket",
)

# Attach the policy to the instance role
aws.iam.RolePolicyAttachment(
    "geographies-api-instance-role-s3-policy-attachment",
    role=geographies_api_instance_role.name,
    policy_arn=s3_access_policy.arn,
)

geographies_api_apprunner_service = aws.apprunner.Service(
    "geographies-api-apprunner-service",
    auto_scaling_configuration_arn=f"arn:aws:apprunner:eu-west-1:{account_id}:autoscalingconfiguration/DefaultConfiguration/1/00000000000000000000000000000001",
    health_check_configuration=aws.apprunner.ServiceHealthCheckConfigurationArgs(
        interval=10,
        protocol="TCP",
        timeout=5,
    ),
    instance_configuration=aws.apprunner.ServiceInstanceConfigurationArgs(
        instance_role_arn=geographies_api_instance_role.arn,
    ),
    network_configuration=aws.apprunner.ServiceNetworkConfigurationArgs(
        ingress_configuration=aws.apprunner.ServiceNetworkConfigurationIngressConfigurationArgs(
            is_publicly_accessible=True,
        ),
        ip_address_type="IPV4",
    ),
    observability_configuration=aws.apprunner.ServiceObservabilityConfigurationArgs(
        observability_enabled=False,
    ),
    service_name="geographies-api",
    source_configuration=aws.apprunner.ServiceSourceConfigurationArgs(
        authentication_configuration=aws.apprunner.ServiceSourceConfigurationAuthenticationConfigurationArgs(
            access_role_arn=geographies_api_role.arn,
        ),
        image_repository=aws.apprunner.ServiceSourceConfigurationImageRepositoryArgs(
            image_configuration=aws.apprunner.ServiceSourceConfigurationImageRepositoryImageConfigurationArgs(
                port="8080",  # @related: PORT_NUMBER
                runtime_environment_variables={
                    "GEOGRAPHIES_BUCKET": config.require("geographies_bucket"),
                    "CDN_URL": config.require("cdn_url"),
                },
            ),
            image_identifier=f"{account_id}.dkr.ecr.eu-west-1.amazonaws.com/geographies-api:latest",
            image_repository_type="ECR",
        ),
    ),
    opts=pulumi.ResourceOptions(protect=True),
)

########################################################################
# ECS Express Gateway service
########################################################################

aws_env_stack = pulumi.StackReference(f"climatepolicyradar/aws_env/{stack}")
eu_west_1a_public_subnet_id = aws_env_stack.get_output("eu_west_1a_public_subnet_id")
eu_west_1b_public_subnet_id = aws_env_stack.get_output("eu_west_1b_public_subnet_id")
eu_west_1c_public_subnet_id = aws_env_stack.get_output("eu_west_1c_public_subnet_id")


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

# Reuse the same S3 access policy that App Runner uses — the bucket
# permissions are the same regardless of compute backend.
aws.iam.RolePolicyAttachment(
    f"{NAME_PREFIX}-ecs-task-role-s3-policy-attachment",
    role=ecs_task_role.name,
    policy_arn=s3_access_policy.arn,
)

# SSM access for any secrets the container reads at runtime. Mirrors
# the App Runner SSM policy.
aws.iam.RolePolicy(
    f"{NAME_PREFIX}-ecs-task-role-ssm-policy",
    role=ecs_task_role.id,
    policy=aws.iam.get_policy_document(
        statements=[
            aws.iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                actions=["ssm:GetParameters"],
                resources=[
                    f"arn:aws:ssm:eu-west-1:{account_id}:parameter/geographies-api/*"
                ],
            )
        ]
    ).json,
)


# Container config
primary_container = ExpressGatewayServicePrimaryContainerArgs(
    image=geographies_api_ecr_repository.repository_url.apply(
        lambda url: f"{url}:latest"
    ),
    container_port=8080,  # @related: PORT_NUMBER
    environments=[
        ExpressGatewayServicePrimaryContainerEnvironmentArgs(
            name="GEOGRAPHIES_BUCKET",
            value=config.require("geographies_bucket"),
        ),
        ExpressGatewayServicePrimaryContainerEnvironmentArgs(
            name="CDN_URL",
            value=config.require("cdn_url"),
        ),
    ],
)


ecs_express_service = ExpressGatewayService(
    f"{NAME_PREFIX}-ecs-express-service",
    service_name=NAME_PREFIX,
    cluster=shared.get_output("cluster_arn"),
    execution_role_arn=shared.get_output("task_execution_role_arn"),
    infrastructure_role_arn=shared.get_output("infrastructure_role_arn"),
    task_role_arn=ecs_task_role.arn,  # service-specific
    primary_container=primary_container,
    health_check_path="/health",
    cpu="1024",
    memory="2048",
    scaling_targets=[
        ExpressGatewayServiceScalingTargetArgs(
            auto_scaling_metric="AVERAGE_CPU",
            auto_scaling_target_value=70,
            min_task_count=1,
            max_task_count=4,
        ),
    ],
    network_configurations=[
        ExpressGatewayServiceNetworkConfigurationArgs(
            security_groups=[shared.get_output("alb_security_group_id")],
            subnets=[
                eu_west_1a_public_subnet_id,
                eu_west_1b_public_subnet_id,
                eu_west_1c_public_subnet_id,
            ],
        ),
    ],
)

pulumi.export(
    "ecs_express_service_url",
    ecs_express_service.ingress_paths.apply(
        lambda paths: paths[0].endpoint if paths else None
    ),
)

pulumi.export("apprunner_service_url", geographies_api_apprunner_service.service_url)
