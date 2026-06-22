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

config = pulumi.Config()
stack = pulumi.get_stack()
NAME_PREFIX = f"concepts-api-{stack}"


# TODO: https://linear.app/climate-policy-radar/issue/APP-584/standardise-naming-in-infra
def generate_secret_key(project: str, aws_service: str, name: str):
    return f"/{project}/{aws_service}/{name}"


########################################################################
# Reference to shared API services infra
########################################################################

ecs_infra = pulumi.StackReference(f"climatepolicyradar/ecs-infra/{stack}")
aws_env_stack = pulumi.StackReference(f"climatepolicyradar/aws_env/{stack}")
eu_west_1a_public_subnet_id = aws_env_stack.get_output("eu_west_1a_public_subnet_id")
eu_west_1b_public_subnet_id = aws_env_stack.get_output("eu_west_1b_public_subnet_id")
eu_west_1c_public_subnet_id = aws_env_stack.get_output("eu_west_1c_public_subnet_id")


# IAM role trusted by App Runner
concepts_api_role = aws.iam.Role(
    "concepts-api-role",
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

concepts_api_role_policy = aws.iam.RolePolicy(
    "concepts-api-role-ecr-policy",
    role=concepts_api_role.id,
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

concepts_api_instance_role = aws.iam.Role(
    "concepts-api-instance-role",
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

concepts_api_ssm_policy = aws.iam.RolePolicy(
    "concepts-api-instance-role-ssm-policy",
    role=concepts_api_instance_role.id,
    policy=aws.iam.get_policy_document(
        statements=[
            aws.iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                actions=["ssm:GetParameters"],
                resources=[
                    f"arn:aws:ssm:eu-west-1:{account_id}:parameter/concepts-api/apprunner/*"
                ],
            )
        ]
    ).json,
)

concepts_api_ecr_repository = aws.ecr.Repository(
    "concepts-api-ecr-repository",
    encryption_configurations=[
        aws.ecr.RepositoryEncryptionConfigurationArgs(
            encryption_type="AES256",
        )
    ],
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=False,
    ),
    image_tag_mutability="MUTABLE",
    name="concepts-api",
    opts=pulumi.ResourceOptions(protect=True),
)

concepts_api_apprunner_service = aws.apprunner.Service(
    "concepts-api-apprunner-service",
    auto_scaling_configuration_arn=f"arn:aws:apprunner:eu-west-1:{account_id}:autoscalingconfiguration/DefaultConfiguration/1/00000000000000000000000000000001",
    health_check_configuration=aws.apprunner.ServiceHealthCheckConfigurationArgs(
        interval=10,
        protocol="TCP",
        timeout=5,
    ),
    instance_configuration=aws.apprunner.ServiceInstanceConfigurationArgs(
        instance_role_arn=concepts_api_instance_role.arn,
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
    service_name="concepts-api",
    source_configuration=aws.apprunner.ServiceSourceConfigurationArgs(
        authentication_configuration=aws.apprunner.ServiceSourceConfigurationAuthenticationConfigurationArgs(
            access_role_arn=concepts_api_role.arn,
        ),
        image_repository=aws.apprunner.ServiceSourceConfigurationImageRepositoryArgs(
            image_configuration=aws.apprunner.ServiceSourceConfigurationImageRepositoryImageConfigurationArgs(
                port="8080",
                runtime_environment_variables={"Environment": pulumi.get_stack()},
            ),
            image_identifier=f"{account_id}.dkr.ecr.eu-west-1.amazonaws.com/concepts-api:latest",
            image_repository_type="ECR",
        ),
    ),
    opts=pulumi.ResourceOptions(protect=True),
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

# SSM access for any secrets the container reads at runtime.
aws.iam.RolePolicy(
    f"{NAME_PREFIX}-ecs-task-role-ssm-policy",
    role=ecs_task_role.id,
    policy=aws.iam.get_policy_document(
        statements=[
            aws.iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                actions=["ssm:GetParameters"],
                resources=[
                    f"arn:aws:ssm:eu-west-1:{account_id}:parameter/concepts-api/*"
                ],
            )
        ]
    ).json,
)

# Container config
primary_container = ExpressGatewayServicePrimaryContainerArgs(
    image=concepts_api_ecr_repository.repository_url.apply(lambda url: f"{url}:latest"),
    container_port=8080,  # @related: PORT_NUMBER
    environments=[
        ExpressGatewayServicePrimaryContainerEnvironmentArgs(
            name="Environment",
            value=pulumi.get_stack(),
        ),
    ],
)

ecs_express_service = ExpressGatewayService(
    f"{NAME_PREFIX}-ecs-express-service",
    service_name=NAME_PREFIX,
    cluster=ecs_infra.get_output("cluster_arn"),
    execution_role_arn=ecs_infra.get_output("task_execution_role_arn"),
    infrastructure_role_arn=ecs_infra.get_output("infrastructure_role_arn"),
    task_role_arn=ecs_task_role.arn,  # service-specific
    primary_container=primary_container,
    health_check_path="/health",
    cpu="1024",
    memory="2048",
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


pulumi.export(
    "ecs_express_service_url",
    ecs_express_service.ingress_paths.apply(
        lambda paths: paths[0].endpoint.removeprefix("https://") if paths else None
    ),
)

pulumi.export("apprunner_service_url", concepts_api_apprunner_service.service_url)
