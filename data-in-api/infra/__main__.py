"""
Infrastructure for this service is currently provisioned in ../../data-in-pipeline/infra/__main__.py
"""

import json
import re
from typing import cast

import pulumi
import pulumi_aws as aws

config = pulumi.Config()
environment = pulumi.get_stack()
name = pulumi.get_project()
account_id = aws.get_caller_identity().account_id

# ---------------------------------------------------------------------------
# Review stack detection
# ---------------------------------------------------------------------------
is_review_stack = environment.startswith("pr-")

if is_review_stack:
    match = re.search(r"(\d+)$", environment)
    pr_number = match.group(1) if match else environment[-8:]
else:
    pr_number = None

tags = {
    "CPR-Created-By": "pulumi",
    "CPR-Pulumi-Stack-Name": environment,
    "CPR-Pulumi-Project-Name": pulumi.get_project(),
    "CPR-Tag": f"{environment}-{name}",
    "Environment": "review" if is_review_stack else environment,
}

if is_review_stack:
    import pulumi_docker_build as docker_build

    # Shared resources for review stacks (from backend-platform).
    platform_stack = pulumi.StackReference("climatepolicyradar/backend-platform/staging")
    shared_ecr_url = platform_stack.get_output(
        "data-in-api_review_ecr_repository_url"
    )
    shared_access_role_arn = platform_stack.get_output("apprunner_ecr_access_role_arn")
    shared_instance_role_arn = platform_stack.get_output("apprunner_instance_role_arn")

    # Always point at staging data-in-pipeline for Aurora access.
    data_in_pipeline_stack = pulumi.StackReference(
        "climatepolicyradar/data-in-pipeline/staging"
    )

    ecr_auth = aws.ecr.get_authorization_token_output()
    review_image = docker_build.Image(
        "review-data-in-api-image",
        tags=[pulumi.Output.concat(shared_ecr_url, ":", environment)],
        context=docker_build.BuildContextArgs(
            location="../..",
        ),
        dockerfile=docker_build.DockerfileArgs(
            location="../../data-in-api/Dockerfile",
        ),
        platforms=[docker_build.Platform.LINUX_AMD64],
        push=True,
        registries=[
            docker_build.RegistryArgs(
                address=shared_ecr_url,
                username=ecr_auth.user_name,
                password=ecr_auth.password,
            ),
        ],
        build_on_preview=False,
    )

    # Read staging Aurora SSM parameter ARNs for runtime secrets.
    aurora_read_replica_db_url_parameter = data_in_pipeline_stack.get_output(
        "aurora-read-replica-db-url-parameter-name"
    ).apply(lambda param_name: aws.ssm.get_parameter(name=param_name))

    aurora_read_replica_db_name_parameter = data_in_pipeline_stack.get_output(
        "aurora-read-replica-db-name-parameter-name"
    ).apply(lambda param_name: aws.ssm.get_parameter(name=param_name))

    aurora_read_replica_db_username_parameter = data_in_pipeline_stack.get_output(
        "aurora-read-replica-db-username-parameter-name"
    ).apply(lambda param_name: aws.ssm.get_parameter(name=param_name))

    # Review: lightweight App Runner without VPC egress (no Aurora direct access
    # needed -- uses staging SSM params which contain the connection details).
    apprunner_service = aws.apprunner.Service(
        "data-in-api-apprunner-service",
        auto_scaling_configuration_arn=(
            f"arn:aws:apprunner:eu-west-1:{account_id}:autoscalingconfiguration"
            "/DefaultConfiguration/1/00000000000000000000000000000001"
        ),
        health_check_configuration=aws.apprunner.ServiceHealthCheckConfigurationArgs(
            interval=2,
            protocol="HTTP",
            path="/health",
            timeout=5,
            unhealthy_threshold=2,
        ),
        instance_configuration=aws.apprunner.ServiceInstanceConfigurationArgs(
            instance_role_arn=cast(str, shared_instance_role_arn),
        ),
        network_configuration=aws.apprunner.ServiceNetworkConfigurationArgs(
            ip_address_type="IPV4",
            ingress_configuration=aws.apprunner.ServiceNetworkConfigurationIngressConfigurationArgs(
                is_publicly_accessible=True,
            ),
        ),
        observability_configuration=aws.apprunner.ServiceObservabilityConfigurationArgs(
            observability_enabled=False,
        ),
        source_configuration=aws.apprunner.ServiceSourceConfigurationArgs(
            authentication_configuration=aws.apprunner.ServiceSourceConfigurationAuthenticationConfigurationArgs(
                access_role_arn=cast(str, shared_access_role_arn),
            ),
            image_repository=aws.apprunner.ServiceSourceConfigurationImageRepositoryArgs(
                image_configuration=aws.apprunner.ServiceSourceConfigurationImageRepositoryImageConfigurationArgs(
                    runtime_environment_secrets={
                        "DB_URL": aurora_read_replica_db_url_parameter.arn,
                        "DB_NAME": aurora_read_replica_db_name_parameter.arn,
                        "DB_USERNAME": aurora_read_replica_db_username_parameter.arn,
                    },
                    runtime_environment_variables={
                        "DB_PORT": "5432",
                        "AWS_REGION": "eu-west-1",
                        "CDN_URL": "https://cdn.dev.climatepolicyradar.org",
                        "DB_SSLMODE": "require",
                    },
                ),
                image_identifier=cast(
                    str,
                    pulumi.Output.concat(shared_ecr_url, ":", environment),
                ),
                image_repository_type="ECR",
            ),
        ),
        tags={**tags, "PRNumber": pr_number or "unknown"},
        opts=pulumi.ResourceOptions(
            depends_on=[review_image],
        ),
    )

else:
    # -----------------------------------------------------------------------
    # Non-review (staging / production) resources
    # -----------------------------------------------------------------------
    import components.aws as components_aws

    data_in_pipeline_stack = pulumi.StackReference(
        f"climatepolicyradar/data-in-pipeline/{environment}"
    )

    # AppRunner and related components
    ecr_repository = components_aws.ecr.Repository(
        name=f"{name}-ecr-repository",
        aws_ecr_repository_args=aws.ecr.RepositoryArgs(
            name="data-in-api",
            encryption_configurations=[
                aws.ecr.RepositoryEncryptionConfigurationArgs(
                    encryption_type="AES256",
                )
            ],
            image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
                scan_on_push=False,
            ),
            image_tag_mutability="MUTABLE",
        ),
    )

    access_role = aws.iam.Role(
        "data-in-api-access-role",
        name="data-in-api-access-role",
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
    access_role_policy = aws.iam.RolePolicy(
        "data-in-api-access-role-policy",
        name="data-in-api-access-role-policy",
        role=access_role.id,
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

    instance_role = aws.iam.Role(
        "data-in-api-instance-role",
        name="data-in-api-instance-role",
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

    aurora_read_replica_db_url_parameter = data_in_pipeline_stack.get_output(
        "aurora-read-replica-db-url-parameter-name"
    ).apply(lambda param_name: aws.ssm.get_parameter(name=param_name))

    aurora_read_replica_db_name_parameter = data_in_pipeline_stack.get_output(
        "aurora-read-replica-db-name-parameter-name"
    ).apply(lambda param_name: aws.ssm.get_parameter(name=param_name))

    aurora_read_replica_db_username_parameter = data_in_pipeline_stack.get_output(
        "aurora-read-replica-db-username-parameter-name"
    ).apply(lambda param_name: aws.ssm.get_parameter(name=param_name))

    aurora_read_replica_db_security_group_id = data_in_pipeline_stack.get_output(
        "aurora-read-replica-security-group-id"
    )

    aurora_cluster_resource_id = data_in_pipeline_stack.require_output(
        "aurora-cluster-resource-id"
    )

    instance_role_policy = aws.iam.RolePolicy(
        "data-in-api-instance-role-policy",
        name="data-in-api-instance-role-policy",
        role=instance_role.id,
        policy=pulumi.Output.all(
            aurora_read_replica_db_url_parameter=aurora_read_replica_db_url_parameter.arn,
            aurora_read_replica_db_name_parameter=aurora_read_replica_db_name_parameter.arn,
            aurora_read_replica_db_username_parameter=aurora_read_replica_db_username_parameter.arn,
            aurora_cluster_resource_id=aurora_cluster_resource_id,
            db_username=aurora_read_replica_db_username_parameter.value,
        ).apply(
            lambda args: (
                aws.iam.get_policy_document(
                    statements=[
                        aws.iam.GetPolicyDocumentStatementArgs(
                            effect="Allow",
                            actions=[
                                "ssm:GetParameter",
                                "ssm:GetParameters",
                                "ssm:DescribeParameters",
                            ],
                            resources=[
                                args["aurora_read_replica_db_url_parameter"],
                                args["aurora_read_replica_db_name_parameter"],
                                args["aurora_read_replica_db_username_parameter"],
                            ],
                        ),
                        aws.iam.GetPolicyDocumentStatementArgs(
                            effect="Allow",
                            actions=["rds-db:connect"],
                            resources=[
                                f"arn:aws:rds-db:eu-west-1:{account_id}:dbuser:{args['aurora_cluster_resource_id']}/{args['db_username']}"
                            ],
                        ),
                    ]
                ).json
            )
        ),
    )

    aws_env_stack = pulumi.StackReference(
        f"climatepolicyradar/aws_env/{environment}"
    )
    vpc_id = aws_env_stack.get_output("vpc_id")
    eu_west_1a_private_subnet_id = aws_env_stack.get_output(
        "eu_west_1a_private_subnet_id"
    )
    eu_west_1b_private_subnet_id = aws_env_stack.get_output(
        "eu_west_1b_private_subnet_id"
    )
    eu_west_1c_private_subnet_id = aws_env_stack.get_output(
        "eu_west_1c_private_subnet_id"
    )
    private_subnets = [
        eu_west_1a_private_subnet_id,
        eu_west_1b_private_subnet_id,
        eu_west_1c_private_subnet_id,
    ]
    vpc_sg = aws.ec2.SecurityGroup(
        "data-in-api-vpc-sg",
        name="data-in-api-vpc-sg",
        vpc_id=vpc_id,
        egress=[
            aws.ec2.SecurityGroupEgressArgs(
                protocol="-1", from_port=0, to_port=0, cidr_blocks=["0.0.0.0/0"]
            )
        ],
    )
    vpc_connector = aws.apprunner.VpcConnector(
        "data-in-api-vpc-connector",
        vpc_connector_name="data-in-api-vpc-connector",
        subnets=private_subnets,
        security_groups=[vpc_sg.id],
    )
    data_in_api_to_aurora_read_replica = aws.ec2.SecurityGroupRule(
        "data-in-api-to-aurora-read-replica",
        type="ingress",
        security_group_id=aurora_read_replica_db_security_group_id,
        source_security_group_id=vpc_sg.id,
        protocol="tcp",
        from_port=5432,
        to_port=5432,
        description="Allow Postgres from load API VPC SG",
    )

    apprunner_service = aws.apprunner.Service(
        "data-in-api-apprunner-service",
        service_name="data-in-api",
        # This is the default ASG
        auto_scaling_configuration_arn=(
            f"arn:aws:apprunner:eu-west-1:{account_id}:autoscalingconfiguration"
            "/DefaultConfiguration/1/00000000000000000000000000000001"
        ),
        health_check_configuration=aws.apprunner.ServiceHealthCheckConfigurationArgs(
            interval=2,  # seconds
            protocol="HTTP",
            path="/health",
            timeout=5,  # seconds
            unhealthy_threshold=2,  # seconds
        ),
        instance_configuration=aws.apprunner.ServiceInstanceConfigurationArgs(
            instance_role_arn=instance_role.arn,
        ),
        network_configuration=aws.apprunner.ServiceNetworkConfigurationArgs(
            ip_address_type="IPV4",
            ingress_configuration=aws.apprunner.ServiceNetworkConfigurationIngressConfigurationArgs(
                is_publicly_accessible=True,
            ),
            egress_configuration=aws.apprunner.ServiceNetworkConfigurationEgressConfigurationArgs(
                egress_type="VPC",
                vpc_connector_arn=vpc_connector.arn,
            ),
        ),
        observability_configuration=aws.apprunner.ServiceObservabilityConfigurationArgs(
            observability_enabled=False,
        ),
        source_configuration=aws.apprunner.ServiceSourceConfigurationArgs(
            authentication_configuration=aws.apprunner.ServiceSourceConfigurationAuthenticationConfigurationArgs(
                access_role_arn=access_role.arn,
            ),
            image_repository=aws.apprunner.ServiceSourceConfigurationImageRepositoryArgs(
                image_configuration=aws.apprunner.ServiceSourceConfigurationImageRepositoryImageConfigurationArgs(
                    runtime_environment_secrets={
                        "DB_URL": aurora_read_replica_db_url_parameter.arn,
                        "DB_NAME": aurora_read_replica_db_name_parameter.arn,
                        "DB_USERNAME": aurora_read_replica_db_username_parameter.arn,
                    },
                    runtime_environment_variables={
                        "DB_PORT": "5432",
                        "AWS_REGION": "eu-west-1",
                        "CDN_URL": config.require("cdn-url"),
                        "DB_SSLMODE": "require",
                    },
                ),
                image_identifier=ecr_repository.aws_ecr_repository.repository_url.apply(
                    lambda url: f"{url}:latest"
                ),
                image_repository_type="ECR",
            ),
        ),
        opts=pulumi.ResourceOptions(
            protect=False,
        ),
    )

    # role for github actions to pulumi up resources in this stack
    data_in_pipeline_load_api_github_actions_role = aws.iam.Role(
        f"{name}-{environment}-github-actions",
        assume_role_policy=json.dumps(
            {
                "Statement": [
                    {
                        "Action": "sts:AssumeRoleWithWebIdentity",
                        "Condition": {
                            "StringEquals": {
                                "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
                            },
                            "StringLike": {
                                "token.actions.githubusercontent.com:sub": "repo:climatepolicyradar/navigator-backend"
                            },
                        },
                        "Effect": "Allow",
                        "Principal": {
                            "Federated": f"arn:aws:iam::{account_id}:oidc-provider/token.actions.githubusercontent.com"
                        },
                    }
                ],
                "Version": "2012-10-17",
            }
        ),
        inline_policies=[
            {
                "name": f"{name}-{environment}-github-actions",
                "policy": json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Action": [
                                    "ecr:GetAuthorizationToken",
                                    "ecr:InitiateLayerUpload",
                                    "ecr:UploadLayerPart",
                                    "ecr:CompleteLayerUpload",
                                    "ecr:PutImage",
                                    "iam:PassRole",
                                    "ecr:DescribeRepositories",
                                    "ecr:CreateRepository",
                                    "ecr:BatchGetImage",
                                    "ecr:BatchCheckLayerAvailability",
                                    "ecr:DescribeImages",
                                    "ecr:GetDownloadUrlForLayer",
                                    "ecr:ListImages",
                                    "iam:ListAccountAliases",
                                    "iam:GetPolicy",
                                    "iam:GetRole",
                                    "acm:DescribeCertificate",
                                    "iam:PutRolePolicy",
                                    "iam:DeleteRolePolicy",
                                ],
                                "Effect": "Allow",
                                "Resource": "*",
                            }
                        ],
                    }
                ),
            }
        ],
        name=f"{name}-{environment}-github-actions",
        tags=tags,
        opts=pulumi.ResourceOptions(protect=True),
    )

    pulumi.export("role_arn", data_in_pipeline_load_api_github_actions_role.arn)
    pulumi.export("role_name", data_in_pipeline_load_api_github_actions_role.name)

pulumi.export(
    "apprunner_service_url",
    apprunner_service.service_url,
)
