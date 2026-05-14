import re
from typing import cast

import pulumi
import pulumi_aws as aws

account_id = aws.get_caller_identity().account_id


# TODO: https://linear.app/climate-policy-radar/issue/APP-584/standardise-naming-in-infra
def generate_secret_key(project: str, aws_service: str, name: str) -> str:
    return f"/{project}/{aws_service}/{name}"


stack = pulumi.get_stack()

# ---------------------------------------------------------------------------
# Review stack detection
# ---------------------------------------------------------------------------
is_review_stack = stack.startswith("pr-")

if is_review_stack:
    match = re.search(r"(\d+)$", stack)
    pr_number = match.group(1) if match else stack[-8:]
else:
    pr_number = None

# ---------------------------------------------------------------------------
# Docker image build for review stacks
# ---------------------------------------------------------------------------
review_image = None
if is_review_stack:
    import pulumi_docker_build as docker_build

    # Shared resources for review stacks (from backend-platform).
    platform_stack = pulumi.StackReference("climatepolicyradar/backend-platform/staging")
    shared_ecr_url = platform_stack.get_output(
        "concepts-api_review_ecr_repository_url"
    )
    shared_access_role_arn = platform_stack.get_output("apprunner_ecr_access_role_arn")
    shared_instance_role_arn = platform_stack.get_output("apprunner_instance_role_arn")

    ecr_auth = aws.ecr.get_authorization_token_output()
    review_image = docker_build.Image(
        "review-concepts-api-image",
        tags=[pulumi.Output.concat(shared_ecr_url, ":", stack)],
        context=docker_build.BuildContextArgs(
            location="../..",
        ),
        dockerfile=docker_build.DockerfileArgs(
            location="../../concepts-api/Dockerfile",
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

    concepts_api_apprunner_service = aws.apprunner.Service(
        "concepts-api-apprunner-service",
        auto_scaling_configuration_arn=(
            f"arn:aws:apprunner:eu-west-1:{account_id}:autoscalingconfiguration"
            "/DefaultConfiguration/1/00000000000000000000000000000001"
        ),
        health_check_configuration=aws.apprunner.ServiceHealthCheckConfigurationArgs(
            interval=10,
            protocol="TCP",
            timeout=5,
        ),
        instance_configuration=aws.apprunner.ServiceInstanceConfigurationArgs(
            instance_role_arn=cast(str, shared_instance_role_arn),
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
        source_configuration=aws.apprunner.ServiceSourceConfigurationArgs(
            authentication_configuration=aws.apprunner.ServiceSourceConfigurationAuthenticationConfigurationArgs(
                access_role_arn=cast(str, shared_access_role_arn),
            ),
            image_repository=aws.apprunner.ServiceSourceConfigurationImageRepositoryArgs(
                image_configuration=aws.apprunner.ServiceSourceConfigurationImageRepositoryImageConfigurationArgs(
                    port="8080",
                    runtime_environment_variables={
                        "Environment": "staging",
                    },
                ),
                image_identifier=cast(
                    str, pulumi.Output.concat(shared_ecr_url, ":", stack)
                ),
                image_repository_type="ECR",
            ),
        ),
        tags={"Environment": "review", "PRNumber": pr_number or "unknown"},
        opts=pulumi.ResourceOptions(
            depends_on=[review_image] if review_image else [],
        ),
    )

else:
    # -----------------------------------------------------------------------
    # Non-review (staging / production) resources
    # -----------------------------------------------------------------------

    # This stuff is being encapsulated in navigator-infra and we should use that once it is ready
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

    # Attach ECR access policy to the role
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

    # Allow access to specific SSM Parameter Store secrets
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
        auto_scaling_configuration_arn=(
            f"arn:aws:apprunner:eu-west-1:{account_id}:autoscalingconfiguration"
            "/DefaultConfiguration/1/00000000000000000000000000000001"
        ),
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
                    runtime_environment_variables={
                        "Environment": pulumi.get_stack()
                    },
                ),
                image_identifier=f"{account_id}.dkr.ecr.eu-west-1.amazonaws.com/concepts-api:latest",
                image_repository_type="ECR",
            ),
        ),
        opts=pulumi.ResourceOptions(protect=True),
    )

pulumi.export("apprunner_service_url", concepts_api_apprunner_service.service_url)
