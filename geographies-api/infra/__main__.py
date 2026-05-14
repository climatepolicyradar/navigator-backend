import re
from typing import cast

import pulumi
import pulumi_aws as aws

account_id = aws.get_caller_identity().account_id


# TODO: https://linear.app/climate-policy-radar/issue/APP-584/standardise-naming-in-infra
def generate_secret_key(project: str, aws_service: str, name: str) -> str:
    return f"/{project}/{aws_service}/{name}"


config = pulumi.Config()
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
    platform_stack = pulumi.StackReference(
        "climatepolicyradar/backend-platform/staging"
    )
    shared_ecr_url = platform_stack.get_output(
        "geographies-api_review_ecr_repository_url"
    )
    shared_access_role_arn = platform_stack.get_output("apprunner_ecr_access_role_arn")
    shared_instance_role_arn = platform_stack.get_output("apprunner_instance_role_arn")

    ecr_auth = aws.ecr.get_authorization_token_output()
    review_image = docker_build.Image(
        "review-geographies-api-image",
        tags=[pulumi.Output.concat(shared_ecr_url, ":", stack)],
        context=docker_build.BuildContextArgs(
            location="../..",
        ),
        dockerfile=docker_build.DockerfileArgs(
            location="../../geographies-api/Dockerfile",
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

    # Review: use staging bucket and CDN values.
    geographies_api_apprunner_service = aws.apprunner.Service(
        "geographies-api-apprunner-service",
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
                    port="8080",  # @related: PORT_NUMBER
                    runtime_environment_variables={
                        "GEOGRAPHIES_BUCKET": "cpr-staging-document-cache",
                        "CDN_URL": "https://cdn.dev.climatepolicyradar.org",
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
                    resources=[
                        f"arn:aws:s3:::{config.require('geographies_bucket')}/*"
                    ],
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

pulumi.export("apprunner_service_url", geographies_api_apprunner_service.service_url)
