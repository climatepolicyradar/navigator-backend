import re
from typing import cast

import pulumi
import pulumi_aws as aws
import pulumi_docker_build as docker_build

account_id = aws.get_caller_identity().account_id


# TODO: https://linear.app/climate-policy-radar/issue/APP-584/standardise-naming-in-infra
def generate_secret_key(project: str, aws_service: str, name: str) -> str:
    return f"/{project}/{aws_service}/{name}"


pulumi_config = pulumi.Config()
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
# Shared resources for review stacks (from backend-platform)
# ---------------------------------------------------------------------------
platform_stack = pulumi.StackReference("climatepolicyradar/backend-platform/staging")

# ---------------------------------------------------------------------------
# Docker image build for review stacks
# ---------------------------------------------------------------------------
review_image: docker_build.Image | None = None
if is_review_stack:
    shared_ecr_url = platform_stack.get_output("families-api_review_ecr_repository_url")
    shared_access_role_arn = platform_stack.get_output("apprunner_ecr_access_role_arn")
    shared_instance_role_arn = platform_stack.get_output("apprunner_instance_role_arn")

    ecr_auth = aws.ecr.get_authorization_token_output()
    review_image = docker_build.Image(
        "review-families-api-image",
        tags=[pulumi.Output.concat(shared_ecr_url, ":", stack)],
        context=docker_build.BuildContextArgs(
            location="../..",
        ),
        dockerfile=docker_build.DockerfileArgs(
            location="../../families-api/Dockerfile",
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

    # Review: use shared roles and ECR from backend-platform.
    # Point at staging SSM params (the existing ones created by the staging stack).
    staging_db_url_arn = (
        f"arn:aws:ssm:eu-west-1:{account_id}:parameter"
        f"{generate_secret_key('families-api', 'apprunner', 'NAVIGATOR_DATABASE_URL')}"
    )
    staging_cdn_url_arn = (
        f"arn:aws:ssm:eu-west-1:{account_id}:parameter"
        f"{generate_secret_key('families-api', 'apprunner', 'CDN_URL')}"
    )

    families_api_apprunner_service = aws.apprunner.Service(
        "families-api-apprunner-service",
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
                    runtime_environment_secrets={
                        "NAVIGATOR_DATABASE_URL": staging_db_url_arn,
                        "CDN_URL": staging_cdn_url_arn,
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
    apprunner_vpc_connector_arn = pulumi_config.require("apprunner_vpc_connector_arn")

    # This stuff is being encapsulated in navigator-infra and we should use that once it is ready
    # IAM role trusted by App Runner
    families_api_role = aws.iam.Role(
        "families-api-role",
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
    families_api_role_policy = aws.iam.RolePolicy(
        "families-api-role-ecr-policy",
        role=families_api_role.id,
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

    families_api_instance_role = aws.iam.Role(
        "families-api-instance-role",
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
    families_api_ssm_policy = aws.iam.RolePolicy(
        "families-api-instance-role-ssm-policy",
        role=families_api_instance_role.id,
        policy=aws.iam.get_policy_document(
            statements=[
                aws.iam.GetPolicyDocumentStatementArgs(
                    effect="Allow",
                    actions=["ssm:GetParameters"],
                    resources=[
                        f"arn:aws:ssm:eu-west-1:{account_id}:parameter/families-api/apprunner/*"
                    ],
                )
            ]
        ).json,
    )

    families_api_apprunner_navigator_database_url = aws.ssm.Parameter(
        "families-api-apprunner-navigator-database-url",
        name=generate_secret_key(
            "families-api", "apprunner", "NAVIGATOR_DATABASE_URL"
        ),
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

    families_api_apprunner_cdn_url = aws.ssm.Parameter(
        "families-api-apprunner-cdn-url",
        name=generate_secret_key("families-api", "apprunner", "CDN_URL"),
        description="Root URL of the CDN",
        type=aws.ssm.ParameterType.STRING,
        # TODO: we could look this up based on the stack - but this is easy to change for now
        value=(
            "https://cdn.climatepolicyradar.org"
            if stack == "production"
            else "https://cdn.dev.climatepolicyradar.org"
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

    families_api_apprunner_autoscaling_configuration = (
        aws.apprunner.AutoScalingConfigurationVersion(
            # len(name) < 32
            "families-api-autoscaling-config",
            auto_scaling_configuration_name="families-api-autoscaling-config",
            max_concurrency=10,
            max_size=10,
            min_size=2 if stack == "production" else 1,
        )
    )

    families_api_apprunner_service = aws.apprunner.Service(
        "families-api-apprunner-service",
        auto_scaling_configuration_arn=families_api_apprunner_autoscaling_configuration.arn,
        health_check_configuration=aws.apprunner.ServiceHealthCheckConfigurationArgs(
            interval=10,
            protocol="TCP",
            timeout=5,
        ),
        instance_configuration=aws.apprunner.ServiceInstanceConfigurationArgs(
            instance_role_arn=families_api_instance_role.arn,
        ),
        network_configuration=aws.apprunner.ServiceNetworkConfigurationArgs(
            egress_configuration=aws.apprunner.ServiceNetworkConfigurationEgressConfigurationArgs(
                egress_type="VPC",
                # This is only needed because we have hidden the RDS store in a different VPC to all our other resources
                vpc_connector_arn=apprunner_vpc_connector_arn,
            ),
            ingress_configuration=aws.apprunner.ServiceNetworkConfigurationIngressConfigurationArgs(
                is_publicly_accessible=True,
            ),
            ip_address_type="IPV4",
        ),
        observability_configuration=aws.apprunner.ServiceObservabilityConfigurationArgs(
            observability_enabled=False,
        ),
        service_name="families-api",
        source_configuration=aws.apprunner.ServiceSourceConfigurationArgs(
            authentication_configuration=aws.apprunner.ServiceSourceConfigurationAuthenticationConfigurationArgs(
                access_role_arn=families_api_role.arn,
            ),
            image_repository=aws.apprunner.ServiceSourceConfigurationImageRepositoryArgs(
                image_configuration=aws.apprunner.ServiceSourceConfigurationImageRepositoryImageConfigurationArgs(
                    runtime_environment_secrets={
                        "NAVIGATOR_DATABASE_URL": families_api_apprunner_navigator_database_url.arn,
                        "CDN_URL": families_api_apprunner_cdn_url.arn,
                    },
                ),
                image_identifier=f"{account_id}.dkr.ecr.eu-west-1.amazonaws.com/families-api:latest",
                image_repository_type="ECR",
            ),
        ),
        opts=pulumi.ResourceOptions(protect=True),
    )

pulumi.export("apprunner_service_url", families_api_apprunner_service.service_url)
