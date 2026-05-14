"""Shared resources for backend microservice review stacks.

Creates long-lived infrastructure that ephemeral PR review stacks depend on:
- Shared ECR repositories (one per microservice, tagged per PR)
- Shared App Runner ECR access role (avoids per-PR IAM role creation)
- OIDC IAM role for Pulumi Deployments
- ESC environments providing AWS credentials
- Deployment settings for review template stacks

Deploy once to staging; review stacks reference these outputs.
"""

import json
from typing import cast

import pulumi
import pulumi_aws as aws
import pulumi_pulumiservice as pulumiservice

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
org_name = "climatepolicyradar"
aws_account = aws.get_caller_identity()

# The microservice projects that get review stacks.
REVIEW_SERVICES = [
    "families-api",
    "geographies-api",
    "concepts-api",
    "data-in-api",
]

# ---------------------------------------------------------------------------
# OIDC Identity Provider (managed in aws_env, referenced here)
# ---------------------------------------------------------------------------
aws_env_stack = pulumi.StackReference(f"{org_name}/aws_env/staging")
oidc_provider_arn = cast(str, aws_env_stack.get_output("staging-oidc-provider-arn"))

# ---------------------------------------------------------------------------
# IAM Role for Pulumi Deployments (backend review stacks)
# ---------------------------------------------------------------------------
# This role is assumed by Pulumi Deployments and ESC environments to manage
# backend review infrastructure. The trust policy allows both:
# - Pulumi Deployments (pulumi:deploy:...) for each microservice project
# - Pulumi ESC environments (pulumi:environments:...) for dynamic credentials
deployment_role = aws.iam.Role(
    "staging-backend-pulumi-oidc-deployment-role",
    name="staging-backend-pulumi-oidc-deployment-role",
    description=(
        "Role for Pulumi Deployments and ESC to manage backend review "
        "infrastructure via OIDC."
    ),
    assume_role_policy=pulumi.Output.from_input(oidc_provider_arn).apply(
        lambda provider_arn: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Federated": provider_arn},
                        "Action": "sts:AssumeRoleWithWebIdentity",
                        "Condition": {
                            "StringLike": {
                                "api.pulumi.com/oidc:aud": [
                                    org_name,
                                    f"aws:{org_name}",
                                ],
                                "api.pulumi.com/oidc:sub": [
                                    f"pulumi:deploy:org:{org_name}:project:{svc}:*"
                                    for svc in REVIEW_SERVICES
                                ]
                                + [
                                    f"pulumi:environments:org:{org_name}:env:{svc}/*"
                                    for svc in REVIEW_SERVICES
                                ]
                                + [
                                    f"pulumi:deploy:org:{org_name}:project:backend-platform:*",
                                    f"pulumi:environments:org:{org_name}:env:backend-platform/*",
                                ],
                            },
                        },
                    }
                ],
            }
        )
    ),
    max_session_duration=3600,
)

# Attach AdministratorAccess (matches frontend-platform pattern).
aws.iam.RolePolicyAttachment(
    "staging-deployment-role-admin-policy",
    role=deployment_role.name,
    policy_arn="arn:aws:iam::aws:policy/AdministratorAccess",
)
pulumi.export("staging_deployment_role_arn", deployment_role.arn)

# ---------------------------------------------------------------------------
# Shared App Runner ECR Access Role
# ---------------------------------------------------------------------------
# A single IAM role that grants App Runner permission to pull images from ECR.
# Shared across all review stacks to avoid per-stack role creation and the
# 64-character IAM name limit.
apprunner_ecr_access_role = aws.iam.Role(
    "shared-backend-apprunner-ecr-access-role",
    name="shared-backend-apprunner-ecr-access-role",
    description=(
        "Shared role for App Runner services to pull images from ECR. "
        "Used by all backend review stacks."
    ),
    assume_role_policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "sts:AssumeRole",
                    "Effect": "Allow",
                    "Principal": {"Service": "build.apprunner.amazonaws.com"},
                },
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "tasks.apprunner.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                },
            ],
        }
    ),
    max_session_duration=3600,
)

apprunner_ecr_access_policy = aws.iam.Policy(
    "shared-backend-apprunner-ecr-access-policy",
    description="Grants ECR read access for backend App Runner review services.",
    policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "ecr:GetDownloadUrlForLayer",
                        "ecr:BatchGetImage",
                        "ecr:DescribeImages",
                        "ecr:GetAuthorizationToken",
                        "ecr:BatchCheckLayerAvailability",
                    ],
                    "Resource": "*",
                }
            ],
        }
    ),
)

aws.iam.RolePolicyAttachment(
    "shared-backend-apprunner-ecr-role-policy-attachment",
    role=apprunner_ecr_access_role.name,
    policy_arn=apprunner_ecr_access_policy.arn,
)

pulumi.export("apprunner_ecr_access_role_arn", apprunner_ecr_access_role.arn)

# ---------------------------------------------------------------------------
# Shared App Runner Instance Role for Review Stacks
# ---------------------------------------------------------------------------
# Review microservices need an instance role for SSM access, S3 access, etc.
# This shared role covers the union of permissions needed by all review services.
apprunner_instance_role = aws.iam.Role(
    "shared-backend-apprunner-instance-role",
    name="shared-backend-apprunner-instance-role",
    description=(
        "Shared instance role for backend App Runner review services. "
        "Grants SSM, S3, and RDS access needed by review microservices."
    ),
    assume_role_policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "tasks.apprunner.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    ),
    max_session_duration=3600,
)

# Grant SSM read access for all review microservice parameters.
account_id = aws_account.account_id
aws.iam.RolePolicy(
    "shared-backend-instance-role-ssm-policy",
    role=apprunner_instance_role.id,
    policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "ssm:GetParameter",
                        "ssm:GetParameters",
                        "ssm:DescribeParameters",
                    ],
                    "Resource": [
                        f"arn:aws:ssm:eu-west-1:{account_id}:parameter/{svc}/apprunner/*"
                        for svc in REVIEW_SERVICES
                    ]
                    + [
                        f"arn:aws:ssm:eu-west-1:{account_id}:parameter/data-in-pipeline/*",
                        f"arn:aws:ssm:eu-west-1:{account_id}:parameter/data-in-pipeline-load-api/*",
                        f"arn:aws:ssm:eu-west-1:{account_id}:parameter/data_in_pipeline/*",
                    ],
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "secretsmanager:GetSecretValue",
                        "secretsmanager:DescribeSecret",
                    ],
                    "Resource": f"arn:aws:secretsmanager:eu-west-1:{account_id}:secret:*",
                },
                {
                    "Effect": "Allow",
                    "Action": ["rds-db:connect"],
                    "Resource": f"arn:aws:rds-db:eu-west-1:{account_id}:dbuser:*/*",
                },
            ],
        }
    ),
)

# Grant S3 access for geographies-api review stacks.
aws.iam.RolePolicy(
    "shared-backend-instance-role-s3-policy",
    role=apprunner_instance_role.id,
    policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:PutObject",
                        "s3:PutObjectAcl",
                        "s3:GetObject",
                        "s3:DeleteObject",
                    ],
                    "Resource": "arn:aws:s3:::cpr-staging-document-cache/*",
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket"],
                    "Resource": "arn:aws:s3:::cpr-staging-document-cache",
                },
            ],
        }
    ),
)

pulumi.export("apprunner_instance_role_arn", apprunner_instance_role.arn)

# ---------------------------------------------------------------------------
# Shared ECR Repositories for Review Stacks
# ---------------------------------------------------------------------------
# One ECR repo per microservice, shared across all PR review stacks.
# Each PR pushes images tagged with the stack name to avoid collisions.
for service in REVIEW_SERVICES:
    review_ecr_repo = aws.ecr.Repository(
        f"review-{service}",
        name=f"review-{service}",
        image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
            scan_on_push=False,
        ),
        image_tag_mutability="MUTABLE",
    )
    pulumi.export(f"{service}_review_ecr_repository_url", review_ecr_repo.repository_url)

# ---------------------------------------------------------------------------
# ESC Environments
# ---------------------------------------------------------------------------
# Shared AWS credentials environment for backend review deployments.
aws_creds_env_yaml = deployment_role.arn.apply(
    lambda role_arn: (
        "values:\n"
        "  aws:\n"
        "    login:\n"
        "      fn::open::aws-login:\n"
        "        oidc:\n"
        f"          roleArn: {role_arn}\n"
        "          sessionName: pulumi-backend-review-deployments\n"
        "          duration: 1h\n"
        "  environmentVariables:\n"
        "    AWS_ACCESS_KEY_ID: ${aws.login.accessKeyId}\n"
        "    AWS_SECRET_ACCESS_KEY: ${aws.login.secretAccessKey}\n"
        "    AWS_SESSION_TOKEN: ${aws.login.sessionToken}\n"
        "    AWS_REGION: eu-west-1\n"
    )
)

# Create a shared AWS creds environment under the backend-platform project.
aws_creds_env = pulumiservice.Environment(
    "aws-creds-staging",
    organization=org_name,
    project="backend-platform",
    name="aws-creds-staging",
    yaml=aws_creds_env_yaml.apply(lambda y: pulumi.StringAsset(y)),
)

# Create review ESC environments for each microservice project.
# These import the shared AWS credentials and provide review-specific config.
for service in REVIEW_SERVICES:
    review_env_yaml = apprunner_ecr_access_role.arn.apply(
        lambda role_arn, svc=service: (
            "imports:\n"
            "  - backend-platform/aws-creds-staging\n"
            "\n"
            "values:\n"
            "  environmentVariables:\n"
            "    DEPLOY_FROM_MAIN_BRANCH_ONLY: 'false'\n"
            "    DEPLOY_TO_PROD_STACK_ALLOWED: 'false'\n"
        )
    )

    pulumiservice.Environment(
        f"{service}-review",
        organization=org_name,
        project=service,
        name="review",
        yaml=review_env_yaml.apply(lambda y: pulumi.StringAsset(y)),
        opts=pulumi.ResourceOptions(depends_on=[aws_creds_env]),
    )

# ---------------------------------------------------------------------------
# Deployment Settings for Review Template Stacks
# ---------------------------------------------------------------------------
# Each microservice gets a "review" template stack with deployment settings.
# PR stacks inherit settings from these templates.
for service in REVIEW_SERVICES:
    pulumiservice.DeploymentSettings(
        f"{service}-review-deployment-settings",
        organization=org_name,
        project=service,
        stack="review",
        source_context=pulumiservice.DeploymentSettingsSourceContextArgs(
            git=pulumiservice.DeploymentSettingsGitSourceArgs(
                branch="main",
                repo_dir=f"{service}/infra",
            ),
        ),
        vcs=pulumiservice.DeploymentSettingsVcsArgs(
            provider="github",
            repository="climatepolicyradar/navigator-backend",
            pull_request_template=False,
            deploy_commits=False,
            preview_pull_requests=False,
        ),
        operation_context=pulumiservice.DeploymentSettingsOperationContextArgs(
            options=pulumiservice.OperationContextOptionsArgs(
                skip_intermediate_deployments=True,
            ),
        ),
    )

# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------
pulumi.export("deployment_role_arn", deployment_role.arn)
