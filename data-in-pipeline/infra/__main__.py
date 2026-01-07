from pathlib import Path

import components.aws as components_aws
import pulumi
import pulumi_aws as aws

config = pulumi.Config()
environment = pulumi.get_stack()
name = pulumi.get_project()

ROOT_DIR = Path(__file__).parent.parent

tags = {
    "CPR-Created-By": "pulumi",
    "CPR-Pulumi-Stack-Name": environment,
    "CPR-Pulumi-Project-Name": pulumi.get_project(),
    "CPR-Tag": f"{environment}-{name}-store",
    "Environment": environment,
}

#######################################################################
# Create the ECR repository for the Data In Pipeline.
#######################################################################
data_in_pipeline_aws_ecr_repository = components_aws.ecr.Repository(
    name="data-in-pipeline-ecr-repository",
    aws_ecr_repository_args=aws.ecr.RepositoryArgs(
        encryption_configurations=[
            aws.ecr.RepositoryEncryptionConfigurationArgs(
                encryption_type="AES256",
            )
        ],
        image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
            scan_on_push=False,
        ),
        image_tag_mutability="MUTABLE",
        name="data-in-pipeline",
    ),
)

#######################################################################
# Create the IAM role for the Data In Pipeline.
#######################################################################

data_in_pipeline_role = aws.iam.Role(
    "prefect-data-in-pipeline-load-aurora-role",
    name="prefect-data-in-pipeline-load-aurora-role",
    description="IAM role for Data In Pipeline Aurora",
    assume_role_policy=aws.iam.get_policy_document(
        statements=[
            aws.iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                principals=[
                    aws.iam.GetPolicyDocumentStatementPrincipalArgs(
                        type="Service",
                        identifiers=[
                            "ecs-tasks.amazonaws.com"
                        ],  # ECS task execution environment
                    )
                ],
                actions=["sts:AssumeRole"],
            )
        ]
    ).json,
    tags=tags,
)

data_in_pipeline_role_policy = aws.iam.RolePolicy(
    "data-in-pipeline-role-ecr-policy",
    name="data-in-pipeline-role-ecr-policy",
    role=data_in_pipeline_role.id,
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
pulumi.export("data_in_pipeline_role_id", data_in_pipeline_role.id)
