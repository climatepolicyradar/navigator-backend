import pulumi
import pulumi_aws as aws

# This stuff is being encapsulated in navigator-infra and we should use that once it is ready
# IAM role trusted by App Runner
data_in_pipeline_role = aws.iam.Role(
    "data-in-pipeline-role",
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
data_in_pipeline_role_policy = aws.iam.RolePolicy(
    "data-in-pipeline-role-ecr-policy",
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

data_in_pipeline_ecr_repository = aws.ecr.Repository(
    "data-in-pipeline-ecr-repository",
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
    opts=pulumi.ResourceOptions(protect=True),
)


# Export the name of the bucket
pulumi.export("ecr_repository_url", data_in_pipeline_ecr_repository.repository_url)
