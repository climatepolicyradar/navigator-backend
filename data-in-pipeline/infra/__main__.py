import pulumi
import pulumi_aws as aws

# This stuff is being encapsulated in navigator-infra and we should use that once it is ready
# ECS Task Execution Role - used by Fargate to pull images from ECR and write logs.
data_in_pipeline_execution_role = aws.iam.Role(
    "data-in-pipeline-execution-role",
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

# Attach ECR access policy to the execution role.
data_in_pipeline_execution_role_ecr_policy = aws.iam.RolePolicy(
    "data-in-pipeline-execution-role-ecr-policy",
    role=data_in_pipeline_execution_role.id,
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

# Attach CloudWatch Logs policy to the execution role.
data_in_pipeline_execution_role_logs_policy = aws.iam.RolePolicy(
    "data-in-pipeline-execution-role-logs-policy",
    role=data_in_pipeline_execution_role.id,
    policy=aws.iam.get_policy_document(
        statements=[
            aws.iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                actions=[
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            )
        ]
    ).json,
)

# ECS Task Role - used by the container itself to access AWS services.
data_in_pipeline_task_role = aws.iam.Role(
    "data-in-pipeline-task-role",
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


# Export the repository URL and role ARNs
pulumi.export("ecr_repository_url", data_in_pipeline_ecr_repository.repository_url)
pulumi.export("execution_role_arn", data_in_pipeline_execution_role.arn)
pulumi.export("task_role_arn", data_in_pipeline_task_role.arn)
