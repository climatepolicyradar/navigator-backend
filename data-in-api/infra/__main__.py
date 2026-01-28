"""
Infrastructure for this service is currently provisioned in ../../data-in-pipeline/infra/__main__.py
"""

import json

import pulumi
import pulumi_aws as aws

config = pulumi.Config()
environment = pulumi.get_stack()
name = pulumi.get_project()
aws_account = aws.get_caller_identity()

tags = {
    "CPR-Created-By": "pulumi",
    "CPR-Pulumi-Stack-Name": environment,
    "CPR-Pulumi-Project-Name": pulumi.get_project(),
    "CPR-Tag": f"{environment}-{name}",
    "Environment": environment,
}

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
                        "Federated": f"arn:aws:iam::{aws_account.account_id}:oidc-provider/token.actions.githubusercontent.com"
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
