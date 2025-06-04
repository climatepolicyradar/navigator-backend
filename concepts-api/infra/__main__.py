import json

import pulumi
import pulumi_aws as aws

account_id = aws.get_caller_identity().account_id


# TODO: https://linear.app/climate-policy-radar/issue/APP-584/standardise-naming-in-infra
def generate_secret_key(project: str, aws_service: str, name: str):
    return f"/{project}/{aws_service}/{name}"


# This stuff is being encapsulated in navigator-infra and we should use that once it is ready
# IAM role trusted by App Runner
concepts_api_role = aws.iam.Role(
    "concepts-api-role",
    assume_role_policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "build.apprunner.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    ),
)

# Attach ECR access policy to the role
concepts_api_role_policy = aws.iam.RolePolicy(
    "concepts-api-role-ecr-policy",
    role=concepts_api_role.id,
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

concepts_api_instance_role = aws.iam.Role(
    "concepts-api-instance-role",
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
)

# Allow access to specific SSM Parameter Store secrets
concepts_api_ssm_policy = aws.iam.RolePolicy(
    "concepts-api-instance-role-ssm-policy",
    role=concepts_api_instance_role.id,
    policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["ssm:GetParameters"],
                    "Resource": [
                        f"arn:aws:ssm:eu-west-1:{account_id}:parameter/geographies-api/apprunner/*"
                    ],
                }
            ],
        }
    ),
)


concepts_api_ecr_repository = aws.ecr.Repository(
    "concepts-api-ecr-repository",
    encryption_configurations=[
        {
            "encryption_type": "AES256",
        }
    ],
    image_scanning_configuration={
        "scan_on_push": False,
    },
    image_tag_mutability="MUTABLE",
    name="concepts-api",
    opts=pulumi.ResourceOptions(protect=True),
)


concepts_api_apprunner_service = aws.apprunner.Service(
    "concepts-api-apprunner-service",
    auto_scaling_configuration_arn=f"arn:aws:apprunner:eu-west-1:{account_id}:autoscalingconfiguration/DefaultConfiguration/1/00000000000000000000000000000001",
    health_check_configuration={
        "interval": 10,
        "protocol": "TCP",
        "timeout": 5,
    },
    instance_configuration={
        "instance_role_arn": concepts_api_instance_role.arn,
    },
    network_configuration={
        "ingress_configuration": {
            "is_publicly_accessible": True,
        },
        "ip_address_type": "IPV4",
    },
    observability_configuration={
        "observability_enabled": False,
    },
    service_name="concepts-api",
    source_configuration={
        "authentication_configuration": {
            "access_role_arn": concepts_api_role.arn,
        },
        "image_repository": {
            "image_configuration": aws.apprunner.ServiceSourceConfigurationImageRepositoryImageConfigurationArgs(
                port="8080",
                runtime_environment_variables={"Environment": pulumi.get_stack()},
            ),
            "image_identifier": f"{account_id}.dkr.ecr.eu-west-1.amazonaws.com/concepts-api:latest",
            "image_repository_type": "ECR",
        },
    },
    opts=pulumi.ResourceOptions(protect=True),
)

pulumi.export("apprunner_service_url", concepts_api_apprunner_service.service_url)
