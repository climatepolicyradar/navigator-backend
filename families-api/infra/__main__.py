import json

import pulumi
import pulumi_aws as aws

account_id = aws.get_caller_identity().account_id


# TODO: https://linear.app/climate-policy-radar/issue/APP-584/standardise-naming-in-infra
def generate_secret_key(project: str, aws_service: str, name: str):
    return f"/{project}/{aws_service}/{name}"


# TODO: once we get VPS info from the aws_env in navigator-infra, we should use that once it is ready
pulumi_config = pulumi.Config()
apprunner_vpc_connector_arn = pulumi_config.require("apprunner_vpc_connector_arn")

# This stuff is being encapsulated in navigator-infra and we should use that once it is ready
# IAM role trusted by App Runner
families_api_role = aws.iam.Role(
    "families-api-role",
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
families_api_role_policy = aws.iam.RolePolicy(
    "families-api-role-ecr-policy",
    role=families_api_role.id,
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

families_api_instance_role = aws.iam.Role(
    "families-api-instance-role",
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
families_api_ssm_policy = aws.iam.RolePolicy(
    "families-api-instance-role-ssm-policy",
    role=families_api_instance_role.id,
    policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["ssm:GetParameters"],
                    "Resource": [
                        f"arn:aws:ssm:eu-west-1:{account_id}:parameter/families-api/apprunner/*"
                    ],
                }
            ],
        }
    ),
)

families_api_apprunner_navigator_database_url = aws.ssm.Parameter(
    "families-api-apprunner-navigator-database-url",
    name=generate_secret_key("families-api", "apprunner", "NAVIGATOR_DATABASE_URL"),
    description="The URL string to connect to the navigator database",
    type=aws.ssm.ParameterType.SECURE_STRING,
    # This value is managed directly in SSM
    value="PLACEHOLDER",
    opts=pulumi.ResourceOptions(
        # This value is managed directly in SSM
        ignore_changes=["value"],
        # For a new invironment, you need to import a manually created parameter
        # import_=f"arn:aws:ssm:eu-west-1:{account_id}:parameter{generate_secret_key('families-api', 'apprunner', 'NAVIGATOR_DATABASE_URL')}",
    ),
)

families_api_ecr_repository = aws.ecr.Repository(
    "families-api-ecr-repository",
    encryption_configurations=[
        {
            "encryption_type": "AES256",
        }
    ],
    image_scanning_configuration={
        "scan_on_push": False,
    },
    image_tag_mutability="MUTABLE",
    name="families-api",
    opts=pulumi.ResourceOptions(protect=True),
)


families_api_apprunner_service = aws.apprunner.Service(
    "families-api-apprunner-service",
    auto_scaling_configuration_arn=f"arn:aws:apprunner:eu-west-1:{account_id}:autoscalingconfiguration/DefaultConfiguration/1/00000000000000000000000000000001",
    health_check_configuration={
        "interval": 10,
        "protocol": "TCP",
        "timeout": 5,
    },
    instance_configuration={
        "instance_role_arn": families_api_instance_role.arn,
    },
    network_configuration={
        "egress_configuration": {
            "egress_type": "VPC",
            # This is only needed because we have hidden the RDS store in a different VPC to all our other resources
            "vpc_connector_arn": apprunner_vpc_connector_arn,
        },
        "ingress_configuration": {
            "is_publicly_accessible": True,
        },
        "ip_address_type": "IPV4",
    },
    observability_configuration={
        "observability_enabled": False,
    },
    service_name="families-api",
    source_configuration={
        "authentication_configuration": {
            "access_role_arn": families_api_role.arn,
        },
        "image_repository": {
            "image_configuration": {
                "runtime_environment_secrets": {
                    "NAVIGATOR_DATABASE_URL": families_api_apprunner_navigator_database_url.arn,
                },
            },
            "image_identifier": f"{account_id}.dkr.ecr.eu-west-1.amazonaws.com/families-api:latest",
            "image_repository_type": "ECR",
        },
    },
    opts=pulumi.ResourceOptions(protect=True),
)

pulumi.export("apprunner_service_url", families_api_apprunner_service.service_url)
