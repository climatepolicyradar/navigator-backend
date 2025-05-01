"""An AWS Python Pulumi program"""

import pulumi
import pulumi_aws as aws

account_id = aws.get_caller_identity().account_id

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
    network_configuration={
        "egress_configuration": {
            "egress_type": "DEFAULT",
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
            "access_role_arn": f"arn:aws:iam::{account_id}:role/production-backend-apprunner-access-role-642db82",
        },
        "image_repository": {
            "image_configuration": {
                "runtime_environment_variables": {
                    "NAVIGATOR_DATABASE_URL": "XXX",
                },
            },
            "image_identifier": f"{account_id}.dkr.ecr.eu-west-1.amazonaws.com/families-api:latest",
            "image_repository_type": "ECR",
        },
    },
    opts=pulumi.ResourceOptions(protect=True),
)
