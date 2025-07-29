import pulumi
import pulumi_aws as aws

account_id = aws.get_caller_identity().account_id


# TODO: https://linear.app/climate-policy-radar/issue/APP-584/standardise-naming-in-infra
def generate_secret_key(project: str, aws_service: str, name: str):
    return f"/{project}/{aws_service}/{name}"


# TODO: once we get VPS info from the aws_env in navigator-infra, we should use that once it is ready
pulumi_config = pulumi.Config()
apprunner_vpc_connector_arn = pulumi_config.require("apprunner_vpc_connector_arn")
stack = pulumi.get_stack()

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
    name=generate_secret_key("families-api", "apprunner", "NAVIGATOR_DATABASE_URL"),
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


families_api_apprunner_service = aws.apprunner.Service(
    "families-api-apprunner-service",
    auto_scaling_configuration_arn=f"arn:aws:apprunner:eu-west-1:{account_id}:autoscalingconfiguration/DefaultConfiguration/1/00000000000000000000000000000001",
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
