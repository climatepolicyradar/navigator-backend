from pathlib import Path

import components.aws as components_aws
import pulumi
import pulumi_aws as aws

config = pulumi.Config()
environment = pulumi.get_stack()
name = pulumi.get_project()

ROOT_DIR = Path(__file__).parent.parent

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

# Add cross account permissions so production Prefect account can pull the staging image
# to create a staging deployment in the production account. We accept this security
# tradeoff as we do not have a staging workspace in Prefect, so all deployments are
# in the production workspace.
if environment == "staging":
    prod_aws_account_id = config.get("prod_aws_account_id")
    data_in_pipeline_repo_policy = aws.iam.get_policy_document(
        statements=[
            aws.iam.GetPolicyDocumentStatementArgs(
                sid="CrossAccountPermission",
                effect="Allow",
                actions=[
                    "ecr:BatchGetImage",
                    "ecr:GetDownloadUrlForLayer",
                ],
                principals=[
                    aws.iam.GetPolicyDocumentStatementPrincipalArgs(
                        type="AWS",
                        identifiers=[f"arn:aws:iam::{prod_aws_account_id}:root"],
                    )
                ],
            ),
            aws.iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                actions=[
                    "ecr:BatchGetImage",
                    "ecr:GetDownloadUrlForLayer",
                ],
                principals=[
                    aws.iam.GetPolicyDocumentStatementPrincipalArgs(
                        type="Service",
                        identifiers=["batch.amazonaws.com"],
                    )
                ],
                conditions=[
                    aws.iam.GetPolicyDocumentStatementConditionArgs(
                        test="StringLike",
                        variable="aws:sourceArn",
                        values=[
                            f"arn:aws:batch:eu-west-1:{prod_aws_account_id}:job/*",
                        ],
                    )
                ],
            ),
        ],
    )

    # Attach the policy to the underlying aws.ecr.Repository
    data_in_pipeline_aws_ecr_repository_policy = aws.ecr.RepositoryPolicy(
        "data-in-pipeline-ecr-repository-policy",
        repository=data_in_pipeline_aws_ecr_repository.aws_ecr_repository.name,
        policy=data_in_pipeline_repo_policy.json,
    )

#######################################################################
# Create the Aurora service.
#######################################################################
aws_env_stack = pulumi.StackReference(f"climatepolicyradar/aws_env/{environment}")

db_port = 5432
db_name = config.require("db_name")
account_id = config.require("validation_account_id")

tags = {
    "CPR-Created-By": "pulumi",
    "CPR-Pulumi-Stack-Name": environment,
    "CPR-Pulumi-Project-Name": pulumi.get_project(),
    "CPR-Tag": f"{environment}-{name}-store",
    "Environment": environment,
}

vpc_id = aws_env_stack.get_output("vpc_id")

aurora_security_group = aws.ec2.SecurityGroup(
    f"{name}-aurora-sg",
    name=f"{name}-aurora-sg",
    vpc_id=vpc_id,
    description=f"Security group for {name} Aurora DB",
    # ingress rules are controlled via security groups below
    egress=[
        aws.ec2.SecurityGroupEgressArgs(
            from_port=0,
            to_port=0,
            protocol="-1",
            cidr_blocks=["0.0.0.0/0"],
        ),
    ],
    tags=tags,
)

eu_west_1a_private_subnet_id = aws_env_stack.get_output("eu_west_1a_private_subnet_id")
eu_west_1b_private_subnet_id = aws_env_stack.get_output("eu_west_1b_private_subnet_id")
eu_west_1c_private_subnet_id = aws_env_stack.get_output("eu_west_1c_private_subnet_id")
private_subnets = [
    eu_west_1a_private_subnet_id,
    eu_west_1b_private_subnet_id,
    eu_west_1c_private_subnet_id,
]

aurora_subnet_group = aws.rds.SubnetGroup(
    f"{name}-aurora-subnet-group",
    name=f"{name}-aurora-subnet-group",
    description="Subnet group for Data in Pipeline Aurora DB",
    subnet_ids=private_subnets,
    tags=tags,
)

#######################################################################
# Allow our Bastion to access the Aurora cluster.
#######################################################################

# Get backend stack reference to access bastion security group
backend_stack = pulumi.StackReference(f"climatepolicyradar/backend/{environment}")

# Allow bastion SG ingress to RDS SG
bastion_ingress_to_rds = aws.ec2.SecurityGroupRule(
    f"{name}-{environment}-bastion-ingress-to-rds",
    type="ingress",
    from_port=db_port,
    to_port=db_port,
    protocol="tcp",
    security_group_id=aurora_security_group.id,
    source_security_group_id=backend_stack.get_output("bastion_security_group_id"),
    description="Allow Postgres from bastion SG",
)

# Allow bastion SG egress to RDS SG (needed for socat tunnel)
bastion_egress_to_rds = aws.ec2.SecurityGroupRule(
    f"{name}-{environment}-bastion-egress-to-rds",
    type="egress",
    from_port=db_port,
    to_port=db_port,
    protocol="tcp",
    security_group_id=backend_stack.get_output("bastion_security_group_id"),
    source_security_group_id=aurora_security_group.id,
    description="Allow Postgres to RDS SG from bastion",
)

#######################################################################
# Create the Aurora cluster for the Data In Pipeline.
#######################################################################

cluster_name = f"{name}-{environment}-aurora-cluster"
load_db_user = config.require("load_db_user")

min_instances = int(config.require("aurora_min_instances"))
max_instances: int = int(config.require("aurora_max_instances"))
retention_period_days = int(config.require("aurora_retention_period_days"))
aurora_cluster = aws.rds.Cluster(
    cluster_name,
    cluster_identifier=cluster_name,
    engine="aurora-postgresql",
    engine_version="17.6",
    database_name=db_name,
    manage_master_user_password=True,
    master_username=config.require("aurora_master_username"),
    db_subnet_group_name=aurora_subnet_group.name,
    vpc_security_group_ids=[aurora_security_group.id],
    backup_retention_period=retention_period_days,  # Retention is included in Aurora pricing for up to 7 days. Longer retention would add charges.
    preferred_backup_window="02:00-03:00",
    iam_database_authentication_enabled=False,  # TODO: Reenable later
    preferred_maintenance_window="sun:04:00-sun:05:00",
    deletion_protection=True,
    serverlessv2_scaling_configuration=aws.rds.ClusterServerlessv2ScalingConfigurationArgs(
        min_capacity=min_instances,
        max_capacity=max_instances,
    ),
    tags=tags,
)

aurora_instances = [
    aws.rds.ClusterInstance(
        f"{name}-{environment}-aurora-instance-{i}",
        identifier=f"{name}-{environment}-aurora-instance-{i}",
        cluster_identifier=aurora_cluster.id,
        instance_class="db.serverless",
        engine=aurora_cluster.engine,
        publicly_accessible=False,
        auto_minor_version_upgrade=True,
        tags=tags,
    )
    for i in range(max_instances)
]

pulumi.export(
    f"{name}-{environment}-aurora-cluster-name", aurora_cluster.cluster_identifier
)
pulumi.export(f"{name}-{environment}-aurora-cluster-arn", aurora_cluster.arn)
pulumi.export(
    f"{name}-{environment}-aurora-cluster-resource-id",
    aurora_cluster.cluster_resource_id,
)
pulumi.export(
    f"{name}-{environment}-aurora-instance-ids",
    [instance.identifier for instance in aurora_instances],
)
pulumi.export(f"{name}-{environment}-aurora-endpoint", aurora_cluster.endpoint)
pulumi.export(
    f"{name}-{environment}-aurora-reader-endpoint", aurora_cluster.reader_endpoint
)

# Get the ARN of the secret holding the master password
# When manage_master_user_password=True, master_user_secrets contains exactly one secret
data_in_pipeline_load_api_cluster_password_secret = (
    aurora_cluster.master_user_secrets.apply(
        lambda secrets: (secrets[0] if secrets and len(secrets) == 1 else None)
    )
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

# Construct IAM policy with cluster resource ID using apply to handle Output
app_runner_connect_role_policy = aws.iam.RolePolicy(
    f"{name}-aurora-iam-connect-policy",
    name="data-in-pipeline-aurora-iam-connect-policy",
    role=data_in_pipeline_role.id,
    policy=aurora_cluster.cluster_resource_id.apply(
        lambda resource_id: aws.iam.get_policy_document(
            statements=[
                aws.iam.GetPolicyDocumentStatementArgs(
                    effect="Allow",
                    actions=["rds-db:connect"],
                    resources=[
                        f"arn:aws:rds-db:eu-west-1:{account_id}:dbuser:{resource_id}/{load_db_user}"
                    ],
                ),
                # Optional: discovery permissions
                aws.iam.GetPolicyDocumentStatementArgs(
                    effect="Allow",
                    actions=[
                        "rds:DescribeDBClusters",
                        "rds:DescribeDBInstances",
                    ],
                    resources=["*"],
                ),
            ]
        ).json
    ),
)

#######################################################################
# Create the Load API Service.
#######################################################################

data_in_pipeline_load_api_role = aws.iam.Role(
    "data-in-pipeline-load-api-role",
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
data_in_pipeline_load_api_role_policy = aws.iam.RolePolicy(
    "data-in-pipeline-load-api-role-ecr-policy",
    role=data_in_pipeline_load_api_role.id,
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

data_in_pipeline_load_api_instance_role = aws.iam.Role(
    "data-in-pipeline-load-api-instance-role",
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

data_in_pipeline_load_api_load_database_url = aws.ssm.Parameter(
    "data-in-pipeline-load-api-load-database-url",
    name="/data-in-pipeline-load-api/load-database-url",
    description="The URL string to connect to the load database",
    type=aws.ssm.ParameterType.SECURE_STRING,
    # This value is managed directly in SSM
    value=aurora_cluster.endpoint,
    opts=pulumi.ResourceOptions(
        # This value is managed directly in SSM
        ignore_changes=["value"],
    ),
)

data_in_pipeline_load_api_cdn_url = aws.ssm.Parameter(
    "data-in-pipeline-load-api-cdn-url",
    name="/data-in-pipeline-load-api/cdn-url",
    description="Root URL of the CDN",
    type=aws.ssm.ParameterType.STRING,
    value=config.require("cdn-url"),
)

# Allow access to SSM Parameter Store and Secrets Manager
data_in_pipeline_load_api_instance_role_policy = aws.iam.RolePolicy(
    "data-in-pipeline-load-api-instance-role-policy",
    role=data_in_pipeline_load_api_instance_role.id,
    policy=pulumi.Output.all(
        data_in_pipeline_load_api_load_database_url.arn,
        data_in_pipeline_load_api_cdn_url.arn,
        data_in_pipeline_load_api_cluster_password_secret.secret_arn,
    ).apply(
        lambda args: aws.iam.get_policy_document(
            statements=[
                aws.iam.GetPolicyDocumentStatementArgs(
                    effect="Allow",
                    actions=[
                        "ssm:GetParameter",
                        "ssm:GetParameters",
                        "ssm:DescribeParameters",
                    ],
                    resources=[
                        f"arn:aws:ssm:eu-west-1:{account_id}:parameter/data-in-pipeline-load-api/*",
                        f"arn:aws:ssm:eu-west-1:{account_id}:parameter/data_in_pipeline/*",
                    ],
                ),
                aws.iam.GetPolicyDocumentStatementArgs(
                    effect="Allow",
                    actions=[
                        "secretsmanager:GetSecretValue",
                        "secretsmanager:DescribeSecret",
                    ],
                    resources=args,
                ),
            ]
        ).json
    ),
)


data_in_pipeline_load_api_ecr_repository = aws.ecr.Repository(
    "data-in-pipeline-load-api-ecr-repository",
    encryption_configurations=[
        aws.ecr.RepositoryEncryptionConfigurationArgs(
            encryption_type="AES256",
        )
    ],
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=False,
    ),
    image_tag_mutability="MUTABLE",
    name="data-in-pipeline-load-api",
    opts=pulumi.ResourceOptions(protect=True),
)

data_in_pipeline_load_api_vpc_sg = aws.ec2.SecurityGroup(
    "data-in-pipeline-load-api-vpc-sg",
    vpc_id=vpc_id,
    egress=[
        aws.ec2.SecurityGroupEgressArgs(
            protocol="-1", from_port=0, to_port=0, cidr_blocks=["0.0.0.0/0"]
        )
    ],
)

vpc_connector = aws.apprunner.VpcConnector(
    "data-in-pipeline-load-api-vpc-connector",
    vpc_connector_name="data-in-pipeline-load-api-vpc-connector",
    subnets=private_subnets,
    security_groups=[data_in_pipeline_load_api_vpc_sg.id],
)

# Security group for VPC endpoint (ingress) - allows traffic from Prefect ECS tasks
vpc_endpoint_sg = aws.ec2.SecurityGroup(
    "data-in-pipeline-load-api-vpc-endpoint-sg",
    vpc_id=vpc_id,
    description="Security group for App Runner VPC endpoint ingress",
    ingress=[
        aws.ec2.SecurityGroupIngressArgs(
            from_port=443,
            to_port=443,
            protocol="tcp",
            cidr_blocks=["10.0.0.0/8"],  # Allow from VPC CIDR - adjust if needed
            description="Allow HTTPS from VPC",
        )
    ],
    egress=[
        aws.ec2.SecurityGroupEgressArgs(
            protocol="-1", from_port=0, to_port=0, cidr_blocks=["0.0.0.0/0"]
        )
    ],
    tags=tags,
)

# VPC endpoint for App Runner (PrivateLink) - enables private access to App Runner service
# Note: App Runner's VPC endpoint service does not support private DNS, so we disable it
# The VPC Ingress Connection will still provide a domain name for accessing the service
vpc_endpoint = aws.ec2.VpcEndpoint(
    "data-in-pipeline-load-api-vpc-endpoint",
    vpc_id=vpc_id,
    service_name="com.amazonaws.eu-west-1.apprunner.requests",
    vpc_endpoint_type="Interface",
    subnet_ids=private_subnets,
    security_group_ids=[vpc_endpoint_sg.id],
    private_dns_enabled=False,  # App Runner service doesn't support private DNS
    tags=tags,
)

# Allow load API connector to reach Aurora
allow_data_in_pipeline_load_api_to_aurora = aws.ec2.SecurityGroupRule(
    "allow-data-in-pipeline-load-api-to-aurora",
    type="ingress",
    security_group_id=aurora_security_group.id,
    source_security_group_id=data_in_pipeline_load_api_vpc_sg.id,
    protocol="tcp",
    from_port=5432,
    to_port=5432,
    description="Allow Postgres from load API VPC SG",
)


data_in_pipeline_load_api_apprunner_service = aws.apprunner.Service(
    "data-in-pipeline-load-api-apprunner-service",
    auto_scaling_configuration_arn=config.require("auto_scaling_configuration_arn"),
    health_check_configuration=aws.apprunner.ServiceHealthCheckConfigurationArgs(
        interval=10,
        protocol="HTTP",
        path="/load/health",
        timeout=5,
    ),
    instance_configuration=aws.apprunner.ServiceInstanceConfigurationArgs(
        instance_role_arn=data_in_pipeline_load_api_instance_role.arn,
    ),
    network_configuration=aws.apprunner.ServiceNetworkConfigurationArgs(
        egress_configuration=aws.apprunner.ServiceNetworkConfigurationEgressConfigurationArgs(
            egress_type="VPC",
            vpc_connector_arn=vpc_connector.arn,
        ),
        ingress_configuration=aws.apprunner.ServiceNetworkConfigurationIngressConfigurationArgs(
            # Private endpoint - ingress only. Egress via VPC connector (above) is separate
            # and will continue to allow Aurora connectivity via existing security group rules.
            # If external access is needed, create a VPC Ingress Connection (PrivateLink).
            is_publicly_accessible=False,
        ),
        ip_address_type="IPV4",
    ),
    observability_configuration=aws.apprunner.ServiceObservabilityConfigurationArgs(
        observability_enabled=False,
    ),
    service_name="data-in-pipeline-load-api",
    source_configuration=aws.apprunner.ServiceSourceConfigurationArgs(
        authentication_configuration=aws.apprunner.ServiceSourceConfigurationAuthenticationConfigurationArgs(
            access_role_arn=data_in_pipeline_load_api_role.arn,
        ),
        image_repository=aws.apprunner.ServiceSourceConfigurationImageRepositoryArgs(
            image_configuration=aws.apprunner.ServiceSourceConfigurationImageRepositoryImageConfigurationArgs(
                runtime_environment_secrets={
                    "LOAD_DATABASE_URL": data_in_pipeline_load_api_load_database_url.arn,
                    "CDN_URL": data_in_pipeline_load_api_cdn_url.arn,
                    "MANAGED_DB_PASSWORD": data_in_pipeline_load_api_cluster_password_secret.secret_arn,
                },
                runtime_environment_variables={
                    "DB_MASTER_USERNAME": config.require("aurora_master_username"),
                    "DB_PORT": "5432",
                    "DB_NAME": config.require("db_name"),
                    "AWS_REGION": "eu-west-1",
                },
            ),
            image_identifier=f"{account_id}.dkr.ecr.eu-west-1.amazonaws.com/data-in-pipeline-load-api:latest",
            image_repository_type="ECR",
        ),
    ),
    opts=pulumi.ResourceOptions(protect=True),
)

# VPC Ingress Connection - links App Runner service to VPC endpoint for private access
vpc_ingress_connection = aws.apprunner.VpcIngressConnection(
    "data-in-pipeline-load-api-vpc-ingress-connection",
    name=f"data-in-pipeline-load-api-vic-{environment}",
    service_arn=data_in_pipeline_load_api_apprunner_service.arn,
    ingress_vpc_configuration=aws.apprunner.VpcIngressConnectionIngressVpcConfigurationArgs(
        vpc_id=vpc_id,
        vpc_endpoint_id=vpc_endpoint.id,
    ),
    tags=tags,
)

data_in_pipeline_load_api_url = aws.ssm.Parameter(
    "data-in-pipeline-load-api-url",
    name="/data-in-pipeline-load-api/url",
    description="URL of the load API service (via VPC Ingress Connection)",
    type=aws.ssm.ParameterType.STRING,
    # Use VPC Ingress Connection domain name for private access
    value=vpc_ingress_connection.domain_name.apply(lambda d: f"https://{d}"),
)

# Service URL is not available when is_publicly_accessible=False
# Use VPC Ingress Connection domain instead (exported below)
pulumi.export(
    "data-in-pipeline-load-api-vpc-ingress-connection-domain",
    vpc_ingress_connection.domain_name,
)

#######################################################################
# Create environment variables and secrets for Prefect flows/tasks.
#######################################################################

# Export environment variables (plain values)
prefect_otel_endpoint = f"https://otel.{"prod" if environment == "production" else "staging"}.climatepolicyradar.org"
pulumi.export(
    "prefect_runtime_environment_variables",
    {
        "API_BASE_URL": config.require("api_base_url"),
        "DISABLE_OTEL_LOGGING": config.require("disable_otel_logging"),
        "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
        "OTEL_EXPORTER_OTLP_ENDPOINT": prefect_otel_endpoint,
        "OTEL_PYTHON_LOGGER_PROVIDER": "sdk",
        "OTEL_PYTHON_LOG_CORRELATION": True,
        "OTEL_PYTHON_LOG_LEVEL": config.require("otel_python_log_level"),
        "OTEL_RESOURCE_ATTRIBUTES": f"deployment.environment={environment},service.namespace=data-fetching",
        "OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED": True,
        "PREFECT_CLOUD_ENABLE_ORCHESTRATION_TELEMETRY": True,
        "PREFECT_LOGGING_TO_API_ENABLED": True,
        "PREFECT_LOGGING_EXTRA_LOGGERS": "app",
    },
)

# Export secrets (ARNs) - these can be linked to Secrets Manager secrets
prefect_secrets_arns_output = pulumi.Output.from_input(
    {
        "MANAGED_DB_PASSWORD": data_in_pipeline_load_api_cluster_password_secret.secret_arn,
    }
)
pulumi.export("prefect_secrets_arns", prefect_secrets_arns_output)

# Create SSM parameter for Aurora writer endpoint (for Prefect flows)
data_in_pipeline_aurora_writer_endpoint = aws.ssm.Parameter(
    "data-in-pipeline-aurora-writer-endpoint",
    name="/data-in-pipeline/aurora-writer-endpoint",
    description="Aurora cluster writer endpoint for Prefect flows",
    type=aws.ssm.ParameterType.SECURE_STRING,
    value=aurora_cluster.endpoint,
    tags=tags,
)

# Export SSM Parameter ARNs for secrets that should be referenced via SSM
# These are for SSM SecureString parameters that should be used as secrets
prefect_ssm_secrets_output = pulumi.Output.from_input(
    {
        "AURORA_WRITER_ENDPOINT": data_in_pipeline_aurora_writer_endpoint.arn,
    }
)
pulumi.export("prefect_ssm_secrets", prefect_ssm_secrets_output)

# Update IAM role to allow access to Prefect SSM parameters and secrets
# Note: ECS tasks require the task execution role (not task role) to have permissions
# to access secrets. Prefect ECS worker may use a separate execution role.
# This policy is attached to the task role for application-level access.
# If Prefect uses a different execution role, ensure it has similar permissions.
prefect_secrets_access_policy = aws.iam.RolePolicy(
    "data-in-pipeline-prefect-secrets-access-policy",
    role=data_in_pipeline_role.id,
    policy=pulumi.Output.all(
        prefect_secrets_arns_output, prefect_ssm_secrets_output
    ).apply(
        lambda args: aws.iam.get_policy_document(
            statements=[
                aws.iam.GetPolicyDocumentStatementArgs(
                    effect="Allow",
                    actions=[
                        "ssm:GetParameter",
                        "ssm:GetParameters",
                        "ssm:DescribeParameters",
                    ],
                    resources=[
                        f"arn:aws:ssm:eu-west-1:{account_id}:parameter/data-in-pipeline*",
                    ],
                ),
            ]
            + (
                [
                    aws.iam.GetPolicyDocumentStatementArgs(
                        effect="Allow",
                        actions=[
                            "secretsmanager:GetSecretValue",
                            "secretsmanager:DescribeSecret",
                        ],
                        resources=list(args[0].values()),
                    )
                ]
                if args[0] and len(args[0]) > 0
                else []
            )
        ).json
    ),
)
