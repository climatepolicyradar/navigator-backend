import pulumi
import pulumi_aws as aws

config = pulumi.Config()
environment = pulumi.get_stack()
name = pulumi.get_project()

#######################################################################
# Create the ECR repository for the Data In Pipeline.
#######################################################################

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
    ingress=[
        aws.ec2.SecurityGroupIngressArgs(
            description="Allow PostgreSQL access",
            protocol="tcp",
            from_port=db_port,
            to_port=db_port,
            security_groups=[],  # TODO
        )
    ],
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
    master_username=config.get("aurora_master_username"),
    db_subnet_group_name=aurora_subnet_group.name,
    vpc_security_group_ids=[aurora_security_group.id],
    backup_retention_period=retention_period_days,  # Retention is included in Aurora pricing for up to 7 days. Longer retention would add charges.
    preferred_backup_window="02:00-03:00",
    iam_database_authentication_enabled=True,
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

load_api_role = aws.iam.Role(
    "load-api-role",
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
load_api_role_policy = aws.iam.RolePolicy(
    "load-api-role-ecr-policy",
    role=load_api_role.id,
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

load_api_instance_role = aws.iam.Role(
    "load-api-instance-role",
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
load_api_ssm_policy = aws.iam.RolePolicy(
    "load-api-instance-role-ssm-policy",
    role=load_api_instance_role.id,
    policy=aws.iam.get_policy_document(
        statements=[
            aws.iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                actions=["ssm:GetParameters"],
                resources=[f"arn:aws:ssm:eu-west-1:{account_id}:parameter/load-api/*"],
            )
        ]
    ).json,
)

load_api_load_database_url = aws.ssm.Parameter(
    "load-api-load-database-url",
    name="/load-api/load-database-url",
    description="The URL string to connect to the load database",
    type=aws.ssm.ParameterType.SECURE_STRING,
    # This value is managed directly in SSM
    value=aurora_cluster.endpoint,
    opts=pulumi.ResourceOptions(
        # This value is managed directly in SSM
        ignore_changes=["value"],
    ),
)

load_api_cdn_url = aws.ssm.Parameter(
    "load-api-cdn-url",
    name="/load-api/cdn-url",
    description="Root URL of the CDN",
    type=aws.ssm.ParameterType.STRING,
    value=config.require("cdn-url"),
)

load_api_ecr_repository = aws.ecr.Repository(
    "load-api-ecr-repository",
    encryption_configurations=[
        aws.ecr.RepositoryEncryptionConfigurationArgs(
            encryption_type="AES256",
        )
    ],
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=False,
    ),
    image_tag_mutability="MUTABLE",
    name="load-api",
    opts=pulumi.ResourceOptions(protect=True),
)

load_api_vpc_sg = aws.ec2.SecurityGroup(
    "load-api-vpc-sg",
    vpc_id=vpc_id,
    egress=[
        aws.ec2.SecurityGroupEgressArgs(
            protocol="-1", from_port=0, to_port=0, cidr_blocks=["0.0.0.0/0"]
        )
    ],
)

vpc_connector = aws.apprunner.VpcConnector(
    "load-api-vpc-connector",
    vpc_connector_name="load-api-vpc-connector",
    subnets=private_subnets,
    security_groups=[load_api_vpc_sg.id],
)

# Allow Documents API connector to reach Aurora
aws.ec2.SecurityGroupRule(
    "allow-load-api-to-aurora",
    type="ingress",
    security_group_id=aurora_security_group.id,
    source_security_group_id=load_api_vpc_sg.id,
    protocol="tcp",
    from_port=5432,
    to_port=5432,
)


load_api_apprunner_service = aws.apprunner.Service(
    "load-api-apprunner-service",
    auto_scaling_configuration_arn=config.require("auto_scaling_configuration_arn"),
    health_check_configuration=aws.apprunner.ServiceHealthCheckConfigurationArgs(
        interval=10,
        protocol="TCP",
        timeout=5,
    ),
    instance_configuration=aws.apprunner.ServiceInstanceConfigurationArgs(
        instance_role_arn=load_api_instance_role.arn,
    ),
    network_configuration=aws.apprunner.ServiceNetworkConfigurationArgs(
        egress_configuration=aws.apprunner.ServiceNetworkConfigurationEgressConfigurationArgs(
            egress_type="VPC",
            vpc_connector_arn=vpc_connector.arn,
        ),
        ingress_configuration=aws.apprunner.ServiceNetworkConfigurationIngressConfigurationArgs(
            is_publicly_accessible=True,
        ),
        ip_address_type="IPV4",
    ),
    observability_configuration=aws.apprunner.ServiceObservabilityConfigurationArgs(
        observability_enabled=False,
    ),
    service_name="load-api",
    source_configuration=aws.apprunner.ServiceSourceConfigurationArgs(
        authentication_configuration=aws.apprunner.ServiceSourceConfigurationAuthenticationConfigurationArgs(
            access_role_arn=load_api_role.arn,
        ),
        image_repository=aws.apprunner.ServiceSourceConfigurationImageRepositoryArgs(
            image_configuration=aws.apprunner.ServiceSourceConfigurationImageRepositoryImageConfigurationArgs(
                runtime_environment_secrets={
                    "LOAD_DATABASE_URL": load_api_load_database_url.arn,
                    "CDN_URL": load_api_cdn_url.arn,
                },
            ),
            image_identifier=f"{account_id}.dkr.ecr.eu-west-1.amazonaws.com/load-api:latest",
            image_repository_type="ECR",
        ),
    ),
    opts=pulumi.ResourceOptions(protect=True),
)

pulumi.export("load-api-apprunner_service_url", load_api_apprunner_service.service_url)
