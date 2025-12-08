import pulumi
import pulumi_aws as aws

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
# Create the Aurora service for the Document Store.
#######################################################################
environment = pulumi.get_stack()
aws_env_stack = pulumi.StackReference(f"climatepolicyradar/aws_env/{environment}")

config = pulumi.Config()
name = pulumi.get_project()
db_name = config.require("db_name")
account_id = config.require("validation_account_id")

tags = {
    "CPR-Created-By": "pulumi",
    "CPR-Pulumi-Stack-Name": pulumi.get_stack(),
    "CPR-Pulumi-Project-Name": pulumi.get_project(),
    "CPR-Tag": f"{environment}-{name}-store",
    "Environment": environment,
}

vpc_id = aws_env_stack.get_output("vpc_id")

aurora_security_group = aws.ec2.SecurityGroup(
    f"{name}-aurora-sg",
    vpc_id=vpc_id,
    description=f"Security group for {name} Aurora DB",
    ingress=[
        aws.ec2.SecurityGroupIngressArgs(
            description="Allow PostgreSQL access",
            protocol="tcp",
            from_port=5432,
            to_port=5432,
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
    from_port=5432,
    to_port=5432,
    protocol="tcp",
    security_group_id=aurora_security_group.id,
    source_security_group_id=backend_stack.get_output("bastion_security_group_id"),
    description="Allow Postgres from bastion SG",
)

# Allow bastion SG egress to RDS SG (needed for socat tunnel)
bastion_egress_to_rds = aws.ec2.SecurityGroupRule(
    f"{name}-{environment}-bastion-egress-to-rds",
    type="egress",
    from_port=5432,
    to_port=5432,
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

min_instances = config.require("aurora_min_instances")
max_instances = config.require("aurora_max_instances")
retention_period_days = config.require("aurora_retention_period_days")
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
    backup_retention_period=int(
        retention_period_days
    ),  # Retention is included in Aurora pricing for up to 7 days. Longer retention would add charges.
    preferred_backup_window="02:00-03:00",
    iam_database_authentication_enabled=True,
    preferred_maintenance_window="sun:04:00-sun:05:00",
    deletion_protection=True,
    serverlessv2_scaling_configuration=aws.rds.ClusterServerlessv2ScalingConfigurationArgs(
        min_capacity=int(min_instances),
        max_capacity=int(max_instances),
    ),
    tags=tags,
    opts=pulumi.ResourceOptions(
        # Ignore AWS-managed and computed properties to prevent conflicts
        # after importing existing clusters. These properties may differ
        # between code definition and actual AWS state.
        ignore_changes=[
            "manage_master_user_password",
            "master_username",
            "master_password",
            "engine_version",
            "db_subnet_group_name",  # May differ if subnet group was imported separately
            "vpc_security_group_ids",  # Security group IDs may differ
            "preferred_backup_window",
            "preferred_maintenance_window",
            "serverlessv2_scaling_configuration",
        ],
    ),
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
        opts=pulumi.ResourceOptions(
            # Ignore engine version changes after import to prevent conflicts
            ignore_changes=["engine_version"],
        ),
    )
    for i in range(int(max_instances))
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
