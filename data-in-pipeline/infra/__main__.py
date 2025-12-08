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
# Create the Aurora service.
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

cluster_name = f"{name}-{environment}-aurora-cluster"
load_db_user = config.require("load_db_user")

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
    backup_retention_period=7,  # Retention is included in Aurora pricing for up to 7 days. Longer retention would add charges.
    preferred_backup_window="02:00-03:00",
    iam_database_authentication_enabled=True,
    preferred_maintenance_window="sun:04:00-sun:05:00",
    deletion_protection=True,
    serverlessv2_scaling_configuration=aws.rds.ClusterServerlessv2ScalingConfigurationArgs(
        min_capacity=0,
        max_capacity=2,
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
    for i in range(2)
]


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

app_runner_connect_role_policy = aws.iam.RolePolicy(
    f"{name}-aurora-iam-connect-policy",
    role=data_in_pipeline_role.id,
    policy=aws.iam.get_policy_document(
        statements=[
            aws.iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                actions=["rds-db:connect"],
                resources=[
                    f"arn:aws:rds-db:eu-west-1:{account_id}:dbuser:{aurora_cluster.cluster_resource_id}/{load_db_user}"
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
    ).json,
)


pulumi.export(f"{name}-{environment}-aurora-cluster-name", aurora_cluster._name)
pulumi.export(f"{name}-{environment}-aurora-cluster-arn", aurora_cluster.arn)
pulumi.export(
    f"{name}-{environment}-aurora-cluster-resource-id",
    aurora_cluster.cluster_resource_id,
)
pulumi.export(
    f"{name}-{environment}-aurora-instance-ids",
    [instance.id for instance in aurora_instances],
)
pulumi.export(f"{name}-{environment}-aurora-endpoint", aurora_cluster.endpoint)
pulumi.export(
    f"{name}-{environment}-aurora-reader-endpoint", aurora_cluster.reader_endpoint
)
