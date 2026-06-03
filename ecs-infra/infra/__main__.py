"""Shared ECS infrastructure for API microservices."""

import pulumi
import pulumi_aws as aws
from pulumi_aws import ecs, iam

CONFIG = pulumi.Config()
STACK = pulumi.get_stack()
PROJECT = pulumi.get_project()
NAME_PREFIX = f"api-services-{STACK}"

ACCOUNT_ID = aws.get_caller_identity().account_id

DEFAULT_TAGS = {
    "CPR-Created-By": "pulumi",
    "CPR-Pulumi-Project-Name": PROJECT,
    "CPR-Pulumi-Stack-Name": STACK,
    "Environment": STACK,
}


aws_env_stack = pulumi.StackReference(f"climatepolicyradar/aws_env/{STACK}")
vpc_id = aws_env_stack.get_output("vpc_id")
cloudfront_origin_prefix_list_id = aws_env_stack.get_output(
    "cloudfront_origin_prefix_list_id"
)

########################################################################
# Shared ECS cluster
########################################################################

cluster = ecs.Cluster(
    f"{NAME_PREFIX}-cluster",
    name=NAME_PREFIX,
    settings=[
        ecs.ClusterSettingArgs(
            name="containerInsights",
            value="enabled",
        ),
    ],
    tags=DEFAULT_TAGS,
)


########################################################################
# IAM roles shared across all services in this cluster
########################################################################

# Execution role: used by ECS itself to pull the image from ECR, write
# task logs to CloudWatch, and inject any secrets configured on the task
# definition. Per-service task roles (for application-level AWS access)
# stay in each service's own Pulumi project.
task_execution_role = iam.Role(
    f"{NAME_PREFIX}-task-execution-role",
    name=f"{NAME_PREFIX}-task-execution-role",
    assume_role_policy=iam.get_policy_document(
        statements=[
            iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                principals=[
                    iam.GetPolicyDocumentStatementPrincipalArgs(
                        type="Service",
                        identifiers=["ecs-tasks.amazonaws.com"],
                    )
                ],
                actions=["sts:AssumeRole"],
            ),
        ]
    ).json,
    managed_policy_arns=[
        "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
    ],
    tags=DEFAULT_TAGS,
)

# Infrastructure role: used by ECS Express Gateway to provision and
# manage the load balancer, target groups, and security group rules
# on the service's behalf.
infrastructure_role = iam.Role(
    f"{NAME_PREFIX}-infrastructure-role",
    name=f"{NAME_PREFIX}-infrastructure-role",
    assume_role_policy=iam.get_policy_document(
        statements=[
            iam.GetPolicyDocumentStatementArgs(
                effect="Allow",
                principals=[
                    iam.GetPolicyDocumentStatementPrincipalArgs(
                        type="Service",
                        identifiers=["ecs.amazonaws.com"],
                    )
                ],
                actions=["sts:AssumeRole"],
            ),
        ]
    ).json,
    managed_policy_arns=[
        "arn:aws:iam::aws:policy/service-role/"
        "AmazonECSInfrastructureRoleforExpressGatewayServices",
    ],
    tags=DEFAULT_TAGS,
)


########################################################################
# Shared security group for ALBs/tasks fronted by CloudFront
########################################################################

# This SG is attached to every API microservice's ExpressGatewayService.
# Because ExpressGatewayService uses the same SG for both the ALB and
# the tasks behind it, anything that needs to reach the tasks directly
# (e.g., RDS allow-listing) should reference this SG as a source.
#
# Services that need additional access (e.g. to a private RDS not
# already allow-listing this SG) can attach a second, service-specific
# SG via their own network_configurations.security_groups list.
alb_security_group = aws.ec2.SecurityGroup(
    f"{NAME_PREFIX}-alb-sg",
    name=f"{NAME_PREFIX}-alb-sg",
    description=(
        "Shared ingress SG for API microservice ALBs. "
        "Allows HTTP from CloudFront edge locations only."
    ),
    vpc_id=vpc_id,
    ingress=[
        aws.ec2.SecurityGroupIngressArgs(
            description="HTTP from CloudFront edge locations",
            from_port=80,
            to_port=80,
            protocol="tcp",
            prefix_list_ids=[cloudfront_origin_prefix_list_id],
        ),
    ],
    egress=[
        aws.ec2.SecurityGroupEgressArgs(
            description="All outbound",
            from_port=0,
            to_port=0,
            protocol="-1",
            cidr_blocks=["0.0.0.0/0"],
        ),
    ],
    tags=DEFAULT_TAGS,
)


########################################################################
# Exports — consumed by individual API microservice Pulumi projects
########################################################################

pulumi.export("cluster_arn", cluster.arn)
pulumi.export("cluster_name", cluster.name)
pulumi.export("task_execution_role_arn", task_execution_role.arn)
pulumi.export("infrastructure_role_arn", infrastructure_role.arn)
pulumi.export("alb_security_group_id", alb_security_group.id)
