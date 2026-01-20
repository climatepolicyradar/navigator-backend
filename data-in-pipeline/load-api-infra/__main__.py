import pulumi
import pulumi_aws as aws

config = pulumi.Config()
environment = pulumi.get_stack()
name = pulumi.get_project()

tags = {
    "CPR-Created-By": "pulumi",
    "CPR-Pulumi-Stack-Name": environment,
    "CPR-Pulumi-Project-Name": pulumi.get_project(),
    "CPR-Tag": f"{environment}-{name}-store",
    "Environment": environment,
}


# Reference Prefect orchestrator stack for VPC and networking
orchestrator_stack = pulumi.StackReference("climatepolicyradar/prefect_mvp/prod")
prefect_vpc_id = orchestrator_stack.get_output("prefect_vpc_id")
prefect_security_group_id = orchestrator_stack.get_output("prefect_security_group_id")
prefect_subnet_id = orchestrator_stack.get_output("prefect_ecs_service_subnet_id")
prefect_subnet_ids = prefect_subnet_id.apply(lambda x: [x])

# Create a minimal App Runner service for testing private connectivity
# Uses a public nginx image that responds on port 80
data_in_load_api_apprunner_service = aws.apprunner.Service(
    "data-in-load-api-apprunner-service",
    service_name="data-in-load-api",
    source_configuration=aws.apprunner.ServiceSourceConfigurationArgs(
        auto_deployments_enabled=False,
        image_repository=aws.apprunner.ServiceSourceConfigurationImageRepositoryArgs(
            image_identifier="public.ecr.aws/nginx/nginx:stable-alpine-slim",
            image_repository_type="ECR_PUBLIC",
            image_configuration=aws.apprunner.ServiceSourceConfigurationImageRepositoryImageConfigurationArgs(
                port="80",
            ),
        ),
    ),
    health_check_configuration=aws.apprunner.ServiceHealthCheckConfigurationArgs(
        protocol="HTTP",
        path="/",
        interval=10,
        timeout=5,
    ),
    network_configuration=aws.apprunner.ServiceNetworkConfigurationArgs(
        ingress_configuration=aws.apprunner.ServiceNetworkConfigurationIngressConfigurationArgs(
            is_publicly_accessible=False,
        ),
        ip_address_type="IPV4",
    ),
    tags={**tags, "Name": "data-in-load-api"},
)

# Security group for VPC endpoint in Prefect `production` VPC
data_in_load_api_vpc_endpoint_sg = aws.ec2.SecurityGroup(
    "data-in-load-api-vpc-endpoint-sg",
    name="data-in-load-api-vpc-endpoint-sg",
    vpc_id=prefect_vpc_id,
    description="Security group for App Runner VPC endpoint",
    ingress=[
        aws.ec2.SecurityGroupIngressArgs(
            from_port=443,
            to_port=443,
            protocol="tcp",
            cidr_blocks=["10.0.0.0/16"],  # Allow from entire Prefect VPC CIDR
            description="Allow HTTPS from Prefect VPC",
        )
    ],
    egress=[
        aws.ec2.SecurityGroupEgressArgs(
            protocol="-1",
            from_port=0,
            to_port=0,
            cidr_blocks=["0.0.0.0/0"],
        )
    ],
    tags={**tags, "Name": "data-in-load-api-vpc-endpoint-sg"},
)

# Create VPC endpoint for App Runner in Prefect VPC
data_in_load_api_vpc_endpoint = aws.ec2.VpcEndpoint(
    "data-in-load-api-vpc-endpoint",
    vpc_id=prefect_vpc_id,
    service_name="com.amazonaws.eu-west-1.apprunner.requests",
    vpc_endpoint_type="Interface",
    subnet_ids=prefect_subnet_ids,
    security_group_ids=[data_in_load_api_vpc_endpoint_sg.id],
    private_dns_enabled=False,
    tags={**tags, "Name": "data-in-load-api-vpc-endpoint"},
)

# VPC Ingress Connection - links App Runner to VPC endpoint
data_in_load_api_vpc_ingress_connection = aws.apprunner.VpcIngressConnection(
    "data-in-load-api-vpc-ingress-connection",
    name="data-in-load-api-vpc-ingress-connection",
    service_arn=data_in_load_api_apprunner_service.arn,
    ingress_vpc_configuration=aws.apprunner.VpcIngressConnectionIngressVpcConfigurationArgs(
        vpc_id=prefect_vpc_id,
        vpc_endpoint_id=data_in_load_api_vpc_endpoint.id,
    ),
    tags={**tags, "Name": "data-in-load-api-vpc-ingress-connection"},
    opts=pulumi.ResourceOptions(depends_on=[data_in_load_api_vpc_endpoint]),
)

# ECR repository for the connectivity test image
# This allows Prefect ECS tasks to pull the test image
data_in_load_api_ecr_repository = aws.ecr.Repository(
    "data-in-load-api-ecr-repository",
    encryption_configurations=[
        aws.ecr.RepositoryEncryptionConfigurationArgs(
            encryption_type="AES256",
        )
    ],
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=False,
    ),
    image_tag_mutability="MUTABLE",
    name="data-in-load-api",
    tags={**tags, "Name": "data-in-load-api"},
)

pulumi.export(
    "prefect_vpc_id",
    prefect_vpc_id,
)
pulumi.export(
    "prefect_security_group_id",
    prefect_security_group_id,
)
pulumi.export(
    "prefect_subnet_id",
    prefect_subnet_id,
)

# Add cross-account permissions so production Prefect account can pull the sandbox image
# TODO: add cross-account access for production to get staging
# if environment == "staging":
#     data_in_load_api_repo_policy = aws.iam.get_policy_document(
#         statements=[
#             aws.iam.GetPolicyDocumentStatementArgs(
#                 sid="CrossAccountPermission",
#                 effect="Allow",
#                 actions=[
#                     "ecr:BatchGetImage",
#                     "ecr:GetDownloadUrlForLayer",
#                 ],
#                 principals=[
#                     aws.iam.GetPolicyDocumentStatementPrincipalArgs(
#                         type="AWS",
#                         identifiers=[f"arn:aws:iam::{prod_aws_account_id}:root"],
#                     )
#                 ],
#             ),
#         ],
#     )

#     data_in_load_api_ecr_repository_policy = aws.ecr.RepositoryPolicy(
#         "data-in-load-api-ecr-repository-policy",
#         repository=data_in_load_api_ecr_repository.name,
#         policy=data_in_load_api_repo_policy.json,
#     )
