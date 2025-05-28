import json

import pulumi
import pulumi_aws as aws

account_id = aws.get_caller_identity().account_id


# TODO: https://linear.app/climate-policy-radar/issue/APP-584/standardise-naming-in-infra
def generate_secret_key(project: str, aws_service: str, name: str):
    return f"/{project}/{aws_service}/{name}"


stack = pulumi.get_stack()
geographies_api_stack = pulumi.StackReference(
    f"climatepolicyradar/geographies-api/{stack}"
)
families_api_stack = pulumi.StackReference(f"climatepolicyradar/families-api/{stack}")
concepts_api_stack = pulumi.StackReference(f"climatepolicyradar/concepts-api/{stack}")

# URLs
config = pulumi.Config()
url = config.require("url")

api_route53_zone = aws.route53.Zone(url, name=url)

api_certificate = aws.acm.Certificate(
    "api-climatepolicyradar-org-cert",
    domain_name=url,
    validation_method="DNS",
    opts=pulumi.ResourceOptions(
        provider=aws.Provider("us-east-1", region="us-east-1")
    ),  # CloudFront requires certificates in us-east-1
)

# We are fine to use the first in the list as we know this will be a DNS validation from the certificate generated above
api_certificate_validation_dns_record = aws.route53.Record(
    "api-climatepolicyradar-org-cert-validation-dns-record",
    zone_id=api_route53_zone.zone_id,
    name=api_certificate.domain_validation_options[0].resource_record_name,
    type=api_certificate.domain_validation_options[0].resource_record_type,
    records=[api_certificate.domain_validation_options[0].resource_record_value],
    ttl=60,  # 1 minute
)

api_cache_policy = aws.cloudfront.CachePolicy(
    "api-climatepolicyradar-org-cache-policy",
    name="api-cache-policy",
    default_ttl=3600,
    max_ttl=86400,
    min_ttl=0,
    parameters_in_cache_key_and_forwarded_to_origin={
        "cookies_config": {
            "cookie_behavior": "none",
        },
        "headers_config": {
            "header_behavior": "none",
        },
        "query_strings_config": {
            "query_string_behavior": "all",
        },
    },
)

api_distribution = aws.cloudfront.Distribution(
    url,
    comment="API",
    origins=[
        {
            "domain_name": families_api_stack.get_output("apprunner_service_url"),
            "origin_id": "families-api-apprunner",
            "custom_origin_config": {
                "http_port": 80,
                "https_port": 443,
                "origin_protocol_policy": "https-only",
                "origin_ssl_protocols": ["TLSv1.2"],
            },
        },
        {
            "domain_name": geographies_api_stack.get_output("apprunner_service_url"),
            "origin_id": "geographies-api-apprunner",
            "custom_origin_config": {
                "http_port": 80,
                "https_port": 443,
                "origin_protocol_policy": "https-only",
                "origin_ssl_protocols": ["TLSv1.2"],
            },
        },
        {
            "domain_name": concepts_api_stack.get_output("apprunner_service_url"),
            "origin_id": "concepts-api-apprunner",
            "custom_origin_config": {
                "http_port": 80,
                "https_port": 443,
                "origin_protocol_policy": "https-only",
                "origin_ssl_protocols": ["TLSv1.2"],
            },
        },
    ],
    enabled=True,
    is_ipv6_enabled=True,
    aliases=[
        url,
    ],
    default_cache_behavior={
        "allowed_methods": [
            "HEAD",
            "GET",
            "OPTIONS",
        ],
        "cached_methods": [
            "HEAD",
            "GET",
            "OPTIONS",
        ],
        "target_origin_id": "concepts-api-apprunner",
        "viewer_protocol_policy": "redirect-to-https",
        "cache_policy_id": api_cache_policy.id,
    },
    ordered_cache_behaviors=[
        {
            "path_pattern": "/concepts/*",
            "allowed_methods": [
                "HEAD",
                "GET",
                "OPTIONS",
            ],
            "cached_methods": [
                "HEAD",
                "GET",
                "OPTIONS",
            ],
            "target_origin_id": "concepts-api-apprunner",
            "viewer_protocol_policy": "redirect-to-https",
            "cache_policy_id": api_cache_policy.id,
        },
        {
            "path_pattern": "/families/*",
            "allowed_methods": [
                "HEAD",
                "GET",
                "OPTIONS",
            ],
            "cached_methods": [
                "HEAD",
                "GET",
                "OPTIONS",
            ],
            "target_origin_id": "families-api-apprunner",
            "viewer_protocol_policy": "redirect-to-https",
            "cache_policy_id": api_cache_policy.id,
        },
        {
            "path_pattern": "/geographies/*",
            "allowed_methods": [
                "HEAD",
                "GET",
                "OPTIONS",
            ],
            "cached_methods": [
                "HEAD",
                "GET",
                "OPTIONS",
            ],
            "target_origin_id": "geographies-api-apprunner",
            "viewer_protocol_policy": "redirect-to-https",
            "cache_policy_id": api_cache_policy.id,
        },
    ],
    restrictions={
        "geo_restriction": {
            "restriction_type": "none",
        }
    },
    viewer_certificate={
        "acm_certificate_arn": api_certificate.arn,
        "ssl_support_method": "sni-only",
        "minimum_protocol_version": "TLSv1.2_2021",
    },
)


api_certificate_validation_dns_record = aws.route53.Record(
    "api-climatepolicyradar-org-alias",
    zone_id=api_route53_zone.zone_id,
    name=url,
    type="A",
    aliases=[
        {
            "name": api_distribution.domain_name,
            "zone_id": api_distribution.hosted_zone_id,
            "evaluate_target_health": False,
        }
    ],
)

# deployment
navigator_backend_github_actions_deploy = aws.iam.Role(
    "navigator-backend-github-actions-deploy",
    assume_role_policy=json.dumps(
        {
            "Statement": [
                {
                    "Action": "sts:AssumeRoleWithWebIdentity",
                    "Condition": {
                        "StringEquals": {
                            "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
                        },
                        "StringLike": {
                            "token.actions.githubusercontent.com:sub": "repo:climatepolicyradar/*"
                        },
                    },
                    "Effect": "Allow",
                    "Principal": {
                        "Federated": f"arn:aws:iam::{account_id}:oidc-provider/token.actions.githubusercontent.com"
                    },
                }
            ],
            "Version": "2012-10-17",
        }
    ),
    inline_policies=[
        {
            "name": "navigator-backend-github-actions-deploy",
            "policy": json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Action": [
                                "ecr:*",
                            ],
                            "Effect": "Allow",
                            "Resource": "*",
                        }
                    ],
                }
            ),
        }
    ],
    managed_policy_arns=["arn:aws:iam::aws:policy/AWSAppRunnerFullAccess"],
    name="navigator-backend-github-actions-deploy",
)
