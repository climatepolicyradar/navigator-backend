# api-services-infra-shared

Shared ECS infrastructure for the API microservices that sit behind
`api.climatepolicyradar.org`.

## What's in here

This Pulumi project provisions resources that are common across all API
microservices:

- **ECS cluster** — a Fargate-only cluster (`api-services-infra-shared-<stack>`)
  with Container Insights enabled
- **Task execution role** — used by ECS to pull images from ECR, write logs to
  CloudWatch, and inject configured secrets at container startup
- **Infrastructure role** — used by ECS Express Gateway to provision and manage
  ALBs, target groups, and security groups on each service's behalf
- **Shared ALB security group** — allows inbound HTTP from the CloudFront
  origin-facing managed prefix list only, so individual services don't each need
  to recreate this rule

The project also re-exports common networking values (VPC ID, public subnet IDs,
CloudFront prefix list ID) so individual service stacks don't have to carry
these in their own configs.

## What's NOT in here

- **The CloudFront distribution** — lives in [`../api`](../api). That project
  reads each service's ingress URL via `StackReference` and composes them into a
  path-routed distribution.
- **The individual services** — each microservice (`concepts-api`,
  `families-api`, `geographies-api`, `data-in-api`, `search`, `backend-api`) is
  its own Pulumi project. They reference this stack to obtain the cluster ARN
  and other shared resources.
- **Service-specific IAM** — application-level AWS access (S3 buckets, SSM
  parameters, etc.) is defined per-service. This project only owns the roles ECS
  itself needs.
- **`admin-backend`** — uses its own cluster, since it's not part of the API
  surface. See [`../admin_backend`](../admin_backend).

## Architecture

```text
                  api.climatepolicyradar.org
                            │
                            ▼
              ┌──────────────────────────┐
              │   CloudFront (../api)    │
              └──────────────────────────┘
                            │
                  ┌─────────┼─────────┬───────┐
                  ▼         ▼         ▼       ▼
              /concepts /families /geographies /...
                  │         │         │       │
                  ▼         ▼         ▼       ▼
         ┌─────────────────────────────────────────┐
         │   Shared ECS cluster (this project)     │
         │   ┌────────┐  ┌────────┐  ┌──────────┐  │
         │   │concepts│  │families│  │geographies│ │
         │   │  task  │  │  task  │  │   task   │  │
         │   └────────┘  └────────┘  └──────────┘  │
         └─────────────────────────────────────────┘
```

Each service runs as its own ECS Express Gateway service inside the shared
cluster. The services are independent — separate Pulumi projects, separate task
definitions, separate deploys, separate scaling.

## How services reference this stack

A migrated API service references this project's outputs via `StackReference`:

```python
import pulumi

stack = pulumi.get_stack()
shared = pulumi.StackReference(f"climatepolicyradar/api-services-infra-shared/{stack}")

ecs_express_service = ExpressGatewayService(
    f"concepts-api-{stack}-ecs-express-service",
    cluster=shared.get_output("cluster_arn"),
    execution_role_arn=shared.get_output("task_execution_role_arn"),
    infrastructure_role_arn=shared.get_output("infrastructure_role_arn"),
    primary_container=primary_container,
    health_check_path="/health",
    network_configurations=[
        ExpressGatewayServiceNetworkConfigurationArgs(
            security_groups=[shared.get_output("alb_security_group_id")],
            subnets=[
                shared.get_output("vpc_public_subnet_1_id"),
                shared.get_output("vpc_public_subnet_2_id"),
                shared.get_output("vpc_public_subnet_3_id"),
            ],
        ),
    ],
    # ... service-specific config
)
```

If a service needs access to additional resources (e.g., a private RDS), it
should attach an additional service-specific SG to
`network_configurations.security_groups` alongside the shared one. Don't add
service-specific rules to the shared SG.

## Deployment

```bash
cd navigator-infra/api-services-infra-shared
pulumi stack select staging   # or production
pulumi up
```

**Deploy ordering matters:** this stack must exist before any service stack that
references it can deploy. If you `pulumi destroy` this stack, every dependent
service stack will fail until it's recreated.

## Stack config

Each stack needs the following config keys (in `Pulumi.<stack>.yaml`):

| Key                                | Example                 | Notes                                                                                                                                                      |
| ---------------------------------- | ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vpc_id`                           | `vpc-**************`    | The VPC the cluster operates in                                                                                                                            |
| `vpc_public_subnet_1_id`           | `subnet-**************` | Public subnet, eu-west-1a                                                                                                                                  |
| `vpc_public_subnet_2_id`           | `subnet-**************` | Public subnet, eu-west-1b                                                                                                                                  |
| `vpc_public_subnet_3_id`           | `subnet-**************` | Public subnet, eu-west-1c                                                                                                                                  |
| `cloudfront_origin_prefix_list_id` | `pl-**************`     | Region-specific; look up with `aws ec2 describe-managed-prefix-lists --filters Name=prefix-list-name,Values=com.amazonaws.global.cloudfront.origin-facing` |

Subnets must have a route to an Internet Gateway. Don't trust the `-public-` in
the name tag — verify with `describe-route-tables` and look for an `igw-...`
route.

## Outputs

| Output                             | Type   | Used by                                                  |
| ---------------------------------- | ------ | -------------------------------------------------------- |
| `cluster_arn`                      | string | Every API service stack                                  |
| `cluster_name`                     | string | Reference / debugging                                    |
| `task_execution_role_arn`          | string | Every API service stack                                  |
| `infrastructure_role_arn`          | string | Every API service stack                                  |
| `alb_security_group_id`            | string | Every API service stack                                  |
| `vpc_id`                           | string | Service stacks that need additional VPC-scoped resources |
| `vpc_public_subnet_*_id`           | string | Every API service stack                                  |
| `cloudfront_origin_prefix_list_id` | string | Service stacks that create additional SGs                |

## Migration context

This project was created as part of the App Runner → ECS Express migration. The
first service migrated was `admin-backend`, which has its own cluster (it's not
part of the API surface). The subsequent migrations (`geographies-api`,
`concepts-api`, `families-api`, `data-in-api`, `backend-api`) all share this
cluster.

For runbook and gotchas from the first migration, see the migration runbook and
addendum.

## Related projects

- [`../api`](../api) — CloudFront distribution and Route53 records for the API
- [`../admin_backend`](../admin_backend) — Admin backend (separate cluster)
- [`../concepts-api`](../concepts-api), [`../families-api`](../families-api),
  [`../geographies-api`](../geographies-api),
  [`../data-in-api`](../data-in-api), [`../backend-api`](../backend-api) — API
  microservices that reference this stack
