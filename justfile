GITHUB_SHA := `git rev-parse HEAD`
GITHUB_REMOTE_URL := `git config --get remote.origin.url`

# dev
dev service environment="production":
    uv sync --all-packages
    just _prebuild {{service}} {{environment}}
    docker compose -f {{service}}/docker-compose.yml --profile dev up --build

# test
test service:
    docker compose -f {{service}}/docker-compose.yml --profile test up --abort-on-container-exit --exit-code-from test --remove-orphans

# build/deploy
build service environment tag:
    just _prebuild {{service}} {{environment}}

    # @related: GITHUB_SHA_ENV_VAR
    # @related: ENV_ENV_VAR
    docker buildx build \
        --platform linux/amd64 \
        --output type=docker \
        --build-arg SERVICE={{service}} \
        --build-arg ENV={{environment}} \
        --build-arg GITHUB_SHA={{GITHUB_SHA}} \
        --label org.opencontainers.image.source={{GITHUB_REMOTE_URL}} \
        --label org.opencontainers.image.revision={{GITHUB_SHA}} \
        --label org.opencontainers.image.created=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
        --tag {{service}}:{{tag}} \
        --tag {{service}}:{{GITHUB_SHA}} \
        --file {{service}}/Dockerfile .

deploy service environment tag:
    # search for any override steps
    if just -f {{service}}/justfile --summary | grep -qe 'deploy-override'; then \
        just -f {{service}}/justfile deploy-override {{service}} {{environment}} {{tag}}; \
    else \
        just build {{service}} {{environment}} {{tag}}; \
        docker tag {{service}}:{{tag}} $(aws sts get-caller-identity --query 'Account' --output text).dkr.ecr.eu-west-1.amazonaws.com/{{service}}:{{tag}}; \
        docker tag {{service}}:{{GITHUB_SHA}} $(aws sts get-caller-identity --query 'Account' --output text).dkr.ecr.eu-west-1.amazonaws.com/{{service}}:{{GITHUB_SHA}}; \
        docker push $(aws sts get-caller-identity --query 'Account' --output text).dkr.ecr.eu-west-1.amazonaws.com/{{service}}:{{tag}}; \
        docker push $(aws sts get-caller-identity --query 'Account' --output text).dkr.ecr.eu-west-1.amazonaws.com/{{service}}:{{GITHUB_SHA}}; \
    fi

# private method for accessing the optional child recipe
_prebuild service environment:
    # search for the optional prebuild step
    if just -f {{service}}/justfile --summary | grep -qe 'prebuild'; then \
        just -f {{service}}/justfile prebuild {{environment}}; \
    fi

# util
aws-ecr-login:
    aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin $(aws sts get-caller-identity --query 'Account' --output text).dkr.ecr.eu-west-1.amazonaws.com
