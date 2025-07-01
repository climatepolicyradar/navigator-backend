# dev
aws-ecr-login:
    aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin $(aws sts get-caller-identity --query 'Account' --output text).dkr.ecr.eu-west-1.amazonaws.com

# build/deploy
build service environment tag:
    # search for the optional prebuild step
    if just -f {{service}}/justfile --summary | grep -qe 'prebuild'; then \
        just -f {{service}}/justfile prebuild {{environment}}; \
    fi

    # @related: GITHUB_SHA_ENV_VAR
    # @related: ENV_ENV_VAR
    docker buildx build --platform linux/amd64 --output type=docker --build-arg SERVICE={{service}} --build-arg ENV={{environment}} --build-arg GITHUB_SHA=$(git rev-parse HEAD) -t {{service}}:{{tag}} -f {{service}}/Dockerfile .

deploy service environment tag:
    just build {{service}} {{environment}} {{tag}}
    docker tag {{service}}:{{tag}} $(aws sts get-caller-identity --query 'Account' --output text).dkr.ecr.eu-west-1.amazonaws.com/{{service}}:{{tag}}
    docker push $(aws sts get-caller-identity --query 'Account' --output text).dkr.ecr.eu-west-1.amazonaws.com/{{service}}:{{tag}}
