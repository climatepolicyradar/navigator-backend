# API Commands

build service tag environment:
    # does the service have a prebuild step?
    if just -f {{service}}/justfile --list | grep -qx prebuild; then
        just -f {{service}}/justfile prebuild {{environment}}
    fi
    # @related: GITHUB_SHA_ENV_VAR
    docker buildx build --platform linux/amd64 --output type=docker --build-arg GITHUB_SHA=$(git rev-parse HEAD) -t {{service}}:{{tag}} -f {{service}}/Dockerfile .

deploy service tag environment:
    build {{service}} {{tag}} {{environment}}
    docker tag {{service}}:{{tag}} $(aws sts get-caller-identity --query 'Account' --output text).dkr.ecr.eu-west-1.amazonaws.com/{{service}}:{{tag}}
    docker push $(aws sts get-caller-identity --query 'Account' --output text).dkr.ecr.eu-west-1.amazonaws.com/{{service}}:{{tag}}
