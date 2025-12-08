from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    github_sha: str = "unknown"


# pydantic settings are set from the env variables passed in via docker / apprunner
# there is a pyright error
# @see: https://github.com/pydantic/pydantic/issues/3753
settings = Settings()  # type: ignore[call-arg] # TODO: Fix this.
