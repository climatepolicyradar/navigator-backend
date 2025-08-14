from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    github_sha: str = "unknown"  # @related: GITHUB_SHA_ENV_VAR


settings = Settings()  # type: ignore[call-arg] # TODO: Fix this.
