from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    github_sha: str = "unknown"


# pydantic settings are set from the env variables passed in via docker / apprunner
settings = Settings()
