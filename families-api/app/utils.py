from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    navigator_database_url: str
    cdn_url: str
    github_sha: str = "unknown"


settings = Settings()  # type: ignore[call-arg] # TODO: Fix this.


def get_navigator_database_url():
    return settings.navigator_database_url


def get_cdn_url():
    return settings.cdn_url
