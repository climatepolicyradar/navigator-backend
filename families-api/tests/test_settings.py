from pydantic import BaseModel, computed_field

from app.settings import settings


class TestPublic(BaseModel):
    @computed_field
    @property
    def cdn_object(self) -> str:
        return f"{settings.navigator_database_url}/navigator/123.pdf"


def test_settings_hides_secrets():
    test_instance = TestPublic()
    assert (
        test_instance.model_dump().get("cdn_object") == "**********/navigator/123.pdf"
    )
