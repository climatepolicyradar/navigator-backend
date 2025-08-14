from sqlmodel import Session, create_engine

from app.settings import settings

navigator_engine = create_engine(settings.navigator_database_url.get_secret_value())


def get_session():
    with Session(navigator_engine) as session:
        yield session
