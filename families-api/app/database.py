from sqlmodel import Session, create_engine

from app.utils import get_navigator_database_url

navigator_engine = create_engine(get_navigator_database_url())


def get_session():
    with Session(navigator_engine) as session:
        yield session
