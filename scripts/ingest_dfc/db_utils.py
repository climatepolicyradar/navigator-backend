from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.core import config


engine = create_engine(
    config.SQLALCHEMY_DATABASE_URI,
    pool_pre_ping=True,
    # TODO: configure as part of scaling work
    pool_size=10,
    max_overflow=240,
)


SessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=engine, expire_on_commit=False
)
