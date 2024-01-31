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
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# TODO: Update to async db connection
# https://fastapi.tiangolo.com/advanced/async-sql-databases/
# async def get_session() -> AsyncSession:
#     async with async_session() as session:
#         yield session
#         await session.commit()
#
# class DB(AsyncSession):
#     def __new__(cls,db:AsyncSession = Depends(get_session)):
#         return db
#
# # in the route:
# @app.post("/my-route/")
# async def do_something(db: DB = Depends()): ...
