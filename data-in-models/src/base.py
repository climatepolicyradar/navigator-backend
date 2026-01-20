from sqlalchemy.ext.declarative import declarative_base
from sqlmodel import SQLModel

# Declarative base object
Base = declarative_base(metadata=SQLModel.metadata)

# Aliased type annotation useful for type hints
AnyModel = Base
