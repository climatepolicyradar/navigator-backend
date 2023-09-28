from enum import Enum
import sqlalchemy as sa
from sqlalchemy import PrimaryKeyConstraint

from app.db.session import Base


class AppUser(Base):
    """Table of app users in the system."""

    __tablename__ = "app_user"

    email = sa.Column(sa.String, primary_key=True, nullable=False)
    name = sa.Column(sa.String)
    hashed_password = sa.Column(sa.String)
    is_superuser = sa.Column(sa.Boolean, default=False, nullable=False)


class Organisation(Base):
    """Table of organisations to which admin users may belong."""

    __tablename__ = "organisation"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    description = sa.Column(sa.String)
    organisation_type = sa.Column(sa.String)


class OrganisationUser(Base):
    """Link table for admin and organisation."""

    __tablename__ = "organisation_admin"

    appuser_email = sa.Column(sa.String, sa.ForeignKey(AppUser.email), nullable=False)
    organisation_id = sa.Column(sa.ForeignKey(Organisation.id), nullable=False)
    job_title = sa.Column(sa.String)
    is_active = sa.Column(sa.Boolean, default=False, nullable=False)
    is_admin = sa.Column(sa.Boolean, default=False, nullable=False)

    PrimaryKeyConstraint(appuser_email, organisation_id)


class CountedEntity(str, Enum):
    """Entities that are to be counted."""

    Collection = "collection"
    Family = "family"
    Document = "document"
    Event = "event"


class OrganisationDatasource(Base):
    """
    A list of datasources (document corpuses) for an organisation.

    Example:
        Organisation LSE has a datasource CCLW and another UNFCCC
        There would be two entries in this table with different prefixes.
        The format of id to get produced would be:
            <datasource.prefix>.<entity>.<counter>.<n>

    """

    __tablename__ = "organisation_datasource"

    id = sa.Column(sa.Integer, primary_key=True)
    organisation_id = sa.Column(
        sa.Integer, sa.ForeignKey(Organisation.id), nullable=False
    )
    description = sa.Column(sa.String)
    prefix = sa.Column(sa.String)
    counter = sa.Column(sa.Integer)

    def get_import_id(self, entity: CountedEntity, n: int = 0) -> str:
        """gets an import id"""
        # TODO Read and increment counter
        return f"{self.prefix}.{entity.value}.{self.counter}.{n}"
