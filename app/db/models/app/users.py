import sqlalchemy as sa
from sqlalchemy import PrimaryKeyConstraint

from app.db.session import Base


class Admin(Base):
    """Table of admin users in the system."""

    __tablename__ = "admin"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    email = sa.Column(sa.String, unique=True, index=True, nullable=False)
    password = sa.Column(sa.String)


class Organisation(Base):
    """Table of organisations to which admin users may belong."""

    __tablename__ = "organisation"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    description = sa.Column(sa.String)
    organisation_type = sa.Column(sa.String)


class AdminOrganisation(Base):
    """Link table for admin and organisation."""

    __tablename__ = "admin_organisation"

    admin_id = sa.Column(sa.Integer, sa.ForeignKey(Admin.id), nullable=False)
    organisation_id = sa.Column(sa.ForeignKey(Organisation.id), nullable=False)
    job_title = sa.Column(sa.String)

    PrimaryKeyConstraint(admin_id, organisation_id)
