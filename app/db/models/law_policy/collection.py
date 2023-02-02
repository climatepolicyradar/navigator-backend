import sqlalchemy as sa
from app.db.models.law_policy import Family
from app.db.models.app import Organisation

from app.db.session import Base


class Collection(Base):

    __tablename__ = "collection"

    id = sa.Column(sa.Integer, primary_key=True)
    title = sa.Column(sa.Text, nullable=False)
    description = sa.Column(sa.Text, nullable=False)


class CollectionFamily(Base):

    __tablename__ = "collection_family"

    collection_id = sa.Column(sa.ForeignKey(Collection.id), nullable=False)
    family_id = sa.Column(sa.ForeignKey(Family.id), nullable=False)
    sa.PrimaryKeyConstraint(collection_id, family_id)


class CollectionOrganisation(Base):

    __tablename__ = "collection_organisation"

    collection_id = sa.Column(sa.ForeignKey(Collection.id), nullable=False)
    organisation_id = sa.Column(sa.ForeignKey(Organisation.id), nullable=False)

    sa.PrimaryKeyConstraint(collection_id, organisation_id)
