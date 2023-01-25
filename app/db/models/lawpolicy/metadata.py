import sqlalchemy as sa

from app.db.models.app.users import Organisation
from .family import Family

from app.db.session import Base


class MetadataTaxonomy(Base):

    __tablename__ = "metadata_taxonomy"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.Text, nullable=False)
    description = sa.Column(sa.Text, nullable=False)
    valid_metadata = sa.Column(sa.JSON, default={})


class FamilyMetadata(Base):

    __tablename__ = "family_metadata"

    family_id = sa.Column(sa.ForeignKey(Family.id))
    taxonomy_id = sa.Column(sa.ForeignKey(MetadataTaxonomy.id))
    value = sa.Column(sa.JSON, default={})

    sa.PrimaryKeyConstraint(family_id, taxonomy_id)


class MetadataOrganisation(Base):

    __tablename__ = "metadata_organisation"

    metadata_id = sa.Column(sa.ForeignKey(MetadataTaxonomy.id))
    organisation_id = sa.Column(sa.ForeignKey(Organisation.id))

    sa.PrimaryKeyConstraint(metadata_id, organisation_id)
