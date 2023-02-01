import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from app.db.models.app.users import Organisation
from .family import Family

from app.db.session import Base


class MetadataTaxonomy(Base):

    __tablename__ = "metadata_taxonomy"

    name = sa.Column(sa.Text, primary_key=True)
    description = sa.Column(sa.Text, nullable=False)
    valid_metadata = sa.Column(postgresql.JSONB, nullable=False)


class FamilyMetadata(Base):

    __tablename__ = "family_metadata"

    family_id = sa.Column(sa.ForeignKey(Family.id))
    taxonomy_name = sa.Column(sa.ForeignKey(MetadataTaxonomy.name))
    value = sa.Column(postgresql.JSONB, nullable=False)

    sa.PrimaryKeyConstraint(family_id, taxonomy_name)


class MetadataOrganisation(Base):

    __tablename__ = "metadata_organisation"

    taxonomy_name = sa.Column(sa.ForeignKey(MetadataTaxonomy.name))
    organisation_id = sa.Column(sa.ForeignKey(Organisation.id))

    sa.PrimaryKeyConstraint(taxonomy_name, organisation_id)
