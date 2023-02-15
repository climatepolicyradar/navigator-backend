import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from app.db.models.app.users import Organisation
from .family import Family

from app.db.session import Base


class MetadataTaxonomy(Base):
    """A description of metadata taxonomies in terms of valid keys & values."""

    __tablename__ = "metadata_taxonomy"

    name = sa.Column(sa.Text, primary_key=True)
    description = sa.Column(sa.Text, nullable=False)
    valid_metadata = sa.Column(postgresql.JSONB, nullable=False)


class FamilyMetadata(Base):
    """A set of metadata values linked to a Family."""

    __tablename__ = "family_metadata"

    family_import_id = sa.Column(sa.ForeignKey(Family.import_id))
    taxonomy_name = sa.Column(sa.ForeignKey(MetadataTaxonomy.name))
    value = sa.Column(postgresql.JSONB, nullable=False)

    sa.PrimaryKeyConstraint(family_import_id, taxonomy_name)


class MetadataOrganisation(Base):
    """A link from an Organisation to their metadata taxonomy."""

    __tablename__ = "metadata_organisation"

    taxonomy_name = sa.Column(sa.ForeignKey(MetadataTaxonomy.name))
    organisation_id = sa.Column(sa.ForeignKey(Organisation.id))

    # FIXME: we only support 1 taxonomy->organisation, so maybe just use ord_id as PK?
    sa.PrimaryKeyConstraint(taxonomy_name, organisation_id)
