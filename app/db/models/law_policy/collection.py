import sqlalchemy as sa
from app.db.models.law_policy import Family
from app.db.models.app import Organisation

from app.db.session import Base


class Collection(Base):
    """A collection of document families."""

    __tablename__ = "collection"

    import_id = sa.Column(sa.Text, primary_key=True)
    title = sa.Column(sa.Text, nullable=False)
    description = sa.Column(sa.Text, nullable=False)


class CollectionFamily(Base):
    """Relationship table connecting collections to families."""

    __tablename__ = "collection_family"

    collection_import_id = sa.Column(
        sa.ForeignKey(Collection.import_id), nullable=False
    )
    family_import_id = sa.Column(sa.ForeignKey(Family.import_id), nullable=False)
    sa.PrimaryKeyConstraint(collection_import_id, family_import_id)


class CollectionOrganisation(Base):
    """Relationship representing ownership of a collection by an organisation."""

    __tablename__ = "collection_organisation"

    collection_import_id = sa.Column(
        sa.ForeignKey(Collection.import_id), nullable=False
    )
    organisation_id = sa.Column(sa.ForeignKey(Organisation.id), nullable=False)

    # FIXME: We should only have one owning organisation for a collection, so PK should
    #        probably be just `collection_import_id`
    sa.PrimaryKeyConstraint(collection_import_id, organisation_id)
