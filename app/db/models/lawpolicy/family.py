import sqlalchemy as sa
from app.db.models.lawpolicy.geography import Geography
from app.db.session import Base


class FamilyCategory(Base):
    """A document-family category

    Currently:
        Policy, (executive)
        Law, (legislative)
        Case, (litigation)
    """

    __tablename__ = "family_category"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.Text, nullable=False, unique=True)
    description = sa.Column(sa.Text, nullable=False)


class FamilyType(Base):
    """A family type.

    E.g. Decree, Act, Strategy, Policy. Programme, Law etc
    """

    __tablename__ = "family_type"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.Text, nullable=False, unique=True)
    description = sa.Column(sa.Text, nullable=False)


class Family(Base):

    __tablename__ = "family"

    id = sa.Column(sa.Integer, primary_key=True)
    title = sa.Column(sa.String, nullable=False)
    import_id = sa.Column(sa.Integer)
    description = sa.Column(sa.String, nullable=False)
    geography_id = sa.Column(sa.Integer, sa.ForeignKey(Geography.id, nullable=False))
    category_id = sa.Column(
        sa.Integer, sa.ForeignKey(FamilyCategory.id, nullable=False)
    )
    family_type_id = sa.Column(sa.Integer, sa.ForeignKey(FamilyType.id, nullable=False))


class DocumentFamily(Base):
    __tablename__ = "family"
