import sqlalchemy as sa
from app.db.session import Base


class PhysicalDocument(Base):
    """
    A physical document.

    Representation of a document that exists in the real world.
    Owned and updated only by the pipeline.
    """

    __tablename__ = "physical_document"

    id = sa.Column(sa.Integer, primary_key=True)
    title = sa.Column(sa.Text, nullable=False)
    md5_sum = sa.Column(sa.Text, nullable=True)
    cdn_object = sa.Column(sa.Text, nullable=True)
    source_url = sa.Column(sa.Text, nullable=True)
    content_type = sa.Column(sa.Text, nullable=True)


class Language(Base):
    """
    A language used to identify the content of a document.

    Note: moved from deprecated.
    """

    __tablename__ = "language"

    id = sa.Column(sa.Integer, primary_key=True)
    language_code = sa.Column(sa.CHAR(length=3), nullable=False, unique=True)
    part1_code = sa.Column(sa.CHAR(length=2))
    part2_code = sa.Column(sa.CHAR(length=3))
    name = sa.Column(sa.Text)


class PhysicalDocumentLanguage(Base):
    """A link between a document and its languages."""

    __tablename__ = "physical_document_language"

    language_id = sa.Column(sa.ForeignKey(Language.id), nullable=False)
    document_id = sa.Column(
        sa.ForeignKey(PhysicalDocument.id, ondelete="CASCADE"),
        nullable=False,
    )

    sa.PrimaryKeyConstraint(language_id, document_id)
