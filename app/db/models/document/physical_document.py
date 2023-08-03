from typing import Sequence

import sqlalchemy as sa
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from app.db.models.app.enum import BaseModelEnum
from app.db.session import Base


# TODO Our current process for updating languages in the database relies on
# all the part1_code's being unique/null.
#  Thus, we should enforce null or uniqueness on part1_code's in the database.
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


class PhysicalDocument(Base):
    """
    A physical document.

    Representation of a document that exists in the real world.
    Owned and updated only by the pipeline.
    """

    __tablename__ = "physical_document"
    __allow_unmapped__ = True

    id = sa.Column(sa.Integer, primary_key=True)
    title = sa.Column(sa.Text, nullable=False)
    md5_sum = sa.Column(sa.Text, nullable=True)
    cdn_object = sa.Column(sa.Text, nullable=True)
    source_url = sa.Column(sa.Text, nullable=True)
    content_type = sa.Column(sa.Text, nullable=True)

    language_wrappers = relationship("PhysicalDocumentLanguage")

    @hybrid_property
    def languages(self) -> Sequence[Language]:
        """Uses the language_wrapper to now return the languages."""
        return [w.language for w in self.language_wrappers]


class LanguageSource(BaseModelEnum):
    """The source of where the language came from"""

    USER = "User"
    MODEL = "Model"


class PhysicalDocumentLanguage(Base):
    """A link between a document and its languages."""

    __tablename__ = "physical_document_language"

    language_id = sa.Column(sa.ForeignKey(Language.id), nullable=False)
    document_id = sa.Column(
        sa.ForeignKey(PhysicalDocument.id, ondelete="CASCADE"),
        nullable=False,
    )

    source = sa.Column(
        sa.Enum(LanguageSource), default=LanguageSource.MODEL, nullable=False
    )
    visible = sa.Column(sa.Boolean, default=False, nullable=False)
    language = relationship("Language")

    sa.PrimaryKeyConstraint(language_id, document_id)
