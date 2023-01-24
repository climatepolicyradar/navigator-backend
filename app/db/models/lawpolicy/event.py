import sqlalchemy as sa
from app.db.models.document import PhysicalDocument
from .family import Family

from app.db.session import Base


class EventType(Base):

    __tablename__ = "event_type"

    id = sa.Column(sa.Integer, primary_key=True)
    type = sa.Column(sa.String, nullable=False)
    description = sa.Column(sa.Text, nullable=False)


class FamilyEvent(Base):

    __tablename__ = "family_event"

    id = sa.Column(sa.Integer, primary_key=True)
    family_id = sa.Column(sa.Integer, sa.ForeignKey(Family.id), nullable=False)
    event_type = sa.Column(sa.Integer, sa.ForeignKey(EventType.id), nullable=False)
    description = sa.Column(sa.Text, nullable=False)
    date = sa.Column(sa.DateTime, nullable=False)


class EventDocument(Base):

    __tablename__ = "event_document"

    family_event_id = sa.Column(
        sa.Integer, sa.ForeignKey(FamilyEvent.id), nullable=False
    )
    physical_document_id = sa.Column(
        sa.Integer, sa.ForeignKey(PhysicalDocument.id), nullable=False
    )
    sa.PrimaryKeyConstraint(family_event_id, physical_document_id)
