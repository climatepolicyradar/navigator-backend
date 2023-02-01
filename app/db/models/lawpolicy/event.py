# TODO: Implement events

"""Events have been removed for now - as its not clear the best way to represent them.

The code below is some illustrative suggestions - however it is best to wait 
until we have some code to determine the best way to structure this.

import sqlalchemy as sa
from app.db.models.document import PhysicalDocument
from .family import Family

from app.db.session import Base


class FamilyEvent(Base):

    __tablename__ = "family_event"

    id = sa.Column(sa.Integer, primary_key=True)
    family_id = sa.Column(sa.ForeignKey(Family.id), nullable=False)
    family_document_id = sa.Column(sa.ForeignKey(FamilyDocument.physical_document_id), nullable=True)
    event_type = sa.Column(sa.ForeignKey(EventType.id), nullable=False)
    description = sa.Column(sa.Text, nullable=False)
    date = sa.Column(sa.DateTime, nullable=False)

    family = sa_orm.relationship(Family)
    family_document = sa_orm.relationship(FamilyDocument)

    @sa_orm.validates('family_document')
    def validate_family_document(self, _, field: FamilyDocument):
        if field.family_id != self.family_id:
            raise AssertionError(
                "family_document_id must refer to a document that is a member of "
                f"family with id '{self.family_id}'"
            )
        return field

class EventType(Base):

    __tablename__ = "event_type"

    id = sa.Column(sa.Integer, primary_key=True)
    type = sa.Column(sa.String, nullable=False)
    description = sa.Column(sa.Text, nullable=False)


class FamilyEvent(Base):

    __tablename__ = "family_event"

    id = sa.Column(sa.Integer, primary_key=True)
    family_id = sa.Column(sa.ForeignKey(Family.id), nullable=False)
    event_type = sa.Column(sa.ForeignKey(EventType.id), nullable=False)
    description = sa.Column(sa.Text, nullable=False)
    date = sa.Column(sa.DateTime, nullable=False)


class EventDocument(Base):

    __tablename__ = "event_document"

    family_event_id = sa.Column(
        sa.ForeignKey(FamilyEvent.id), nullable=False
    )
    physical_document_id = sa.Column(
        sa.ForeignKey(PhysicalDocument.id), nullable=False
    )
    sa.PrimaryKeyConstraint(family_event_id, physical_document_id)

"""
