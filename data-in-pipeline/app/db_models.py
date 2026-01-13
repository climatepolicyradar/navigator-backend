from datetime import UTC, datetime

from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Document(Base):
    __tablename__ = "documents"

    id = Column(String(255), primary_key=True)
    title = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now(UTC), nullable=False)
    updated_at = Column(
        DateTime,
        default=datetime.now(UTC),
        onupdate=datetime.now(UTC),
        nullable=False,
    )

    # Table Relationships
    label_relationships = relationship(
        "DocumentLabel", back_populates="document", cascade="all, delete-orphan"
    )
    source_relationships = relationship(
        "DocumentRelationship",
        foreign_keys="DocumentRelationship.source_document_id",
        back_populates="source_document",
        cascade="all, delete-orphan",
    )
    related_relationships = relationship(
        "DocumentRelationship",
        foreign_keys="DocumentRelationship.related_document_id",
        back_populates="related_document",
    )
    items = relationship(
        "Item", back_populates="document", cascade="all, delete-orphan"
    )

    __table_args__ = (Index("idx_documents_title", "title"),)


class Label(Base):
    __tablename__ = "labels"

    id = Column(String(255), primary_key=True)
    title = Column(Text, nullable=False)
    type = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=datetime.now(UTC), nullable=False)

    # Table Relationships
    document_relationships = relationship(
        "DocumentLabel", back_populates="label", cascade="all, delete-orphan"
    )


class DocumentLabel(Base):
    __tablename__ = "document_labels"

    id = Column(Integer, primary_key=True, autoincrement=True)
    document_id = Column(
        String(255), ForeignKey("documents.id", ondelete="CASCADE"), nullable=False
    )
    label_id = Column(
        String(255), ForeignKey("labels.id", ondelete="CASCADE"), nullable=False
    )
    relationship_type = Column(String(100), nullable=False)
    timestamp = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.now(UTC), nullable=False)

    # Table Relationships
    document = relationship("Document", back_populates="label_relationships")
    label = relationship("Label", back_populates="document_relationships")

    __table_args__ = (
        Index("idx_document_labels_document_id", "document_id"),
        Index("idx_document_labels_label_id", "label_id"),
    )


class DocumentRelationship(Base):
    __tablename__ = "document_relationships"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_document_id = Column(
        String(255), ForeignKey("documents.id", ondelete="CASCADE"), nullable=False
    )
    related_document_id = Column(
        String(255), ForeignKey("documents.id", ondelete="CASCADE"), nullable=False
    )
    relationship_type = Column(String(100), nullable=False)
    timestamp = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.now(UTC), nullable=False)

    # Table Relationships
    source_document = relationship(
        "Document",
        foreign_keys=[source_document_id],
        back_populates="source_relationships",
    )
    related_document = relationship(
        "Document",
        foreign_keys=[related_document_id],
        back_populates="related_relationships",
    )

    __table_args__ = (
        Index("idx_document_relationships_source", "source_document_id"),
        Index("idx_document_relationships_related", "related_document_id"),
    )


class Item(Base):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    document_id = Column(
        String(255), ForeignKey("documents.id", ondelete="CASCADE"), nullable=False
    )
    url = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now(UTC), nullable=False)

    # Table Relationships
    document = relationship("Document", back_populates="items")

    __table_args__ = (Index("idx_items_document_id", "document_id"),)
