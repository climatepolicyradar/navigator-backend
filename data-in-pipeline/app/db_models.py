from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    title: Mapped[str] = mapped_column(Text)
    description: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, insert_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, insert_default=func.now(), onupdate=func.now()
    )

    # Table Relationships
    label_relationships: Mapped[list["DocumentLabel"]] = relationship(
        back_populates="document", cascade="all, delete-orphan"
    )
    source_relationships: Mapped[list["DocumentRelationship"]] = relationship(
        foreign_keys="DocumentRelationship.source_document_id",
        back_populates="source_document",
        cascade="all, delete-orphan",
    )
    related_relationships: Mapped[list["DocumentRelationship"]] = relationship(
        foreign_keys="DocumentRelationship.related_document_id",
        back_populates="related_document",
    )
    items: Mapped[list["Item"]] = relationship(
        back_populates="document", cascade="all, delete-orphan"
    )

    __table_args__ = (Index("idx_documents_title", "title"),)


class Label(Base):
    __tablename__ = "labels"

    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    title: Mapped[str] = mapped_column(Text)
    type: Mapped[str] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(DateTime, insert_default=func.now())

    # Table Relationships
    document_relationships: Mapped[list["DocumentLabel"]] = relationship(
        back_populates="label", cascade="all, delete-orphan"
    )


class DocumentLabel(Base):
    __tablename__ = "document_labels"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    document_id: Mapped[str] = mapped_column(
        String(255), ForeignKey("documents.id", ondelete="CASCADE")
    )
    label_id: Mapped[str] = mapped_column(
        String(255), ForeignKey("labels.id", ondelete="CASCADE")
    )
    relationship_type: Mapped[str] = mapped_column(String(100))
    timestamp: Mapped[datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, insert_default=func.now())

    # Table Relationships
    document: Mapped["Document"] = relationship(back_populates="label_relationships")
    label: Mapped["Label"] = relationship(back_populates="document_relationships")

    __table_args__ = (
        Index("idx_document_labels_document_id", "document_id"),
        Index("idx_document_labels_label_id", "label_id"),
    )


class DocumentRelationship(Base):
    __tablename__ = "document_relationships"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source_document_id: Mapped[str] = mapped_column(
        String(255), ForeignKey("documents.id", ondelete="CASCADE")
    )
    related_document_id: Mapped[str] = mapped_column(
        String(255), ForeignKey("documents.id", ondelete="CASCADE")
    )
    relationship_type: Mapped[str] = mapped_column(String(100))
    timestamp: Mapped[datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, insert_default=func.now())

    # Table Relationships
    source_document: Mapped["Document"] = relationship(
        foreign_keys=[source_document_id],
        back_populates="source_relationships",
    )
    related_document: Mapped["Document"] = relationship(
        foreign_keys=[related_document_id],
        back_populates="related_relationships",
    )

    __table_args__ = (
        Index("idx_document_relationships_source", "source_document_id"),
        Index("idx_document_relationships_related", "related_document_id"),
    )


class Item(Base):
    __tablename__ = "items"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    document_id: Mapped[str] = mapped_column(
        String(255), ForeignKey("documents.id", ondelete="CASCADE")
    )
    url: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, insert_default=func.now())

    # Table Relationships
    document: Mapped["Document"] = relationship(back_populates="items")

    __table_args__ = (Index("idx_items_document_id", "document_id"),)
