from typing import Mapping, Optional, Sequence, cast
from sqlalchemy import Column

from sqlalchemy.orm import Session

from db_client.models import AnyModel


def has_rows(db: Session, table: AnyModel) -> bool:
    return db.query(table).count() > 0


def load_tree(
    db: Session,
    table: AnyModel,
    data_tree_list: Sequence[Mapping[str, Mapping]],
) -> None:
    """
    Load a tree of data stored as JSON into a database table

    :param [Session] db: An open database session
    :param [AnyModel] table: The table (and therefore type) of entries to create
    :param [Sequence[Mapping[str, Mapping]]] data_tree_list: A tree-list of data to load
    """
    _load_tree(db=db, table=table, data_tree_list=data_tree_list, parent_id=None)


def _load_tree(
    db: Session,
    table: AnyModel,
    data_tree_list: Sequence[Mapping[str, Mapping]],
    parent_id: Optional[int] = None,
) -> None:
    for entry in data_tree_list:
        data = entry["node"]

        parent_db_entry = table(parent_id=parent_id, **data)
        db.add(parent_db_entry)

        child_nodes = cast(Sequence[Mapping[str, Mapping]], entry["children"])
        if child_nodes:
            db.flush()
            _load_tree(db, table, child_nodes, parent_db_entry.id)


def load_list(db: Session, table: AnyModel, data_list: Sequence[Mapping]) -> None:
    """
    Load a list of data stored as JSON into a database table

    :param [Session] db: An open database session
    :param [AnyModel] table: The table (and therefore type) of entries to create
    :param [Sequence[Mapping]] data_list: A list of data objects to load
    """
    for entry in data_list:
        db.add(table(**entry))


def load_list_idempotent(
    db: Session,
    table: AnyModel,
    unique_column: Column,
    data_key: str,
    data_list: Sequence[Mapping],
) -> None:
    """
    Load a list of data stored as JSON into a database table

    :param [Session] db: An open database session
    :param [AnyModel] table: The table (and therefore type) of entries to create
    :param [Column] unique_column: The column on the table that has the unique value
    :param [str] data_key: The key in the `data_list` objects that relates to the
                            unique_column.
    :param [Sequence[Mapping]] data_list: A list of data objects to load
    """
    for entry in data_list:
        found = db.query(table).filter(unique_column == entry[data_key]).one_or_none()
        if found is None:
            db.add(table(**entry))
            db.flush()
