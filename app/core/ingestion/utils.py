import abc
from dataclasses import dataclass
import enum
from typing import Any, Callable, Optional, TypeVar, cast
from app.db.session import AnyModel
from sqlalchemy.orm import Session


_DbModel = TypeVar("_DbModel", bound=AnyModel)


def create(db: Session, model: _DbModel, **kwargs) -> _DbModel:
    """
    Creates a row represented by model, and described by kwargs.

    :param [Session] db: connection to the database.
    :param [_DbModel] model: the model (table) you are querying.
    :param kwargs: a list of attributes to describe the row you are interested in.
        - if kwargs contains an `extra` key then this will be used during
        creation.
        - if kwargs contains an `after_create` key then the value should
        be a callback function that is called after an object is created.

    :return [_DbModel]: The object that was either created or retrieved, or None
    """
    extra, after_create = _vars_from_kwargs(kwargs)

    return _create_instance(db, extra, after_create, model, **kwargs)


def get_or_create(db: Session, model: _DbModel, **kwargs) -> _DbModel:
    """
    Get or create a row represented by model, and described by kwargs.

    :param [Session] db: connection to the database.
    :param [_DbModel] model: the model (table) you are querying.
    :param kwargs: a list of attributes to describe the row you are interested in.
        - if kwargs contains an `extra` key then this will be used during
        creation.
        - if kwargs contains an `after_create` key then the value should
        be a callback function that is called after an object is created.

    :return [_DbModel]: The object that was either created or retrieved, or None
    """
    extra, after_create = _vars_from_kwargs(kwargs)

    instance = db.query(model).filter_by(**kwargs).one_or_none()

    if instance is not None:
        return instance

    return _create_instance(db, extra, after_create, model, **kwargs)


def _create_instance(
    db: Session,
    extra: dict,
    after_create: Optional[Callable],
    model: AnyModel,
    **kwargs,
):
    # Add the extra args in for creation
    for k, v in extra.items():
        kwargs[k] = v
    instance = model(**kwargs)
    db.add(instance)
    db.flush()
    if after_create:
        after_create(instance)
    return instance


def _vars_from_kwargs(kwargs: dict[str, Any]) -> tuple[dict, Optional[Callable]]:
    extra = {}
    after_create = None
    if "extra" in kwargs.keys():
        extra = kwargs["extra"]
        del kwargs["extra"]
    if "after_create" in kwargs.keys():
        after_create = kwargs["after_create"]
        del kwargs["after_create"]
    return cast(dict, extra), after_create


def _sanitize(value: str) -> str:
    """
    Sanitizes a string by parsing out the class name and truncating.

    Used by `to_dict()`

    :param [str] value: the string to be sanitized.
    :return [str]: the sanitized string.
    """
    s = str(value)
    if s.startswith("<class"):
        # Magic parsing of class name
        return s[8:-2].split(".")[-1]
    if len(s) > 80:
        return s[:80] + "..."
    return s


def to_dict(base_object: AnyModel) -> dict:
    """
    Returns a dict of the attributes of the db Base object.

    This also adds the class name.
    """
    extra = ["__class__"]
    return dict(
        (col, _sanitize(getattr(base_object, col)))
        for col in base_object.__table__.columns.keys() + extra
    )


class ResultType(str, enum.Enum):
    """Result type used when processing metadata values."""

    OK = "Ok"
    RESOLVED = "Resolved"
    ERROR = "Error"


@dataclass
class Result:
    """Augmented result class for reporting extra details about processed metadata."""

    type: ResultType = ResultType.OK
    details: str = ""


@dataclass
class ConsistentFields:
    """CSV entity-fields for an entity which is defined multiple times."""

    name: str
    summary: str
    # TODO@ add status: str


class ConsistencyValidator:
    """Used by validation to ensure consistency for families and collections."""

    families: dict[str, ConsistentFields]
    collections: dict[str, ConsistentFields]

    def __init__(self) -> None:
        self.families = {}
        self.collections = {}

    def check_family(
        self,
        n_row: int,
        family_id: str,
        family_name: str,
        family_summary: str,
        errors: list[Result],
    ) -> None:
        """Check the consistency of this family with one previously defined."""
        self._check_(
            self.families,
            "Family",
            n_row,
            family_id,
            family_name,
            family_summary,
            errors,
        )

    def check_collection(
        self,
        n_row: int,
        collection_id: str,
        collection_name: str,
        collection_summary: str,
        errors: list[Result],
    ) -> None:
        """Check the consistency of this collection with one previously defined."""
        self._check_(
            self.families,
            "Collection",
            n_row,
            collection_id,
            collection_name,
            collection_summary,
            errors,
        )

    @staticmethod
    def _check_(
        entities: dict[str, ConsistentFields],
        entity_name: str,
        n_row: int,
        id: str,
        name: str,
        summary: str,
        errors: list[Result],
    ) -> None:
        error_start = f"{entity_name} {id} has differing"
        on_row = f"on row {n_row}"
        fields = entities.get(id)
        if fields:
            if fields.name != name:
                error = Result(ResultType.ERROR, f"{error_start} name {on_row}")
                errors.append(error)
            if fields.summary != summary:
                error = Result(ResultType.ERROR, f"{error_start} summary {on_row}")
                errors.append(error)
        else:
            entities[id] = ConsistentFields(name, summary)


@dataclass
class IngestContext(abc.ABC):
    """Context used when processing."""

    org_name: str
    org_id: int
    results: list[Result]


@dataclass
class UNFCCCIngestContext(IngestContext):
    """Ingest Context for UNFCCC"""

    collection_ids_defined: list[str]
    collection_ids_referenced: list[str]
    # Just for families:
    consistency_validator: ConsistencyValidator

    def __init__(self, org_name="UNFCCC", org_id=2, results=None):
        self.collection_ids_defined = []
        self.collection_ids_referenced = []
        self.consistency_validator = ConsistencyValidator()
        self.org_name = org_name
        self.org_id = org_id
        self.results = [] if results is None else results


@dataclass
class CCLWIngestContext(IngestContext):
    """Ingest Context for CCLW"""

    consistency_validator: ConsistencyValidator

    def __init__(self, org_name="CCLW", org_id=1, results=None):
        self.consistency_validator = ConsistencyValidator()
        self.org_name = org_name
        self.org_id = org_id
        self.results = [] if results is None else results


@dataclass
class ValidationResult:
    """Returned when validating a CSV"""

    message: str
    errors: list[Result]


def get_result_counts(results: list[Result]) -> tuple[int, int, int]:
    rows = len(results)
    fails = len([r for r in results if r.type == ResultType.ERROR])
    resolved = len([r for r in results if r.type == ResultType.RESOLVED])
    return rows, fails, resolved


def update_if_changed(updated: dict, updated_key: str, source: Any, dest: Any):
    if getattr(dest, updated_key) != source:
        setattr(dest, updated_key, source)
        updated[updated_key] = source
    return updated
