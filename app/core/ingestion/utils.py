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
    model: _DbModel,
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


class ResultType(enum.Enum):
    """Result type used when processing metadata values."""

    OK = 0
    RESOLVED = 10
    ERROR = 20


@dataclass
class Result:
    """Augmented result class for reporting extra details about processed metadata."""

    type: ResultType = ResultType.OK
    details: str = ""


@dataclass
class IngestContext:
    """Context used when processing."""

    org_id: int
    results: list[Result]


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
