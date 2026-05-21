from typing import Annotated, Literal

from pydantic import BaseModel, Field


class CouldNotTransform(Exception):
    """Raised when a transformer function cannot be found for the given input."""

    pass


class NoMatchingTransformations(Exception):
    """Raised when the transformer finds no matching transformations for the given input."""

    pass


# ---------------------------------------------------------------------------
# Warnings (non-fatal)
# ---------------------------------------------------------------------------
# A warning means "I produced a Document, but here's something I had to skip
# or couldn't resolve." The transform still completes; warnings are an audit
# log so callers can observe and report what happened. Compare with
# `CouldNotTransform`, which is used at the boundary for genuinely fatal
# failures where no Document can be produced at all.


class UnknownParentLabel(BaseModel):
    """A litigation concept referenced a parent label that wasn't in the input.

    The child label is still emitted; only the unresolved `subconcept_of`
    link is dropped.
    """

    kind: Literal["unknown_parent_label"] = "unknown_parent_label"
    family_import_id: str
    relation: str
    parent_name: str


class UnknownGeography(BaseModel):
    """A geography ID was supplied that isn't in the geographies lookup table.

    The geography is silently skipped; this warning surfaces it for inspection.
    Note: the sentinel `XAA` ("No Geography") is intentionally excluded and
    does NOT produce a warning.
    """

    kind: Literal["unknown_geography"] = "unknown_geography"
    family_import_id: str
    geography_id: str


class MissingRegionMapping(BaseModel):
    """A known country could not be mapped to a region."""

    kind: Literal["missing_region_mapping"] = "missing_region_mapping"
    family_import_id: str
    geography_id: str


# Discriminated union — `kind` is the discriminator field.
# @see: https://docs.pydantic.dev/latest/concepts/unions/#discriminated-unions
type TransformWarning = Annotated[
    UnknownParentLabel | UnknownGeography | MissingRegionMapping,
    Field(discriminator="kind"),
]
