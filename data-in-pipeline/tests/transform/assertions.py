from collections.abc import Sequence

from pydantic import BaseModel


def assert_model_equality(actual: BaseModel, expected: BaseModel):  # noqa: F821
    """
    Compare two Pydantic models for equality.

    This is used for better diffing.
    A BaseModel diff is hard to read but dict diffs are a lot more clear.
    """
    assert actual.model_dump() == expected.model_dump()


def assert_model_list_equality(
    actual: Sequence[BaseModel], expected: Sequence[BaseModel]
):  # noqa: F821
    """
    Compare two lists of Pydantic models for equality.

    This is used for better diffing.
    A BaseModel diff is hard to read but dict diffs are a lot more clear.
    """
    actual_dumped = [model.model_dump() for model in actual]
    expected_dumped = [model.model_dump() for model in expected]

    assert actual_dumped == expected_dumped
