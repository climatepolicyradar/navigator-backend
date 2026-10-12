import time

import pytest

from app.service.pipeline import IMPORT_ID_MATCHER

# Budget for rejecting a non-matching id. The fixed pattern needs microseconds,
# so this sits five orders of magnitude above it: generous enough not to flake
# on slow CI, tight enough that a revert to the backtracking pattern (~30s on
# these inputs) fails in seconds.
MAX_REJECTION_SECONDS = 0.5


@pytest.mark.parametrize(
    "valid_id",
    [
        "CCLW.exec.1.2",
        "CCLW-2.exec.1.2",
        "CCLW_2.exec-1-2.1.2",
        "CCLW.exec_leg-3.1-2-3-4.2",
        "UNFCCC_p1-d3.exec.1.2-3-4-5-6",
        "UNFCCC.exec-1-2_3_4-5.1.2",
    ],
)
def test_valid_import_ids(valid_id):
    assert IMPORT_ID_MATCHER.match(valid_id)


@pytest.mark.parametrize(
    "invalid_id",
    [
        "CCLW.exec.1.2.3",  # too many elements
        "CCLW-2.exec.-1.2",  # element starts with -
        "_2.exec-1-2.1.2",  # element starts with _
        "CCLW.exec__leg-3.1-2-3-4.2",  # chained -_ chars
        "UNFCCC_p1-d3.exec.1.2-3-4-5--6",  # chained -_ chars
        "UNFCCC.1.2",  # too few elements
        "CCLW..1.2",  # empty elements
        "CCLW...2",  # many empty elements
        "CCLW.%25.1.2",  # invalid character
        "CCLW.%25.1%20%25.2",  # many invalid characters
        "CCLW.^25.1'25.2",  # many invalid characters
    ],
)
def test_invalid_import_ids(invalid_id):
    assert IMPORT_ID_MATCHER.match(invalid_id) is None


@pytest.mark.parametrize(
    "slug_shaped_id",
    [
        # The slug that froze every backend-api task on 2026-10-12.
        "CPR-RUS-2006-ce834ca5cf77b221394a8106b6f1bb2a-law",
        # A valid four-part prefix with an invalid character in the final
        # element: the fixed prefix pins the dots and multiplies the cost of
        # failing, so a dot-count pre-check would not have helped here.
        # Kept just long enough to blow the budget (~30s under the old
        # pattern) so a regression fails in seconds rather than hanging CI.
        "CCLW.executive.1." + "a" * 18 + "%",
    ],
)
def test_non_matching_ids_are_rejected_quickly(slug_shaped_id):
    """Guard against catastrophic backtracking (ReDoS).

    `get_family_document_by_import_id_or_slug` runs this pattern against values
    that are deliberately slugs, so non-matching input is the normal case and
    must stay cheap. Under the previous pattern these inputs took hours and
    froze the whole process, since `re` holds the GIL for the duration.

    The budget is five orders of magnitude above the microseconds the fixed
    pattern needs, so it will not flake on slow CI but a revert fails fast.
    """
    start = time.perf_counter()
    result = IMPORT_ID_MATCHER.match(slug_shaped_id)
    elapsed = time.perf_counter() - start

    assert result is None
    assert elapsed < MAX_REJECTION_SECONDS, (
        f"rejection took {elapsed:.3f}s, expected well under "
        f"{MAX_REJECTION_SECONDS}s -- the backtracking regex may be back"
    )


@pytest.mark.parametrize(
    "invalid_separator_id",
    [
        "CCLW.exec.1.rtl--174",  # adjacent separators
        "CCLW.exec.1.rtl__174",  # adjacent separators
        "CCLW.exec.1.-rtl_174",  # leading separator
        "CCLW.exec.1.rtl_174-",  # trailing separator
        "CCLW.executive_.1.2",  # trailing separator mid-id
    ],
)
def test_separator_placement_is_rejected(invalid_separator_id):
    """Pin separator placement, the behaviour the ReDoS fix touches.

    Note these cases are rejected by both the old and new patterns, so they do
    not by themselves catch a revert -- `test_non_matching_ids_are_rejected_
    quickly` is what does that. They are here to pin that making the separator
    mandatory did not change which ids are considered valid.
    """
    assert IMPORT_ID_MATCHER.match(invalid_separator_id) is None


@pytest.mark.parametrize(
    "valid_separator_id",
    ["CCLW.executive.9724.rtl_174", "UNFCCC.non-party.1267.0"],
)
def test_single_separators_inside_elements_are_accepted(valid_separator_id):
    """The fix must not narrow what counts as a valid import id."""
    assert IMPORT_ID_MATCHER.match(valid_separator_id)
