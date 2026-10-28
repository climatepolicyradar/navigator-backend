import time

import pytest

from app.service.pipeline import MAX_IMPORT_ID_LENGTH, is_valid_import_id

# Budget for rejecting a non-import-id. The check needs microseconds, so this
# sits well above it: generous enough not to flake on slow CI, tight enough to
# catch a regression that makes validation expensive again.
MAX_VALIDATION_SECONDS = 0.5


@pytest.mark.parametrize(
    "valid_id",
    [
        "CCLW.family.10014.0",
        "CCLW.document.i00000004.n0000",
        "CCLW.executive.9724.rtl_174",
        "UNFCCC.non-party.1267.0",
        "CCLW.exec_leg-3.1-2-3-4.2",
    ],
)
def test_accepts_import_ids(valid_id):
    assert is_valid_import_id(valid_id)


@pytest.mark.parametrize(
    "invalid_id",
    [
        # Slugs: the shape that actually reaches these endpoints in error.
        "CPR-RUS-2006-ce834ca5cf77b221394a8106b6f1bb2a-law",
        "some-ordinary-looking-slug",
        # Structurally wrong import ids.
        "CCLW.exec.1.2.3",  # too many elements
        "UNFCCC.1.2",  # too few elements
        "CCLW..1.2",  # empty element
        "CCLW.%25.1.2",  # invalid character
        "CCLW.exec.1.rtl--174",  # adjacent separators
        "CCLW.exec.1.-rtl_174",  # leading separator
        "",  # empty
    ],
)
def test_rejects_non_import_ids(invalid_id):
    assert not is_valid_import_id(invalid_id)


def test_rejects_values_over_the_length_bound():
    """Length is checked before the pattern, so an over-long value is cheap."""
    over_long = "CCLW.family." + "a" * MAX_IMPORT_ID_LENGTH + ".0"

    assert len(over_long) > MAX_IMPORT_ID_LENGTH
    assert not is_valid_import_id(over_long)


@pytest.mark.parametrize(
    "hostile_value",
    [
        # The slug that froze every backend-api task on 2026-10-12.
        "CPR-RUS-2006-ce834ca5cf77b221394a8106b6f1bb2a-law",
        # A valid four-part prefix with an invalid final character. The fixed
        # prefix pins the dots and multiplies the cost of failing, so a
        # dot-count pre-check alone would not have helped here. Kept just long
        # enough to blow the budget (~30s under the old pattern) so a
        # regression fails in seconds rather than hanging CI.
        "CCLW.executive.1." + "a" * 18 + "%",
        # Long enough to be expensive, short enough to pass the length bound,
        # so the pattern genuinely has to run.
        "a" * 200,
    ],
)
def test_rejects_hostile_values_quickly(hostile_value):
    """Guard against catastrophic backtracking (ReDoS) at the API boundary.

    `re` does not release the GIL during a match, so an expensive match freezes
    the whole process rather than one request -- which is why a single slug
    could take out every task.
    """
    start = time.perf_counter()
    result = is_valid_import_id(hostile_value)
    elapsed = time.perf_counter() - start

    assert result is False
    assert elapsed < MAX_VALIDATION_SECONDS, (
        f"validation took {elapsed:.3f}s, expected well under "
        f"{MAX_VALIDATION_SECONDS}s -- backtracking may be back"
    )
