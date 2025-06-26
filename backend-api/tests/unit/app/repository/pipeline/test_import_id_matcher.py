import pytest

from app.service.pipeline import IMPORT_ID_MATCHER


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
