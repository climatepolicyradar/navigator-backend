from app.flow import pipeline


def test_add_flow():
    assert pipeline() == [11, 22, 33]
