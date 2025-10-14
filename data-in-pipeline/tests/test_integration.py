from src.main import main


def test_add_flow():
    assert main() == [11, 22, 34]
