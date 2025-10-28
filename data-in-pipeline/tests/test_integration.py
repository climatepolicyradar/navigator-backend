from app.flow import process_document_updates


def test_process_document_updates_flow():
    assert process_document_updates(["CCLW.legislative.10695.6311"]) == [
        "CCLW.legislative.10695.6311"
    ]
