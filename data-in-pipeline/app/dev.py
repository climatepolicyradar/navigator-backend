import app.bootstrap_telemetry  # noqa: F401 - Initialize telemetry first
from app.navigator_document_etl_pipeline import process_document_updates

if __name__ == "__main__":
    process_document_updates(ids=["CCLW.legislative.10695.6311"])
