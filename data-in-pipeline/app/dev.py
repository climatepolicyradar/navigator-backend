import app.bootstrap_telemetry  # noqa: F401 - Initialise telemetry first
from app.navigator_family_etl_pipeline import etl_pipeline

if __name__ == "__main__":
    etl_pipeline(ids=["CCLW.legislative.10695.6311"])
