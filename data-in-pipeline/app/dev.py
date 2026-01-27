import app.bootstrap_telemetry  # noqa: F401 - Initialize telemetry first
from app.navigator_family_etl_pipeline import data_in_pipeline

if __name__ == "__main__":
    data_in_pipeline(ids=["CCLW.legislative.10695.6311"])
