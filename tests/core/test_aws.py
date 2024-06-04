def test_s3client_get_latest_ingest_start(test_s3_client):
    start_date = test_s3_client.get_latest_ingest_start(
        pipeline_bucket="test_pipeline_bucket", ingest_trigger_root="input"
    )
    assert start_date == "2024-03-22"
