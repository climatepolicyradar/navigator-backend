# build_duckdb.py
import duckdb

DB_PATH = ".data_cache/documents.duckdb"
DATA_GLOB = ".data_cache/transformed_navigator_families/*.json"

con = duckdb.connect(DB_PATH, config={"memory_limit": "100GB"})

con.execute(
    """
    CREATE OR REPLACE TABLE documents AS
    SELECT * FROM read_json_auto(?);
    """,
    [DATA_GLOB],
)


con.close()
print("DuckDB created at", DB_PATH)
