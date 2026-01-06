# build_duckdb.py
import duckdb

DB_PATH = ".data_cache/documents.duckdb"
DATA_GLOB = ".data_cache/transformed_navigator_families/*.json"

con = duckdb.connect(DB_PATH, config={"memory_limit": "100GB"})

con.execute(
    """
    CREATE OR REPLACE TABLE documents AS
    SELECT * FROM read_json(
        ?,
        columns={
            id: 'VARCHAR',
            title: 'VARCHAR',
            labels: 'STRUCT(
                type VARCHAR,
                label STRUCT(id VARCHAR, title VARCHAR, type VARCHAR),
                timestamp TIMESTAMP
            )[]',
            relationships: 'STRUCT(
                type VARCHAR,
                document STRUCT(
                    id VARCHAR,
                    title VARCHAR,
                    labels STRUCT(
                        type VARCHAR,
                        label STRUCT(id VARCHAR, title VARCHAR, type VARCHAR),
                        timestamp TIMESTAMP
                    )[]
                ),
                timestamp TIMESTAMP
            )[]',
            items: 'STRUCT(url VARCHAR)[]'
        }
    );
    """,
    [DATA_GLOB],
)


con.close()
print("DuckDB created at", DB_PATH)
