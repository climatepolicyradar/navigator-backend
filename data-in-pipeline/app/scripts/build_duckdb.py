# build_duckdb.py
import duckdb

DB_PATH = ".data_cache/documents.duckdb"
DATA_GLOB = ".data_cache/transformed_navigator_families/*.json"

con = duckdb.connect(DB_PATH)

# 1) Raw documents table, keeps the nested labels
con.execute(
    """
    CREATE OR REPLACE TABLE documents AS
    SELECT * FROM read_json_auto(?);
    """,
    [DATA_GLOB],
)

# 2) Flatten labels into a separate table for easy querying
con.execute(
    """
    CREATE OR REPLACE TABLE doc_labels AS
    SELECT
        d.id                  AS document_id,
        l.type                AS label_type,
        l.label.id            AS label_id,
        l.label.title         AS label_title,
        l.label.type          AS label_entity_type
    FROM documents d
    CROSS JOIN UNNEST(d.labels) AS t(l);
"""
)

con.close()
print("DuckDB created at", DB_PATH)
