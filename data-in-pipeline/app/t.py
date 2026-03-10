import io
import itertools
import json

import pyarrow as pa
import pyarrow.parquet as pq

from data_in_models.models import Document


def cache_parquet_to_s3(documents: list[Document], run_id: str | None = None):
    """Upload documents as parquet to S3 cache."""
    buffer = io.BytesIO()
    writer = None
    for chunk in itertools.batched(documents, 10_000):
        rows = [doc.model_dump(mode="json") for doc in chunk]
        for row in rows:
            row["attributes"] = json.dumps(row.get("attributes", {}))
        table = pa.Table.from_pylist(rows)
        if writer is None:
            writer = pq.ParquetWriter(buffer, table.schema)
        writer.write_table(table)
    if writer:
        writer.close()

    value = buffer.getvalue()
    print(value)


cache_parquet_to_s3(
    [
        Document(
            title="Title",
            id="1",
            attributes={},
            documents=[],
            labels=[],
        ),
        Document(
            title="Title",
            id="1",
            attributes={"asd": 1, "l": "l"},
            documents=[],
            labels=[],
        ),
    ]
)
