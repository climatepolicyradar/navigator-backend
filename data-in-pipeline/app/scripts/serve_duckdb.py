import duckdb
from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

DB_PATH = ".data_cache/documents.duckdb"

app = FastAPI(title="Documents API (DuckDB)")


class Label(BaseModel):
    id: str
    title: str
    type: str


class DocumentLabelRelationship(BaseModel):
    type: str
    label: Label


class BaseDocument(BaseModel):
    id: str
    title: str
    labels: list[DocumentLabelRelationship] = Field(default_factory=list)


class DocumentDocumentRelationship(BaseModel):
    type: str
    document: "DocumentWithoutRelationships"


class Document(BaseDocument):
    relationships: list[DocumentDocumentRelationship] = Field(default_factory=list)


class DocumentWithoutRelationships(BaseDocument):
    pass


def get_connection():
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        yield con
    finally:
        con.close()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/documents", response_model=list[Document])
def list_documents(
    label_id: str | None = Query(
        None, description="Filter by labels.label.id", alias="labels.label.id"
    ),
    label_type: str | None = Query(
        None, description="Filter by labels.label.type", alias="labels.label.type"
    ),
    limit: int = Query(100, le=500),
    offset: int = 0,
    con=Depends(get_connection),
):
    """
    Query documents directly by nested labels JSON fields.
    """
    base_query = """
        SELECT d.id, d.title, d.labels, d.relationships
        FROM documents AS d
        WHERE 1=1
    """
    params: list[str | int] = []

    if label_id or label_type:
        base_query += """
            AND EXISTS (
                SELECT 1
                FROM UNNEST(d.labels) AS l(lbl)
                WHERE 1=1
        """
        if label_id:
            base_query += " AND lbl.label.id = ?"
            params.append(label_id)
        if label_type:
            base_query += " AND lbl.label.type = ?"
            params.append(label_type)
        base_query += ")"

    base_query += " ORDER BY d.title LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    try:
        result = con.execute(base_query, params)
    except duckdb.Error as e:
        raise HTTPException(status_code=500, detail=f"DuckDB error: {e!s}")

    # Turn tuples into dicts
    colnames = [c[0] for c in result.description]
    rows = result.fetchall()

    docs = []
    for row in rows:
        record = dict(zip(colnames, row))
        docs.append(Document(**record))

    return docs
