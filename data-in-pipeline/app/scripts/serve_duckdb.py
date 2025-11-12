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
    labels: list[DocumentLabelRelationship] = []


class DocumentDocumentRelationship(BaseModel):
    type: str
    document: "DocumentWithoutRelationships"


class Document(BaseDocument):
    relationships: list[DocumentDocumentRelationship] = []


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
    params: list = [limit, offset]

    base_query = """
        SELECT id, title, labels, relationships
        FROM documents
        WHERE 1=1
    """

    if label_id or label_type:
        base_query += " AND id IN ("
        base_query += """
            SELECT d.id
            FROM documents d
            CROSS JOIN UNNEST(d.labels) AS t(l)
            WHERE 1=1
        """
        if label_id:
            base_query += " AND l.label.id = ?"
            params.insert(0, label_id)
        if label_type:
            base_query += " AND l.label.type = ?"
            params.insert(0, label_type)
        base_query += ")"

    base_query += " ORDER BY title LIMIT ? OFFSET ?"

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
