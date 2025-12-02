import duckdb
from fastapi import Depends, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


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


DB_PATH = ".data_cache/documents.duckdb"

app = FastAPI(title="Documents API (DuckDB)")
origins = [
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_connection():
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        yield con
    finally:
        con.close()


@app.get("/health")
def health():
    return {"status": "ok"}


class DocumentResponse(BaseModel):
    data: list[Document]
    total: int


@app.get("/documents", response_model=DocumentResponse)
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

    where = "WHERE 1=1"
    params = []

    if label_id or label_type:
        subquery_parts = []
        if label_id:
            subquery_parts.append("l.label.id = ?")
            params.append(label_id)
        if label_type:
            subquery_parts.append("l.label.type = ?")
            params.append(label_type)

        # we use nosec B608 as there is never any user input injected into the SQL
        # and any fix makes it really unreadable
        where += f"""
            AND id IN (
                SELECT d.id 
                FROM documents d
                CROSS JOIN UNNEST(d.labels) AS t(l)
                WHERE {" AND ".join(subquery_parts)}
            )
        """  # nosec B608

    # total
    total = con.execute(
        f"SELECT COUNT(*) FROM documents {where}", params  # nosec B608
    ).fetchone()[0]

    # data
    result = con.execute(
        f"SELECT id, title, labels, relationships FROM documents {where} ORDER BY title, id LIMIT ? OFFSET ?",  # nosec B608
        params + [limit, offset],
    )
    colnames = [c[0] for c in result.description]
    rows = result.fetchall()
    docs = []
    for row in rows:
        record = dict(zip(colnames, row))
        docs.append(Document(**record))

    return {
        "data": docs,
        "total": total,
    }
