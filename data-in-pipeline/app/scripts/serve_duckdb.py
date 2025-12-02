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


class Aggregation(BaseModel):
    id: str
    type: str
    count: int


class DocumentResponse(BaseModel):
    total: int
    aggregations: dict[str, list[Aggregation]]
    data: list[Document]


@app.get("/documents", response_model=DocumentResponse)
def list_documents(
    label_id: list[str] | None = Query(
        None, description="Filter by labels.label.id", alias="labels.label.id"
    ),
    label_id_exclude: list[str] | None = Query(None, alias="-labels.label.id"),
    label_type: list[str] | None = Query(
        None, description="Filter by labels.label.type", alias="labels.label.type"
    ),
    label_type_exclude: list[str] | None = Query(None, alias="-labels.label.type"),
    relationship_type: list[str] | None = Query(
        None, description="Filter by relationships.type", alias="relationships.type"
    ),
    relationship_type_exclude: list[str] | None = Query(
        None, alias="-relationships.type"
    ),
    limit: int = Query(100, le=500),
    offset: int = 0,
    con=Depends(get_connection),
):

    where = "WHERE 1=1"
    params = []

    # Build inclusion filter (documents that have ALL of these - AND)
    if label_id or label_type or relationship_type:
        include_parts = []
        if label_id:
            placeholders = ",".join("?" * len(label_id))
            include_parts.append(f"l.label.id IN ({placeholders})")
            params.extend(label_id)
        if label_type:
            placeholders = ",".join("?" * len(label_type))
            include_parts.append(f"l.label.type IN ({placeholders})")
            params.extend(label_type)
        if relationship_type:
            placeholders = ",".join("?" * len(relationship_type))
            include_parts.append(f"r.type IN ({placeholders})")
            params.extend(relationship_type)

        # we use nosec B608 as there is never any user input injected into the SQL
        # and any fix makes it really unreadable
        where += f"""
            AND id IN (
                SELECT d.id
                FROM documents d
                CROSS JOIN UNNEST(d.labels) AS t(l)
                CROSS JOIN UNNEST(d.relationships) AS t(r)
                WHERE {" AND ".join(include_parts)}
            )
        """  # nosec B608

    # Build exclusion filter (documents that DON'T have ALL of these - NOT (a AND b AND c))
    if label_id_exclude or label_type_exclude or relationship_type_exclude:
        exclude_parts = []
        if label_id_exclude:
            placeholders = ",".join("?" * len(label_id_exclude))
            exclude_parts.append(f"l.label.id IN ({placeholders})")
            params.extend(label_id_exclude)
        if label_type_exclude:
            placeholders = ",".join("?" * len(label_type_exclude))
            exclude_parts.append(f"l.label.type IN ({placeholders})")
            params.extend(label_type_exclude)
        if relationship_type_exclude:
            placeholders = ",".join("?" * len(relationship_type_exclude))
            exclude_parts.append(f"r.type IN ({placeholders})")
            params.extend(relationship_type_exclude)

        # we use nosec B608 as there is never any user input injected into the SQL
        # and any fix makes it really unreadable
        where += f"""
            AND id NOT IN (
                SELECT d.id
                FROM documents d
                CROSS JOIN UNNEST(d.labels) AS t(l)
                CROSS JOIN UNNEST(d.relationships) AS t(r)
                WHERE {" AND ".join(exclude_parts)}
            )
        """  # nosec B608

    # aggregations
    # labels aggregation
    labels_result = con.execute(
        """
        SELECT 
            l.label.id,
            l.label.type,
            COUNT(*) AS count
        FROM documents
        CROSS JOIN UNNEST(labels) AS t(l)
        GROUP BY l.label.id, l.label.type
        """
    )
    labels_colnames = [c[0] for c in labels_result.description]
    labels_rows = labels_result.fetchall()
    labels = []
    for label_row in labels_rows:
        record = dict(zip(labels_colnames, label_row))
        labels.append(Aggregation(**record))

    # relationships aggregation
    relationships_result = con.execute(
        """
        SELECT 
            r.type as id,
            r.type,
            COUNT(*) AS count
        FROM documents
        CROSS JOIN UNNEST(relationships) AS t(r)
        GROUP BY r.type, r.type
        """
    )
    relationships_colnames = [c[0] for c in relationships_result.description]
    relationships_rows = relationships_result.fetchall()
    relationships = []
    for relationship_row in relationships_rows:
        record = dict(zip(relationships_colnames, relationship_row))
        relationships.append(Aggregation(**record))

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
        "aggregations": {
            "labels": labels,
            "relationships": relationships,
        },
        "total": total,
        "data": docs,
    }
