from datetime import datetime

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
    timestamp: datetime | None = None


class BaseDocument(BaseModel):
    id: str
    title: str
    labels: list[DocumentLabelRelationship] = []


class DocumentDocumentRelationship(BaseModel):
    type: str
    document: "DocumentWithoutRelationships"
    timestamp: datetime | None = None


class Item(BaseModel):
    url: str | None = None


class Document(BaseDocument):
    relationships: list[DocumentDocumentRelationship] = []
    """
    This needs work, but is a decent placeholder while we work through the model.
    It is lightly based on the FRBR ontology.

    @see: https://en.wikipedia.org/wiki/Functional_Requirements_for_Bibliographic_Records
    """
    items: list[Item] = []


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


class DocumentsResponse(BaseModel):
    total: int
    aggregations: dict[str, list[Aggregation]]
    data: list[Document]


@app.get("/documents", response_model=DocumentsResponse)
def list_documents(
    label_ids: list[str] | None = Query(
        None, description="Filter by labels.label.id", alias="labels.label.id"
    ),
    label_ids_exclude: list[str] | None = Query(None, alias="-labels.label.id"),
    label_types: list[str] | None = Query(
        None, description="Filter by labels.label.type", alias="labels.label.type"
    ),
    label_types_exclude: list[str] | None = Query(None, alias="-labels.label.type"),
    relationship_types: list[str] | None = Query(
        None, description="Filter by relationships.type", alias="relationships.type"
    ),
    relationship_types_exclude: list[str] | None = Query(
        None, alias="-relationships.type"
    ),
    relationships_len: int | None = Query(None, alias="len(relationships)"),
    labels_len: int | None = Query(None, alias="len(labels)"),
    limit: int = Query(100, le=500),
    offset: int = 0,
    con=Depends(get_connection),
):

    where = "WHERE 1=1"
    params = []

    for label_id in label_ids or []:
        where += " AND list_contains(list_transform(labels, l -> l.label.id), ?)"
        params.append(label_id)
    for label_id in label_ids_exclude or []:
        where += " AND NOT list_contains(list_transform(labels, l -> l.label.id), ?)"
        params.append(label_id)

    for label_type in label_types or []:
        where += " AND list_contains(list_transform(labels, l -> l.label.type), ?)"
        params.append(label_type)
    for label_type in label_types_exclude or []:
        where += " AND NOT list_contains(list_transform(labels, l -> l.label.type), ?)"
        params.append(label_type)

    for relationship_type in relationship_types or []:
        where += " AND list_contains(list_transform(relationships, r -> r.type), ?)"
        params.append(relationship_type)
    for relationship_type in relationship_types_exclude or []:
        where += " AND NOT list_contains(list_transform(relationships, r -> r.type), ?)"
        params.append(relationship_type)

    if relationships_len is not None:
        where += " AND array_length(relationships) = ?"
        params.append(relationships_len)

    if labels_len is not None:
        where += " AND array_length(labels) = ?"
        params.append(labels_len)

    # aggregations - these are never filtered as filter logic on aggregations is hard
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
        f"SELECT id, title, labels, relationships, items FROM documents {where} ORDER BY title, id LIMIT ? OFFSET ?",  # nosec B608
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


class AggregationResponse(BaseModel):
    total: int
    data: list[Aggregation]


@app.get("/labels", response_model=AggregationResponse)
def list_labels(
    con=Depends(get_connection),
):
    labels_result = con.execute(
        """
        SELECT
            l.label.id,
            l.label.type,
            COUNT(*) AS count
        FROM documents
        CROSS JOIN UNNEST(labels) AS t(l)
        GROUP BY l.label.id, l.label.type
        ORDER BY l.label.type, l.label.id
        """
    )
    labels_colnames = [c[0] for c in labels_result.description]
    labels_rows = labels_result.fetchall()
    labels = []

    for label_row in labels_rows:
        record = dict(zip(labels_colnames, label_row))
        labels.append(Aggregation(**record))

    return {
        "total": 10,
        "data": labels,
    }


@app.get("/relationships", response_model=AggregationResponse)
def list_relationships(
    con=Depends(get_connection),
):
    relationships_result = con.execute(
        """
        SELECT
            r.type as id,
            r.type,
            COUNT(*) AS count
        FROM documents
        CROSS JOIN UNNEST(relationships) AS t(r)
        GROUP BY r.type, r.type
        ORDER BY r.type
        """
    )
    relationships_colnames = [c[0] for c in relationships_result.description]
    relationships_rows = relationships_result.fetchall()
    relationships = []

    for relationship_row in relationships_rows:
        record = dict(zip(relationships_colnames, relationship_row))
        relationships.append(Aggregation(**record))

    return {
        "total": 10,
        "data": relationships,
    }


class DocumentResponse(BaseModel):
    data: Document


@app.get("/documents/{id}", response_model=DocumentResponse)
def read_document(
    id: str,
    con=Depends(get_connection),
):
    document_result = con.execute(
        """
        SELECT
            id,
            title,
            labels,
            relationships,
            items,
        FROM documents
        WHERE id = ?
        """,
        (id,),
    )
    document_colnames = [c[0] for c in document_result.description]
    document_row = document_result.fetchone()
    document = Document(**dict(zip(document_colnames, document_row)))

    return {"data": document}
