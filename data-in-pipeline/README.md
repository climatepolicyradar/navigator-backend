# Documents pipline

## Architecture diagram [DRAFT]

```mermaid

flowchart LR
    SourceSystem -- SourceDocument --> Identifier -- IdentifiedSourceDocument --> Transformer -- Document --> DocumentStore

```

## Next steps

- Dockerise prefect flow
- Deploy workflow locally to cloud?
- Trigger it in cloud & locally
- Get access to prefect
