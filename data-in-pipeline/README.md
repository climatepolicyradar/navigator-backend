# Documents pipline

## Architecture diagram [DRAFT]

```mermaid

flowchart LR
    SourceSystem -- SourceDocument --> Identifier -- IdentifiedSourceDocument --> Transformer -- Document --> DocumentStore

```

## Notes

- The Prefect `AWS_ENV` is always `prod` for `staging` and `production` Pulumi
  stacks
