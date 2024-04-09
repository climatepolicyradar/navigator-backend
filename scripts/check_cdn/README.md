# Check CDN Script

## Prerequisites

The environment is sufficiently configured so that you can `psql` to the
database of your choice. This means configuring the following:

```ini
PGPASSWORD=password
PGUSER=navigator
PGHOST=localhost
PGPORT=5432
```

## What the script does

1. First it checks for any physical documents that have no CDN object
