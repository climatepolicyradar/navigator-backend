# Navigator Full Stack Local Setup

## Introduction

Prior to pushing to staging or production it can be desirable to test complicated
searches locally before deploying. This stops issues like subsequent PR's being
merged on top of erroneous commits and deployed etc. resulting in a rather
sticky mess to unpick.

The navigator stack consists of the following sub systems:

- frontend
- backend api
- postgres database
- vespa search database

This document outlines how to setup ones stack locally to test such changes.

## Setup Overview

When we spin up the stack locally via the `make start` the postgres database is
populated with only the data required for the admin service to function.
E.g. we enter no families and thus no family related data like family_geographies
but we do enter data via migrations in the `navigator-db-client` for tables like
the geography table. It is therefore necessary to load the postgres database
locally from a dump of production or staging (this takes the form of a .sql
file and requires ssh-ing into the bastion to interact with the database).

Vespa is spun up with some test data the can be found in this repo at the following
path: `tests/search/vespa/fixtures`. The issue is the corpus's and family documents
etc. won't match what's in the postgres instance from production or staging.
Therefore, it's necessary to take a similar sample or dump of data from vespa
and load this into ones local instance that matches what's in the local
postgres instance. We'll go through how to do that below:

## 1. Set Config to Staging Vespa

The following commands configure your Vespa instance to connect to the staging environment.

### Spin up your local vespa, postgres and backend

> ⚠️ **Warning:** You will need to set `SECRET_KEY` and `TOKEN_SECRET_KEY`,
> I believe these can be taken from the staging stack in navigator-infra.

```shell
cp .env.example .env
make start
```

### Authenticate with Vespa

```shell
vespa auth login
```

### Set the target to the Vespa Cloud

```shell
vespa config set target cloud
```

### Set the specific Vespa Cloud target URL

Note: This can be found in vespa cloud

```shell
vespa config set target $VESPA_ENDPOINT
```

### List the Vespa configuration directory

You should have a directory with the application name you're looking to connect to,
this should have a public key and private cert.

```shell
ls ~/.vespa/
```

### Set the application to the staging environment

```shell
vespa config set application climate-policy-radar.${APP}.${INSTANCE}
```

### Verify the current Vespa configuration

```shell
vespa config get
```

### Run a test query to verify connection

```shell
vespa query 'select corpus_import_id from family_document where true limit 10'
```

### Sample some documents from the family_document table

```shell
echo $(vespa visit --slices 1000 --slice-id 0 --selection "family_document") > family_document_sample.jsonl
```

### Read the related documents based on document_import_id

```shell
echo $(python -m extract_document_import_ids $family_document_sample.jsonl | xargs -I {} vespa visit --selection 'document_passage AND id="id:doc_search:document_passage::{}.1"') > document_passage_sample.jsonl
```

### Set your vespa cli to point to local

Vespa config set target local
vespa config set application default.application.default

### Feed the documents into the local instance

> ⚠️ **Warning:** I had some issues with the `family_document_sample.jsonl`,
> I copied the first line into `family_document_sample_1.json` and ensured
> the `family_publication_ts` was correct as initially this parsed incorrectly.

```shell
vespa feed family_document_sample.jsonl
vespa feed document_passage_sample.jsonl
```

### Run the frontend application

Load up the frontend repository and run:

> ⚠️ **Warning:** You will need to set the following token: `NEXT_PUBLIC_APP_TOKEN`.
> Also check the backed api url is correct.
> You may also need to clear your browser cache as it may have cached prod or staging.

```shell
cp .env.example .env
make start
```

### Test the application

Navigate to the frontend endpoint in browser and test your feature.
