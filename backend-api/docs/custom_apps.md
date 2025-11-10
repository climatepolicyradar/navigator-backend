# Custom Apps

## Creating a new custom app configuration token

Custom app configuration tokens can be generated from the root of the backend
repository, using the following code snippet

```bash
python -c "from app import config; from app.service.custom_app import AppTokenFactory; t = AppTokenFactory(); print(t.create_configuration_token('$CORPORA_IDS;$THEME;$AUDIENCE'))"
```

where:

- `CORPORA_IDS` is a comma separated list of corpora IDs you want to show in the
  custom app
- `THEME` is the name of the theme or organisation - it must not contain any
  special characters or spaces
- `AUDIENCE` is the host name of the custom app, without HTTP scheme. This must
  not end with a trailing slash OR start with a trailing slash.

## Example usage

### Encoding

Note: remember to use the secret key that corresponds to the AWS environment you
want to generate the token for.

```bash
export TOKEN_SECRET_KEY=secret_key
export CORPORA_IDS=CCLW.corpus.i00000001.n0000,UNFCCC.corpus.i00000001.n0000
export THEME=CPR
export AUDIENCE=cpr.staging.climatepolicyradar.org
poetry shell
python -c "from app import config; from app.service.custom_app import AppTokenFactory; t = AppTokenFactory(); print(t.create_configuration_token('$CORPORA_IDS;$THEME;$AUDIENCE'))"
```

### Decoding

Assuming you have your Docker containers running locally in your
`navigator-backend` repo:

```bash
docker compose build
docker compose up -d
export TOKEN=token
docker exec -it navigator-backend-backend-1 python -c "from app.core import config; from app.service.custom_app import decode_configuration_token; print(decode_configuration_token('$TOKEN', '$APP_DOMAIN'))"
```
