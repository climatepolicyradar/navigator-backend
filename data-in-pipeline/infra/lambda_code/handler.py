import json
import logging
import os
from http import HTTPStatus
from typing import Any

import boto3
import psycopg
from mangum import Mangum
from psycopg import sql

_LOGGER = logging.getLogger(__name__)


def _get_secret(arn: str) -> dict[str, Any]:
    """Retrieve secret from AWS Secrets Manager.

    :param arn: ARN of the secret to retrieve
    :type arn: str
    :return: Secret value as a dictionary
    :rtype: dict[str, Any]
    """
    sm = boto3.client("secretsmanager")
    return json.loads(sm.get_secret_value(SecretId=arn)["SecretString"])


def _load_sql(path: str, identifiers: dict[str, str]):
    """Load SQL file and safely inject identifiers using psycopg.sql.

    :param path: Path to the SQL file
    :type path: str
    :param identifiers: Dictionary mapping placeholder names to identifier
        values. Should contain: DB_NAME, LOAD_DB_USER, APP_SCHEMA
    :type identifiers: dict[str, str]
    :return: SQL object with safely injected identifiers
    :rtype: sql.SQL
    """
    with open(path, "r", encoding="utf-8") as f:
        sql_template = f.read()

    # For string comparison in DO block, we need a literal
    # For role/database/schema names, we need identifiers
    load_db_user = identifiers["LOAD_DB_USER"]
    sql_placeholders = {
        "LOAD_DB_USER": sql.Identifier(load_db_user),
        "LOAD_DB_USER_LITERAL": sql.Literal(load_db_user),
        "DB_NAME": sql.Identifier(identifiers["DB_NAME"]),
        "APP_SCHEMA": sql.Identifier(identifiers["APP_SCHEMA"]),
    }

    # Use sql.SQL().format() to safely inject identifiers and literals
    return sql.SQL(sql_template).format(**sql_placeholders)


def handler(event, context):
    host = os.environ["AURORA_WRITER_ENDPOINT"]
    db = os.environ["DB_NAME"]
    port = int(os.environ.get("DB_PORT", "5432"))
    master_secret_arn = os.environ["ADMIN_SECRET_ARN"]
    sql_path = os.environ.get("SQL_PATH", "/var/task/create_iam_user.sql")
    load_user = os.environ.get("LOAD_DB_USER", "load_db_user")
    schema = os.environ.get("APP_SCHEMA", "public")

    master_creds = _get_secret(master_secret_arn)
    sql_query = _load_sql(
        sql_path,
        {"DB_NAME": db, "LOAD_DB_USER": load_user, "APP_SCHEMA": schema},
    )

    _LOGGER.info(
        f"Connecting to Aurora cluster at {host} with database {db} and user {master_creds['username']}"
    )
    conn = psycopg.connect(
        host=host,
        port=port,
        dbname=db,
        user=master_creds["username"],
        password=master_creds["password"],
        sslmode="require",
    )
    with conn, conn.cursor() as cur:
        _LOGGER.info(f"Executing SQL: {sql_query.as_string(conn)}")

        try:
            cur.execute(sql_query)
            cur.execute("SELECT current_user, current_database();")
            _LOGGER.info(cur.fetchone())
        except Exception as e:
            _LOGGER.exception("Error executing SQL")
            raise e
        finally:
            _LOGGER.info(f"Disconnecting from Aurora cluster at {host}")
            conn.close()

    return {"statusCode": HTTPStatus.OK, "body": json.dumps({"status": "ok"})}


# AWS Lambda entrypoint
handler = Mangum(handler)
