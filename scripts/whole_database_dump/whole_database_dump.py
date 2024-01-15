import datetime
import os
from io import StringIO
from pathlib import Path
from typing import Union

import boto3
import botocore
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

AWS_PROFILE = os.getenv("AWS_PROFILE", "")
if not AWS_PROFILE or AWS_PROFILE == "":
    raise RuntimeError("'{AWS_PROFILE}' environment variable must be set")
AWS_ENVIRONMENT = "production" if "prod" in AWS_PROFILE else "staging"
print(f"Using AWS_PROFILE '{AWS_ENVIRONMENT}'")

SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "")
if not SQLALCHEMY_DATABASE_URI or SQLALCHEMY_DATABASE_URI == "":
    raise RuntimeError("'{DATABASE_URL}' environment variable must be set")

if AWS_ENVIRONMENT == "staging":
    PUBLIC_APP_URL = "https://app.dev.climatepolicyradar.org"
else:
    PUBLIC_APP_URL = "https://app.climatepolicyradar.org"
url_base = f"{PUBLIC_APP_URL}/documents/"


def connect_to_postgres(database_url: str) -> Union[sqlalchemy.engine.Connection, bool]:
    try:
        engine = create_engine(database_url)
        conn = engine.connect()
        return conn
    except SQLAlchemyError:
        msg = f"Failed to connect to the database at URL '{SQLALCHEMY_DATABASE_URI}'."
        print(msg)
        return False


def get_whole_database_dump(
    conn: sqlalchemy.engine.Connection, sql_query_file: str = "whole_database_dump.sql"
):
    sql_query_file = Path(sql_query_file).read_text()
    with conn, conn.begin():
        df = pd.read_sql_query(sql_query_file, conn)
        return df


def replace_slug_with_qualified_url(
    df: pd.DataFrame, url_cols: list[str] = ["Family Slug", "Document Slug"]
) -> pd.DataFrame:
    for col in url_cols:
        df[col] = url_base + df[col].astype(str)

    df.columns = df.columns.str.replace("Slug", "URL")
    return df


def convert_dump_to_csv(df: pd.DataFrame):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, sep=",", index=False)
    return csv_buffer


def check_aws_validity(aws_profile: str) -> bool:
    try:
        session = boto3.Session(profile_name=aws_profile)
        status_code = session.client("sts").get_caller_identity()["ResponseMetadata"][
            "HTTPStatusCode"
        ]
        if status_code == 200:
            return True
        else:
            print(f"Connecting to AWS session returns status code '{status_code}'")
    except Exception as e:
        print(e)
    return False


def get_s3_bucket(aws_profile: str, bucket_name: str):
    session = boto3.Session(profile_name=aws_profile)
    s3 = session.resource("s3")

    bucket = s3.Bucket(bucket_name)

    bucket_valid = check_bucket_exists(bucket)
    if bucket_valid:
        return s3
    return None


def check_bucket_exists(bucket) -> bool:
    try:
        if bucket.creation_date:
            print("The bucket exists")
            return True
        else:
            print("The bucket does not exist")
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = int(e.response["Error"]["Code"])
        if error_code == 403:
            print("Private Bucket. Forbidden Access!")
        elif error_code == 404:
            print("Bucket Does Not Exist!")
    return False


def upload_to_s3(s3, bucket_name: str, aws_env: str, csv_buffer: StringIO):
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    file_key = f"navigator/{aws_env}_data_dump_{current_date}.csv"

    print("Uploading to S3...")
    try:
        object = s3.Object(bucket_name, file_key)
        status_code = object.put(Body=csv_buffer.getvalue())["ResponseMetadata"][
            "HTTPStatusCode"
        ]
        if status_code == 200:
            print("Upload succeeded")
            return True
        else:
            print(f"Uploading dump to S3 returned status code '{status_code}'")
    except Exception as e:
        print(e)

    return False


if __name__ == "__main__":
    # 1. Connect to the database (Postgres)
    conn = connect_to_postgres(SQLALCHEMY_DATABASE_URI)
    if conn is False:
        print("Exiting...")
        exit(1)

    # 2. Execute the infinite query and create CSV data.
    df = get_whole_database_dump(conn)
    df = replace_slug_with_qualified_url(df)
    df_as_csv = convert_dump_to_csv(df)

    # 3. Connect to AWS.
    valid_credentials = check_aws_validity(AWS_PROFILE)
    if not valid_credentials:
        print("Exiting...")
        exit(1)

    # 4. Upload to S3
    bucket_name = f"cpr-{AWS_ENVIRONMENT}-document-cache"
    s3 = get_s3_bucket(AWS_PROFILE, bucket_name)
    if s3 is not None:
        upload_to_s3(s3, bucket_name, AWS_ENVIRONMENT, df_as_csv)
