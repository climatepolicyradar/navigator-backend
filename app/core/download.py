"""Functions to support browsing the RDS document structure"""

import os
from io import BytesIO
from logging import getLogger

import boto3
import botocore
import pandas as pd
from fastapi import Depends
from sqlalchemy.orm import Session

from app.db.session import get_db

_LOGGER = getLogger(__name__)


def create_query(db: Session) -> str:
    """Browse RDS"""

    # query = (
    #     db.query(Family, Geography, Organisation)
    #     .join(Geography, Family.geography_id == Geography.id)
    #     .join(
    #         FamilyOrganisation, FamilyOrganisation.family_import_id == Family.import_id
    #     )
    #     .join(Organisation, Organisation.id == FamilyOrganisation.organisation_id)
    # )
    query = """
WITH 
deduplicated_family_slugs as (
    select
      distinct ON (slug.family_import_id) slug.family_import_id, slug.created, slug.name
    from (
      SELECT
        slug.family_import_id as "family_import_id",
        count(*) as count
        from slug
        where slug.family_import_id is not null
        group by slug.family_import_id
        having count(*) > 1
    ) duplicates
    left join slug
      on duplicates.family_import_id = slug.family_import_id
    order by slug.family_import_id desc, slug.created desc, slug.ctid desc
),
unique_family_slugs as (
    select
      distinct ON (slug.family_import_id) slug.family_import_id, slug.created, slug.name
    from (
      SELECT
        slug.family_import_id as "family_import_id",
        count(*) as count
        from slug
        where slug.family_import_id is not null
        group by slug.family_import_id
        having count(*) = 1
    ) non_duplicates
    left join slug
      on non_duplicates.family_import_id = slug.family_import_id
    order by slug.family_import_id desc, slug.created desc, slug.ctid desc
), most_recent_family_slugs as (
    select 
        deduplicated_family_slugs.family_import_id as "family_import_id",
        deduplicated_family_slugs.created as "created",
        deduplicated_family_slugs.name as "name"
    from deduplicated_family_slugs
    UNION ALL
    select 
        unique_family_slugs.family_import_id as "family_import_id",
        unique_family_slugs.created as "created",
        unique_family_slugs.name as "name"
    from unique_family_slugs
    order by family_import_id desc, created desc
), deduplicated_doc_slugs as (
    select
      distinct ON (slug.family_document_import_id) slug.family_document_import_id, slug.created, slug.name
    from (
      SELECT
        slug.family_document_import_id as "family_document_import_id",
        count(*) as count
        from slug
        where slug.family_document_import_id is not null
        group by slug.family_document_import_id
        having count(*) > 1
    ) duplicates
    left join slug
      on duplicates.family_document_import_id = slug.family_document_import_id
    order by slug.family_document_import_id desc, slug.created desc, slug.ctid desc
),
unique_doc_slugs as (
    select
      distinct ON (slug.family_document_import_id) slug.family_document_import_id, slug.created, slug.name
    from (
      SELECT
        slug.family_document_import_id as "family_document_import_id",
        count(*) as count
        from slug
        where slug.family_document_import_id is not null
        group by slug.family_document_import_id
        having count(*) = 1
    ) non_duplicates
    left join slug
      on non_duplicates.family_document_import_id = slug.family_document_import_id
    order by slug.family_document_import_id desc, slug.created desc, slug.ctid desc
), most_recent_doc_slugs as (
    select 
        deduplicated_doc_slugs.family_document_import_id as "family_document_import_id",
        deduplicated_doc_slugs.created,
        deduplicated_doc_slugs.name
    from deduplicated_doc_slugs
    UNION ALL
    select 
        unique_doc_slugs.family_document_import_id as "family_document_import_id",
        unique_doc_slugs.created,
        unique_doc_slugs.name
    from unique_doc_slugs
    order by family_document_import_id desc, created desc
), event_dates as (
select 
    family_event.family_import_id as family_import_id,
    min(case 
            when family_event.event_type_name='Passed/Approved' then family_event.date::date
            else family_event.date::date
    end) published_date,
    max(family_event.date::date) last_changed
from family_event 
group by family_import_id
)

SELECT
    n1.collection_import_ids as "Collection ID(s)",
    n1.collection_titles as "Collection Title(s)",
    n1.collection_descriptions as "Collection Description(s)",
    f.import_id as "Family ID",
    f.title as "Family Title",
    f.description as "Family Summary",
    fs.name as "Family Slug",
    d.import_id as "Document ID",
    p.title as "Document Title",
    INITCAP(d.document_role::TEXT) as "Document Role",
    d.variant_name as "Document Variant",
    ds.name as "Document Slug",
    p.source_url as "Document Content URL",
    d.document_type as "Document Type",
    CASE
       WHEN f.family_category = 'UNFCCC' THEN 'UNFCCC'
       ELSE INITCAP(f.family_category::TEXT)
       END "Document Category",
    n2.language as "Language",
    o.name as "Source",
    g.value as "Geography ISO",
    g.display_value as "Geography",
    array_to_string(ARRAY(SELECT jsonb_array_elements_text(fm.value->'topic')), ';') as "Topic/Response",
    array_to_string(ARRAY(SELECT jsonb_array_elements_text(fm.value->'hazard')), ';') as "Hazard",
    array_to_string(ARRAY(SELECT jsonb_array_elements_text(fm.value->'sector')), ';') as "Sector",
    array_to_string(ARRAY(SELECT jsonb_array_elements_text(fm.value->'keyword')), ';') as "Keyword",
    array_to_string(ARRAY(SELECT jsonb_array_elements_text(fm.value->'framework')), ';') as "Framework",
    array_to_string(ARRAY(SELECT jsonb_array_elements_text(fm.value->'instrument')), ';') as "Instrument",
    array_to_string(ARRAY(SELECT jsonb_array_elements_text(fm.value->'author')), ';') as "Author",
    array_to_string(ARRAY(SELECT jsonb_array_elements_text(fm.value->'author_type')), ';') as "Author Type",
    fp.published_date as "First event in timeline",
    fp.last_changed as "Last event in timeline",
    n3.event_type_names as "Full timeline of events (types)",
    n3.event_dates as "Full timeline of events (dates)",
    d.created::date as "Date Added to System",
    f.last_modified::date as " Last Modified on System"
FROM physical_document p 
JOIN family_document d
    ON p.id = d.physical_document_id
JOIN family f
    ON d.family_import_id = f.import_id
inner join geography g
    on g.id = f.geography_id
join family_organisation fo
    on fo.family_import_id = f.import_id
join organisation o
    on o.id = fo.organisation_id
join family_metadata fm
    on fm.family_import_id = f.import_id


FULL JOIN (
    SELECT
        collection_family.family_import_id as "family_import_id",
        string_agg(collection.import_id, ';') AS collection_import_ids,
        string_agg(collection.title, ';') AS collection_titles,
        string_agg(collection.description, ';') AS collection_descriptions
    FROM
        collection
    INNEr JOIN  collection_family ON collection_family.collection_import_id = collection.import_id
    GROUP  BY collection_family.family_import_id
) n1 ON n1.family_import_id=f.import_id

    
left JOIN (
    SELECT 
        p.id as "id",
        string_agg(l.name, ';' ORDER BY l.name) AS "language"
    FROM   physical_document p
    left join physical_document_language pdl
        on pdl.document_id = p.id
    left join language l
        on l.id = pdl.language_id
    GROUP  BY p.id
) n2 ON n2.id=d.physical_document_id


FULL JOIN (
    SELECT
        family_event.family_import_id,
        string_agg(family_event.import_id, ';') AS event_import_ids,
        string_agg(family_event.title, ';') AS event_titles,
        string_agg(family_event.event_type_name, ';') AS event_type_names,
        string_agg(family_event.date::date::text, ';') AS event_dates
    FROM
        family_event
    INNEr JOIN  family ON family.import_id = family_event.family_import_id
    GROUP  BY family_event.family_import_id
) n3 ON n3.family_import_id=f.import_id


LEFT JOIN most_recent_doc_slugs ds
    on ds.family_document_import_id = d.import_id
LEFT JOIN most_recent_family_slugs fs
    on fs.family_import_id = f.import_id
    
LEFT JOIN event_dates fp
    on fp.family_import_id = f.import_id

ORDER BY d.created desc, n1.family_import_id
"""
    return query


def get_whole_database_dump(db=Depends(get_db)):
    df = pd.read_sql_query(create_query(db), db.bind)
    df = replace_slug_with_qualified_url(df)
    return df


def replace_slug_with_qualified_url(
    df: pd.DataFrame, url_cols: list[str] = ["Family Slug", "Document Slug"]
) -> pd.DataFrame:
    public_app_url = os.getenv("PUBLIC_APP_URL", "https://app.climatepolicyradar.org")
    url_base = f"{public_app_url}/documents/"

    for col in url_cols:
        df[col] = url_base + df[col].astype(str)

    df.columns = df.columns.str.replace("Slug", "URL")
    return df


def convert_dump_to_csv(df: pd.DataFrame):
    csv_buffer = BytesIO()
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


def check_aws_credentials(aws_access_key_id, aws_secret_access_key, aws_region):
    try:
        client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region,
        )
        print(client.list_buckets())
        return True

    except Exception as e:
        if "The AWS Access Key Id you provided does not exist" not in str(e):
            return True
        return False


# def get_s3_client(aws_access_key_id, aws_secret_access_key, aws_region):
#     client = boto3.client(
#         "s3",
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key,
#         region_name=aws_region,
#     )
#     return client


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


def file_exists_in_s3(bucket_name: str, object_key: str):
    s3 = boto3.resource("s3")
    try:
        s3.Object(bucket_name, object_key).load()
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            _LOGGER.error("Cannot find the object in the given bucket")
        else:
            _LOGGER.exception("Something else has gone wrong.")
    return False


def get_file_from_s3(bucket_name, object_key):
    s3 = boto3.resource("s3")
    try:
        csv_obj = s3.Object(bucket_name, object_key).load()
        # csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
        body = csv_obj["Body"]
        csv_string = body.read().decode("utf-8")
        return csv_string
    except botocore.exceptions.ClientError:
        _LOGGER.exception("Something else has gone wrong.")


def upload_to_s3(
    s3, bucket_name: str, aws_env: str, file_key: str, csv_buffer: BytesIO
):
    print("Uploading to S3...")
    try:
        object = s3.Object(bucket_name, file_key)
        status_code = object.put(Body=csv_buffer)["ResponseMetadata"]["HTTPStatusCode"]
        if status_code == 200:
            print("Upload succeeded")
            return True
        else:
            print(f"Uploading dump to S3 returned status code '{status_code}'")
    except Exception as e:
        print(e)

    return False
