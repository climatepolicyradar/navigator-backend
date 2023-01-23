import os
from dotenv import load_dotenv

load_dotenv()

PIPELINE_BUCKET: str = os.environ.get("PIPELINE_BUCKET")
S3_PREFIXES: list[str] = os.environ.get(
    "S3_PREFIXES", "parser_input,embeddings_input,indexer_input"
).split(",")
AWS_REGION = os.environ.get("AWS_REGION", "eu-west-1")
API_HOST_LOCAL = os.environ.get("API_HOST_LOCAL", "localhost:8888")
