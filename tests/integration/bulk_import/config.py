import os
from dotenv import load_dotenv

load_dotenv()

PIPELINE_BUCKET: str = os.getenv("PIPELINE_BUCKET")
S3_PREFIXES: list[str] = os.getenv(
    "S3_PREFIXES", "parser_input,embeddings_input,indexer_input"
).split(",")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-1")
API_HOST = os.getenv("API_HOST", "http://localhost:8888")
