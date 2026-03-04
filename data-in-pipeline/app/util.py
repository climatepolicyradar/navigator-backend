import logging
import os
import uuid

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from prefect.settings import PREFECT_UI_URL
from prefect_slack import SlackCredentials

from app.bootstrap_telemetry import pipeline_metrics
from app.pipeline_metrics import ErrorType, Operation

_LOGGER = logging.getLogger(__name__)

AWS_REGION = "eu-west-1"


def generate_envelope_uuid() -> str:
    """Generate a unique UUID for an extracted envelope."""
    return str(uuid.uuid4())


def get_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        config=Config(
            connect_timeout=2, retries={"max_attempts": 3, "mode": "standard"}
        ),
    )


def get_api_url() -> str:
    """
    Returns the API URL based on the environment.
    """
    return os.getenv("API_BASE_URL", "https://api.staging.climatepolicyradar.org")


def upload_file(
    client,
    json_content,
    bucket: str,
    key: str,
    content_type: str | None = "application/json; charset=utf-8",
) -> None:
    """
    Upload a file to an S3 bucket by providing its filename.

    :param [str] file_name: name of the file to upload.
    :param [str] bucket: name of the bucket to upload the file to.
    :param [str | None] key: filename of the resulting file on s3. Should include
        the file extension. If not provided, the name of the local file is used.
    :param [str | None] content_type: optional content-type of the file
    """

    # Upload the file
    try:
        client.put_object(
            Body=json_content, Bucket=bucket, Key=key, ContentType=content_type
        )

    except Exception:
        _LOGGER.exception(f"Uploading {key} encountered an error")
        pipeline_metrics.record_error(Operation.LOAD, ErrorType.STORAGE)
        raise


def get_aws_session() -> boto3.Session:
    """
    Get a boto3 session configured with the AWS profile and region from config.

    In local development, uses the AWS_PROFILE.
    In containerized environments (ECS), uses the task IAM role (profile_name=None).
    """
    return boto3.Session(
        profile_name=os.getenv("AWS_PROFILE"), region_name=os.getenv("AWS_REGION")
    )


def get_ssm_client() -> BaseClient:
    """Get an SSM client using the configured session."""
    session = get_aws_session()
    return session.client("ssm")


def get_ssm_parameter(name: str, with_decryption: bool = True) -> str:
    """
    Get a parameter from AWS Systems Manager Parameter Store.

    Args:
        name: The name of the parameter to retrieve
        with_decryption: Whether to decrypt SecureString parameters (default: True)

    Returns:
        The parameter value as a string
    """
    ssm = get_ssm_client()
    response = ssm.get_parameter(Name=name, WithDecryption=with_decryption)
    return response["Parameter"]["Value"]


class SlackNotify:
    """Notify a Slack channel through a Prefect Slack webhook."""

    # Message templates
    FLOW_RUN_URL = "{prefect_base_url}/flow-runs/flow-run/{flow_run.id}"
    SLACK_NAVIGATOR_NOTIFIER_BOT_BLOCK = "slack-navigator-notifier-bot"
    BASE_MESSAGE = (
        "💥 Flow run <{ui_url}|{flow.name}/{flow_run.name}> "
        "state `{flow_run.state.name}` at {flow_run.state.timestamp}.\n"
        "Error message: {state.message}"
    )

    # Block name
    slack_channel_name = "alerts-prod"

    @classmethod
    def get_environment(cls) -> str:
        """Get the current environment."""
        return os.getenv("AWS_ENV", "sandbox")

    @classmethod
    def get_slack_block_name(cls) -> str:
        """Get the slack block name for the current environment."""
        return f"slack-webhook-{cls.slack_channel_name}-prefect-mvp-{cls.get_environment()}"

    @classmethod
    async def message(cls, flow, flow_run, state):
        """
        Send a notification to a Slack channel about the state of a Prefect flow run.

        Intended to be called from prefect flow hooks:

        ```python
        @flow(on_failure=[SlackNotify.message])
        def my_flow():
            pass
        ```
        """
        if cls.get_environment() != "prod":
            return None

        ui_url = cls.FLOW_RUN_URL.format(
            prefect_base_url=PREFECT_UI_URL.value(), flow_run=flow_run
        )

        slack_credentials_block = SlackCredentials.load(
            cls.SLACK_NAVIGATOR_NOTIFIER_BOT_BLOCK
        )
        client = slack_credentials_block.get_client()

        msg = cls.BASE_MESSAGE.format(
            flow=flow,
            flow_run=flow_run,
            state=state,
            ui_url=ui_url,
            environment=cls.get_environment(),
        )

        await client.chat_postMessage(
            channel=str(cls.slack_channel_name),
            text=msg,
            blocks=cls.SLACK_NAVIGATOR_NOTIFIER_BOT_BLOCK,
        )
