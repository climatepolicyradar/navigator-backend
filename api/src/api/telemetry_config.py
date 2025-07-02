import json
import socket
from pathlib import Path

from opentelemetry.sdk.resources import Resource
from pydantic import BaseModel, Field, ValidationError, field_validator
from pydantic_settings import BaseSettings


class ServiceManifest(BaseModel):
    class Input(BaseModel):
        type: str
        name: str

    service_name: str = Field(alias="service.name")
    service_namespace: str = Field(alias="service.namespace")
    team: str
    inputs: list[Input]
    outputs: list[str]
    repos: list[str]

    @classmethod
    def from_file(cls, file_path: str | Path) -> "ServiceManifest":
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
                return cls.model_validate(data)
        except FileNotFoundError:
            raise FileNotFoundError(f"Manifest file not found: {file_path}")
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Invalid JSON in manifest file: {str(e)}", e.doc, e.pos
            )
        except ValidationError as e:
            raise ValueError(f"Invalid manifest schema: {str(e)}")


class TelemetryConfig(BaseSettings):
    """
    Configuration for the telemetry SDK using Pydantic BaseSettings.

    Order of preference for config:
    1. Config object passed to constructor
    2. Environment variables (automatically handled by Pydantic)
    3. Secrets manager (via custom validators) (TODO)
    """

    # Required parameters
    service_name: str
    environment: str

    # Optional in OTLP conventions, but I think should be required for us
    namespace_name: str
    service_version: str

    # Optional parameters with defaults
    component_name: str = Field(default="MAIN")
    otlp_endpoint: str = Field(default="")
    resource_attributes: str = Field(default="")
    log_level: str = Field(default="INFO")

    # Automatic attributes
    hostname: str = Field(default="")
    service_instance_id: str = Field(default="")

    @classmethod
    def from_service_manifest(
        cls, service_manifest: ServiceManifest, environment: str, version: str
    ):
        """
        Create a TelemetryConfig from a service manifest.

        :param service_manifest: A dictionary containing service manifest information
        :return: A TelemetryConfig instance
        """

        otel_endpoint = (
            "https://otel.prod.climatepolicyradar.org"
            if environment == "production"
            else "https://otel.staging.climatepolicyradar.org"
        )

        service_instance_id = (
            f"{service_manifest.service_name}-{environment}-{socket.gethostname()}"
        )

        return cls(
            service_name=service_manifest.service_name,
            namespace_name=service_manifest.service_namespace,
            environment=environment,
            service_instance_id=service_instance_id,
            otlp_endpoint=otel_endpoint,
            service_version=version,
        )

    def __str__(self):
        return f"TelemetryConfig(service_name={self.service_name}, namespace_name={self.namespace_name}, environment={self.environment}, service_instance_id={self.service_instance_id}, otlp_endpoint={self.otlp_endpoint}, service_version={self.service_version})"

    class Config:
        """Pydantic config"""

        env_prefix = ""  # Use exact environment variable names (e.g., SERVICE_NAME)
        case_sensitive = False  # So we can uppercase our env vars

    @field_validator("hostname", mode="before")
    @classmethod
    def set_hostname(cls, v):
        """Set hostname automatically if not provided"""
        return v or socket.gethostname()

    @field_validator("service_instance_id", mode="before")
    @classmethod
    def set_service_instance_id(cls, v, info):
        """Generate service instance ID from other fields"""
        if v:
            return v

        values = info.data
        service_name = values.get("service_name")
        environment = values.get("environment")
        hostname = values.get("hostname") or socket.gethostname()

        if all([service_name, environment, hostname]):
            return f"{service_name}-{environment}-{hostname}"
        return v

    def to_resource(self) -> Resource:
        """Returns an opentelemetry resource hydrated with config values"""
        return Resource(
            attributes={
                "service.name": self.service_name,
                "host.name": self.hostname,
                "service.instance.id": self.service_instance_id,
                "resource.attributes": self.resource_attributes,
                "component.name": self.component_name,
                "environment": self.environment,
            }
        )

    def get_logging_config(self) -> dict:
        """Returns a python logging config dict to standardise logging across services"""
        return {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
                },
            },
            "handlers": {
                "default": {
                    "level": self.log_level,
                    "formatter": "standard",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",  # Default is stderr
                },
            },
            "loggers": {
                "": {  # root logger
                    "level": self.log_level,
                    "handlers": ["default"],
                    "propagate": False,
                },
                "uvicorn.error": {
                    "level": "DEBUG",
                    "handlers": ["default"],
                },
                "uvicorn.access": {
                    "level": "DEBUG",
                    "handlers": ["default"],
                },
            },
        }
