import json
from pathlib import Path

from pydantic import BaseModel, Field, ValidationError


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
