import logging

from fastapi import APIRouter, HTTPException, status

from app.api.api_v1.schemas.custom_app import CustomAppConfigDTO
from app.core.custom_app import encode_configuration_token

custom_app_router = r = APIRouter()

_LOGGER = logging.getLogger(__file__)


@r.post("/custom_app_tokens")
async def create_config_token(form_data: CustomAppConfigDTO):
    _LOGGER.info(
        "Custom app configuration token requested",
        extra={"props": {"allowed_corpora_ids": form_data.allowed_corpora_ids}},
    )

    token = encode_configuration_token(form_data.allowed_corpora_ids, form_data.years)
    if token is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unable to generate custom app configuration token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    _LOGGER.info(
        "Custom app configuration token generated",
        extra={"props": {"allowed_corpora_ids": form_data.allowed_corpora_ids}},
    )
    return {"access_token": token, "token_type": "bearer"}
