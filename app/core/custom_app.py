import logging
import os
from datetime import datetime
from typing import Optional, cast

import jwt
from dateutil.relativedelta import relativedelta
from db_client.models.dfce.family import Corpus
from fastapi import HTTPException, Request, status
from jwt import PyJWTError
from pydantic_core import Url
from sqlalchemy import distinct, select
from sqlalchemy.orm import Session

from app.api.api_v1.schemas.custom_app import CustomAppConfigDTO
from app.core import security

_LOGGER = logging.getLogger(__name__)
TOKEN_SECRET_KEY = os.environ["TOKEN_SECRET_KEY"]


class AppTokenFactory:
    def __init__(self) -> None:
        """
        :param Session db: The DB session to connect to.
        """
        self.db: Optional[Session] = None

        # TODO: revisit/configure access token expiry
        self.custom_app_token_expire_years: int = 10  # token valid for 10 years
        self.expected_args_length: int = 3

        # These will only be populated after a token has been decoded.
        self.allowed_corpora_ids: Optional[list[str]] = None
        self.exp: Optional[datetime] = None
        self.iat: Optional[datetime] = None
        self.iss: str = "Climate Policy Radar"
        self.sub: Optional[str] = None
        self.aud: Optional[str] = None

    @staticmethod
    def _contains_special_chars(input: str) -> bool:
        """Check if string contains any non alpha numeric characters.

        :param str input: A string to check.
        :return bool: True if string contains special chars, False otherwise.
        """
        if any(not char.isalnum() for char in input):
            return True
        return False

    @staticmethod
    def _parse_and_sort_corpora_ids(corpora_ids_str: str) -> list[str]:
        """Parse and sort the comma separated string of corpora IDs.

        :param str corpora_ids_str: A comma separated string containing the
            corpus import IDs that the custom app should show.
        :return list[str]: A list of corpora IDs sorted alphanumerically.
        """
        corpora_ids = corpora_ids_str.split(",")
        corpora_ids.sort()
        return corpora_ids

    def create_configuration_token(
        self, input: str, years: Optional[int] = None
    ) -> str:
        """Create a custom app configuration token.

        :param str input: A semi-colon delimited string containing in this
            order:
            1. A comma separated string containing the corpus import IDs
                that the custom app should show.
            2. A string containing the name of the theme.
            3. A string containing the hostname of the custom app.
        :return str: A JWT token containing the encoded allowed corpora.
        """
        expiry_years = years or self.custom_app_token_expire_years
        issued_at = datetime.utcnow()
        expire = issued_at + relativedelta(years=expiry_years)

        parts = input.split(";")
        if len(parts) != self.expected_args_length or any(
            len(part) < 1 for part in parts
        ):
            _LOGGER.error(f"Expected exactly {self.expected_args_length} arguments")
            raise ValueError

        corpora_ids, subject, audience = parts

        config = CustomAppConfigDTO(
            allowed_corpora_ids=self._parse_and_sort_corpora_ids(corpora_ids),
            subject=subject,
            issuer=self.iss,
            audience=audience,
            expiry=expire,
            issued_at=int(
                datetime.timestamp(issued_at.replace(microsecond=0))
            ),  # No microseconds
        )

        if self._contains_special_chars(config.subject):
            _LOGGER.error(
                "Subject must not contain any special characters, including spaces"
            )
            raise ValueError

        msg = "Creating custom app configuration token that expires on "
        msg += f"{expire.strftime('%a %d %B %Y at %H:%M:%S:%f')} "
        msg += f"for the following corpora: {corpora_ids}"
        print(msg)

        to_encode = {
            "allowed_corpora_ids": config.allowed_corpora_ids,
            "exp": config.expiry,
            "iat": config.issued_at,
            "iss": config.issuer,
            "sub": config.subject,
            "aud": str(config.audience),
        }
        return jwt.encode(to_encode, TOKEN_SECRET_KEY, algorithm=security.ALGORITHM)

    @staticmethod
    def get_origin(request: Request) -> Optional[str]:
        origin = request.headers.get("origin")

        if origin is not None:
            origin = Url(origin).host
        return origin

    def verify_corpora_in_db(self, db: Session, any_exist: bool = True) -> bool:
        """Validate given corpus IDs against the existing corpora in DB.

        :param bool any_exist: Whether to check any or all corpora are
            valid.
        :return bool: Return whether or not the corpora are valid.
        """
        if self.allowed_corpora_ids is None:
            return False

        corpora_ids_from_db = cast(
            list, db.scalars(select(distinct(Corpus.import_id))).all()
        )

        if any_exist:
            validate_success = any(
                corpus in corpora_ids_from_db for corpus in self.allowed_corpora_ids
            )
        else:
            validate_success = all(
                corpus in corpora_ids_from_db for corpus in self.allowed_corpora_ids
            )

        if validate_success:
            not_in_db = set(self.allowed_corpora_ids).difference(corpora_ids_from_db)
            if not_in_db != set():
                _LOGGER.warning(f"Some corpora in app token {not_in_db} not in DB")

        return validate_success

    @staticmethod
    def validate_corpora_ids(
        corpora_ids: set[str], valid_corpora_ids: set[str]
    ) -> bool:
        """Validate all given corpus IDs against a list of allowed corpora.

        :param set[str] corpora_ids: The corpus import IDs we want to
            validate.
        :param set[str] valid_corpora_ids: The corpus import IDs
            we want to validate against.
        :return bool: Return whether or not all the corpora are valid.
        """
        validate_success = corpora_ids.issubset(valid_corpora_ids)
        if not validate_success:
            invalid_corpora = set(corpora_ids).difference(valid_corpora_ids)
            _LOGGER.warning(
                f"Some corpora in search request params {invalid_corpora}"
                "forbidden to search against."
            )
        return validate_success

    def decode(self, token: str, audience: Optional[str]) -> list[str]:
        """Decodes a configuration token.

        :param str token : A JWT token that has been encoded with a list of
            allowed corpora ids that the custom app should show, an expiry
            date and an issued at date.
        :param Optional[str] audience: An audience to verify against.
        :return list[str]: A decoded list of valid corpora ids.
        """
        try:
            decoded_token = jwt.decode(
                token,
                TOKEN_SECRET_KEY,
                algorithms=[security.ALGORITHM],
                issuer=self.iss,
                audience=audience,
                options={"verify_aud": False},
            )
        except PyJWTError as e:
            _LOGGER.error(e)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Could not decode configuration token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        self.allowed_corpora_ids = decoded_token.get("allowed_corpora_ids")
        self.aud = decoded_token.get("aud")
        self.exp = decoded_token.get("exp")
        self.iat = decoded_token.get("iat")
        self.iss = decoded_token.get("iss")
        self.sub = decoded_token.get("sub")
        self.iat = decoded_token.get("iat")

        return decoded_token

    def validate(self, db: Session, any_exist: bool = True) -> None:
        if not self.verify_corpora_in_db(db, any_exist):
            msg = "Error verifying corpora IDs."
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=msg,
                headers={"WWW-Authenticate": "Bearer"},
            )

    def validate_subset(
        self, corpora_ids: set[str], valid_corpora_ids: set[str]
    ) -> None:
        if not self.validate_corpora_ids(corpora_ids, valid_corpora_ids):
            msg = "Error validating corpora IDs."
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=msg,
                headers={"WWW-Authenticate": "Bearer"},
            )

    def decode_and_validate(
        self, db: Session, request: Request, token: str, any_exist: bool = True
    ):
        origin = self.get_origin(request)

        # Decode the app token and validate it.
        self.decode(token, origin)

        # First corpora validation is app token against DB. At least one of the app token
        # corpora IDs must be present in the DB to continue the search request.
        self.validate(db, any_exist)
