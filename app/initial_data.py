#!/usr/bin/env python3

from http.client import OK
import os
from sys import argv
from time import sleep
import requests

from sqlalchemy.exc import IntegrityError

from app.core.security import get_password_hash
from app.db.models.deprecated import User
from app.db.session import SessionLocal

from app.data_migrations import (
    populate_document_type,
    populate_document_role,
    populate_document_variant,
    populate_event_type,
    populate_geo_statistics,
    populate_geography,
    populate_language,
    populate_taxonomy,
)


def run_data_migrations(db):
    """Populate lookup tables with standard values"""
    populate_document_type(db)
    populate_document_role(db)
    populate_document_variant(db)
    populate_event_type(db)
    populate_geography(db)
    populate_language(db)
    populate_taxonomy(db)

    db.flush()  # Geography data is used by geo-stats so flush

    populate_geo_statistics(db)
    # TODO - framework, keyword, instrument, hazard


def create_user(db, email, password):
    with db.begin_nested():
        db_user = User(
            email=email,
            hashed_password=get_password_hash(password),
            is_active=True,
            is_superuser=True,
        )
        db.add(db_user)
        db.flush


def create_superuser(db) -> None:
    superuser_email = os.getenv("SUPERUSER_EMAIL")
    try:
        create_user(db, superuser_email, os.getenv("SUPERUSER_PASSWORD"))
    except IntegrityError:
        print(
            f"Skipping - superuser already exists with email/username {superuser_email}"
        )


def populate_initial_data(db):
    print("Creating superuser...")
    create_superuser(db)

    print("Running data migrations...")
    run_data_migrations(db)


def wait_for_app():
    url = os.getenv("API_HOST")
    health = f"{url}/health"

    # wait for health url
    for i in range(100):
        try:
            response = requests.get(health)
            if response.status_code == OK:
                return
        except requests.ConnectionError:
            pass

        sleep(1)
    raise TimeoutError()


if __name__ == "__main__":
    print("Creating initial data...")
    skip_wait = len(argv) > 1 and argv[1].lower() == "skip-wait"

    if not skip_wait:
        wait_for_app()

    db = SessionLocal()
    populate_initial_data(db)
    db.commit()
    print("Done creating initial data")
