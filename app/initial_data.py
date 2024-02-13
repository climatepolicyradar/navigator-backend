#!/usr/bin/env python3

from http.client import OK
import os
from sys import argv
from time import sleep
import requests

from db_client.initial_data import populate_initial_data

from app.db.session import SessionLocal
from app.core.security import get_password_hash


def wait_for_app():
    url = os.getenv("API_HOST")
    health = f"{url}/health"
    attempts = 100

    # wait for health url
    for i in range(attempts):
        try:
            response = requests.get(health)
            if response.status_code == OK:
                return
            else:
                print(f"Health code: {response.status_code}, retry: {i}/{attempts}")
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
    populate_initial_data(db, get_password_hash)
    db.commit()
    print("Done creating initial data")
