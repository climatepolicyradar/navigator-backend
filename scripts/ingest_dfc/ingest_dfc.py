#!/usr/bin/env python3

import sys
from app.db.session import SessionLocal
from sqlalchemy.orm import Session

from dfc_csv_reader import read
from dfc_processor import get_dfc_processor


if __name__ == "__main__":
    print("")
    print("Migrating to new schema...")
    db = SessionLocal()
    validate, process = get_dfc_processor(db)

    print("Checking database is in a valid state...")
    if not validate():
        print(" *** Database not in correct state to perform the script!")
        exit(1)

    print("Checking arguments...")
    if len(sys.argv) != 2:
        print(" *** Need to supply one CSV file as an argument")
        exit(1)

    filename = sys.argv[1]
    print(f"Reading CSV file {filename}...")
    read(filename, process)
    print("Complete")
