#!/usr/bin/env python3

import sys
from app.db.models.deprecated import Document
from app.db.models.document import PhysicalDocument
from app.db.session import SessionLocal
from sqlalchemy.orm import Session


def valid_db_state(db: Session):

    num_old_documents = db.query(Document).count()
    num_new_documents = db.query(PhysicalDocument).count()
    print(f"Found {num_new_documents} new and {num_old_documents} old documents")
    total = num_old_documents + num_new_documents
    return total > 0


if __name__ == "__main__":
    print("Migrating to new schema...")
    db = SessionLocal()

    print("Checking database is in a valid state...")
    if not valid_db_state(db):
        print(" *** Database not in correct state to perform the script!")
        exit(1)

    if len(sys.argv) != 2:
        print(" *** Need to supply one CSV file as an argument")
        exit(1)

    filename = sys.argv[1]
    print(f"Reading CSV file {filename}...")
    

    print("Complete")