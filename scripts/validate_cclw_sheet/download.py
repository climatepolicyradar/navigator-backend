"""
Script to download the Google Sheet

Requires:
1) A Service Account to be configured
2) The sheet in question has been "Shared" with this account's email
3) GOOGLE_SERVICE_ACCOUNT is set in the current environment, with the
    contents of the json service account details (downloaded when created)
"""

from __future__ import print_function

import json
from os import environ

# https://developers.google.com/sheets/api/quickstart/python
# https://googleapis.dev/python/google-auth/latest/_modules/google/oauth2/service_account.html
from google.oauth2 import service_account
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = "10xcBKgiYoT7eWQoyoWYlELO_z7TC1XE-_CIkC0vONWk"
RANGE_NAME = "CPR_MASTER_DOCS_1.0.2!A:ZZ"


def double_quote_quotes(col: str) -> str:
    return col.replace('"', '""')


def quote_string(col: str) -> str:
    return f'"{double_quote_quotes(col)}"'


def print_as_csv():
    """Prints the sheet as a CSV"""
    client_config = json.loads(environ["GOOGLE_SERVICE_ACCOUNT"])
    credentials = service_account.Credentials.from_service_account_info(
        client_config, scopes=SCOPES
    )

    try:
        service = build("sheets", "v4", credentials=credentials)

        # Call the Sheets API
        sheet: Resource = service.spreadsheets()

        result = (
            sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
        )
        values = result.get("values", [])

        if not values:
            print("No data found.")
            return

        for row in values:
            print(",".join(quote_string(col) for col in row))

    except HttpError as err:
        print(err)


if __name__ == "__main__":
    print_as_csv()
