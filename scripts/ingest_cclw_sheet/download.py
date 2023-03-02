from __future__ import print_function
import json

from os import environ

from google.oauth2 import service_account
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError


# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = "121apkfpDhIMKctUL47egb7iFT4KS_vW3DarWyGcu9rg"
RANGE_NAME = "A:AI"


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
