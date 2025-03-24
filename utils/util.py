import configparser
import hashlib
import os
import pandas as pd
import requests

from fastapi import Security, HTTPException
from fastapi.security.api_key import APIKeyHeader
from google.auth.impersonated_credentials import Credentials as ImpersonatedCredentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from googleapiclient.discovery import build

from utils.decorators import retry, timeout


CONFIG = configparser.ConfigParser()
CONFIG.read("app-config.ini")
_WEBHOOK = CONFIG.get("notifications", "slack")
_SCOPES = CONFIG.get("google", "scopes").split(",")
_API_KEY_HEADER = APIKeyHeader(name=CONFIG.get("security", "header"), auto_error=False)

@retry(Exception, 3, 5)
@timeout(30)
def get_credentials() -> ImpersonatedCredentials:

    source_creds = ServiceAccountCredentials.from_service_account_file(
        CONFIG.get("google", "credentials"), scopes=_SCOPES
    )
    priv_creds = ImpersonatedCredentials(
        source_credentials=source_creds,
        target_principal=CONFIG.get("google", "priv_account"),
        target_scopes=_SCOPES,
        lifetime=180,
    )
    return priv_creds


@retry(Exception, 3, 5)
@timeout(30)
def write_to_google_sheet(values, sheet_name):
    credentials = get_credentials()
    drive_api = build("drive", "v3", credentials=credentials)
    file_metadata = {
        "name": sheet_name,
        "parents": [CONFIG.get("google", "parent_folder_id")],
        "mimeType": "application/vnd.google-apps.spreadsheet",
    }
    spreadsheet = (
        drive_api.files().create(body=file_metadata, supportsAllDrives=True).execute()
    )
    sheets_api = build("sheets", "v4", credentials=credentials)
    # fmt: off
    result = sheets_api.spreadsheets().values().update(
        body={"values": values},
        spreadsheetId=spreadsheet["id"],
        range="Sheet1!A1",
        valueInputOption="USER_ENTERED",
    ).execute()
    # fmt: on
    print(result)


@retry(Exception, 3, 5)
@timeout(30)
def notify(message: str):
    response = requests.post(
        _WEBHOOK, headers={"content-type": "application/json"}, json={"text": message}
    )
    response.raise_for_status()


def dedupe_data(data: list[dict], dedupe_on_fields: list[str]):
    df = pd.DataFrame(data)
    df.drop_duplicates(subset=["datashake_review_uuid"], inplace=True)
    df.drop_duplicates(subset=dedupe_on_fields, inplace=True)
    df.date = df.date.apply(lambda d: d.isoformat())
    return df.to_dict("records")


def validate_api_key(api_key_header: str = Security(_API_KEY_HEADER)):
    if api_key_header is None:
        raise HTTPException(status_code=400, detail="authentication is required")
    if not verify_api_key(api_key_header):
        raise HTTPException(status_code=401, detail="invalid authentication")
    return api_key_header


def verify_api_key(api_key: str):
    expected_key = CONFIG.get("security", "hashed_key")
    salt = CONFIG.get("security", "salt")
    salted_api_key = bytes.fromhex(salt) + api_key.encode("utf-8")
    return hashlib.sha256(salted_api_key).hexdigest() == expected_key
