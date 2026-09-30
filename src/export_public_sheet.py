import os
import pandas as pd
import snowflake.connector
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


def build_export_views():
    return {
        "games": "PUBLIC_EXPORT.GAMES",
        "team_game": "PUBLIC_EXPORT.TEAM_GAME",
        "batting": "PUBLIC_EXPORT.BATTING",
        "pitching": "PUBLIC_EXPORT.PITCHING",
    }


def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("USER"),
        account=os.getenv("ACCOUNT"),
        password=os.getenv("JWT_TOKEN"),
        warehouse=os.getenv("WAREHOUSE"),
        database=os.getenv("DATABASE"),
        schema=os.getenv("SCHEMA"),
        role=os.getenv("ROLE"),
    )


def get_google_sheet_client():
    creds_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not creds_path:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON is not set")

    creds = Credentials.from_service_account_file(
        creds_path,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
        ],
    )
    return gspread.authorize(creds)


def open_workbook(client):
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if sheet_id:
        return client.open_by_key(sheet_id)

    workbook_name = os.getenv("GOOGLE_SHEET_NAME", "MLB Public Dashboard")
    return client.open(workbook_name)


def fetch_view(conn, view_name: str) -> pd.DataFrame:
    query = f"SELECT * FROM {view_name}"
    return pd.read_sql(query, conn)


def write_sheet(workbook, sheet_name: str, df: pd.DataFrame):
    if sheet_name in [ws.title for ws in workbook.worksheets()]:
        worksheet = workbook.worksheet(sheet_name)
        worksheet.clear()
    else:
        worksheet = workbook.add_worksheet(title=sheet_name, rows="1000", cols="50")

    if df.empty:
        worksheet.update([[]])
        return

    headers = list(df.columns)
    values = [headers] + df.astype(object).where(pd.notna(df), None).values.tolist()
    worksheet.update(values)


def export_all():
    conn = get_connection()
    try:
        client = get_google_sheet_client()
        workbook = open_workbook(client)

        views = build_export_views()

        for sheet_name, view_name in views.items():
            df = fetch_view(conn, view_name)
            write_sheet(workbook, sheet_name, df)
            print(f"Exported {len(df)} rows to {sheet_name}")

    finally:
        conn.close()


if __name__ == "__main__":
    export_all()
