import os
import uuid
import snowflake.connector
from dotenv import load_dotenv
import pandas as pd
import numpy as np

load_dotenv(override=True)


# Snowflake connection and loading logic
def get_connection():
    user = os.getenv("USER")
    account = os.getenv("ACCOUNT")
    jwt_token = os.getenv("JWT_TOKEN")

    conn = snowflake.connector.connect(
        user=user,
        account=account,
        password=jwt_token,
        warehouse=os.getenv("WAREHOUSE"),
        database=os.getenv("DATABASE"),
        schema=os.getenv("SCHEMA"),
        role=os.getenv("ROLE"),
    )
    return conn


def _infer_key_columns(table_name: str):
    """Infer key column(s) from common table name patterns."""
    name = table_name.lower()
    if "teams" in name:
        return ["team_id"]
    if "players" in name and "player_game" not in name:
        return ["player_id"]
    if "games" in name:
        return ["game_id"]
    if "batting" in name or "player_game_batting" in name:
        return ["game_id", "player_id"]
    if "pitching" in name or "player_game_pitching" in name:
        return ["game_id", "player_id"]
    # Fallback: try common id
    return ["id"]


def upsert_dataframe(conn, table_name: str, df: pd.DataFrame, key_columns: list = None):
    """Upsert a DataFrame into a Snowflake table using a staging temp table + MERGE.

    - Creates a temporary table LIKE the target.
    - Inserts rows into the temp table.
    - MERGEs temp into target using key_columns for matching.

    If `key_columns` is None, the function will attempt to infer sensible keys from the
    `table_name` string.
    """
    if df is None or df.empty:
        print(f"Skipping {table_name}: no rows")
        return

    # Replace NaN values with None (which becomes NULL in SQL)
    df = df.where(pd.notna(df), None)

    if key_columns is None:
        key_columns = _infer_key_columns(table_name)

    cols = list(df.columns)
    if not set(key_columns).issubset(set(cols)):
        raise ValueError(f"Key columns {key_columns} not present in DataFrame columns {cols}")

    # Prepare temporary staging table name
    tmp_name = f"TMP_STG_{uuid.uuid4().hex[:8].upper()}"

    cursor = conn.cursor()
    try:
        # Create temp table like target
        cursor.execute(f"CREATE TEMPORARY TABLE {tmp_name} LIKE {table_name}")

        # Insert into temp table
        col_list = ", ".join(cols)
        placeholders = ", ".join(["%s"] * len(cols))
        insert_sql = f"INSERT INTO {tmp_name} ({col_list}) VALUES ({placeholders})"

        rows = []
        for row in df.itertuples(index=False, name=None):
            cleaned_row = tuple(None if (isinstance(val, float) and np.isnan(val)) else val for val in row)
            rows.append(cleaned_row)

        if rows:
            # executemany in reasonably sized batches to avoid giant statements
            batch_size = 1000
            for i in range(0, len(rows), batch_size):
                cursor.executemany(insert_sql, rows[i:i+batch_size])

        # Build MERGE statement
        on_clause = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])

        update_set = ", ".join([f"{col} = source.{col}" for col in cols if col not in key_columns])
        insert_cols = ", ".join(cols)
        insert_vals = ", ".join([f"source.{col}" for col in cols])

        merge_sql = f"MERGE INTO {table_name} AS target USING {tmp_name} AS source ON {on_clause} "
        if update_set:
            merge_sql += f"WHEN MATCHED THEN UPDATE SET {update_set} "
        merge_sql += f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"

        cursor.execute(merge_sql)
        conn.commit()
        print(f"Upserted {len(rows)} rows into {table_name} (temp {tmp_name})")
    finally:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {tmp_name}")
        except Exception:
            pass
        cursor.close()

