import os
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

# Insert a DataFrame into a Snowflake table
def insert_dataframe(conn, table_name: str, df):
    if df is None or df.empty:
        print(f"Skipping {table_name}: no rows")
        return

    # Replace NaN values with None (which becomes NULL in SQL)
    df = df.where(pd.notna(df), None)

    cursor = conn.cursor()
    try:
        cols = list(df.columns)
        col_list = ", ".join(cols)
        placeholders = ", ".join(["%s"] * len(cols))
        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

        rows = []
        for row in df.itertuples(index=False, name=None):
            # Convert numpy nan to None
            cleaned_row = tuple(None if (isinstance(val, float) and np.isnan(val)) else val for val in row)
            rows.append(cleaned_row)
        
        cursor.executemany(sql, rows)
        conn.commit()
        print(f"Inserted {len(rows)} rows into {table_name}")
    finally:
        cursor.close()

