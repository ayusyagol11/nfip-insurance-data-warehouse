"""
Bronze layer loader using Python + pyodbc.

BULK INSERT is not supported on Azure SQL Edge, so this is the primary
loader for the Bronze layer. Connects to SQL Server, reads each state
CSV with pandas, and inserts into the Bronze tables with metadata columns.

Usage:
    python scripts/bronze/load_via_python.py
"""

import os
import time
import pandas as pd
import pyodbc

# Try ODBC Driver 18 first, fall back to 17
ODBC_DRIVER = None
for driver in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]:
    if driver in pyodbc.drivers():
        ODBC_DRIVER = driver
        break

if ODBC_DRIVER is None:
    raise RuntimeError(
        "No suitable ODBC driver found. Install ODBC Driver 17 or 18 for SQL Server."
    )

CONNECTION_STRING = (
    f"Driver={{{ODBC_DRIVER}}};"
    "Server=localhost,1433;"
    "Database=NfipInsuranceWarehouse;"
    "UID=sa;"
    "PWD=NfipWarehouse2026!;"
    "TrustServerCertificate=yes"
)

TARGET_STATES = ["FL", "LA", "TX", "NJ", "NY"]
BATCH_ID = 1

DATASETS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "datasets")
CLAIMS_DIR = os.path.join(DATASETS_DIR, "claims")
POLICIES_DIR = os.path.join(DATASETS_DIR, "policies")

CLAIMS_API = "https://www.fema.gov/api/open/v2/FimaNfipClaims"
POLICIES_API = "https://www.fema.gov/api/open/v2/FimaNfipPolicies"


def to_clean_rows(df):
    """Convert DataFrame to list of tuples with only str or None values."""
    clean_rows = []
    for _, row in df.iterrows():
        clean_row = []
        for val in row:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                clean_row.append(None)
            elif pd.isna(val):
                clean_row.append(None)
            else:
                s = str(val)
                if s in ('nan', 'None', 'NaT', '', 'null', 'NULL'):
                    clean_row.append(None)
                else:
                    clean_row.append(s)
        clean_rows.append(tuple(clean_row))
    return clean_rows


def get_connection():
    """Establish pyodbc connection to SQL Server."""
    print(f"Connecting to SQL Server (driver: {ODBC_DRIVER})...")
    conn = pyodbc.connect(CONNECTION_STRING)
    conn.autocommit = True
    print("Connected.")
    return conn


def load_claims(conn):
    """Load all claims CSVs into bronze.nfip_claims_raw."""
    cursor = conn.cursor()
    cursor.fast_executemany = True
    cursor.execute("TRUNCATE TABLE bronze.nfip_claims_raw")
    print("\nTruncated bronze.nfip_claims_raw")

    total = 0
    for state in TARGET_STATES:
        csv_path = os.path.join(CLAIMS_DIR, f"{state}_claims.csv")
        if not os.path.exists(csv_path):
            print(f"  {state}: CSV not found, skipping")
            continue

        start = time.time()
        df = pd.read_csv(csv_path, dtype=str)

        # Log null profile before adding metadata
        null_counts = df.isna().sum()
        print(f"  {state} null profile: {null_counts[null_counts > 0].to_dict()}")

        df["batch_id"] = str(BATCH_ID)
        df["ingestion_timestamp"] = pd.Timestamp.now().isoformat()
        df["source_state"] = state
        df["source_api_endpoint"] = CLAIMS_API

        cols = list(df.columns)
        placeholders = ",".join(["?"] * len(cols))
        col_names = ",".join(cols)
        sql = f"INSERT INTO bronze.nfip_claims_raw ({col_names}) VALUES ({placeholders})"

        rows = to_clean_rows(df)
        batch_size = 1000
        for i in range(0, len(rows), batch_size):
            try:
                cursor.executemany(sql, rows[i:i + batch_size])
            except pyodbc.DataError as e:
                print(f"  DataError in batch starting at row {i}. Diagnosing...")
                for idx, row in enumerate(rows[i:i + batch_size]):
                    try:
                        cursor.execute(sql, row)
                    except pyodbc.DataError:
                        print(f"  Problem row {i + idx}: {row}")
                        print(f"  Types: {[type(v).__name__ for v in row]}")
                        break
                raise

        elapsed = time.time() - start
        print(f"  {state}: {len(df):,} claims loaded in {elapsed:.1f}s")
        total += len(df)

    print(f"  Total claims loaded: {total:,}")
    return total


def load_policies(conn):
    """Load all policies CSVs into bronze.nfip_policies_raw."""
    cursor = conn.cursor()
    cursor.fast_executemany = True
    cursor.execute("TRUNCATE TABLE bronze.nfip_policies_raw")
    print("\nTruncated bronze.nfip_policies_raw")

    total = 0
    for state in TARGET_STATES:
        csv_path = os.path.join(POLICIES_DIR, f"{state}_policies.csv")
        if not os.path.exists(csv_path):
            print(f"  {state}: CSV not found, skipping")
            continue

        start = time.time()
        df = pd.read_csv(csv_path, dtype=str)

        # Log null profile before adding metadata
        null_counts = df.isna().sum()
        print(f"  {state} null profile: {null_counts[null_counts > 0].to_dict()}")

        df["batch_id"] = str(BATCH_ID)
        df["ingestion_timestamp"] = pd.Timestamp.now().isoformat()
        df["source_state"] = state
        df["source_api_endpoint"] = POLICIES_API

        cols = list(df.columns)
        placeholders = ",".join(["?"] * len(cols))
        col_names = ",".join(cols)
        sql = f"INSERT INTO bronze.nfip_policies_raw ({col_names}) VALUES ({placeholders})"

        rows = to_clean_rows(df)
        batch_size = 1000
        for i in range(0, len(rows), batch_size):
            try:
                cursor.executemany(sql, rows[i:i + batch_size])
            except pyodbc.DataError as e:
                print(f"  DataError in batch starting at row {i}. Diagnosing...")
                for idx, row in enumerate(rows[i:i + batch_size]):
                    try:
                        cursor.execute(sql, row)
                    except pyodbc.DataError:
                        print(f"  Problem row {i + idx}: {row}")
                        print(f"  Types: {[type(v).__name__ for v in row]}")
                        break
                raise

        elapsed = time.time() - start
        print(f"  {state}: {len(df):,} policies loaded in {elapsed:.1f}s")
        total += len(df)

    print(f"  Total policies loaded: {total:,}")
    return total


def main():
    print("=" * 60)
    print("Bronze Layer Python Loader")
    print("=" * 60)

    conn = get_connection()

    claims_total = load_claims(conn)
    policies_total = load_policies(conn)

    conn.close()

    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Claims:   {claims_total:>10,}")
    print(f"  Policies: {policies_total:>10,}")
    print(f"  Total:    {claims_total + policies_total:>10,}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
