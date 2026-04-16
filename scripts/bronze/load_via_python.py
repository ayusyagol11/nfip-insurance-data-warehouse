"""
Bronze layer loader using Python + pyodbc.

BULK INSERT is not supported on Azure SQL Edge, so this is the primary
loader for the Bronze layer. Connects to SQL Server, reads each state
CSV with pandas, and inserts into the Bronze tables with metadata columns.

The loader dynamically creates Bronze tables based on actual CSV headers
from the OpenFEMA API, so column names always match regardless of API changes.

Features:
    - Dynamic table creation from CSV headers
    - Connection retry on TCP drops (08S01)
    - Per-batch commit so progress survives crashes
    - Resumable: skips states already loaded (set RELOAD_ALL=True to override)

Usage:
    python scripts/bronze/load_via_python.py
"""

import os
import time
import pandas as pd
import pyodbc

# --- Configuration -----------------------------------------------------------

# Set to True to force reload all states (drops and recreates tables)
RELOAD_ALL = False

BATCH_SIZE = 500
BATCH_DELAY = 0.5       # seconds between batches
MAX_RETRIES = 3
RETRY_DELAY = 5         # seconds before reconnect attempt

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


# --- Helpers -----------------------------------------------------------------

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


def create_bronze_table_from_csv(cursor, table_name, csv_columns, api_endpoint):
    """Drop and recreate a Bronze table based on actual CSV column headers."""
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    # All CSV columns as VARCHAR(255)
    col_defs = ', '.join([f'[{c}] VARCHAR(255)' for c in csv_columns])

    # Add metadata columns
    metadata = (
        "batch_id INT DEFAULT 1, "
        "ingestion_timestamp DATETIME2 DEFAULT GETDATE(), "
        "source_state VARCHAR(2), "
        f"source_api_endpoint VARCHAR(200) DEFAULT '{api_endpoint}'"
    )

    create_sql = f"CREATE TABLE {table_name} ({col_defs}, {metadata})"
    cursor.execute(create_sql)
    print(f"  Created {table_name} with {len(csv_columns)} data columns + 4 metadata columns")


def get_connection():
    """Establish pyodbc connection to SQL Server."""
    print(f"Connecting to SQL Server (driver: {ODBC_DRIVER})...")
    conn = pyodbc.connect(CONNECTION_STRING)
    conn.autocommit = False
    print("Connected.")
    return conn


def reconnect():
    """Re-establish connection after a drop."""
    print("  Reconnecting to SQL Server...")
    time.sleep(RETRY_DELAY)
    conn = pyodbc.connect(CONNECTION_STRING)
    conn.autocommit = False
    print("  Reconnected.")
    return conn


def execute_batch_with_retry(conn, cursor, sql, batch, batch_num):
    """Execute a batch with retry on connection drops. Returns (conn, cursor)."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            cursor.executemany(sql, batch)
            conn.commit()
            return conn, cursor
        except pyodbc.OperationalError as e:
            error_code = getattr(e, 'args', [('',)])[0] if e.args else ''
            if '08S01' in str(error_code) or '08S01' in str(e):
                print(f"  Connection dropped on batch {batch_num} "
                      f"(attempt {attempt}/{MAX_RETRIES}). Retrying...")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = reconnect()
                cursor = conn.cursor()
                cursor.fast_executemany = True
                if attempt == MAX_RETRIES:
                    raise
            else:
                raise
        except pyodbc.DataError as e:
            print(f"  DataError in batch {batch_num}. Diagnosing...")
            for idx, row in enumerate(batch):
                try:
                    cursor.execute(sql, row)
                    conn.commit()
                except pyodbc.DataError:
                    print(f"  Problem row {batch_num * BATCH_SIZE + idx}: {row}")
                    print(f"  Types: {[type(v).__name__ for v in row]}")
                    break
            raise
    return conn, cursor


def state_row_count(cursor, table_name, state):
    """Check how many rows exist for a given state."""
    cursor.execute(
        f"SELECT COUNT(*) FROM {table_name} WHERE source_state = ?", state
    )
    return cursor.fetchone()[0]


# --- Loaders -----------------------------------------------------------------

def load_claims(conn):
    """Load all claims CSVs into bronze.nfip_claims_raw."""
    cursor = conn.cursor()
    cursor.fast_executemany = True

    # Find the first available CSV to read column headers
    first_csv = None
    for state in TARGET_STATES:
        csv_path = os.path.join(CLAIMS_DIR, f"{state}_claims.csv")
        if os.path.exists(csv_path):
            first_csv = csv_path
            break

    if first_csv is None:
        print("\n  No claims CSVs found, skipping claims load")
        return 0

    # Read headers from first CSV and recreate Bronze table
    df_sample = pd.read_csv(first_csv, nrows=0)
    csv_columns = list(df_sample.columns)
    print(f"\n  Actual API claims columns: {csv_columns}")

    if RELOAD_ALL:
        create_bronze_table_from_csv(
            cursor, 'bronze.nfip_claims_raw', csv_columns, CLAIMS_API
        )
        conn.commit()
    else:
        # Check if table exists; if not, create it
        cursor.execute(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = 'nfip_claims_raw'"
        )
        if cursor.fetchone()[0] == 0:
            create_bronze_table_from_csv(
                cursor, 'bronze.nfip_claims_raw', csv_columns, CLAIMS_API
            )
            conn.commit()

    # Build INSERT for CSV data columns only (metadata added separately)
    placeholders = ', '.join(['?' for _ in csv_columns])
    col_names = ', '.join([f'[{c}]' for c in csv_columns])
    sql = f"INSERT INTO bronze.nfip_claims_raw ({col_names}) VALUES ({placeholders})"

    total = 0
    for state in TARGET_STATES:
        csv_path = os.path.join(CLAIMS_DIR, f"{state}_claims.csv")
        if not os.path.exists(csv_path):
            print(f"  {state}: CSV not found, skipping")
            continue

        # Resume support: skip states already loaded
        if not RELOAD_ALL:
            existing = state_row_count(cursor, 'bronze.nfip_claims_raw', state)
            if existing > 0:
                print(f"  Skipping {state}: {existing:,} rows already loaded")
                total += existing
                continue

        start = time.time()
        df = pd.read_csv(csv_path, dtype=str)
        print(f"  {state} CSV columns: {list(df.columns)}")

        # Log null profile
        null_counts = df.isna().sum()
        print(f"  {state} null profile: {null_counts[null_counts > 0].to_dict()}")

        rows = to_clean_rows(df)
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            conn, cursor = execute_batch_with_retry(
                conn, cursor, sql, batch, i // BATCH_SIZE
            )
            if BATCH_DELAY > 0:
                time.sleep(BATCH_DELAY)

        # Update source_state for rows just loaded
        state_col = None
        for candidate in ['state', 'propertyState']:
            if candidate in csv_columns:
                state_col = candidate
                break
        if state_col:
            cursor.execute(
                f"UPDATE bronze.nfip_claims_raw SET source_state = [{state_col}] "
                "WHERE source_state IS NULL"
            )
            conn.commit()

        elapsed = time.time() - start
        print(f"  {state}: {len(df):,} claims loaded in {elapsed:.1f}s")
        total += len(df)

    # Final source_state pass (catch any stragglers)
    state_col = None
    for candidate in ['state', 'propertyState']:
        if candidate in csv_columns:
            state_col = candidate
            break
    if state_col:
        cursor.execute(
            f"UPDATE bronze.nfip_claims_raw SET source_state = [{state_col}] "
            "WHERE source_state IS NULL"
        )
        conn.commit()
        print(f"  source_state populated from [{state_col}] column")
    else:
        print(f"  WARNING: No state column found in {csv_columns}")

    print(f"  Total claims loaded: {total:,}")
    return total


def load_policies(conn):
    """Load all policies CSVs into bronze.nfip_policies_raw."""
    cursor = conn.cursor()
    cursor.fast_executemany = True

    # Find the first available CSV to read column headers
    first_csv = None
    for state in TARGET_STATES:
        csv_path = os.path.join(POLICIES_DIR, f"{state}_policies.csv")
        if os.path.exists(csv_path):
            first_csv = csv_path
            break

    if first_csv is None:
        print("\n  No policies CSVs found, skipping policies load")
        return 0

    # Read headers from first CSV and recreate Bronze table
    df_sample = pd.read_csv(first_csv, nrows=0)
    csv_columns = list(df_sample.columns)
    print(f"\n  Actual API policies columns: {csv_columns}")

    if RELOAD_ALL:
        create_bronze_table_from_csv(
            cursor, 'bronze.nfip_policies_raw', csv_columns, POLICIES_API
        )
        conn.commit()
    else:
        # Check if table exists; if not, create it
        cursor.execute(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = 'nfip_policies_raw'"
        )
        if cursor.fetchone()[0] == 0:
            create_bronze_table_from_csv(
                cursor, 'bronze.nfip_policies_raw', csv_columns, POLICIES_API
            )
            conn.commit()

    # Build INSERT for CSV data columns only (metadata added separately)
    placeholders = ', '.join(['?' for _ in csv_columns])
    col_names = ', '.join([f'[{c}]' for c in csv_columns])
    sql = f"INSERT INTO bronze.nfip_policies_raw ({col_names}) VALUES ({placeholders})"

    total = 0
    for state in TARGET_STATES:
        csv_path = os.path.join(POLICIES_DIR, f"{state}_policies.csv")
        if not os.path.exists(csv_path):
            print(f"  {state}: CSV not found, skipping")
            continue

        # Resume support: skip states already loaded
        if not RELOAD_ALL:
            existing = state_row_count(cursor, 'bronze.nfip_policies_raw', state)
            if existing > 0:
                print(f"  Skipping {state}: {existing:,} rows already loaded")
                total += existing
                continue

        start = time.time()
        df = pd.read_csv(csv_path, dtype=str)
        print(f"  {state} CSV columns: {list(df.columns)}")

        # Log null profile
        null_counts = df.isna().sum()
        print(f"  {state} null profile: {null_counts[null_counts > 0].to_dict()}")

        rows = to_clean_rows(df)
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            conn, cursor = execute_batch_with_retry(
                conn, cursor, sql, batch, i // BATCH_SIZE
            )
            if BATCH_DELAY > 0:
                time.sleep(BATCH_DELAY)

        # Update source_state for rows just loaded
        state_col = None
        for candidate in ['propertyState', 'state']:
            if candidate in csv_columns:
                state_col = candidate
                break
        if state_col:
            cursor.execute(
                f"UPDATE bronze.nfip_policies_raw SET source_state = [{state_col}] "
                "WHERE source_state IS NULL"
            )
            conn.commit()

        elapsed = time.time() - start
        print(f"  {state}: {len(df):,} policies loaded in {elapsed:.1f}s")
        total += len(df)

    # Final source_state pass (catch any stragglers)
    state_col = None
    for candidate in ['propertyState', 'state']:
        if candidate in csv_columns:
            state_col = candidate
            break
    if state_col:
        cursor.execute(
            f"UPDATE bronze.nfip_policies_raw SET source_state = [{state_col}] "
            "WHERE source_state IS NULL"
        )
        conn.commit()
        print(f"  source_state populated from [{state_col}] column")
    else:
        print(f"  WARNING: No state column found in {csv_columns}")

    print(f"  Total policies loaded: {total:,}")
    return total


# --- Main --------------------------------------------------------------------

def main():
    print("=" * 60)
    print("Bronze Layer Python Loader")
    print(f"  RELOAD_ALL = {RELOAD_ALL}")
    print(f"  BATCH_SIZE = {BATCH_SIZE}")
    print("=" * 60)

    conn = get_connection()

    claims_total = load_claims(conn)
    policies_total = load_policies(conn)

    # Print final column summary for Silver layer reference
    print(f"\n{'=' * 60}")
    print("COLUMN REFERENCE (for Silver layer scripts)")
    print(f"{'=' * 60}")

    cursor = conn.cursor()
    for table in ['bronze.nfip_claims_raw', 'bronze.nfip_policies_raw']:
        cursor.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = '{table.split('.')[1]}' "
            "ORDER BY ORDINAL_POSITION"
        )
        cols = [row[0] for row in cursor.fetchall()]
        print(f"\n  {table}:")
        for c in cols:
            print(f"    - {c}")

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
