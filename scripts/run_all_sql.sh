#!/bin/bash
# ================================================================
# Run the entire SQL pipeline: Bronze → Silver → Gold → Tests
# Usage: ./scripts/run_all_sql.sh
# ================================================================

set -e

if ! command -v sqlcmd &> /dev/null; then
  echo "ERROR: sqlcmd not found. Install it with: brew tap microsoft/mssql-release https://github.com/microsoft/homebrew-mssql-release && brew install sqlcmd"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUN_SQL="$SCRIPT_DIR/run_sql.sh"

echo "============================================"
echo "NFIP Insurance Data Warehouse - Full Pipeline"
echo "============================================"
echo ""

# Bronze layer
echo "--- Bronze Layer ---"
"$RUN_SQL" "$SCRIPT_DIR/bronze/01_create_database.sql"
"$RUN_SQL" "$SCRIPT_DIR/bronze/02_create_bronze_tables.sql"
echo "Loading Bronze data via Python (BULK INSERT not supported on Azure SQL Edge)..."
python3 "$SCRIPT_DIR/bronze/load_via_python.py"
echo ""

# Silver layer
echo "--- Silver Layer ---"
"$RUN_SQL" "$SCRIPT_DIR/silver/01_create_silver_tables.sql"
"$RUN_SQL" "$SCRIPT_DIR/silver/02_clean_claims.sql"
"$RUN_SQL" "$SCRIPT_DIR/silver/03_clean_policies.sql"
"$RUN_SQL" "$SCRIPT_DIR/silver/04_create_lookups.sql"
"$RUN_SQL" "$SCRIPT_DIR/silver/05_validate_silver.sql"
echo ""

# Gold layer
echo "--- Gold Layer ---"
"$RUN_SQL" "$SCRIPT_DIR/gold/01_create_gold_tables.sql"
"$RUN_SQL" "$SCRIPT_DIR/gold/02_load_dim_date.sql"
"$RUN_SQL" "$SCRIPT_DIR/gold/03_load_dim_location.sql"
"$RUN_SQL" "$SCRIPT_DIR/gold/04_load_dim_flood_zone.sql"
"$RUN_SQL" "$SCRIPT_DIR/gold/05_load_dim_building_type.sql"
"$RUN_SQL" "$SCRIPT_DIR/gold/06_load_dim_occupancy.sql"
"$RUN_SQL" "$SCRIPT_DIR/gold/07_load_fact_claims.sql"
"$RUN_SQL" "$SCRIPT_DIR/gold/08_load_fact_policies.sql"
"$RUN_SQL" "$SCRIPT_DIR/gold/09_create_analytics_views.sql"
echo ""

# Tests
echo "--- Tests ---"
"$RUN_SQL" "$(dirname "$SCRIPT_DIR")/tests/test_row_counts.sql"
"$RUN_SQL" "$(dirname "$SCRIPT_DIR")/tests/test_referential_integrity.sql"
"$RUN_SQL" "$(dirname "$SCRIPT_DIR")/tests/test_business_rules.sql"
echo ""

echo "============================================"
echo "PIPELINE COMPLETE"
echo "============================================"
