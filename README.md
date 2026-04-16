# NFIP Insurance Data Warehouse

Insurance data warehouse built on FEMA National Flood Insurance Program (NFIP) claims and policy data, ingested via REST API. Medallion Architecture (Bronze, Silver, Gold) with a star schema dimensional model and insurance KPI analytics views.

## Business Context

Insurers manage policies and claims through separate operational systems. Policy administration records what was sold, to whom, and at what price. Claims systems record what happened, how much was paid, and why. In practice, these datasets live in different databases with different schemas, different update cadences, and different owners. The gap between them is where most insurance analytics problems start -- you cannot calculate a loss ratio, assess premium adequacy, or measure claims frequency without joining the two.

This project builds a warehouse that brings NFIP flood insurance claims and policies into a single analytical model. The NFIP dataset was chosen for its scale (2.7 million records across 5 states), public availability (no API key required), and real-world relevance to property and casualty insurance. The data covers claims dating back to 1978 and policies from 2009 onward, providing decades of loss history against a meaningful book of business.

The scope is deliberately constrained to five high-exposure states -- Florida, Louisiana, Texas, New Jersey, and New York -- which together account for the majority of NFIP claims volume. This mirrors how an insurer would approach portfolio segmentation: start with the concentrations that drive the most risk. The architecture and analytics patterns here are directly transferable to Australian general insurance contexts, including the 2022 flood events in Queensland and New South Wales, and the ARPC cyclone reinsurance pool.

## Data Acquisition

Data is sourced from the [OpenFEMA API](https://www.fema.gov/about/openfema/api), a public REST API that requires no authentication.

Two Python ingestion scripts handle the download:
- `fetch_nfip_claims.py` -- claims from `FimaNfipClaims` endpoint
- `fetch_nfip_policies.py` -- policies from `FimaNfipPolicies` endpoint

Each script uses `$filter` for state selection, `$select` to request only the columns needed, and `$top`/`$skip` pagination at 10,000 records per page. Retry logic (3 attempts with exponential backoff) and rate limiting (1-second delay between calls) handle API reliability.

Example API call:
```
https://www.fema.gov/api/open/v2/FimaNfipClaims?$filter=state eq 'FL'&$select=dateOfLoss,state,countyCode,...&$top=10000&$skip=0
```

The resulting CSV files are saved to `datasets/claims/` and `datasets/policies/` and are **gitignored** due to file size (~192 MB claims, ~150 MB policies). See `datasets/README.md` for reproduction instructions.

## Architecture

The warehouse follows a **Medallion Architecture** (Bronze, Silver, Gold), running on Azure SQL Edge in Docker.

<!-- See docs/data_architecture_diagram.png for the full diagram -->

**Bronze** -- Raw staging layer. All source columns are loaded as `VARCHAR(255)` with no transformation. BULK INSERT from CSV files mounted into the Docker container. Metadata columns (`batch_id`, `ingestion_timestamp`, `source_state`) are appended for lineage tracking.

**Silver** -- Cleaned and enriched layer. Type casting via `TRY_CAST`, `COALESCE` for null handling, deduplication via `ROW_NUMBER()`, and derived columns including `amountPaidTotal`, `zone_category`, `is_special_flood_hazard`, and `exposure` (policy term as a fraction of a year, capped 0--1).

**Gold** -- Analytical layer. Star schema with two fact tables and five dimensions, plus seven analytics views that answer specific insurance business questions.

## Star Schema

<!-- See docs/data_models.png for the ERD -->

### Fact Tables
| Table | Grain | Key Measures |
|-------|-------|-------------|
| `fact_claims` | One row per deduplicated claim | `amount_paid_total`, `amount_paid_building`, `amount_paid_contents`, coverage limits |
| `fact_policies` | One row per deduplicated policy | `total_premium`, `exposure`, `building_coverage`, `contents_coverage` |

### Dimension Tables
| Table | Description |
|-------|-------------|
| `dim_date` | Calendar dates 1978--2026 with fiscal year (Oct start) |
| `dim_location` | State + county FIPS with FEMA region |
| `dim_flood_zone` | Zone code mapped to category (A/V/X/D) with SFHA flag |
| `dim_building_type` | Construction class, floors, year-built band, elevated flag, basement type |
| `dim_occupancy` | NFIP occupancy type with residential flag |

## Key Analytics

Seven views in the Gold layer, each answering a specific underwriting or actuarial question:

**vw_loss_ratio_by_state** -- Which states are profitable? Loss ratio (claims paid / premiums collected) by state and year.

**vw_claims_frequency_severity** -- How does risk vary by flood zone? Claims frequency (count / exposure) and average severity by zone category and year.

**vw_large_loss_concentration** -- Where do catastrophic losses concentrate? Flags claims above the 95th percentile and shows geographic concentration by state.

**vw_severity_by_flood_zone** -- How severe are losses across zone types? Average and median severity with building vs. contents split by zone category.

**vw_premium_adequacy** -- Are premiums covering expected losses? Compares pure premium (losses / exposure) against average premium charged, by occupancy and construction type.

**vw_claims_development** -- How do losses develop over time? Simplified accident-year view showing total paid, claim count, and average severity by year of loss. (Note: a true development triangle requires incremental payment dates, which the NFIP dataset does not provide.)

**vw_portfolio_summary** -- What does the book look like year over year? One row per year with total policies, exposure, premium, claims paid, loss ratio, average severity, and YoY growth.

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Data ingestion | Python 3, `requests`, `pandas` |
| Database | Azure SQL Edge (Docker container) |
| SQL dialect | T-SQL |
| Architecture | Medallion (Bronze / Silver / Gold) |
| Data model | Star schema |
| Diagrams | Draw.io |
| Version control | Git / GitHub |

## How to Run

### Prerequisites

- Python 3.x
- Docker Desktop for Mac
- Azure Data Studio (recommended) or sqlcmd via Homebrew

### Install sqlcmd (for command-line execution)

```bash
brew tap microsoft/mssql-release https://github.com/microsoft/homebrew-mssql-release
brew install sqlcmd
```

### 1. Clone and set up Python

```bash
git clone https://github.com/ayusyagol11/nfip-insurance-data-warehouse.git
cd nfip-insurance-data-warehouse
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Download the data

```bash
python scripts/ingest/fetch_nfip_claims.py
python scripts/ingest/fetch_nfip_policies.py
```

Claims takes approximately 10 minutes (1.7M records). Policies takes approximately 8 minutes (1M records, capped at 200K per state).

### 3. Start the database

```bash
docker-compose up -d
# or use the helper:
./scripts/docker_start.sh
```

### 4. Run the SQL pipeline

Execute scripts in order via the helper, or open them in Azure Data Studio connected to `localhost,1433`:

```bash
# Bronze layer
./scripts/run_sql.sh scripts/bronze/01_create_database.sql
./scripts/run_sql.sh scripts/bronze/02_create_bronze_tables.sql
./scripts/run_sql.sh scripts/bronze/03_load_claims.sql
./scripts/run_sql.sh scripts/bronze/04_load_policies.sql

# Silver layer
./scripts/run_sql.sh scripts/silver/01_create_silver_tables.sql
./scripts/run_sql.sh scripts/silver/02_clean_claims.sql
./scripts/run_sql.sh scripts/silver/03_clean_policies.sql
./scripts/run_sql.sh scripts/silver/04_create_lookups.sql
./scripts/run_sql.sh scripts/silver/05_validate_silver.sql

# Gold layer
./scripts/run_sql.sh scripts/gold/01_create_gold_tables.sql
./scripts/run_sql.sh scripts/gold/02_load_dim_date.sql
./scripts/run_sql.sh scripts/gold/03_load_dim_location.sql
./scripts/run_sql.sh scripts/gold/04_load_dim_flood_zone.sql
./scripts/run_sql.sh scripts/gold/05_load_dim_building_type.sql
./scripts/run_sql.sh scripts/gold/06_load_dim_occupancy.sql
./scripts/run_sql.sh scripts/gold/07_load_fact_claims.sql
./scripts/run_sql.sh scripts/gold/08_load_fact_policies.sql
./scripts/run_sql.sh scripts/gold/09_create_analytics_views.sql

# Tests
./scripts/run_sql.sh tests/test_row_counts.sql
./scripts/run_sql.sh tests/test_referential_integrity.sql
./scripts/run_sql.sh tests/test_business_rules.sql
```

If BULK INSERT fails due to CSV format issues, use the Python fallback:
```bash
python scripts/bronze/load_via_python.py
```

## Repository Structure

```
nfip-insurance-data-warehouse/
├── datasets/
│   ├── claims/                         # FL/LA/TX/NJ/NY_claims.csv (gitignored)
│   ├── policies/                       # FL/LA/TX/NJ/NY_policies.csv (gitignored)
│   └── README.md                       # Data acquisition instructions
├── docs/
│   ├── data_catalog.md                 # Gold layer table documentation
│   ├── data_profile_report.md          # Automated data profiling results
│   ├── data_quality_notes.md           # Known issues and resolutions
│   ├── insurance_glossary.md           # Insurance term definitions
│   ├── naming_conventions.md           # Project naming standards
│   └── DIAGRAMS.md                     # Diagram creation specs
├── scripts/
│   ├── ingest/
│   │   ├── fetch_nfip_claims.py        # Claims API ingestion
│   │   ├── fetch_nfip_policies.py      # Policies API ingestion
│   │   └── profile_data.py            # Data profiling script
│   ├── bronze/
│   │   ├── 01_create_database.sql      # Database and schema creation
│   │   ├── 02_create_bronze_tables.sql # Raw staging tables
│   │   ├── 03_load_claims.sql          # BULK INSERT claims
│   │   ├── 04_load_policies.sql        # BULK INSERT policies
│   │   └── load_via_python.py          # Fallback loader via pyodbc
│   ├── silver/
│   │   ├── 01_create_silver_tables.sql # Typed, cleaned tables
│   │   ├── 02_clean_claims.sql         # Claims transformation
│   │   ├── 03_clean_policies.sql       # Policies transformation
│   │   ├── 04_create_lookups.sql       # Reference tables
│   │   └── 05_validate_silver.sql      # Silver validation checks
│   ├── gold/
│   │   ├── 01_create_gold_tables.sql   # Star schema DDL
│   │   ├── 02_load_dim_date.sql        # Date dimension
│   │   ├── 03_load_dim_location.sql    # Location dimension
│   │   ├── 04_load_dim_flood_zone.sql  # Flood zone dimension
│   │   ├── 05_load_dim_building_type.sql # Building type dimension
│   │   ├── 06_load_dim_occupancy.sql   # Occupancy dimension
│   │   ├── 07_load_fact_claims.sql     # Claims fact table
│   │   ├── 08_load_fact_policies.sql   # Policies fact table
│   │   └── 09_create_analytics_views.sql # 7 KPI views
│   ├── docker_start.sh                 # Start container + test connection
│   └── run_sql.sh                      # Execute SQL file against container
├── tests/
│   ├── test_row_counts.sql             # Pipeline row count validation
│   ├── test_referential_integrity.sql  # FK orphan checks
│   └── test_business_rules.sql         # Business rule validation
├── docker-compose.yml                  # Azure SQL Edge container
├── requirements.txt                    # Python dependencies
├── LICENSE                             # MIT License
└── README.md
```

## Related Projects

- [Predictive Claims Liability Model](https://github.com/ayusyagol11/claims-liability-predictor) -- Machine learning model for outstanding claims reserve estimation
- [Macroeconomic Resilience in General Insurance](https://github.com/ayusyagol11) -- Research into economic cycle impacts on insurance portfolios

## Author

**Aayush Yagol** -- Insurance Data Analyst | Claims Advisor, Suncorp Group

*I build predictive models for insurance and risk -- from the inside.*

- Portfolio: [aayushyagol.com](https://aayushyagol.com)
- LinkedIn: [linkedin.com/in/aayush-yagol-046874145](https://linkedin.com/in/aayush-yagol-046874145)
- GitHub: [github.com/ayusyagol11](https://github.com/ayusyagol11)

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
