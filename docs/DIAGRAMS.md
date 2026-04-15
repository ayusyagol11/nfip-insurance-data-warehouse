# Diagrams

This project includes three diagrams to be created manually in [Draw.io](https://app.diagrams.net/) (also available as a VS Code extension). Save the source files as `.drawio` and export PNGs for the README.

---

## 1. Data Architecture Diagram

**File:** `docs/data_architecture_diagram.drawio` → export as `docs/data_architecture_diagram.png`

**Purpose:** End-to-end data flow from source to analytics layer.

**Layout (left to right):**

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────┐    ┌───────────────────────┐
│  OpenFEMA   │    │  Python Scripts   │    │  CSV Files  │    │  Docker / Azure SQL   │
│  REST API   │───→│  fetch_nfip_*     │───→│  datasets/  │───→│  Edge Container       │
│             │    │  (requests +      │    │  claims/    │    │                       │
│  Claims API │    │   pandas)         │    │  policies/  │    │  BULK INSERT or       │
│  Policies   │    │                   │    │             │    │  Python pyodbc loader │
│  API        │    │  Retry + Rate     │    │  .gitignored│    │                       │
└─────────────┘    │  Limiting         │    └─────────────┘    └───────────┬───────────┘
                   └──────────────────┘                                   │
                                                                          ▼
                   ┌──────────────────────────────────────────────────────────┐
                   │                  NfipInsuranceWarehouse                  │
                   │                                                          │
                   │  ┌──────────┐    ┌──────────┐    ┌──────────────────┐   │
                   │  │  Bronze  │───→│  Silver  │───→│  Gold            │   │
                   │  │          │    │          │    │                  │   │
                   │  │ All      │    │ TRY_CAST │    │ dim_date         │   │
                   │  │ VARCHAR  │    │ COALESCE │    │ dim_location     │   │
                   │  │ Raw      │    │ Dedup    │    │ dim_flood_zone   │   │
                   │  │ staging  │    │ Derive   │    │ dim_building     │   │
                   │  │          │    │ Validate │    │ dim_occupancy    │   │
                   │  │          │    │          │    │ fact_claims      │   │
                   │  │          │    │          │    │ fact_policies    │   │
                   │  │          │    │          │    │ 7 analytics views│   │
                   │  └──────────┘    └──────────┘    └──────────────────┘   │
                   └──────────────────────────────────────────────────────────┘
```

**Colour scheme suggestion:** Blue for external sources, green for Python/ETL, orange for Bronze, yellow for Silver, gold for Gold layer.

---

## 2. Data Model ERD

**File:** `docs/data_models.drawio` → export as `docs/data_models.png`

**Purpose:** Entity-Relationship Diagram showing the Gold layer star schema.

**Layout:** Place `fact_claims` and `fact_policies` in the centre. Arrange the five dimension tables around them with FK relationship lines.

```
                         ┌──────────────┐
                         │  dim_date    │
                         │──────────────│
                         │ date_key (PK)│
                         │ full_date    │
                         │ year         │
                         │ quarter      │
                         │ fiscal_year  │
                         └──────┬───────┘
                                │
         ┌──────────────┐       │        ┌──────────────────┐
         │ dim_location │       │        │ dim_flood_zone   │
         │──────────────│       │        │──────────────────│
         │ location_key │       │        │ flood_zone_key   │
         │ state_abbrev │       │        │ flood_zone_code  │
         │ state_name   │       │        │ zone_category    │
         │ county_fips  │       │        │ is_sfha          │
         └──────┬───────┘       │        └────────┬─────────┘
                │               │                 │
                │    ┌──────────┴──────────┐      │
                ├───→│    fact_claims      │←─────┤
                │    │────────────────────│      │
                │    │ claim_sk (PK)      │      │
                │    │ date_key (FK)      │      │
                │    │ location_key (FK)  │      │
                │    │ flood_zone_key (FK)│      │
                │    │ building_type_key  │      │
                │    │ occupancy_key (FK) │      │
                │    │ amount_paid_total  │      │
                │    │ ...               │      │
                │    └────────────────────┘      │
                │                                │
                │    ┌────────────────────┐      │
                ├───→│   fact_policies    │←─────┤
                │    │────────────────────│      │
                │    │ policy_sk (PK)     │      │
                │    │ effective_date_key │      │
                │    │ termination_date_key│     │
                │    │ location_key (FK)  │      │
                │    │ flood_zone_key (FK)│      │
                │    │ building_type_key  │      │
                │    │ occupancy_key (FK) │      │
                │    │ total_premium      │      │
                │    │ exposure           │      │
                │    └──────────┬─────────┘      │
                │               │                │
         ┌──────┴───────┐      │       ┌────────┴─────────┐
         │dim_building  │      │       │ dim_occupancy    │
         │──────────────│      │       │──────────────────│
         │building_type │      │       │ occupancy_key    │
         │  _key        │      │       │ occupancy_type   │
         │construction  │      │       │ residential_flag │
         │floors        │      │       └──────────────────┘
         │year_built_band│     │
         │elevated_flag │      │
         └──────────────┘      │
                               │
                    (date_key shared
                     by both facts)
```

**FK relationships to draw:**
- `fact_claims.date_key` → `dim_date.date_key`
- `fact_claims.location_key` → `dim_location.location_key`
- `fact_claims.flood_zone_key` → `dim_flood_zone.flood_zone_key`
- `fact_claims.building_type_key` → `dim_building_type.building_type_key`
- `fact_claims.occupancy_key` → `dim_occupancy.occupancy_key`
- `fact_policies.effective_date_key` → `dim_date.date_key`
- `fact_policies.termination_date_key` → `dim_date.date_key`
- `fact_policies.location_key` → `dim_location.location_key`
- `fact_policies.flood_zone_key` → `dim_flood_zone.flood_zone_key`
- `fact_policies.building_type_key` → `dim_building_type.building_type_key`
- `fact_policies.occupancy_key` → `dim_occupancy.occupancy_key`

---

## 3. ETL Flow Diagram

**File:** `docs/etl_flow.drawio` → export as `docs/etl_flow.png`

**Purpose:** Detailed transformation steps at each layer of the Medallion Architecture.

**Layout (top to bottom, three swim lanes):**

### Bronze Lane
```
OpenFEMA API → Python fetch scripts → CSV files → BULK INSERT → bronze.nfip_claims_raw / bronze.nfip_policies_raw
- All columns VARCHAR(255)
- Metadata added: batch_id, ingestion_timestamp, source_state, source_api_endpoint
- No transformation — raw landing zone
```

### Silver Lane
```
bronze.nfip_claims_raw → silver.nfip_claims_cleaned
  Transformations:
  1. TRY_CAST: dateOfLoss → DATE, amounts → DECIMAL(18,2), yearOfLoss → INT
  2. COALESCE: NULL amounts → 0
  3. Derive: amountPaidTotal = building + contents
  4. TRIM: all string columns
  5. Dedup: ROW_NUMBER() PARTITION BY natural key, ORDER BY ingestion_timestamp DESC
  6. Filter: exclude NULL dateOfLoss
  7. Derive: accident_year, zone_category, is_special_flood_hazard
  8. Resolve: floodZone = COALESCE(ratedFloodZone, floodZoneCurrent)

bronze.nfip_policies_raw → silver.nfip_policies_cleaned
  Transformations:
  1-6: Same pattern as claims
  7. Derive: exposure = DATEDIFF(DAY, effective, termination) / 365.25, capped [0,1]
  8. Derive: zone_category, is_special_flood_hazard

Reference tables loaded:
  - silver.ref_state_info (5 states, FEMA regions)
  - silver.ref_flood_zone_info (5 zone categories)
```

### Gold Lane
```
silver.nfip_claims_cleaned + silver.nfip_policies_cleaned → Gold star schema

  Dimensions populated:
  1. dim_date: generated 1978-2026 (WHILE loop)
  2. dim_location: DISTINCT state+county, JOIN ref_state_info
  3. dim_flood_zone: DISTINCT zones, JOIN ref_flood_zone_info
  4. dim_building_type: DISTINCT attribute combos, year-built banded
  5. dim_occupancy: DISTINCT types, residential flag derived

  Facts populated:
  6. fact_claims: Silver claims JOIN all 5 dims, COALESCE unresolved → key -1
  7. fact_policies: Silver policies JOIN all 5 dims (2 date keys)

  Analytics views:
  8. 7 views: loss ratio, frequency/severity, large loss, severity by zone,
     premium adequacy, claims development, portfolio summary
```
