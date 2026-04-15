-- ================================================================
-- Document:  data_catalog.md
-- Layer:     Gold
-- Purpose:   Data catalog for all Gold layer dimension and fact
--            tables in the NFIP Insurance Data Warehouse
-- Author:    Aayush Yagol
-- ================================================================

# Gold Layer Data Catalog

## Dimension Tables

---

### gold.dim_date

**Description:** Calendar date dimension covering the full range of claims and policy dates.

**Grain:** One row per calendar date.

**Source:** Generated via WHILE loop (1978-01-01 to 2026-12-31).

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| date_key | INT (PK) | Surrogate key in YYYYMMDD format | 20170825 |
| full_date | DATE | Calendar date | 2017-08-25 |
| year | INT | Calendar year | 2017 |
| quarter | INT | Calendar quarter (1-4) | 3 |
| month | INT | Month number (1-12) | 8 |
| month_name | VARCHAR(20) | Month name | August |
| fiscal_year | INT | Federal fiscal year (Oct = year+1) | 2017 |
| fiscal_quarter | INT | Fiscal quarter (Oct-Dec = Q1) | 4 |
| day_of_week | VARCHAR(10) | Day name | Friday |
| is_weekend | BIT | Weekend flag | 0 |

---

### gold.dim_location

**Description:** Geographic dimension combining state and county identifiers with FEMA region.

**Grain:** One row per unique state + county FIPS combination.

**Source:** Distinct `state` + `countyCode` from `silver.nfip_claims_cleaned` UNION `silver.nfip_policies_cleaned`, joined to `silver.ref_state_info`.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| location_key | INT (PK) | Surrogate key (-1 = Unknown) | 42 |
| state_abbrev | CHAR(2) | Two-letter state abbreviation | FL |
| state_name | VARCHAR(50) | Full state name | Florida |
| county_fips | VARCHAR(5) | FIPS county code | 12086 |
| fema_region | INT | FEMA administrative region | 4 |

---

### gold.dim_flood_zone

**Description:** Flood zone dimension mapping individual zone codes to risk categories.

**Grain:** One row per unique flood zone code.

**Source:** Distinct `floodZone` from Silver claims and policies, joined to `silver.ref_flood_zone_info`.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| flood_zone_key | INT (PK) | Surrogate key (-1 = Unknown) | 7 |
| flood_zone_code | VARCHAR(20) | Original FEMA zone code | AE |
| zone_category | VARCHAR(10) | Normalised category (A/V/X/D) | A |
| zone_description | VARCHAR(100) | Plain-language description | High-risk 1% annual chance |
| is_special_flood_hazard | BIT | SFHA flag (A and V zones) | 1 |

---

### gold.dim_building_type

**Description:** Building characteristics dimension combining structural attributes into a single profile.

**Grain:** One row per unique combination of construction class, floors, year-built band, elevated flag, and basement type.

**Source:** Distinct attribute combos from Silver claims (constructionClass = 'N/A') UNION Silver policies.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| building_type_key | INT (PK) | Surrogate key (-1 = Unknown) | 15 |
| construction_class | VARCHAR(50) | Construction type (N/A for claims) | False |
| number_of_floors | VARCHAR(10) | Number of floors in building | 1.0 |
| year_built_band | VARCHAR(20) | Decade band of construction date | 1990-1999 |
| elevated_flag | BIT | Building is elevated | 0 |
| basement_type | VARCHAR(10) | Basement/enclosure/crawlspace type code | 0.0 |

---

### gold.dim_occupancy

**Description:** Occupancy type dimension with residential classification.

**Grain:** One row per unique NFIP occupancy type code.

**Source:** Distinct `occupancyType` from Silver claims and policies.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| occupancy_key | INT (PK) | Surrogate key (-1 = Unknown) | 1 |
| occupancy_type | VARCHAR(50) | NFIP occupancy code | 1 |
| residential_flag | BIT | Residential property flag | 1 |

**Occupancy Code Reference:**
- 1 = Single Family Residence
- 2 = 2-4 Unit Residential
- 3 = Other Residential (5+ units)
- 4 = Non-Residential
- 6 = Non-Residential Business
- 11 = Residential Condo (unit)
- 12 = Residential Condo (building)
- 13 = Non-Residential Condo (unit)
- 14 = Non-Residential Condo (building)
- 15 = Residential Manufactured/Mobile Home
- 16 = Non-Residential Manufactured/Mobile Home
- 17 = Other Non-Residential
- 18 = Other Residential
- 19 = Unknown/Other

---

## Fact Tables

---

### gold.fact_claims

**Description:** Claims fact table containing one row per deduplicated NFIP flood insurance claim with paid amounts, coverage limits, and dimension keys.

**Grain:** One row per unique claim (deduplicated in Silver by state, county, date of loss, and paid amounts).

**Source:** `silver.nfip_claims_cleaned` joined to all five dimensions.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| claim_sk | INT IDENTITY (PK) | Surrogate key | 1 |
| date_key | INT (FK → dim_date) | Date of loss key (YYYYMMDD) | 20170825 |
| location_key | INT (FK → dim_location) | State + county key | 42 |
| flood_zone_key | INT (FK → dim_flood_zone) | Flood zone key | 7 |
| building_type_key | INT (FK → dim_building_type) | Building profile key | 15 |
| occupancy_key | INT (FK → dim_occupancy) | Occupancy type key | 1 |
| amount_paid_building | DECIMAL(18,2) | Amount paid on building claim | 45000.00 |
| amount_paid_contents | DECIMAL(18,2) | Amount paid on contents claim | 12000.00 |
| amount_paid_total | DECIMAL(18,2) | Total paid (building + contents) | 57000.00 |
| total_building_coverage | DECIMAL(18,2) | Building insurance coverage limit | 250000.00 |
| total_contents_coverage | DECIMAL(18,2) | Contents insurance coverage limit | 100000.00 |
| cause_of_damage | VARCHAR(100) | Cause of damage code | 4 |
| year_of_loss | INT | Year the loss occurred | 2017 |

**Silver Source Mapping:**

| Gold Column | Silver Column |
|-------------|---------------|
| date_key | CONVERT(dateOfLoss) |
| location_key | state + countyCode → dim_location |
| flood_zone_key | floodZone → dim_flood_zone |
| building_type_key | numberOfFloors + yearBuilt + elevated + basement → dim_building_type |
| occupancy_key | occupancyType → dim_occupancy |
| amount_paid_building | amountPaidOnBuildingClaim |
| amount_paid_contents | amountPaidOnContentsClaim |
| amount_paid_total | amountPaidTotal (derived in Silver) |
| cause_of_damage | causeOfDamage |
| year_of_loss | yearOfLoss |

---

### gold.fact_policies

**Description:** Policies fact table containing one row per deduplicated NFIP flood insurance policy with premium, coverage, and exposure metrics.

**Grain:** One row per unique policy (deduplicated in Silver by state, county, effective date, premium, and building coverage).

**Source:** `silver.nfip_policies_cleaned` joined to all five dimensions.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| policy_sk | INT IDENTITY (PK) | Surrogate key | 1 |
| effective_date_key | INT (FK → dim_date) | Policy effective date key | 20230101 |
| termination_date_key | INT (FK → dim_date) | Policy termination date key | 20240101 |
| location_key | INT (FK → dim_location) | State + county key | 42 |
| flood_zone_key | INT (FK → dim_flood_zone) | Flood zone key | 7 |
| building_type_key | INT (FK → dim_building_type) | Building profile key | 15 |
| occupancy_key | INT (FK → dim_occupancy) | Occupancy type key | 1 |
| total_premium | DECIMAL(18,2) | Total insurance premium | 1250.00 |
| deductible_amount | DECIMAL(18,2) | Building deductible code/amount | 2000.00 |
| building_coverage | DECIMAL(18,2) | Building coverage limit | 250000.00 |
| contents_coverage | DECIMAL(18,2) | Contents coverage limit | 100000.00 |
| exposure | DECIMAL(8,6) | Policy term as fraction of year (0-1) | 1.000000 |
| crs_class | VARCHAR(10) | Community Rating System class | 7 |
| policy_count | INT | Policy count (always 1) | 1 |

**Silver Source Mapping:**

| Gold Column | Silver Column |
|-------------|---------------|
| effective_date_key | CONVERT(policyEffectiveDate) |
| termination_date_key | CONVERT(policyTerminationDate) |
| location_key | propertyState + countyCode → dim_location |
| flood_zone_key | floodZone → dim_flood_zone |
| building_type_key | constructionClass + numberOfFloors + yearBuilt + elevated + basement → dim_building_type |
| occupancy_key | occupancyType → dim_occupancy |
| total_premium | totalPremium |
| deductible_amount | deductibleAmount |
| exposure | exposure (derived in Silver) |
| crs_class | crsClassificationCode |
