"""
Fetch NFIP flood insurance policies data from the OpenFEMA API.

Downloads policies for target states (FL, LA, TX, NJ, NY) with pagination,
retry logic, rate limiting, and a per-state record cap. Saves per-state CSV files.
"""

import os
import time
import requests
import pandas as pd

BASE_URL = "https://www.fema.gov/api/open/v2/FimaNfipPolicies"
TARGET_STATES = ["FL", "LA", "TX", "NJ", "NY"]
PAGE_SIZE = 10000
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30
RATE_LIMIT_DELAY = 1
MAX_RECORDS_PER_STATE = 200000

SELECT_COLUMNS = [
    "propertyState", "countyCode", "floodZoneCurrent",
    "totalInsurancePremiumOfThePolicy", "buildingDeductibleCode",
    "totalBuildingInsuranceCoverage", "totalContentsInsuranceCoverage",
    "policyEffectiveDate", "policyTerminationDate", "construction",
    "occupancyType", "numberOfFloorsInInsuredBuilding",
    "primaryResidenceIndicator", "crsClassCode", "ratedFloodZone",
    "originalConstructionDate", "elevatedBuildingIndicator",
    "basementEnclosureCrawlspaceType",
]

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "datasets", "policies")


def fetch_state_policies(state: str) -> pd.DataFrame:
    """Fetch policies for a given state with pagination, retry, and record cap."""
    all_records = []
    skip = 0
    page = 1
    state_start = time.time()
    hit_limit = False

    print(f"\n{'='*60}")
    print(f"Fetching policies for state: {state}")
    print(f"{'='*60}")

    while True:
        params = {
            "$filter": f"propertyState eq '{state}'",
            "$select": ",".join(SELECT_COLUMNS),
            "$top": PAGE_SIZE,
            "$skip": skip,
        }

        records = request_with_retry(params, page, state)

        if records is None:
            print(f"  [ERROR] Failed to fetch page {page} for {state} after {MAX_RETRIES} retries. Stopping state.")
            break

        if len(records) == 0:
            print(f"  Page {page}: 0 records — done with {state}.")
            break

        all_records.extend(records)
        elapsed = time.time() - state_start
        print(f"  Page {page}: {len(records):,} records | Cumulative: {len(all_records):,} | Elapsed: {elapsed:.1f}s")

        if len(all_records) >= MAX_RECORDS_PER_STATE:
            all_records = all_records[:MAX_RECORDS_PER_STATE]
            hit_limit = True
            print(f"  [WARNING] Reached MAX_RECORDS_PER_STATE limit ({MAX_RECORDS_PER_STATE:,}). "
                  f"Truncating {state} results. Increase MAX_RECORDS_PER_STATE for full download.")
            break

        if len(records) < PAGE_SIZE:
            break

        skip += PAGE_SIZE
        page += 1
        time.sleep(RATE_LIMIT_DELAY)

    elapsed = time.time() - state_start
    status = " (TRUNCATED)" if hit_limit else ""
    print(f"  {state} complete{status}: {len(all_records):,} total records in {elapsed:.1f}s")

    return pd.DataFrame(all_records)


def request_with_retry(params: dict, page: int, state: str) -> list | None:
    """Make an API request with exponential backoff retry logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)

            if resp.status_code != 200:
                print(f"  [WARN] Page {page} for {state}: HTTP {resp.status_code} (attempt {attempt}/{MAX_RETRIES})")
                if attempt < MAX_RETRIES:
                    backoff = 2 ** attempt
                    print(f"         Retrying in {backoff}s...")
                    time.sleep(backoff)
                    continue
                return None

            data = resp.json()
            return data.get("FimaNfipPolicies", [])

        except requests.exceptions.Timeout:
            print(f"  [WARN] Page {page} for {state}: timeout (attempt {attempt}/{MAX_RETRIES})")
        except requests.exceptions.RequestException as e:
            print(f"  [WARN] Page {page} for {state}: {e} (attempt {attempt}/{MAX_RETRIES})")

        if attempt < MAX_RETRIES:
            backoff = 2 ** attempt
            print(f"         Retrying in {backoff}s...")
            time.sleep(backoff)

    return None


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    summary = {}
    truncated = []
    grand_start = time.time()

    print("NFIP Policies Data Ingestion")
    print(f"Target states: {', '.join(TARGET_STATES)}")
    print(f"API endpoint: {BASE_URL}")
    print(f"Max records per state: {MAX_RECORDS_PER_STATE:,}")

    for state in TARGET_STATES:
        df = fetch_state_policies(state)
        summary[state] = len(df)

        if len(df) >= MAX_RECORDS_PER_STATE:
            truncated.append(state)

        if not df.empty:
            output_path = os.path.join(OUTPUT_DIR, f"{state}_policies.csv")
            df.to_csv(output_path, index=False)
            print(f"  Saved to {output_path}")
        else:
            print(f"  No records for {state} — skipping CSV.")

    grand_elapsed = time.time() - grand_start

    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    grand_total = 0
    for state, count in summary.items():
        marker = " *TRUNCATED*" if state in truncated else ""
        print(f"  {state}: {count:>10,} records{marker}")
        grand_total += count
    print(f"  {'—'*25}")
    print(f"  TOTAL: {grand_total:>8,} records")
    print(f"  Elapsed: {grand_elapsed:.1f}s")

    if truncated:
        print(f"\n  [WARNING] The following states were truncated at {MAX_RECORDS_PER_STATE:,} records:")
        print(f"  {', '.join(truncated)}")
        print(f"  Increase MAX_RECORDS_PER_STATE to download the full dataset.")

    print(f"{'='*60}")


if __name__ == "__main__":
    main()
