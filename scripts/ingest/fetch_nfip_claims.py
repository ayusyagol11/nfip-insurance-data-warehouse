"""
Fetch NFIP flood insurance claims data from the OpenFEMA API.

Downloads claims for target states (FL, LA, TX, NJ, NY) with pagination,
retry logic, and rate limiting. Saves per-state CSV files.
"""

import os
import time
import requests
import pandas as pd

BASE_URL = "https://www.fema.gov/api/open/v2/FimaNfipClaims"
TARGET_STATES = ["FL", "LA", "TX", "NJ", "NY"]
PAGE_SIZE = 10000
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30
RATE_LIMIT_DELAY = 1

SELECT_COLUMNS = [
    "dateOfLoss", "state", "countyCode", "floodZoneCurrent",
    "amountPaidOnBuildingClaim", "amountPaidOnContentsClaim",
    "totalBuildingInsuranceCoverage", "totalContentsInsuranceCoverage",
    "occupancyType", "yearOfLoss", "causeOfDamage", "ratedFloodZone",
    "primaryResidenceIndicator", "originalConstructionDate",
    "numberOfFloorsInTheInsuredBuilding",
    "elevatedBuildingIndicator", "basementEnclosureCrawlspaceType",
]

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "datasets", "claims")


def fetch_state_claims(state: str) -> pd.DataFrame:
    """Fetch all claims for a given state with pagination and retry logic."""
    all_records = []
    skip = 0
    page = 1
    state_start = time.time()

    print(f"\n{'='*60}")
    print(f"Fetching claims for state: {state}")
    print(f"{'='*60}")

    while True:
        params = {
            "$filter": f"state eq '{state}'",
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

        if len(records) < PAGE_SIZE:
            break

        skip += PAGE_SIZE
        page += 1
        time.sleep(RATE_LIMIT_DELAY)

    elapsed = time.time() - state_start
    print(f"  {state} complete: {len(all_records):,} total records in {elapsed:.1f}s")

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
            return data.get("FimaNfipClaims", [])

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
    grand_start = time.time()

    print("NFIP Claims Data Ingestion")
    print(f"Target states: {', '.join(TARGET_STATES)}")
    print(f"API endpoint: {BASE_URL}")

    for state in TARGET_STATES:
        df = fetch_state_claims(state)
        summary[state] = len(df)

        if not df.empty:
            output_path = os.path.join(OUTPUT_DIR, f"{state}_claims.csv")
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
        print(f"  {state}: {count:>10,} records")
        grand_total += count
    print(f"  {'—'*25}")
    print(f"  TOTAL: {grand_total:>8,} records")
    print(f"  Elapsed: {grand_elapsed:.1f}s")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
