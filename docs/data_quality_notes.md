# Data Quality Notes

Known data quality issues identified during profiling and pipeline development. Record counts marked TBD will be populated after running the full pipeline against the database.

---

## Claims Data Issues

| Issue | Records Affected | Resolution |
|-------|-----------------|------------|
| NULL `dateOfLoss` | TBD | **Excluded** in Silver layer — rows with NULL date of loss are filtered out during Bronze → Silver transformation, as date of loss is required for all downstream analytics |
| Paid amount > coverage limit | TBD | **Retained with awareness** — some claims show `amountPaidOnBuildingClaim` exceeding `totalBuildingInsuranceCoverage`. This is a known NFIP characteristic: Increased Cost of Compliance (ICC) payments and supplemental payments can push totals above the base coverage limit. These rows are kept but flagged in the test suite |
| Negative paid amounts | TBD | **Retained** — negative values (min -$201,667 building, -$80,000 contents) represent claim adjustments, reversals, or recoveries. These are valid accounting entries in the NFIP system. The Silver layer COALESCE converts NULLs to 0 but preserves negatives |
| Duplicate claims | TBD (Bronze - Silver delta) | **Deduplicated** in Silver using ROW_NUMBER() partitioned by state, countyCode, dateOfLoss, amountPaidOnBuildingClaim, and amountPaidOnContentsClaim. The most recent ingestion_timestamp is kept |
| High NULL rate on `floodZoneCurrent` | ~67.9% of claims | **Mitigated** — Silver uses COALESCE(ratedFloodZone, floodZoneCurrent) to resolve flood zone. ratedFloodZone has only ~3.3% nulls and is the better source. Remaining unresolved zones map to the Unknown dimension row |
| High NULL rate on `basementEnclosureCrawlspaceType` | ~74.0% of claims | **Accepted** — this field is not populated for many older records. NULLs map to 'Unknown' in the building type dimension |
| Outlier coverage values | TBD | **Retained** — `totalBuildingInsuranceCoverage` has a max of ~$272.5M, far exceeding the NFIP residential max of $250K. These may be commercial policies or data entry errors. Flagged for manual review |

---

## Policies Data Issues

| Issue | Records Affected | Resolution |
|-------|-----------------|------------|
| NULL `policyEffectiveDate` | TBD | **Excluded** in Silver layer — rows with NULL effective date are filtered out, as policy dates are required for exposure calculation and date dimension joins |
| Exposure > 1.0 (term > 1 year) | TBD | **Capped** at 1.0 in Silver layer — NFIP policies are annual, so exposure should not exceed 1.0. Values above 1.0 are capped; values below 0.0 are floored to 0.0 |
| Negative premiums | TBD | **Retained** — negative values (min -$3,352) represent premium adjustments or refunds. These are valid entries in the NFIP system |
| Duplicate policies | TBD (Bronze - Silver delta) | **Deduplicated** in Silver using ROW_NUMBER() partitioned by propertyState, countyCode, policyEffectiveDate, totalInsurancePremiumOfThePolicy, and totalBuildingInsuranceCoverage |
| High NULL rate on `crsClassCode` | ~46.3% of policies | **Accepted** — not all communities participate in the CRS programme. NULLs indicate non-participating communities. In Gold, these policies still resolve to a valid occupancy and location dimension |
| High NULL rate on `basementEnclosureCrawlspaceType` | ~51.8% of policies | **Accepted** — same as claims, many records lack this field. Maps to 'Unknown' in building type dimension |
| Truncated policy data | 200,000 per state (all 5 truncated) | **Accepted for initial build** — the `MAX_RECORDS_PER_STATE` cap in the ingestion script limits each state to 200K policies for manageable download size. Full dataset requires increasing this parameter. All analysis should note this is a sample, not the complete population |
| Outlier coverage values | TBD | **Retained** — `totalBuildingInsuranceCoverage` max of ~$204M. Same considerations as claims |

---

## Cross-Dataset Issues

| Issue | Impact | Notes |
|-------|--------|-------|
| Claims-to-policies join mismatch | Cannot directly link individual claims to their originating policies | The NFIP datasets do not include a shared policy identifier between claims and policies. Analysis joins on location + flood zone + time period, not at the individual policy level |
| Policy data is a sample | Affects loss ratio and premium adequacy calculations | All 5 states hit the 200K record cap. Loss ratios calculated against this sample will differ from the true population values. Increase `MAX_RECORDS_PER_STATE` for production accuracy |
| Date range mismatch | Claims: 1978-2026, Policies: 2009-2025 | Claims span a much longer history than available policy data. Loss ratio and frequency calculations are only meaningful for the overlapping period (2009-2025) |
