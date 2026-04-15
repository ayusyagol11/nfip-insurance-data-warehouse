# NFIP Data Profile Report

## Claims Dataset

**Total rows:** 1,703,977

### Row Count per State

| State | Rows |
|-------|------|
| FL | 448,275 |
| LA | 484,881 |
| NJ | 202,131 |
| NY | 175,151 |
| TX | 393,539 |

### Column Types

| Column | Dtype |
|--------|-------|
| dateOfLoss | object |
| state | object |
| countyCode | float64 |
| floodZoneCurrent | object |
| amountPaidOnBuildingClaim | float64 |
| amountPaidOnContentsClaim | float64 |
| totalBuildingInsuranceCoverage | float64 |
| totalContentsInsuranceCoverage | float64 |
| occupancyType | float64 |
| yearOfLoss | int64 |
| causeOfDamage | object |
| ratedFloodZone | object |
| primaryResidenceIndicator | bool |
| originalConstructionDate | object |
| numberOfFloorsInTheInsuredBuilding | float64 |
| elevatedBuildingIndicator | bool |
| basementEnclosureCrawlspaceType | float64 |

### Null Analysis

| Column | Null Count | Null % |
|--------|-----------|--------|
| dateOfLoss | 0 | 0.0% |
| state | 0 | 0.0% |
| countyCode | 9,522 | 0.6% |
| floodZoneCurrent | 1,156,164 | 67.9% |
| amountPaidOnBuildingClaim | 342,918 | 20.1% |
| amountPaidOnContentsClaim | 342,918 | 20.1% |
| totalBuildingInsuranceCoverage | 4 | 0.0% |
| totalContentsInsuranceCoverage | 21 | 0.0% |
| occupancyType | 240 | 0.0% |
| yearOfLoss | 0 | 0.0% |
| causeOfDamage | 25,332 | 1.5% |
| ratedFloodZone | 57,064 | 3.3% |
| primaryResidenceIndicator | 0 | 0.0% |
| originalConstructionDate | 1,500 | 0.1% |
| numberOfFloorsInTheInsuredBuilding | 9,587 | 0.6% |
| elevatedBuildingIndicator | 0 | 0.0% |
| basementEnclosureCrawlspaceType | 1,261,374 | 74.0% |

### Numeric Column Statistics

| Column | Min | Max | Mean | Median |
|--------|-----|-----|------|--------|
| countyCode | 12,001.00 | 48,507.00 | 28,331.75 | 22,089.00 |
| amountPaidOnBuildingClaim | -201,667.50 | 10,741,476.93 | 41,805.95 | 14,453.06 |
| amountPaidOnContentsClaim | -80,000.00 | 757,048.95 | 9,034.79 | 128.98 |
| totalBuildingInsuranceCoverage | 0.00 | 272,500,000.00 | 227,000.38 | 150,000.00 |
| totalContentsInsuranceCoverage | 0.00 | 3,000,000.00 | 39,578.03 | 21,000.00 |
| occupancyType | 1.00 | 19.00 | 3.76 | 1.00 |
| yearOfLoss | 1,978.00 | 2,026.00 | 2,004.65 | 2,005.00 |
| numberOfFloorsInTheInsuredBuilding | 1.00 | 6.00 | 1.59 | 1.00 |
| basementEnclosureCrawlspaceType | 0.00 | 4.00 | 0.88 | 0.00 |

### Categorical Column Value Counts (Top 15)

#### floodZoneCurrent

| Value | Count |
|-------|-------|
| AE | 340,613 |
| X | 121,909 |
| VE | 17,023 |
| A | 14,336 |
| B | 8,480 |
| C | 8,018 |
| AHB | 7,618 |
| AH | 4,605 |
| A08 | 4,464 |
| AO | 2,268 |
| A10 | 2,023 |
| A07 | 1,519 |
| A99 | 1,498 |
| A13 | 1,456 |
| A06 | 1,368 |

#### occupancyType

| Value | Count |
|-------|-------|
| 1.0 | 1,081,864 |
| 11.0 | 306,403 |
| 2.0 | 91,598 |
| 4.0 | 78,825 |
| 3.0 | 51,096 |
| 18.0 | 22,423 |
| 15.0 | 20,487 |
| 12.0 | 15,429 |
| 6.0 | 12,466 |
| 16.0 | 9,453 |
| 14.0 | 7,319 |
| 13.0 | 6,079 |
| 19.0 | 190 |
| 17.0 | 105 |

#### state

| Value | Count |
|-------|-------|
| LA | 484,881 |
| FL | 448,275 |
| TX | 393,539 |
| NJ | 202,131 |
| NY | 175,151 |

#### causeOfDamage

| Value | Count |
|-------|-------|
| 4 | 842,905 |
| 1 | 422,661 |
| 2 | 241,286 |
| 0 | 145,010 |
| B | 15,514 |
| 3 | 8,590 |
| 9 | 895 |
| D | 680 |
| A | 431 |
| C | 385 |
| 8 | 149 |
| 7 | 122 |
| 5 | 10 |
| @ | 2 |
| 6 | 2 |

#### ratedFloodZone

| Value | Count |
|-------|-------|
| AE | 648,188 |
| X | 278,393 |
| C | 98,246 |
| A | 82,886 |
| B | 75,918 |
| A01 | 46,966 |
| A04 | 33,405 |
| AHB | 29,309 |
| A08 | 29,145 |
| VE | 28,811 |
| A06 | 28,323 |
| A05 | 27,872 |
| A03 | 25,590 |
| A07 | 24,761 |
| A10 | 22,577 |

#### primaryResidenceIndicator

| Value | Count |
|-------|-------|
| True | 926,787 |
| False | 777,190 |

#### elevatedBuildingIndicator

| Value | Count |
|-------|-------|
| False | 1,442,990 |
| True | 260,987 |

#### basementEnclosureCrawlspaceType

| Value | Count |
|-------|-------|
| 0.0 | 226,437 |
| 2.0 | 111,036 |
| 1.0 | 84,052 |
| 4.0 | 21,078 |

### Date Ranges

| Column | Min | Max |
|--------|-----|-----|
| dateOfLoss | 1978-01-01 | 2026-02-10 |

## Policies Dataset

**Total rows:** 1,000,000

### Row Count per State

| State | Rows |
|-------|------|
| FL | 200,000 |
| LA | 200,000 |
| NJ | 200,000 |
| NY | 200,000 |
| TX | 200,000 |

### Column Types

| Column | Dtype |
|--------|-------|
| propertyState | object |
| countyCode | float64 |
| floodZoneCurrent | object |
| totalInsurancePremiumOfThePolicy | int64 |
| buildingDeductibleCode | object |
| totalBuildingInsuranceCoverage | int64 |
| totalContentsInsuranceCoverage | int64 |
| policyEffectiveDate | object |
| policyTerminationDate | object |
| construction | bool |
| occupancyType | int64 |
| numberOfFloorsInInsuredBuilding | float64 |
| primaryResidenceIndicator | bool |
| crsClassCode | float64 |
| ratedFloodZone | object |
| originalConstructionDate | object |
| elevatedBuildingIndicator | bool |
| basementEnclosureCrawlspaceType | float64 |

### Null Analysis

| Column | Null Count | Null % |
|--------|-----------|--------|
| propertyState | 0 | 0.0% |
| countyCode | 2,757 | 0.3% |
| floodZoneCurrent | 322,806 | 32.3% |
| totalInsurancePremiumOfThePolicy | 0 | 0.0% |
| buildingDeductibleCode | 16,375 | 1.6% |
| totalBuildingInsuranceCoverage | 0 | 0.0% |
| totalContentsInsuranceCoverage | 0 | 0.0% |
| policyEffectiveDate | 0 | 0.0% |
| policyTerminationDate | 0 | 0.0% |
| construction | 0 | 0.0% |
| occupancyType | 0 | 0.0% |
| numberOfFloorsInInsuredBuilding | 891 | 0.1% |
| primaryResidenceIndicator | 0 | 0.0% |
| crsClassCode | 463,395 | 46.3% |
| ratedFloodZone | 828 | 0.1% |
| originalConstructionDate | 262 | 0.0% |
| elevatedBuildingIndicator | 0 | 0.0% |
| basementEnclosureCrawlspaceType | 518,352 | 51.8% |

### Numeric Column Statistics

| Column | Min | Max | Mean | Median |
|--------|-----|-----|------|--------|
| countyCode | 6,053.00 | 72,127.00 | 30,493.80 | 34,023.00 |
| totalInsurancePremiumOfThePolicy | -3,352.00 | 413,560.00 | 914.22 | 478.00 |
| totalBuildingInsuranceCoverage | 0.00 | 204,000,000.00 | 252,563.43 | 250,000.00 |
| totalContentsInsuranceCoverage | 0.00 | 500,000.00 | 60,133.03 | 60,000.00 |
| occupancyType | 1.00 | 19.00 | 3.70 | 1.00 |
| numberOfFloorsInInsuredBuilding | 1.00 | 6.00 | 1.66 | 1.00 |
| crsClassCode | 3.00 | 10.00 | 6.88 | 7.00 |
| basementEnclosureCrawlspaceType | 0.00 | 4.00 | 0.77 | 0.00 |

### Categorical Column Value Counts (Top 15)

#### floodZoneCurrent

| Value | Count |
|-------|-------|
| AE | 312,458 |
| X | 256,257 |
| AHB | 20,978 |
| A | 16,371 |
| C | 14,945 |
| VE | 8,967 |
| A08 | 7,253 |
| B | 6,871 |
| AH | 6,636 |
| AO | 3,765 |
| A07 | 3,742 |
| A05 | 2,660 |
| A99 | 2,504 |
| AOB | 1,887 |
| A06 | 1,681 |

#### occupancyType

| Value | Count |
|-------|-------|
| 1 | 649,761 |
| 11 | 183,955 |
| 2 | 51,413 |
| 3 | 33,318 |
| 6 | 22,854 |
| 4 | 18,290 |
| 18 | 13,861 |
| 12 | 11,025 |
| 16 | 4,441 |
| 15 | 4,000 |
| 14 | 3,727 |
| 13 | 3,096 |
| 19 | 169 |
| 17 | 90 |

#### propertyState

| Value | Count |
|-------|-------|
| FL | 200,000 |
| LA | 200,000 |
| NJ | 200,000 |
| NY | 200,000 |
| TX | 200,000 |

#### ratedFloodZone

| Value | Count |
|-------|-------|
| X | 401,187 |
| AE | 393,204 |
| C | 36,549 |
| A | 27,087 |
| AH | 21,665 |
| B | 21,391 |
| A08 | 14,562 |
| A07 | 11,206 |
| VE | 9,486 |
| A05 | 8,097 |
| AO | 7,014 |
| AHB | 6,716 |
| A04 | 5,731 |
| A06 | 5,566 |
| A01 | 3,849 |

#### construction

| Value | Count |
|-------|-------|
| False | 997,771 |
| True | 2,229 |

#### primaryResidenceIndicator

| Value | Count |
|-------|-------|
| True | 707,449 |
| False | 292,551 |

#### crsClassCode

| Value | Count |
|-------|-------|
| 6.0 | 161,778 |
| 8.0 | 125,747 |
| 7.0 | 95,130 |
| 5.0 | 76,152 |
| 9.0 | 56,345 |
| 10.0 | 13,189 |
| 4.0 | 4,661 |
| 3.0 | 3,603 |

#### elevatedBuildingIndicator

| Value | Count |
|-------|-------|
| False | 834,959 |
| True | 165,041 |

#### basementEnclosureCrawlspaceType

| Value | Count |
|-------|-------|
| 0.0 | 280,125 |
| 2.0 | 99,975 |
| 1.0 | 77,842 |
| 4.0 | 23,706 |

### Date Ranges

| Column | Min | Max |
|--------|-----|-----|
| policyEffectiveDate | 2009-01-01 | 2025-12-31 |
| policyTerminationDate | 2010-01-01 | 2027-11-15 |
