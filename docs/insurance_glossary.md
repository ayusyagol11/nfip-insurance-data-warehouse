# Insurance Glossary

Key insurance and actuarial terms used throughout the NFIP Insurance Data Warehouse.

---

### Loss Ratio
The ratio of claims paid out to premiums collected, expressed as a decimal or percentage. A loss ratio of 1.0 (or 100%) means the insurer paid out exactly as much in claims as it received in premiums. Ratios above 1.0 indicate underwriting losses — the programme paid more in claims than it collected, which is common in catastrophe years for NFIP.

### Pure Premium
The theoretical premium needed to cover expected losses, calculated as total incurred losses divided by total earned exposure. Pure premium does not include expenses, profit margins, or administrative costs. It serves as the floor for adequate pricing — if the actual premium charged is below the pure premium, the rate is inadequate.

### Earned Premium
The portion of a policy's premium that corresponds to the time period that has already elapsed. For a one-year policy halfway through its term, half the written premium has been earned. In this project, earned premium is approximated by multiplying the total premium by the exposure fraction.

### Written Premium
The total premium charged when a policy is issued, regardless of how much time has elapsed. Written premium represents the full contractual amount. In NFIP data, the `totalInsurancePremiumOfThePolicy` field represents the written premium for each policy.

### Exposure
A measure of the risk period covered by a policy, expressed as a fraction of a year. A policy in force for exactly 12 months has an exposure of 1.0. Exposure is critical for normalising claims frequency — without it, a state with more policies in force would appear to have higher frequency simply due to volume. In this project, exposure is derived as the number of days between policy effective and termination dates divided by 365.25, capped between 0 and 1.

### Claims Frequency
The number of claims per unit of exposure, measuring how often losses occur. Calculated as claim count divided by total earned exposure. High frequency in a flood zone indicates that losses are common, even if each individual loss may be small. Frequency and severity together determine the expected loss cost.

### Claims Severity
The average cost per claim, measuring how expensive each loss event is. Calculated as total claims paid divided by the number of claims. Hurricane Harvey (2017) in Texas, for example, produced both high frequency and high severity. Severity analysis often splits building versus contents payments to understand where the damage concentrates.

### SFHA (Special Flood Hazard Area)
A geographic area designated by FEMA as having a 1% or greater annual chance of flooding (the "100-year floodplain"). SFHA zones include all A zones and V zones. Properties in SFHAs with federally backed mortgages are required to carry flood insurance. This mandatory purchase requirement is a major driver of NFIP policy volume.

### Claims Triangle (Loss Development Triangle)
A tabular format showing how cumulative paid claims develop over time from the date of loss. Each row represents an accident year, and each column represents an evaluation point (12 months, 24 months, etc.). Triangles reveal how quickly claims are settled and help actuaries estimate ultimate losses. Note: the NFIP dataset lacks incremental payment dates, so a true development triangle cannot be constructed — only ultimate paid amounts by accident year are available.

### IBNR (Incurred But Not Reported)
An actuarial reserve for claims that have occurred but have not yet been reported to the insurer. After a flood event, it takes time for all policyholders to file claims. IBNR estimates are derived from historical development patterns in claims triangles. Since the NFIP data in this project represents final reported claims, IBNR is not directly modelled but is referenced for context.

### CRS (Community Rating System)
A voluntary FEMA programme that rewards communities for floodplain management practices exceeding NFIP minimum requirements. Communities earn CRS class ratings from 1 (best) to 10 (no discount), and policyholders in participating communities receive premium discounts of 5% to 45%. Lower CRS class numbers indicate better community flood mitigation. In this dataset, `crsClassCode` reflects the community's CRS classification at the time of the policy.

### NFIP (National Flood Insurance Program)
A federal programme managed by FEMA that provides flood insurance to property owners, renters, and businesses in participating communities. Created by the National Flood Insurance Act of 1968, the NFIP aims to reduce the financial impact of flooding by offering affordable insurance and encouraging communities to adopt floodplain management standards. The programme covers approximately 5 million policies nationwide and is the data source for this entire project.

### Flood Zone A
High-risk flood zones with a 1% annual chance of flooding (100-year floodplain). Includes sub-zones A, AE, AH, AO, A1-A30, AR, and A99. These are Special Flood Hazard Areas where flood insurance purchase is mandatory for properties with federally backed mortgages. AE zones have base flood elevations determined; unnumbered A zones do not.

### Flood Zone V
Coastal high-risk flood zones subject to wave action in addition to flooding. Includes V and VE sub-zones. These carry the highest risk and typically the highest premiums. V zones are found along coastlines and are subject to storm surge and wave heights of 3 feet or more during the base flood event.

### Flood Zone X
Moderate-to-low risk areas outside the 100-year floodplain. Includes the former B and C zones. Zone X (shaded) has a 0.2% annual chance of flooding (500-year floodplain). Zone X (unshaded) has minimal flood risk. Flood insurance is not required in X zones but is available and recommended.

### Flood Zone D
Areas where FEMA has not performed a flood hazard analysis and the flood risk is undetermined. No mandatory insurance purchase requirement exists, but flood risk may still be present. These areas are typically unmapped or under study.
