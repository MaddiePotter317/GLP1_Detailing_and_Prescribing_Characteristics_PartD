# GLP-1 Prescriber Characteristics and Industry Payments Analysis

Analysis of Medicare Part D GLP-1 prescriber characteristics and prescribing behaviors, comparing NPIs that received industry advertising payments to those that did not. This repository examines prescribing volume, provider demographics, and county-level contextual factors across 2018–2023.

---

## Project Overview

GLP-1 receptor agonists have become among the most prescribed and commercially promoted drug classes in the United States. Pharmaceutical manufacturers frequently make payments to physicians under the Open Payments program to support promotional activities.

---

## GLP-1 Drugs Analyzed (13 drugs)

| Brand Name | Generic Name |
|---|---|
| Ozempic | Semaglutide |
| Wegovy | Semaglutide |
| Rybelsus | Semaglutide |
| Mounjaro | Tirzepatide |
| Zepbound | Tirzepatide |
| Trulicity | Dulaglutide |
| Victoza | Liraglutide |
| Saxenda | Liraglutide |
| Xultophy | Liraglutide |
| Byetta | Exenatide |
| Bydureon | Exenatide |
| Adlyxin | Lixisenatide |
| Soliqua | Lixisenatide |

> **Note:** Liraglutide is included in data loading and panel construction but excluded from the final merged analysis dataset.

---

## Repository Structure

```
GLP1_Prescriber_Characteristics/
├── README.md                                         # This file
├── Replication_Guide.md                              # Step-by-step data acquisition guide
├── Scripts/
│   └── GLP1_prscrbrs_characteristics_panel_clean.R  # Full analysis pipeline
├── Data/                                             # Raw data files (not tracked in git)
│   ├── PartD/
│   │   └── prescriber/
│   │       └── rawdata/                              # prscrbr_2017.csv through prscrbr_2023.csv
│   ├── Open_Payment/
│   │   └── GLP1/
│   │       └── data/
│   │           └── gendata/                          # Gen_{YEAR}_Filtered_GLP1.csv (2018–2023)
│   ├── countyhealthrankings/                         # {YEAR}_CHR.xlsx (2018–2022)
│   ├── county_area/                                  # ZIP_COUNTY_12{YEAR}.xlsx (2018–2022)
│   └── county_char/                                  # OBESITY_{YEAR}.csv (2018–2022)
└── Outputs/                                          # Cached intermediates and results
    ├── prscrbr_GLP1.csv                              # Cached Part D GLP-1 filter output
    └── tax_all_20172023.csv                          # Cached Part D + NPPES merged output
```

> **Note:** NPPES data is internal and stored separately on the project server. See the Replication Guide for access details.

---

## Key Metrics Calculated

### Table 1 — Prescribing Volume (Days Supply)

Comparisons between paid and unpaid prescribers:

- Mean total days supply of GLP-1 drugs prescribed
- Mean days supply by year (2018–2022)
- Mean days supply by drug (Semaglutide, Dulaglutide, Tirzepatide, Lixisenatide, Exenatide)

### Table 2 — Prescriber Characteristics

- Total number of Medicare patients (beneficiary count, with CMS suppression imputed)
- Years in practice (derived from NPI enumeration date)
- Percent male prescribers

### Table 3 — County-Level Practice Setting Characteristics

- Population density (residents per square mile)
- % rural population
- % Non-Hispanic white population
- % obesity
- % diagnosed diabetes
- % lacking health insurance (ages 18–64)
- % who visited a doctor for an annual check-up

All comparisons are made between paid (pay_indicator = 1) and unpaid (pay_indicator = 0) prescribers using two-sample t-tests.

---

## Script Overview

The analysis is contained in a single R script (`GLP1_prscrbrs_characteristics_panel_clean.R`) organized into ten sections:

| Section | Description |
|---|---|
| 1 | Load and filter Medicare Part D data to GLP-1 drugs |
| 2 | Merge Part D with NPPES provider demographics |
| 3 | Load and process Open Payments data |
| 4 | Build balanced prescriber-level panel dataset |
| 5 | Merge Part D panel with Open Payments by drug |
| 6 | Table 1 analysis — days supply comparisons |
| 7 | Table 2 analysis — prescriber characteristics |
| 8 | Load county-level contextual data (CHR, ZIP crosswalk, Census area, CDC PLACES) |
| 9 | Prepare NPPES-linked panel for county merge |
| 10 | Table 3 analysis — county characteristic comparisons |

---

## Data Sources

| Source | Years | Access |
|---|---|---|
| Medicare Part D Prescriber Data | 2017–2023 | [CMS](https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug) |
| NPPES | 2017–2024 | Internal |
| Open Payments | 2018–2023 | [CMS](https://openpaymentsdata.cms.gov/datasets/download) |
| County Health Rankings (RWJF) | 2018–2022 | [RWJF](https://www.countyhealthrankings.org/health-data/methodology-and-sources/data-documentation) |
| ZIP-to-FIPS Crosswalk (HUD) | 2018–2022 | [HUD](https://www.huduser.gov/portal/datasets/usps_crosswalk.html) |
| CDC PLACES Health Characteristics | 2018–2022 | [CDC](https://data.cdc.gov/500-Cities-Places/PLACES-Local-Data-for-Better-Health-County-Data-20/dv4u-3x3q/about_data) |
| Census County Boundaries | 2018–2022 | Fetched via `tigris` R package |

For full download instructions, file naming conventions, and cleaning steps, see `Replication_Guide.md`.

---

## Important Notes

### Data Privacy
- Medicare Part D, Open Payments, County Health Rankings, and CDC PLACES data are all **publicly available**.
- NPPES is an **internal dataset** maintained by the research team. No PHI or individual-level patient data is used.
- CMS suppresses beneficiary counts below 11. These are imputed as 5 in the analysis, consistent with standard practice in the Part D literature.


---

**Last Updated:** March 2026
