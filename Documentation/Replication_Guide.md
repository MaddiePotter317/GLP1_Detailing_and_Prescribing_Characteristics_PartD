# Step-by-Step Guide: Obtaining and Cleaning Data for GLP-1 Prescriber Characteristics Analysis

**Project:** GLP-1 Prescriber Characteristics and Advertising Payments (2018–2023)  
**Language:** R

---

## Overview

This project examines the characteristics and prescribing behaviors of NPIs (National Provider Identifiers) that received advertising payments for GLP-1 drugs compared to NPIs that did not. The analysis links Medicare Part D prescribing data with Open Payments industry payment records, NPPES provider demographics, and county-level contextual data to produce a prescriber-level panel dataset covering 2018–2023.

This guide walks through each source in order and specifies all cleaning steps required before the files are ready for analysis.

The data sources are:

1. **Medicare Part D Prescriber Data** — prescribing volume by NPI and drug
2. **NPPES** — provider-level demographics and taxonomy
3. **Open Payments** — drug payment records by NPI
4. **County Health Rankings (RWJF)** — county-level health and demographic context
5. **ZIP-to-FIPS Crosswalk** — links provider ZIP codes to county FIPS codes
6. **CDC PLACES Health Characteristics** — county-level obesity, diabetes, and insurance data

> **Note:** Census county boundary data (used to compute population density) is fetched programmatically via the `tigris` R package and does not require a separate download step. See Part 4c for details.

---

## GLP-1 Drugs Covered

| Brand Name | Generic Name |
|---|---|
| Ozempic | Semaglutide |
| Wegovy | Semaglutide |
| Rybelsus | Semaglutide |
| Trulicity | Dulaglutide |
| Victoza | Liraglutide |
| Saxenda | Liraglutide |
| Xultophy | Liraglutide |
| Byetta | Exenatide |
| Bydureon | Exenatide |
| Adlyxin | Lixisenatide |
| Soliqua | Lixisenatide |
| Mounjaro | Tirzepatide |
| Zepbound | Tirzepatide |

> **Note:** Liraglutide is included in the data loading and panel construction steps but is excluded from the final merged analysis dataset.

---

## Part 1: Medicare Part D Prescriber Data

### What it is

The Medicare Part D Prescriber by Provider and Drug files contain prescribing records at the NPI-drug-year level. Each row represents a unique combination of prescriber and drug within a given year and includes prescribing volume metrics such as total days supply, total claims, and beneficiary counts. This is the primary source for measuring GLP-1 prescribing behavior.

### Where to download

**https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug**

Download the annual prescriber-by-drug files for **2017 through 2023**. Files are named:

```
MUP_DPR_RY{RELEASE_YEAR}_P04_V10_DY{DATA_YEAR}_NPIBN.csv
```

You will download 7 files total (one per year, 2017–2023).

> **Note:** 2017 data is loaded to support panel construction (specifically, NPI enumeration year imputation) but the analysis panel itself begins in 2018.

### Folder structure recommendation

```
/PartD/
  prescriber/
    rawdata/
      prscrbr_2017.csv
      prscrbr_2018.csv
      ...
      prscrbr_2023.csv
```

> **Note:** The script expects files named `prscrbr_{YEAR}.csv`. Rename the raw CMS files to match this convention after downloading.

### Key columns used in analysis

| Column | Description |
|---|---|
| `Prscrbr_NPI` | NPI of the prescribing provider (renamed to `NPI` in the script) |
| `Brnd_Name` | Brand name of the drug |
| `Gnrc_Name` | Generic name of the drug |
| `Tot_Day_Suply` | Total days supply prescribed |
| `Tot_Clms` | Total number of claims |
| `Tot_Benes` | Total number of unique Medicare beneficiaries |

### Cleaning steps

The script loads all seven years into an in-memory DuckDB database, unions them into a single table, uppercases the `Gnrc_Name` field, and filters to GLP-1 drugs using a `LIKE` filter on generic name. Several drug name variants are also normalised to their canonical generics:

```r
# Variants cleaned to canonical generic names
Gnrc_Name = gsub("EXENATIDE MICROSPHERES",        "EXENATIDE",    Gnrc_Name)
Gnrc_Name = gsub("INSULIN GLARGINE/LIXISENATIDE", "LIXISENATIDE", Gnrc_Name)
Gnrc_Name = gsub("INSULIN DEGLUDEC/LIRAGLUTIDE",  "LIRAGLUTIDE",  Gnrc_Name)
```

The filtered output is cached as `prscrbr_GLP1.csv`. To regenerate this cache from the raw files, uncomment the `write.csv` line in Section 1 of the script.

---

## Part 2: NPPES

### What it is

The National Plan and Provider Enumeration System (NPPES) contains provider-level demographic and credentialing information for every active NPI. It is used here to add provider gender, practice ZIP code, credential, taxonomy/specialty, and NPI enumeration date to the Part D prescribing records.

The script works with a pre-processed internal parquet file that merges annual NPPES snapshots from 2017 through 2024 and includes taxonomy classifications.

### Where to get it

This is **internal data** maintained by the research team. The file is located at:

```
/N/project/postdobbs/data/NPPES_raw_with_tax/merged_2017_2024.parquet
```

Contact the project team if you need access.

### Key columns used in analysis

| Column | Description |
|---|---|
| `NPI` | Provider NPI |
| `year` | Year of the NPPES snapshot |
| `pgender` | Provider gender (`M` / `F`) |
| `ploczip` | Provider practice ZIP code |
| `pcredential` | Provider credential (e.g. MD, DO) |
| `enumeration_date` | Date the NPI was first issued |
| `taxonomy_code` | Provider taxonomy code |
| `Grouping` / `Classification` / `Specialization` | Taxonomy hierarchy fields |

### Cleaning steps

DuckDB is used to join the NPPES parquet file to the filtered Part D data. One row per NPI per year is retained by keeping the most recent monthly snapshot:

```r
CREATE TABLE nppes_annual AS
SELECT DISTINCT ON (NPI, year) *
FROM nppes_raw
ORDER BY NPI, year, month DESC
```

The merged output is cached as `tax_all_20172023.csv`. To regenerate from the raw parquet, uncomment the `write.csv` line in Section 2 of the script.

---

## Part 3: Open Payments

### What it is

The CMS Open Payments database records financial transfers from pharmaceutical and medical device manufacturers to physicians and teaching hospitals. In this project it is used to identify which prescribers received GLP-1-related industry payments and how much they received, by year. The files used here are **pre-filtered** to GLP-1-related payments only.

### Where to download

**https://openpaymentsdata.cms.gov/datasets/download**

Download the **General Payments** files for **2018 through 2023** and pre-filter to rows where any of the five drug/device name columns contains a GLP-1 brand name. Save one filtered file per year.

Files should be named:

```
Gen_{YEAR}_Filtered_GLP1.csv
```

### Folder structure recommendation

```
/Open_Payment/
  GLP1/
    data/
      gendata/
        Gen_2018_Filtered_GLP1.csv
        Gen_2019_Filtered_GLP1.csv
        ...
        Gen_2023_Filtered_GLP1.csv
```

### Key columns used in analysis

| Column | Description |
|---|---|
| `Covered_Recipient_NPI` | NPI of the payment recipient (renamed to `NPI`) |
| `Total_Amount_of_Payment_USDollars` | Dollar value of the payment |
| `Date_of_Payment` | Date the payment was made |
| `Name_of_Drug_or_Biological_..._1` through `..._5` | Up to five drug/device names associated with the payment |
| `Program_Year` | Year the payment was reported |

### Cleaning steps

For each year, payment counts and total dollar amounts are computed per NPI, and records are deduplicated to one row per unique NPI-year-drug combination:

```r
df <- df %>%
  group_by(NPI, year) %>%
  mutate(
    num_payment_encounters_inYEAR = n(),
    total_payment_inYEAR = sum(Total_Amount_of_Payment_USDollars, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  distinct(NPI, year,
           Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1,
           Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2,
           Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3,
           Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4,
           .keep_all = TRUE)
```

All drug name columns are uppercased for consistent matching. When merging with the Part D panel, Open Payments brand names are mapped back to their canonical generic names before the join, so that each NPI's payment record is matched to the specific GLP-1 class they were paid to promote.

---

## Part 4: County-Level Contextual Data

Three separate data sources are combined to build the county-level panel used in the prescriber characteristics analysis. All three cover **2018 through 2022**.

---

### Part 4a: County Health Rankings (RWJF)

#### What it is

The Robert Wood Johnson Foundation County Health Rankings provide annual county-level data on a wide range of health outcomes and social determinants. In this project they supply population size, percent rural, and percent Non-Hispanic white population by county and year.

#### Where to download

**https://www.countyhealthrankings.org/health-data/methodology-and-sources/data-documentation**

Download the annual Excel files for **2018 through 2022**. Files should be named:

```
{YEAR}_CHR.xlsx
```

#### Folder structure recommendation

```
/countyhealthrankings/
  2018_CHR.xlsx
  2019_CHR.xlsx
  2020_CHR.xlsx
  2021_CHR.xlsx
  2022_CHR.xlsx
```

#### Cleaning steps

Each yearly file is read and tagged with a `year` column. After merging with the ZIP-to-FIPS crosswalk (see Part 4b), artefact columns introduced by the merge are dropped. The number of columns to drop differs slightly between years (columns 14–17 for 2018–2020, columns 14–19 for 2021–2022). All yearly data frames are then renamed to match 2022's column schema before being stacked. Finally, the combined dataset is deduplicated so that each ZIP code maps to only one county per year.

---

### Part 4b: ZIP-to-FIPS Crosswalk

#### What it is

The HUD USPS ZIP-to-County crosswalk file maps ZIP codes to county FIPS codes. It is required because the NPPES provider data contains ZIP codes while the County Health Rankings data is organised by county FIPS. This crosswalk is the bridge between the two.

#### Where to download

**https://www.huduser.gov/portal/datasets/usps_crosswalk.html**

Download the **ZIP-COUNTY** crosswalk files by **Q4 of each year** for **2018 through 2022**. Files should be named:

```
ZIP_COUNTY_12{YEAR}.xlsx
```

#### Folder structure recommendation

```
/county_area/
  ZIP_COUNTY_122018.xlsx
  ZIP_COUNTY_122019.xlsx
  ZIP_COUNTY_122020.xlsx
  ZIP_COUNTY_122021.xlsx
  ZIP_COUNTY_122022.xlsx
```

#### Cleaning steps

The second column of each crosswalk file is renamed to `FIPS` for consistency across years. A `year` column is added before merging with the CHR data.

---

### Part 4c: Census County Boundaries (via `tigris`)

#### What it is

County boundary shapefiles from the U.S. Census Bureau are used to obtain county land area in square meters (`ALAND`), which is converted to square miles to compute population density (residents per square mile).

#### Where to get it

No manual download is required. The `tigris` R package fetches these files directly from the Census Bureau:

```r
library(tigris)
area_df <- counties(year = yr, cb = TRUE)
```

This is called automatically within the script for each year from 2018 to 2022. Ensure you have an active internet connection when running this section for the first time. The `tigris` package caches downloads locally after the first run.

#### Cleaning steps

State and county FIPS components are concatenated to form a 5-digit FIPS code, land area is converted from square meters to square miles using the factor `0.000000386102`, and only the `FIPS`, `area_sqmi`, and `NAME` columns are retained before merging with the CHR data.

---

### Part 4d: CDC PLACES Health Characteristics

#### What it is

The CDC PLACES dataset provides model-based estimates of health-related behaviours and conditions at the county level. In this project it supplies four county-level measures: percent obesity, percent diagnosed diabetes, percent lacking health insurance (ages 18–64), and percent who visited a doctor for a check-up in the past year.

#### Where to download

**https://data.cdc.gov/500-Cities-Places/PLACES-Local-Data-for-Better-Health-County-Data-20/dv4u-3x3q/about_data**

Download the county-level data files for **2018 through 2022**. Files should be named:

```
OBESITY_{YEAR}.csv
```

#### Folder structure recommendation

```
/county_char/
  OBESITY_2018.csv
  OBESITY_2019.csv
  OBESITY_2020.csv
  OBESITY_2021.csv
  OBESITY_2022.csv
```

#### Cleaning steps

Each file is pivoted from long to wide format so that each health measure becomes its own column. The column order of the health measures varies across years, so all files are renamed to a canonical schema after pivoting:

```r
colnames(df) <- c("State", "County", "year",
                  "percent_obesity",
                  "percent_diagnosed_diabetes",
                  "percent_lack_healthinsurance_18to64",
                  "percent_visit_doctor_for_checkup_in_year")
```

The 2022 file contains duplicate rows and requires deduplication before pivoting:

```r
df <- df %>% distinct(StateDesc, Measure, LocationName, Year, .keep_all = TRUE)
```

---

## Final Folder Structure

After downloading everything, your project directory should look like this:

```
/project/
  PartD/
    prescriber/
      rawdata/
        prscrbr_2017.csv  (through prscrbr_2023.csv)
  Open_Payment/
    GLP1/
      data/
        gendata/
          Gen_2018_Filtered_GLP1.csv  (through Gen_2023_Filtered_GLP1.csv)
  countyhealthrankings/
    2018_CHR.xlsx  (through 2022_CHR.xlsx)
  county_area/
    ZIP_COUNTY_122018.xlsx  (through ZIP_COUNTY_122022.xlsx)
  county_char/
    OBESITY_2018.csv  (through OBESITY_2022.csv)
  Scripts/
    GLP1_prscrbrs_characteristics_panel_clean.R
  Outputs/
    prscrbr_GLP1.csv        (cached — regenerate from Section 1)
    tax_all_20172023.csv    (cached — regenerate from Section 2)
```

> **Internal data** (NPPES parquet file) is stored separately on the project server and is not included in the folder structure above.

---

## Summary Checklist

Before moving on to analysis, confirm each of the following:

- [ ] Part D prescriber files downloaded for 2017–2023, renamed to `prscrbr_{YEAR}.csv`, and placed in `rawdata/`
- [ ] `prscrbr_GLP1.csv` cache generated (or confirmed present) in the outputs directory
- [ ] NPPES parquet file accessible at its internal server path; `tax_all_20172023.csv` cache generated or confirmed present
- [ ] Open Payments files pre-filtered to GLP-1 payments and saved as `Gen_{YEAR}_Filtered_GLP1.csv` for 2018–2023
- [ ] County Health Rankings Excel files downloaded for 2018–2022 and named `{YEAR}_CHR.xlsx`
- [ ] ZIP-to-FIPS crosswalk files downloaded for Q4 of 2018–2022 and named `ZIP_COUNTY_12{YEAR}.xlsx`
- [ ] CDC PLACES files downloaded for 2018–2022 and named `OBESITY_{YEAR}.csv`; 2022 file checked for duplicates
- [ ] `tigris` package installed and internet connection available for Census county boundary downloads

Once all data sources are in place and cached outputs are confirmed, you are ready to run the full analysis pipeline.
