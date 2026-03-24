################################################################################
# GLP-1 Prescriber Characteristics Panel
#
# Purpose: Match Medicare Part D GLP-1 prescribers (2017-2023) with Open
#          Payments data to examine whether industry payments are associated
#          with prescribing volume (days supply) and prescriber characteristics.
#
# Outputs feed into:
# https://docs.google.com/spreadsheets/d/1G_p4EjhLXMOOJPQcX84KTlclpa4aCCjTFCKwDNuR9l0/
################################################################################

library(dplyr)
library(tidyr)
library(lubridate)
library(data.table)
library(stringr)
library(stringi)
library(DBI)
library(duckdb)
library(purrr)
library(readxl)
library(tigris)

################################################################################
# CONFIGURATION
################################################################################

# --- File paths ---------------------------------------------------------------
PATH_PARTD_RAW   <- "/N/slate/madkpott/PartD/prescriber/rawdata"
PATH_NPPES       <- "/N/project/postdobbs/data/NPPES_raw_with_tax/merged_2017_2024.parquet"
PATH_OP          <- "/N/project/ClimateAndEnvironment/UserSpecificFiles/Maddie/MaddieWorking/Open_Payment/GLP1/data/gendata"
PATH_OUT         <- "/N/slate/madkpott"
PATH_CHR         <- "/N/slate/madkpott/countyhealthrankings"
PATH_ZIPCOUNTY   <- "/N/slate/madkpott/county_area"
PATH_COUNTY_CHAR <- "/N/slate/madkpott/county_char"

# --- Study years --------------------------------------------------------------
PARTD_YEARS  <- 2017:2023
OP_YEARS     <- 2018:2023
PANEL_START  <- 2018
PANEL_END    <- 2023
CHR_YEARS    <- 2018:2022

# --- Drug name lists ----------------------------------------------------------
DRUGNAMES_GENERIC <- c(
  "LIXISENATIDE", "DULAGLUTIDE", "SEMAGLUTIDE",
  "LIRAGLUTIDE",  "EXENATIDE",   "TIRZEPATIDE"
)

# Maps each generic to its brand name(s) in the Open Payments files
BRAND_TO_GENERIC <- list(
  LIXISENATIDE = c("ADLYXIN",   "SOLIQUA"),
  DULAGLUTIDE  = c("TRULICITY"),
  SEMAGLUTIDE  = c("OZEMPIC",   "RYBELSUS", "WEGOVY"),
  LIRAGLUTIDE  = c("VICTOZA",   "XULTOPHY", "SAXENDA"),
  EXENATIDE    = c("BYDUREON",  "BYETTA"),
  TIRZEPATIDE  = c("MOUNJARO",  "ZEPBOUND")
)

# --- Column name references ---------------------------------------------------
COLS_PARTD_NAMES <- c("Brnd_Name", "Gnrc_Name")

COLS_OP_DRUG <- paste0(
  "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_", 1:5
)

COLS_PANEL_FILL <- c(
  "Prscrbr_Type", "Prscrbr_Last_Org_Name", "Prscrbr_First_Name",
  "Prscrbr_City", "enu_year", "enumeration_date"
)

COLS_OP_SELECT <- c(
  "Covered_Recipient_NPI",
  "Covered_Recipient_First_Name",
  "Covered_Recipient_Last_Name",
  "Total_Amount_of_Payment_USDollars",
  "Date_of_Payment",
  COLS_OP_DRUG,
  "Program_Year",
  "Payment_Publication_Date"
)

# --- County characteristic column names (canonical, from 2022) ----------------
COLS_COUNTY_CHAR <- c(
  "State", "County", "year",
  "percent_obesity", "percent_diagnosed_diabetes",
  "percent_lack_healthinsurance_18to64",
  "percent_visit_doctor_for_checkup_in_year"
)

# GLP-1 LIKE filter for DuckDB query
GLP1_SQL_FILTER <- paste(
  sprintf("Gnrc_Name LIKE '%%%s%%'", DRUGNAMES_GENERIC),
  collapse = "\n   OR "
)

################################################################################
# SECTION 1: LOAD & FILTER MEDICARE PART D DATA
################################################################################

# Load each year into DuckDB, rename NPI column, add year column, then UNION all
# years and filter to GLP-1 drugs only.
# NOTE: prscrbr_GLP1.csv is a cached output. To regenerate, uncomment the
#       write.csv line and comment out the read.csv line below.

con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

load_partd_year <- function(con, yr) {
  path  <- file.path(PATH_PARTD_RAW, paste0("prscrbr_", yr, ".csv"))
  tbl   <- paste0("prscrbr_", yr)
  dbExecute(con, sprintf(
    "CREATE TABLE %s AS
     SELECT *, %d AS year
     FROM read_csv_auto('%s', all_varchar=true, store_rejects=true,
                        strict_mode=false, null_padding=true)",
    tbl, yr, path
  ))
  dbExecute(con, sprintf(
    "ALTER TABLE %s RENAME COLUMN Prscrbr_NPI TO NPI", tbl
  ))
}

invisible(lapply(PARTD_YEARS, load_partd_year, con = con))

union_sql <- paste(
  sprintf("SELECT * FROM prscrbr_%d", PARTD_YEARS),
  collapse = "\nUNION\n"
)
dbExecute(con, sprintf("CREATE TABLE prscrbr AS %s", union_sql)) # ~178,993,800 rows
dbExecute(con, "UPDATE prscrbr SET Gnrc_Name = UPPER(Gnrc_Name)")

prscrbr_GLP1 <- dbGetQuery(
  con,
  sprintf("SELECT * FROM prscrbr WHERE %s", GLP1_SQL_FILTER)
)

# write.csv(prscrbr_GLP1, file.path(PATH_OUT, "prscrbr_GLP1.csv"), row.names = FALSE)
prscrbr_GLP1 <- read.csv(file.path(PATH_OUT, "prscrbr_GLP1.csv"))

dbDisconnect(con, shutdown = TRUE)

################################################################################
# SECTION 2: MERGE PART D WITH NPPES PROVIDER CHARACTERISTICS
################################################################################

# NPPES adds: NPI enumeration year, gender, zip code, credential, and taxonomy.
# DuckDB is used here because the NPPES parquet file is very large.
# NOTE: tax_all_20172023.csv is a cached output. To regenerate, uncomment the
#       write.csv line and comment out the read.csv line below.

con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
duckdb_register(con, "prscrbr_GLP1", prscrbr_GLP1)

dbExecute(con, sprintf(
  "CREATE TABLE nppes_raw AS SELECT * FROM read_parquet('%s')", PATH_NPPES
))

# Keep one row per NPI per year (most recent month)
dbExecute(con, "
  CREATE TABLE nppes_annual AS
  SELECT DISTINCT ON (NPI, year) *
  FROM nppes_raw
  ORDER BY NPI, year, month DESC
")

dbExecute(con, "
  CREATE TABLE nppes_slim AS
  SELECT NPI, year, taxonomy_code, pgender, ploczip, pcredential,
         enumeration_date, Grouping, Classification, Specialization
  FROM nppes_annual
")

partD <- dbGetQuery(con, "
  SELECT *
  FROM prscrbr_GLP1
  LEFT JOIN nppes_slim USING (NPI, year)
")

# write.csv(partD, file.path(PATH_OUT, "tax_all_20172023.csv"), row.names = FALSE)
partD <- read.csv(file.path(PATH_OUT, "tax_all_20172023.csv"))

dbDisconnect(con, shutdown = TRUE)

partD <- partD %>% mutate(prscrb_in_partD = 1)

################################################################################
# SECTION 3: LOAD & PROCESS OPEN PAYMENTS DATA
################################################################################

# Open Payments files are pre-filtered to GLP-1-related payments.
# For each year: compute payment counts and totals per NPI, then stack years.

load_op_year <- function(yr) {
  path <- file.path(
    PATH_OP, sprintf("Gen_%d_Filtered_GLP1.csv", yr)
  )
  df <- fread(path, select = COLS_OP_SELECT)
  colnames(df)[1] <- "NPI"
  df$year <- yr
  df <- df %>%
    group_by(NPI, year) %>%
    mutate(
      num_payment_encounters_inYEAR = n(),
      total_payment_inYEAR = sum(Total_Amount_of_Payment_USDollars, na.rm = TRUE)
    ) %>%
    ungroup() %>%
    mutate(Date_of_Payment = mdy(Date_of_Payment)) %>%
    distinct(NPI, year,
             Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1,
             Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2,
             Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3,
             Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4,
             .keep_all = TRUE)
  df
}

OP <- lapply(OP_YEARS, load_op_year) %>%
  bind_rows() %>%
  mutate(
    payment_encounter = 1,
    across(all_of(COLS_OP_DRUG), toupper)
  )

################################################################################
# SECTION 4: BUILD PART D PANEL DATASET
################################################################################

# Expand the data so every NPI has a row for every year (from their enumeration
# year or 2018, whichever is later) through 2023, crossed with each GLP-1 drug.
# Years with no prescribing are filled with Tot_Day_Suply = 0.

# Clean drug name variants back to their canonical generic names
partD <- partD %>%
  separate(enumeration_date,
           into = c("enu_month", "enu_day", "enu_year"),
           sep  = "/",
           remove = FALSE) %>%
  mutate(
    Gnrc_Name     = gsub("EXENATIDE MICROSPHERES",        "EXENATIDE",    Gnrc_Name),
    Gnrc_Name     = gsub("INSULIN GLARGINE/LIXISENATIDE", "LIXISENATIDE", Gnrc_Name),
    Gnrc_Name     = gsub("INSULIN DEGLUDEC/LIRAGLUTIDE",  "LIRAGLUTIDE",  Gnrc_Name),
    year          = as.numeric(year),
    enu_year      = as.numeric(enu_year) %>% replace_na(PANEL_START),
    Tot_Day_Suply = as.numeric(Tot_Day_Suply)
  )

# Generate all valid (NPI x year x drug) combinations
npi_year_grid <- partD %>%
  distinct(NPI, enu_year) %>%
  mutate(enu_year = replace_na(enu_year, PANEL_START)) %>%
  mutate(year = map(enu_year, ~ seq(max(.x, PANEL_START), PANEL_END))) %>%
  unnest(year) %>%
  distinct(NPI, year) %>%
  crossing(Gnrc_Name = unique(partD$Gnrc_Name))

# Left join observed prescribing data onto the full grid; fill provider-level
# fields across years and set missing days supply to 0
partD_panel <- npi_year_grid %>%
  left_join(partD, by = c("NPI", "year", "Gnrc_Name")) %>%
  group_by(NPI, Gnrc_Name) %>%
  fill(all_of(COLS_PANEL_FILL), .direction = "downup") %>%
  ungroup() %>%
  mutate(
    Tot_Day_Suply   = replace_na(Tot_Day_Suply,   0),
    prscrb_in_partD = replace_na(prscrb_in_partD, 0)
  )

rm(npi_year_grid)

################################################################################
# SECTION 5: MERGE PART D PANEL WITH OPEN PAYMENTS (BY DRUG)
################################################################################

# Open Payments uses brand names; map them back to generics before merging.
# Merge is done drug-by-drug to confirm that an NPI was paid specifically for
# the same class of GLP-1 they were prescribing.

# Build one OP data frame per generic, labeled with Gnrc_Name
op_by_generic <- imap(BRAND_TO_GENERIC, function(brands, generic) {
  OP %>%
    filter(if_any(all_of(COLS_OP_DRUG), ~ str_detect(., paste(brands, collapse = "|")))) %>%
    mutate(Gnrc_Name = generic) %>%
    distinct(NPI, year, Gnrc_Name, .keep_all = TRUE)
})

# Merge each generic's Part D panel slice with the corresponding OP data
merge_drug <- function(generic, op_df) {
  partD_panel %>%
    filter(Gnrc_Name == generic) %>%
    merge(op_df, by = c("NPI", "year", "Gnrc_Name"), all.x = TRUE) %>%
    mutate(
      payment_encounter = replace_na(payment_encounter, 0),
      prscrb_in_partD   = replace_na(prscrb_in_partD,  0)
    )
}

drug_merged_list <- imap(op_by_generic, merge_drug)

# Stack all drugs into one analysis dataset (LIRAGLUTIDE excluded per original)
merge <- bind_rows(
  drug_merged_list[c("SEMAGLUTIDE", "DULAGLUTIDE", "TIRZEPATIDE",
                     "LIXISENATIDE", "EXENATIDE")]
)

rm(op_by_generic, drug_merged_list, partD_panel)

################################################################################
# HELPER FUNCTIONS
################################################################################

# Add a pay_indicator: 1 if the NPI ever received any GLP-1 payment
add_pay_indicator <- function(df, group_vars = "NPI") {
  df %>%
    group_by(across(all_of(group_vars))) %>%
    mutate(pay_indicator = as.integer(any(payment_encounter == 1))) %>%
    ungroup()
}

# Impute suppressed beneficiary counts (NA = suppressed <11 patients -> use 5)
impute_benes <- function(df) {
  df %>%
    mutate(
      Tot_Benes     = as.numeric(as.character(Tot_Benes)),
      Tot_Benes_NA5 = case_when(
        !is.na(Tot_Benes)   ~ Tot_Benes,
        prscrb_in_partD == 1 ~ 5,   # suppressed
        TRUE                 ~ 0    # did not prescribe
      )
    )
}

# Run a t-test on days supply between paid vs. not-paid groups; return results
# alongside unique-NPI counts for each group
run_days_supply_ttest <- function(df, label = NULL) {
  paid     <- df %>% filter(pay_indicator == 1)
  not_paid <- df %>% filter(pay_indicator == 0)
  list(
    label          = label,
    ttest          = t.test(paid$Tot_Day_Suply, not_paid$Tot_Day_Suply),
    count_paid     = n_distinct(paid$NPI),
    count_not_paid = n_distinct(not_paid$NPI)
  )
}

# Run a t-test on an arbitrary continuous variable between paid vs. not-paid
# groups; splits on pay_indicator and calls t.test()
run_county_ttest <- function(df, var, label = NULL) {
  paid     <- df %>% filter(pay_indicator == 1) %>% pull({{ var }})
  not_paid <- df %>% filter(pay_indicator == 0) %>% pull({{ var }})
  list(
    label      = label,
    ttest      = t.test(paid, not_paid),
    mean_paid  = mean(paid,     na.rm = TRUE),
    mean_unpaid = mean(not_paid, na.rm = TRUE)
  )
}

################################################################################
# SECTION 6: TABLE 1 ANALYSIS — DAYS SUPPLY (ALL YEARS, BY YEAR, BY DRUG)
################################################################################

# --- All-years combined -------------------------------------------------------
merge_allyr <- merge %>%
  add_pay_indicator("NPI") %>%
  impute_benes()

ttest_allyears <- run_days_supply_ttest(merge_allyr, label = "All years")

mean_days_by_payment <- merge_allyr %>%
  group_by(pay_indicator) %>%
  summarise(mean_days_supply = mean(Tot_Day_Suply), .groups = "drop")

# --- By year ------------------------------------------------------------------
merge_byyr <- merge %>%
  add_pay_indicator(c("NPI", "year")) %>%
  impute_benes()

mean_days_by_year_payment <- merge_byyr %>%
  group_by(pay_indicator, year) %>%
  summarise(mean_days_supply = mean(Tot_Day_Suply), .groups = "drop")

ttest_by_year <- lapply(OP_YEARS[OP_YEARS <= 2022], function(yr) {
  run_days_supply_ttest(
    merge_byyr %>% filter(year == yr),
    label = as.character(yr)
  )
})
names(ttest_by_year) <- as.character(OP_YEARS[OP_YEARS <= 2022])

# --- By drug ------------------------------------------------------------------
ttest_by_drug <- lapply(DRUGNAMES_GENERIC[DRUGNAMES_GENERIC != "LIRAGLUTIDE"], function(drug) {
  df <- merge %>%
    filter(Gnrc_Name == drug) %>%
    add_pay_indicator("NPI")
  run_days_supply_ttest(df, label = drug)
})
names(ttest_by_drug) <- DRUGNAMES_GENERIC[DRUGNAMES_GENERIC != "LIRAGLUTIDE"]

################################################################################
# SECTION 7: TABLE 2 ANALYSIS — PRESCRIBER CHARACTERISTICS
################################################################################

# --- Number of Medicare Patients ----------------------------------------------
# NAs in Tot_Benes indicate CMS suppression (< 11 patients); imputed as 5.

bene_by_npi <- merge_allyr %>%
  group_by(NPI) %>%
  summarise(
    total_benes   = sum(Tot_Benes_NA5),
    pay_indicator = first(pay_indicator),
    .groups       = "drop"
  )

ttest_benes <- t.test(total_benes ~ pay_indicator, data = bene_by_npi)

# --- Years of Practice --------------------------------------------------------
# Uses the original enumeration_date (not the panel's enu_year) for accuracy.
# Assumes each NPI is active through their latest observed year in the data.

merge_yop <- merge %>%
  separate(enumeration_date,
           into   = c("enu_yop_day", "enu_yop_month", "enu_yop_year"),
           sep    = "/",
           remove = FALSE) %>%
  add_pay_indicator("NPI") %>%
  group_by(NPI) %>%
  arrange(desc(year)) %>%
  distinct(NPI, .keep_all = TRUE) %>%
  ungroup() %>%
  mutate(
    enu_yop_year    = as.numeric(enu_yop_year),
    yrs_of_practice = year - enu_yop_year
  )

yop_paid     <- merge_yop %>% filter(pay_indicator == 1)
yop_not_paid <- merge_yop %>% filter(pay_indicator == 0)

mean_yop_paid     <- mean(yop_paid$yrs_of_practice,     na.rm = TRUE)
mean_yop_not_paid <- mean(yop_not_paid$yrs_of_practice, na.rm = TRUE)
ttest_yop         <- t.test(yop_paid$yrs_of_practice, yop_not_paid$yrs_of_practice)

# --- Percent Male Physician ---------------------------------------------------

merge_gen <- merge %>%
  add_pay_indicator("NPI") %>%
  group_by(NPI) %>%
  arrange(desc(year)) %>%
  distinct(NPI, .keep_all = TRUE) %>%
  ungroup() %>%
  mutate(
    pgender       = na_if(pgender, ""),
    gender_binary = case_when(
      pgender == "M" ~ 1L,
      pgender == "F" ~ 0L,
      TRUE           ~ NA_integer_
    )
  ) %>%
  filter(!is.na(gender_binary))

gen_paid     <- merge_gen %>% filter(pay_indicator == 1)
gen_not_paid <- merge_gen %>% filter(pay_indicator == 0)

gender_counts_paid     <- gen_paid     %>% summarise(count = n(), male = sum(gender_binary))
gender_counts_not_paid <- gen_not_paid %>% summarise(count = n(), male = sum(gender_binary))

pct_male_paid     <- with(gender_counts_paid,     male / count) # 22907/46276 = 0.495
pct_male_not_paid <- with(gender_counts_not_paid, male / count) # 13104/31077 = 0.422

ttest_gender <- t.test(gen_paid$gender_binary, gen_not_paid$gender_binary)

################################################################################
# SECTION 8: LOAD COUNTY-LEVEL CONTEXTUAL DATA
################################################################################

# This section builds a county-level panel (2018-2022) by combining:
#   (1) Robert Wood Johnson Foundation County Health Rankings (CHR)
#   (2) ZIP-to-FIPS crosswalk files
#   (3) Census county area for population density
#   (4) CDC PLACES county-level health characteristics (obesity, diabetes, etc.)

# --- 8a. Load County Health Rankings ------------------------------------------

load_chr_year <- function(yr) {
  df      <- read_excel(file.path(PATH_CHR, paste0(yr, "_CHR.xlsx")))
  df$year <- yr
  df
}

chr_list <- lapply(CHR_YEARS, load_chr_year)
names(chr_list) <- as.character(CHR_YEARS)

# --- 8b. Load ZIP-to-FIPS crosswalk files -------------------------------------
# One file per year; second column renamed to FIPS for consistency.

load_zip_county_year <- function(yr) {
  df       <- read_excel(file.path(PATH_ZIPCOUNTY, sprintf("ZIP_COUNTY_12%d.xlsx", yr)))
  df$year  <- yr
  colnames(df)[2] <- "FIPS"
  df
}

zc_list <- lapply(CHR_YEARS, load_zip_county_year)
names(zc_list) <- as.character(CHR_YEARS)

# --- 8c. Merge CHR with ZIP-to-FIPS, add census area, harmonise columns -------
# Columns 14-17 (2018-2020) or 14-19 (2021-2022) are artefact columns dropped
# after the merge. All yearly frames are then renamed to match 2022's columns.

add_area_to_chr <- function(yr, chr_df, zc_df) {
  # Merge CHR with ZIP crosswalk
  merged <- merge(chr_df, zc_df, by = c("FIPS", "year"))

  # Drop artefact columns introduced by the crosswalk merge
  drop_end <- if (yr >= 2021) 19L else 17L
  merged   <- merged[, -c(14:drop_end)]

  # Fetch Census county boundaries and compute area in square miles
  area_df       <- counties(year = yr, cb = TRUE)
  area_df$FIPS  <- paste0(area_df$STATEFP, area_df$COUNTYFP)
  area_df$area_sqmi <- area_df$ALAND * 0.000000386102
  area_df       <- area_df %>% select(FIPS, area_sqmi, NAME)

  merge(merged, area_df, by = "FIPS", all.x = TRUE)
}

chr_with_area <- mapply(
  add_area_to_chr,
  yr     = CHR_YEARS,
  chr_df = chr_list,
  zc_df  = zc_list,
  SIMPLIFY = FALSE
)

# Harmonise column names to match 2022's schema across all years
target_cols <- colnames(chr_with_area[["2022"]])
chr_with_area <- lapply(chr_with_area, function(df) {
  colnames(df) <- target_cols
  df
})

CHR <- bind_rows(chr_with_area)
rm(chr_list, zc_list, chr_with_area)

# One ZIP per year maps to one county only
CHR <- CHR %>% distinct(ZIP, year, .keep_all = TRUE)

# --- 8d. Load CDC PLACES county health characteristics ------------------------
# Column order varies by year; all frames are renamed to a canonical schema.

load_county_char_year <- function(yr) {
  path <- file.path(PATH_COUNTY_CHAR, sprintf("OBESITY_%d.csv", yr))
  df   <- read.csv(path)

  # 2022 has duplicate rows; deduplicate before pivoting
  if (yr == 2022) {
    df <- df %>% distinct(StateDesc, Measure, LocationName, Year, .keep_all = TRUE)
  }

  df <- pivot_wider(
    data      = df,
    names_from  = Measure,
    values_from = Data_Value,
    id_cols     = c(StateDesc, LocationName, Year)
  )

  colnames(df) <- COLS_COUNTY_CHAR
  df
}

count_char <- lapply(CHR_YEARS, load_county_char_year) %>% bind_rows()

# Merge CDC health characteristics onto CHR
CHR <- merge(CHR, count_char, by = c("year", "State", "County"), all.x = TRUE)

rm(count_char)

################################################################################
# SECTION 9: PREPARE NPPES-LINKED PANEL FOR COUNTY MERGE
################################################################################

# Standardise ZIP codes to 5-digit zero-padded strings, then collapse to one
# row per (NPI x ZIP x year) with a payment indicator.

merge_NPPES <- merge %>%
  mutate(
    ploczip = stri_pad_left(substr(ploczip, 1, 5), 5, pad = "0"),
    ZIP     = ploczip
  ) %>%
  add_pay_indicator(c("NPI", "ploczip", "year")) %>%
  filter(!is.na(ploczip) & ploczip != "") %>%
  distinct(NPI, year, ploczip, .keep_all = TRUE)

# Merge county-level context onto the NPPES panel
NPPES_CHR <- merge(merge_NPPES, CHR, by = c("ZIP", "year"))

rm(merge_NPPES)

################################################################################
# SECTION 10: TABLE 3 ANALYSIS — COUNTY CHARACTERISTICS
################################################################################

# For each county characteristic, compute means and run a t-test comparing
# prescribers who received GLP-1 payments vs. those who did not.

# Pre-compute population density (residents per square mile)
NPPES_CHR <- NPPES_CHR %>%
  mutate(res_per_sqmi = Population / area_sqmi)

# --- Population density -------------------------------------------------------
ttest_pop_density <- run_county_ttest(
  NPPES_CHR, res_per_sqmi,
  label = "Population density (residents/sq mi)"
)

# --- % Rural ------------------------------------------------------------------
ttest_rural <- run_county_ttest(
  NPPES_CHR, `% rural`,
  label = "% rural"
)

# --- % Non-Hispanic White -----------------------------------------------------
ttest_nhw <- run_county_ttest(
  NPPES_CHR, `% Non-Hispanic white`,
  label = "% Non-Hispanic white"
)

# --- % Obesity ----------------------------------------------------------------
ttest_obesity <- run_county_ttest(
  NPPES_CHR, percent_obesity,
  label = "% obesity"
)

# --- % Diagnosed Diabetes -----------------------------------------------------
ttest_diabetes <- run_county_ttest(
  NPPES_CHR, percent_diagnosed_diabetes,
  label = "% diagnosed diabetes"
)

# --- % Without Health Insurance (ages 18-64) ----------------------------------
ttest_no_insurance <- run_county_ttest(
  NPPES_CHR, percent_lack_healthinsurance_18to64,
  label = "% lacking health insurance (18-64)"
)

# --- % Annual Doctor Check-up Visit -------------------------------------------
ttest_checkup <- run_county_ttest(
  NPPES_CHR, percent_visit_doctor_for_checkup_in_year,
  label = "% visited doctor for check-up in year"
)
