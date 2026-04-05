


library(data.table)
library(stringi)
library(stringr)
library(dplyr)
library(ggplot2)
library(tidyr)
library(readxl)
library(zoo)
library(DBI)
library(duckdb)


############################################################################################################
#getting IRA drugs for the first round

#GLP1
GLP1 <- read_excel("/N/slate/madkpott/Meds_Lists/GLP1_ADHD/GLP1_NDC_list_updated_10232025.xlsx")

GLP1$name <- toupper(GLP1$brand_name)
GLP1$generic_name <- toupper(GLP1$generic_name)

GLP1$F3_ndc <- stri_pad_left(str=GLP1$NDC,11, pad = "0")

GLP1 <- GLP1 %>% distinct(F3_ndc, brand_name, .keep_all = TRUE)

#generic names
drugname_gen <- c("LIXISENATIDE", "DULAGLUTIDE", "SEMAGLUTIDE", "LIRAGLUTIDE", "EXENATIDE", "TIRZEPATIDE")


#brand names
drugname_brnd <- c("ADLYXIN", "SOLIQUA", "TRULICITY", "OZEMPIC", "RYBELSUS", "WEGOVY", "SAXENDA", "XULTOPHY", 
                    "VICTOZA", "BYDUREON", "BYETTA", "MOUNJARO", "ZEPBOUND")

#############################################################################################
#duckdb to get Open Payment encounters for GLP1s

#Connect 
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

#Set your file path and list CSVs
file_list <- list.files(
  path = "/N/project/ClimateAndEnvironment/UserSpecificFiles/Maddie/MaddieWorking/Open_Payment/rawdata/2019_2024/", 
  pattern = "OP_gen_.*\\.csv$",  
  full.names = TRUE,
  recursive = TRUE
)

#Common columns 
common_cols <- c(
  "Covered_Recipient_NPI", "Covered_Recipient_First_Name", "Covered_Recipient_Last_Name",
  "Total_Amount_of_Payment_USDollars", "Date_of_Payment",
  "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1",
  "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2",
  "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3",
  "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4",
  "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5",
  "Program_Year", "Payment_Publication_Date",
  "Associated_Drug_or_Biological_NDC_1", "Associated_Drug_or_Biological_NDC_2",
  "Associated_Drug_or_Biological_NDC_3", "Associated_Drug_or_Biological_NDC_4",
  "Associated_Drug_or_Biological_NDC_5"
)

#Function to generate cleaned NDC SQL
ndc_clean_sql <- function(col) {
  cleaned_col <- paste0(col, "_Clean")
  paste0(
    "CASE WHEN ", col, " IS NOT NULL THEN ",
    "LPAD(REGEXP_EXTRACT(", col, ", '^([0-9]+)-[0-9]+-[0-9]+$', 1), 5, '0') || ",
    "LPAD(REGEXP_EXTRACT(", col, ", '^[0-9]+-([0-9]+)-[0-9]+$', 1), 4, '0') || ",
    "LPAD(REGEXP_EXTRACT(", col, ", '^[0-9]+-[0-9]+-([0-9]+)$', 1), 2, '0') ",
    "ELSE NULL END AS ", cleaned_col
  )
}

#Generate SQL for all cleaned NDC columns
ndc_columns <- paste0("Associated_Drug_or_Biological_NDC_", 1:5)
ndc_sql_lines <- vapply(ndc_columns, ndc_clean_sql, character(1))

#Combine all columns for SELECT
select_columns <- c(common_cols, ndc_sql_lines)
select_clause <- paste(select_columns, collapse = ", ")

#Generate SQL query for each file
file_queries <- sapply(file_list, function(file) {
  paste0("SELECT ", select_clause, " FROM read_csv('", file, "')")
})

#Combine all queries with UNION ALL
full_query <- paste(file_queries, collapse = " UNION ALL ")

#Execute query and load result into R
result_df <- dbGetQuery(con, full_query)

#Register NOENT as a DuckDB table 
duckdb_register(con, "GLP1", GLP1)

#Register result_df as a table if needed
duckdb_register(con, "result_df", result_df)

#SQL query to filter rows with matching NDCs
filtered_query <- "
  SELECT *
  FROM result_df
  WHERE 
    Associated_Drug_or_Biological_NDC_1_Clean IN (SELECT F3_ndc FROM GLP1) OR
    Associated_Drug_or_Biological_NDC_2_Clean IN (SELECT F3_ndc FROM GLP1) OR
    Associated_Drug_or_Biological_NDC_3_Clean IN (SELECT F3_ndc FROM GLP1) OR
    Associated_Drug_or_Biological_NDC_4_Clean IN (SELECT F3_ndc FROM GLP1) OR
    Associated_Drug_or_Biological_NDC_5_Clean IN (SELECT F3_ndc FROM GLP1)
"

#Run the query
filtered_df <- dbGetQuery(con, filtered_query)

rm(result_df)

#####################################################################################################
#looping to get every drug on it's own OP graph

ndc_columns_clean <- c("Associated_Drug_or_Biological_NDC_1_Clean", "Associated_Drug_or_Biological_NDC_2_Clean",
                       "Associated_Drug_or_Biological_NDC_3_Clean", "Associated_Drug_or_Biological_NDC_4_Clean", 
                       "Associated_Drug_or_Biological_NDC_5_Clean")

duckdb_register(con, "filtered_df", filtered_df)


#looping by generic name first
for (drug in drugname_gen){
  
  NDCs <- GLP1 %>% filter(generic_name == drug)
  
  drug_ndcs <- paste0("NDCs_", drug)
  
  duckdb_register(con, drug_ndcs, NDCs)
  
  filtered_query <- sprintf("
    SELECT *
    FROM filtered_df
    WHERE 
      Associated_Drug_or_Biological_NDC_1_Clean IN (SELECT F3_ndc FROM %s) OR
      Associated_Drug_or_Biological_NDC_2_Clean IN (SELECT F3_ndc FROM %s) OR
      Associated_Drug_or_Biological_NDC_3_Clean IN (SELECT F3_ndc FROM %s) OR
      Associated_Drug_or_Biological_NDC_4_Clean IN (SELECT F3_ndc FROM %s) OR
      Associated_Drug_or_Biological_NDC_5_Clean IN (SELECT F3_ndc FROM %s)
  ", drug_ndcs, drug_ndcs, drug_ndcs, drug_ndcs, drug_ndcs)
  
  #Run the query
  df <- dbGetQuery(con, filtered_query)
  
  assign(paste0("OP_", drug), df)
  
}



#looping by brand name
for (drug in drugname_brnd){
  
  NDCs <- GLP1 %>% filter(brand_name == drug)
  
  drug_ndcs <- paste0("NDCs_", drug)
  
  duckdb_register(con, drug_ndcs, NDCs)
  
  filtered_query <- sprintf("
    SELECT *
    FROM filtered_df
    WHERE 
      Associated_Drug_or_Biological_NDC_1_Clean IN (SELECT F3_ndc FROM %s) OR
      Associated_Drug_or_Biological_NDC_2_Clean IN (SELECT F3_ndc FROM %s) OR
      Associated_Drug_or_Biological_NDC_3_Clean IN (SELECT F3_ndc FROM %s) OR
      Associated_Drug_or_Biological_NDC_4_Clean IN (SELECT F3_ndc FROM %s) OR
      Associated_Drug_or_Biological_NDC_5_Clean IN (SELECT F3_ndc FROM %s)
  ", drug_ndcs, drug_ndcs, drug_ndcs, drug_ndcs, drug_ndcs)
  
  #Run the query
  df <- dbGetQuery(con, filtered_query)
  
  assign(paste0("OP_", drug), df)
  
}

####################################################################################
#generic name
#grouping by NPI and getting the first date from date_of_payment

datasets <- c("OP_LIXISENATIDE", "OP_DULAGLUTIDE", "OP_SEMAGLUTIDE", 
              "OP_LIRAGLUTIDE", "OP_EXENATIDE", "OP_TIRZEPATIDE")


for (data in datasets){
  
  df <- get(data)
  
  df$Date_of_Payment <- as.Date(df$Date_of_Payment, format = "%Y/%m/%d")
  
  df_group <- df %>%
    group_by(Covered_Recipient_NPI) %>%
    arrange(Date_of_Payment)%>%
    slice(1L)
  
  df_group <- df_group[, c(1, 5)] 
  
  df_group <- df_group %>% distinct(Covered_Recipient_NPI, .keep_all = TRUE)
  
  assign(paste0(data, "_pay"), df_group)
  
}


colnames(OP_LIXISENATIDE_pay)[2] <- "first_payment_LIXISENATIDE"
colnames(OP_DULAGLUTIDE_pay)[2] <- "first_payment_DULAGLUTIDE"
colnames(OP_SEMAGLUTIDE_pay)[2] <- "first_payment_SEMAGLUTIDE"
colnames(OP_LIRAGLUTIDE_pay)[2] <- "first_payment_LIRAGLUTIDE"
colnames(OP_EXENATIDE_pay)[2] <- "first_payment_EXENATIDE"
colnames(OP_TIRZEPATIDE_pay)[2] <- "first_payment_TIRZEPATIDE"

####################################################################################
#brand name
#grouping by NPI and getting the first date from date_of_payment

datasets <- c("ADLYXIN", "SOLIQUA", "TRULICITY", "OZEMPIC", "RYBELSUS", "WEGOVY", "SAXENDA", "XULTOPHY", 
                   "VICTOZA", "BYDUREON", "BYETTA", "MOUNJARO", "ZEPBOUND")

for (data in datasets){
  
  df <- get(data)
  
  df$Date_of_Payment <- as.Date(df$Date_of_Payment, format = "%Y/%m/%d")
  
  df_group <- df %>%
    group_by(Covered_Recipient_NPI) %>%
    arrange(Date_of_Payment)%>%
    slice(1L)
  
  df_group <- df_group[, c(1, 5)] 
  
  df_group <- df_group %>% distinct(Covered_Recipient_NPI, .keep_all = TRUE)
  
  assign(paste0(data, "_pay"), df_group)
  
}


colnames(OP_ADLYXIN_pay)[2] <- "first_payment_ADLYXIN"
colnames(OP_SOLIQUA_pay)[2] <- "first_payment_SOLIQUA"
colnames(OP_TRULICITY_pay)[2] <- "first_payment_TRULICITY"
colnames(OP_OZEMPIC_pay)[2] <- "first_payment_OZEMPIC"
colnames(OP_RYBELSUS_pay)[2] <- "first_payment_RYBELSUS"
colnames(OP_WEGOVY_pay)[2] <- "first_payment_WEGOVY"
colnames(OP_SAXENDA_pay)[2] <- "first_payment_SAXENDA"
colnames(OP_XULTOPHY_pay)[2] <- "first_payment_XULTOPHY"
colnames(OP_VICTOZA_pay)[2] <- "first_payment_VICTOZA"
colnames(OP_BYDUREON_pay)[2] <- "first_payment_BYDUREON"
colnames(OP_BYETTA_pay)[2] <- "first_payment_BYETTA"
colnames(OP_MOUNJARO_pay)[2] <- "first_payment_MOUNJARO"
colnames(OP_ZEPBOUND_pay)[2] <- "first_payment_ZEPBOUND"

###############################################################################################
#left joining

OP_NPI <- filtered_df %>%
  distinct(Covered_Recipient_NPI)%>% na.omit()


OP_fdp <- left_join(OP_NPI, OP_LIXISENATIDE_pay, by = "Covered_Recipient_NPI")
OP_fdp <- left_join(OP_fdp, OP_DULAGLUTIDE_pay, by = "Covered_Recipient_NPI")
OP_fdp <- left_join(OP_fdp, OP_SEMAGLUTIDE_pay, by = "Covered_Recipient_NPI")
OP_fdp <- left_join(OP_fdp, OP_LIRAGLUTIDE_pay, by = "Covered_Recipient_NPI")
OP_fdp <- left_join(OP_fdp, OP_EXENATIDE_pay, by = "Covered_Recipient_NPI")
OP_fdp <- left_join(OP_fdp, OP_TIRZEPATIDE_pay, by = "Covered_Recipient_NPI")
OP_fdp <- left_join(OP_fdp, OP_ADLYXIN_pay, by = "Covered_Recipient_NPI")
OP_fdp <- left_join(OP_fdp, OP_SOLIQUA_pay, by = "Covered_Recipient_NPI")
OP_fdp <- left_join(OP_fdp, OP_TRULICITY_pay, by = "Covered_Recipient_NPI")
OP_fdp <- left_join(OP_fdp, OP_OZEMPIC_pay, by = "Covered_Recipient_NPI")
OP_fdp <- left_join(OP_fdp, OP_WEGOVY_pay, by = "Covered_Recipient_NPI")
OP_fdp <- left_join(OP_fdp, OP_SAXENDA_pay, by = "Covered_Recipient_NPI")
OP_fdp <- left_join(OP_fdp, OP_XULTOPHY_pay, by = "Covered_Recipient_NPI")
OP_fdp <- left_join(OP_fdp, OP_VICTOZA_pay, by = "Covered_Recipient_NPI")
OP_fdp <- left_join(OP_fdp, OP_BYDUREON_pay, by = "Covered_Recipient_NPI")
OP_fdp <- left_join(OP_fdp, OP_BYETTA_pay, by = "Covered_Recipient_NPI")
OP_fdp <- left_join(OP_fdp, OP_MOUNJARO_pay, by = "Covered_Recipient_NPI")
OP_fdp <- left_join(OP_fdp, OP_ZEPBOUND_pay, by = "Covered_Recipient_NPI")
OP_fdp <- left_join(OP_fdp, OP_RYBELSUS_pay, by = "Covered_Recipient_NPI")


write.csv(OP_fdp, "/N/slate/madkpott/GLP1_OP_firstpayment.csv")



