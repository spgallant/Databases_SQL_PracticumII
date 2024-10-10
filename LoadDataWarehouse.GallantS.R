# title: "Practicum II - LoadDataWarehouse"
# author: "Sarah Gallant"
# date: Summer Full 2023


# Load libraries
library(RSQLite)
library(XML)
library(RMySQL)
library(dplyr)

# --------------- Database Connections --------------------
# Function to connect to an existing MySQL database
# inputs: 
#     (1) db_name: the name of the database
#     (2) db_user: the username for accessing the database
#     (3) db_password: the password for accessing the database
#     (4) db_host: the host/hosting site for accessing the database
#     (5) db_port: the access port for the specific database
# output: the variable holding the database connection
connectToDB_MySQL <- function(db_name, db_user, db_password, db_host, db_port) {
  
  # Connect to the database
  mydb <- dbConnect(RMySQL::MySQL(), user = db_user, password = db_password,
                    dbname = db_name, host = db_host, port = db_port)
  
  return(mydb)
}


# Function to connect to a SQLite database, creating a new one
#   if it does not yet exist
# inputs: 
#     (1) db_name: the name of the database
#     (2) fpath: the file path where the database will be saved
# output: the variable holding the database connection
connectToDB_SQLite <- function(db_name, fpath) {
  
  # Connect to the database
  dbcon <- dbConnect(RSQLite::SQLite(), paste0(fpath, db_name)) 
  
  return(dbcon)
}




# ------------ Create MySQL Star Schema DB --------------
# Note: this section is constructed of two star schemas:
#  (1) product_facts fact table with dimension tables for 
#        product, date, and region
#  (2) rep_facts fact table with dimension tables for 
#        rep, date, and product


# Function to create the 'product_dim' dimension table
# The table is first dropped if it exists, then created with appropriate
#   attributes
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but a table is created
create_table_product_dim <- function(dbcon) {
  
  # Drop the table if it already exists
  dbExecute(dbcon, "DROP TABLE IF EXISTS product_dim;")
  
  # Create the table with auto-increment primary key
  dbExecute(dbcon, "
    CREATE TABLE product_dim (
      productID INT AUTO_INCREMENT PRIMARY KEY,
      productName TEXT
    );"
  )
}


# Function to create the 'region_dim' dimension table
# The table is first dropped if it exists, then created with appropriate
#   attributes
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but a table is created
create_table_region_dim <- function(dbcon) {
  
  # Drop the table if it already exists
  dbExecute(dbcon, "DROP TABLE IF EXISTS region_dim;")
  
  # Create the table with auto-increment primary key
  dbExecute(dbcon, "
    CREATE TABLE region_dim (
      regionID INT AUTO_INCREMENT PRIMARY KEY,
      regionName TEXT
    );"
  )
}


# Function to create the 'date_dim' dimension table
# The table is first dropped if it exists, then created with appropriate
#   attributes
# Date Strategy: The year exists as an integer (e.g. '2020', '0'), while
#   the quarter is specified text (e.g. 'Q1', 'ALL'). The timeID is 
#  constructed from a combination of the year and quarter fields 
#  (e.g. '2020-Q1', 'ALL-ALL') and thus has a specified text length type.
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but a table is created
create_table_date_dim <- function(dbcon) {
  
  # Drop the table if it already exists
  dbExecute(dbcon, "DROP TABLE IF EXISTS date_dim;")
  
  # Create the table 
  dbExecute(dbcon, "
    CREATE TABLE date_dim (
      timeID VARCHAR(10) PRIMARY KEY,
      year INT,
      quarter VARCHAR(10)
    );"
  )
}


# Function to create the 'product_facts' fact table
# The table is first dropped if it exists, then created with appropriate
#   attributes
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but a table is created
create_table_product_facts <- function(dbcon) {
  
  # Drop the table if it already exists
  dbExecute(dbcon, "DROP TABLE IF EXISTS product_facts;")
  
  # Create the table - no PK as per typical construction of fact tables
  dbExecute(dbcon, "
    CREATE TABLE product_facts (
      product_key INT,
      time_key VARCHAR(10),
      region_key INT,
      totalSold INT
    );"
  )
}


# Function to add the product_key foreign key to product_facts
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but the table is altered to add the FK
add_product_key_fk <- function(dbcon) {
  
  dbExecute(dbcon, "
    ALTER TABLE product_facts
    ADD CONSTRAINT fk_product_key
    FOREIGN KEY (product_key) REFERENCES product_dim(productID);
  ")
}


# Function to add the time_key foreign key to product_facts
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but the table is altered to add the FK
add_time_key_fk <- function(dbcon) {
  
  dbExecute(dbcon, "
    ALTER TABLE product_facts
    ADD CONSTRAINT fk_time_key
    FOREIGN KEY (time_key) REFERENCES date_dim(timeID);
  ")
}


# Function to add the region_key foreign key to product_facts
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but the table is altered to add the FK
add_region_key_fk <- function(dbcon) {
  
  dbExecute(dbcon, "
    ALTER TABLE product_facts
    ADD CONSTRAINT fk_region_key
    FOREIGN KEY (region_key) REFERENCES region_dim(regionID);
  ")
}


# Function to create the 'rep_dim' dimension table
# The table is first dropped if it exists, then created with appropriate
#   attributes
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but a table is created
create_table_rep_dim <- function(dbcon) {
  
  # Drop the table if it already exists
  dbExecute(dbcon, "DROP TABLE IF EXISTS rep_dim;")
  
  # Create the table with auto-increment primary key
  dbExecute(dbcon, "
    CREATE TABLE rep_dim (
      repID INT AUTO_INCREMENT PRIMARY KEY,
      repName TEXT
    );"
  )
}


# Function to create the 'rep_facts' fact table
# The table is first dropped if it exists, then created with appropriate
#   attributes
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but a table is created
create_table_rep_facts <- function(dbcon) {
  
  # Drop the table if it already exists
  dbExecute(dbcon, "DROP TABLE IF EXISTS rep_facts;")
  
  
  # Create the table - no PK as per typical construction of fact tables
  dbExecute(dbcon, "
    CREATE TABLE rep_facts (
      rep_key INT,
      time_key VARCHAR(10),
      product_key INT,
      totalSold INT
    );"
  )
}


# Function to add the product_key foreign key to rep_facts
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but the table is altered to add the FK
add_product_key_fk_reps <- function(dbcon) {
  
  dbExecute(dbcon, "
    ALTER TABLE rep_facts
    ADD CONSTRAINT fk_product_key_reps
    FOREIGN KEY (product_key) REFERENCES product_dim(productID);
  ")
}


# Function to add the time_key foreign key to rep_facts
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but the table is altered to add the FK
add_time_key_fk_reps <- function(dbcon) {
  
  dbExecute(dbcon, "
    ALTER TABLE rep_facts
    ADD CONSTRAINT fk_time_key_reps
    FOREIGN KEY (time_key) REFERENCES date_dim(timeID);
  ")
}


# Function to add the rep_key foreign key to rep_facts
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but the table is altered to add the FK
add_rep_key_fk_reps <- function(dbcon) {
  
  dbExecute(dbcon, "
    ALTER TABLE rep_facts
    ADD CONSTRAINT fk_rep_key_reps
    FOREIGN KEY (rep_key) REFERENCES rep_dim(repID);
  ")
}




# ------------ ETL Load Between DBs --------------
# Note: As a general ETL process, the functions in this section utilize SQL 
#  for specific queries of the normalized database. That data then exists in a
#  data frame where it can be manipulated to the appropriate format for the 
#  star schema tables. Then, data from the necessary columns is written to the
#  appropriate table. The dimension tables may contain additional statement(s)
#  to add options(s) for 'ALL', if not already captured.


# Function to pull territory/region data from the normalized db and load it to 
#  the region_dim table
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_territories_data <- function(sqlite_dbcon, mysql_dbcon) {
  
  # Extract data from SQLite
  query <- "SELECT territoryName FROM territories;"
  territories_data <- dbGetQuery(sqlite_dbcon, query)
  
  # Rename the 'territoryName' column to 'regionName'
  territories_data <- data.frame(regionName = territories_data$territoryName)
  
   # Load data into MySQL table
  dbWriteTable(mysql_dbcon, "region_dim", territories_data, append = TRUE, row.names = FALSE)
  
  # Add a row to the table for 'ALL' regions w/ an id setting of 0
  dbExecute(mysql_dbcon, "INSERT INTO region_dim (regionID, regionName) VALUES (0, 'ALL');")
}


# Function to pull product data from the normalized db and load it to the 
#  product_dim table
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_product_data <- function(sqlite_dbcon, mysql_dbcon) {
  
  # Extract data from SQLite
  query <- "SELECT productName FROM products;"
  product_data <- dbGetQuery(sqlite_dbcon, query)
 
  # Load data into MySQL table
  dbWriteTable(mysql_dbcon, "product_dim", product_data, append = TRUE, row.names = FALSE)
  
  # Add a row to the table for 'ALL' products w/ an id setting of 0
  dbExecute(mysql_dbcon, "INSERT INTO product_dim (productID, productName) VALUES (0, 'ALL');")
}


# Function to pull date data from the normalized db and load it to the 
#  date_dim table
# Date Strategy: The initial query pulls data from the singular date field of the 
#  salestxn table in the normalized db, extracting separately as year and quarter, 
#  with quarter being built as a series of cases based on the extracted month.
#  The timeID is then built with concatenation of this data as part of the 
#  manipulation step of the process.
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_date_data <- function(sqlite_dbcon, mysql_dbcon) {
  
  # Extract data from SQLite, with cases for quarter
  query <- "SELECT DISTINCT strftime('%Y', date) AS year, 
                     CASE 
                       WHEN strftime('%m', date) BETWEEN '01' AND '03' THEN 'Q1'
                       WHEN strftime('%m', date) BETWEEN '04' AND '06' THEN 'Q2'
                       WHEN strftime('%m', date) BETWEEN '07' AND '09' THEN 'Q3'
                       ELSE 'Q4'
                     END AS quarter
            FROM salestxn;"
  date_data <- dbGetQuery(sqlite_dbcon, query)
  
  
  
  # Create "ALL" year and quarter rows
  all_year <- "ALL" # option for year
  all_quarter <- "ALL" #option for quarter
  
  #'ALL-ALL'
  all_row <- data.frame(year = all_year, quarter = all_quarter)
  date_data <- rbind(date_data, all_row)
  
  # Create "ALL" quarter rows for specific years
  unique_years <- unique(date_data$year) #list of unique years
  all_quarter_row <- data.frame(year = unique_years, quarter = all_quarter) 
  date_data <- rbind(date_data, all_quarter_row)
  
  
  # Concatenate year and quarter into a single column
  date_data$timeID <- paste(date_data$year, date_data$quarter, sep = "-")
  
  
  # Load data into MySQL
  dbWriteTable(mysql_dbcon, "date_dim", date_data, append = TRUE, row.names = FALSE)
}


# Function to pull data from the normalized db and load it to the 
#  product_facts table: total sold per product, quarter, and region
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_product_facts_quarter_data <- function(sqlite_dbcon, mysql_dbcon) {
  
  # Extract data from SQLite
  query <- "
    SELECT p.productName, strftime('%Y', s.date) AS year,
           CASE 
             WHEN strftime('%m', s.date) BETWEEN '01' AND '03' THEN 'Q1'
             WHEN strftime('%m', s.date) BETWEEN '04' AND '06' THEN 'Q2'
             WHEN strftime('%m', s.date) BETWEEN '07' AND '09' THEN 'Q3'
             ELSE 'Q4'
           END AS quarter,
           t.territoryName, SUM(s.amount) AS totalSold
    FROM salestxn AS s
    JOIN products AS p ON s.productID = p.productID
    JOIN reps AS r ON s.repID = r.repID
    JOIN territories AS t ON r.territory = t.territoryID
    GROUP BY p.productName, year, quarter, t.territoryName;"
  product_facts_data <- dbGetQuery(sqlite_dbcon, query)
  
  
  # Pull related data from dimension tables and merge into main df
  query <- "SELECT productID, productName FROM product_dim;"
  product_data <- dbGetQuery(mysql_dbcon, query)
  product_facts_data <- merge(product_facts_data, product_data, by.x = "productName", by.y = "productName")
  
  query <- "SELECT regionID, regionName FROM region_dim;"
  region_data <- dbGetQuery(mysql_dbcon, query)
  product_facts_data <- merge(product_facts_data, region_data, by.x = "territoryName", by.y = "regionName")
  
  
  # Assign columns for product_key, time_key, and region_key
  # Time_key created with concatenation of the year & quarter, as with date_dim table
  product_facts_data$product_key <- product_facts_data$productID
  product_facts_data$time_key <- paste(product_facts_data$year, product_facts_data$quarter, sep = "-")
  product_facts_data$region_key <- product_facts_data$regionID
  
  # Select only the relevant columns from df to load to the table
  product_facts_data <- product_facts_data[, c("product_key", "time_key", "region_key", "totalSold")]
  
  # Load data into MySQL
  dbWriteTable(mysql_dbcon, "product_facts", product_facts_data, append = TRUE, row.names = FALSE)
  
}


# Function to pull data from the normalized db and load it to the 
#  product_facts table: total sold per quarter
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_product_facts_quarter_total_data <- function(sqlite_dbcon, mysql_dbcon) {
  
  # Extract data from SQLite
  query <- "
    SELECT strftime('%Y', s.date) AS year, 
          CASE 
             WHEN strftime('%m', s.date) BETWEEN '01' AND '03' THEN 'Q1'
             WHEN strftime('%m', s.date) BETWEEN '04' AND '06' THEN 'Q2'
             WHEN strftime('%m', s.date) BETWEEN '07' AND '09' THEN 'Q3'
             ELSE 'Q4'
           END AS quarter,
           SUM(s.amount) AS totalSold
    FROM salestxn AS s
    GROUP BY year, quarter;"
  product_facts_data <- dbGetQuery(sqlite_dbcon, query)
 
  
  # Pull related data from dimension tables and merge into main df, assign FK columns
  product_facts_data$productName <- "ALL" #product set to all
  query <- "SELECT productID, productName FROM product_dim;" 
  product_data <- dbGetQuery(mysql_dbcon, query)
  product_facts_data <- merge(product_facts_data, product_data, by.x = "productName", by.y = "productName")
  product_facts_data$product_key <- product_facts_data$productID #assign key
  
  product_facts_data$regionName <- "ALL" #region set to all
  query <- "SELECT regionID, regionName FROM region_dim;"
  region_data <- dbGetQuery(mysql_dbcon, query)
  product_facts_data <- merge(product_facts_data, region_data, by.x = "regionName", by.y = "regionName")
  product_facts_data$region_key <- product_facts_data$regionID #assign key
  
  
  # Time_key assigned; created with concatenation of the year & quarter, as with date_dim table
  product_facts_data$time_key <- paste(product_facts_data$year, product_facts_data$quarter, sep = "-")
  
  # Select only the relevant columns from df to load to the table
  product_facts_data <- product_facts_data[, c("product_key", "time_key", "region_key", "totalSold")]
  
  # Load data into MySQL
  dbWriteTable(mysql_dbcon, "product_facts", product_facts_data, append = TRUE, row.names = FALSE)
  
}


# Function to pull data from the normalized db and load it to the 
#  product_facts table: total sold per product per quarter (all regions)
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_product_facts_quarter_prod_data <- function(sqlite_dbcon, mysql_dbcon) {
  
  # Extract data from SQLite
  query <- "
    SELECT p.productName, strftime('%Y', s.date) AS year,
           CASE 
             WHEN strftime('%m', s.date) BETWEEN '01' AND '03' THEN 'Q1'
             WHEN strftime('%m', s.date) BETWEEN '04' AND '06' THEN 'Q2'
             WHEN strftime('%m', s.date) BETWEEN '07' AND '09' THEN 'Q3'
             ELSE 'Q4'
           END AS quarter,
           SUM(s.amount) AS totalSold
    FROM salestxn AS s
    JOIN products AS p ON s.productID = p.productID
    GROUP BY p.productName, year, quarter;"
  product_facts_data <- dbGetQuery(sqlite_dbcon, query)
  
  
  # Pull related data from dimension tables and merge into main df, assign FK columns
  query <- "SELECT productID, productName FROM product_dim;"
  product_data <- dbGetQuery(mysql_dbcon, query)
  product_facts_data <- merge(product_facts_data, product_data, by.x = "productName", by.y = "productName")
  product_facts_data$product_key <- product_facts_data$productID #key assigned
  
  product_facts_data$regionName <- "ALL" #region set to all
  query <- "SELECT regionID, regionName FROM region_dim;"
  region_data <- dbGetQuery(mysql_dbcon, query)
  product_facts_data <- merge(product_facts_data, region_data, by.x = "regionName", by.y = "regionName")
  product_facts_data$region_key <- product_facts_data$regionID #key assigned
  
  
  # Time_key assigned; created with concatenation of the year & quarter, as with date_dim table
  product_facts_data$time_key <- paste(product_facts_data$year, product_facts_data$quarter, sep = "-")
  
  # Select only the relevant columns
  product_facts_data <- product_facts_data[, c("product_key", "time_key", "region_key", "totalSold")]
  
  # Load data into MySQL
  dbWriteTable(mysql_dbcon, "product_facts", product_facts_data, append = TRUE, row.names = FALSE)

}


# Function to pull data from the normalized db and load it to the 
#  product_facts table: total sold per product per year (all regions, all quarters)
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_product_facts_year_prod_data <- function(sqlite_dbcon, mysql_dbcon) {
  
  # Extract data from SQLite
  query <- "
    SELECT p.productName, strftime('%Y', s.date) AS year,
           SUM(s.amount) AS totalSold
    FROM salestxn AS s
    JOIN products AS p ON s.productID = p.productID
    GROUP BY p.productName, year;"
  product_facts_data <- dbGetQuery(sqlite_dbcon, query)
  
 
  # Pull related data from dimension tables and merge into main df, assign FK columns
  query <- "SELECT productID, productName FROM product_dim;"
  product_data <- dbGetQuery(mysql_dbcon, query)
  product_facts_data <- merge(product_facts_data, product_data, by.x = "productName", by.y = "productName")
  product_facts_data$product_key <- product_facts_data$productID #assign FK
  
  product_facts_data$regionName <- "ALL" #set region to all
  query <- "SELECT regionID, regionName FROM region_dim;"
  region_data <- dbGetQuery(mysql_dbcon, query)
  product_facts_data <- merge(product_facts_data, region_data, by.x = "regionName", by.y = "regionName")
  product_facts_data$region_key <- product_facts_data$regionID #assign FK
  
  
  # Time_key assigned; created with concatenation of the year & quarter, as with date_dim table
  product_facts_data$time_key <- paste(product_facts_data$year, "ALL", sep = "-") #quarter set to all
  
  # Select only the relevant columns
  product_facts_data <- product_facts_data[, c("product_key", "time_key", "region_key", "totalSold")]
  
  # Load data into MySQL
  dbWriteTable(mysql_dbcon, "product_facts", product_facts_data, append = TRUE, row.names = FALSE)

}


# Function to pull data from the normalized db and load it to the 
#  product_facts table: total sold per year
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_product_facts_year_data <- function(sqlite_dbcon, mysql_dbcon) {
  
  # Extract data from SQLite
  query <- "
    SELECT strftime('%Y', s.date) AS year,
           SUM(s.amount) AS totalSold
    FROM salestxn AS s
    GROUP BY year;"
  product_facts_data <- dbGetQuery(sqlite_dbcon, query)
  
 
  # Pull related data from dimension tables and merge into main df, assign FK columns
  product_facts_data$productName <- "ALL" #product set to all
  query <- "SELECT productID, productName FROM product_dim;"
  product_data <- dbGetQuery(mysql_dbcon, query)
  product_facts_data <- merge(product_facts_data, product_data, by.x = "productName", by.y = "productName")
  product_facts_data$product_key <- product_facts_data$productID #set FK
  
  product_facts_data$regionName <- "ALL" #region set to all
  query <- "SELECT regionID, regionName FROM region_dim;"
  region_data <- dbGetQuery(mysql_dbcon, query)
  product_facts_data <- merge(product_facts_data, region_data, by.x = "regionName", by.y = "regionName")
  product_facts_data$region_key <- product_facts_data$regionID #set FK
  
  
  # Time_key assigned; created with concatenation of the year & quarter, as with date_dim table
  product_facts_data$time_key <- paste(product_facts_data$year, "ALL", sep = "-") #quarter set to all
  
  # Select only the relevant columns
  product_facts_data <- product_facts_data[, c("product_key", "time_key", "region_key", "totalSold")]
  
  # Load data into MySQL
  dbWriteTable(mysql_dbcon, "product_facts", product_facts_data, append = TRUE, row.names = FALSE)
  
}


# Function to pull data from the normalized db and load it to the 
#  product_facts table: total sold per product
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_product_facts_per_product_data <- function(sqlite_dbcon, mysql_dbcon) {

  # Extract data from SQLite
  query <- "
    SELECT p.productName, SUM(s.amount) AS totalSold
    FROM salestxn AS s
    JOIN products AS p ON s.productID = p.productID
    GROUP BY p.productName;"
  product_facts_data <- dbGetQuery(sqlite_dbcon, query)
  
  
  # Pull related data from dimension tables and merge into main df, assign FK columns
  query <- "SELECT productID, productName FROM product_dim;"
  product_data <- dbGetQuery(mysql_dbcon, query)
  product_facts_data <- merge(product_facts_data, product_data, by.x = "productName", by.y = "productName")
  product_facts_data$product_key <- product_facts_data$productID #assign FK
  
  product_facts_data$regionName <- "ALL" #region set to all
  query <- "SELECT regionID, regionName FROM region_dim;"
  region_data <- dbGetQuery(mysql_dbcon, query)
  product_facts_data <- merge(product_facts_data, region_data, by.x = "regionName", by.y = "regionName")
  product_facts_data$region_key <- product_facts_data$regionID #set FK
  
  
  # Time_key is added df column set to "ALL-ALL" for years and quarters
  product_facts_data$time_key <- "ALL-ALL"
  
  # Select only the relevant columns
  product_facts_data <- product_facts_data[, c("product_key", "time_key", "region_key", "totalSold")]
  
  # Load data into MySQL
  dbWriteTable(mysql_dbcon, "product_facts", product_facts_data, append = TRUE, row.names = FALSE)
  
}


# Function to pull data from the normalized db and load it to the 
#  product_facts table: total sold per region 
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_product_facts_per_region_data <- function(sqlite_dbcon, mysql_dbcon) {
  
  # Extract data from SQLite
  query <- "
    SELECT t.territoryName, SUM(s.amount) AS totalSold
    FROM salestxn AS s
    JOIN reps AS r ON s.repID = r.repID
    JOIN territories AS t ON r.territory = t.territoryID
    GROUP BY t.territoryName;"
  product_facts_data <- dbGetQuery(sqlite_dbcon, query)
  
  
  # Pull related data from dimension tables and merge into main df, assign FK columns
  product_facts_data$productName <- "ALL" #product set to all
  query <- "SELECT productID, productName FROM product_dim;"
  product_data <- dbGetQuery(mysql_dbcon, query)
  product_facts_data <- merge(product_facts_data, product_data, by.x = "productName", by.y = "productName")
  product_facts_data$product_key <- product_facts_data$productID #assign FK
  
  query <- "SELECT regionID, regionName FROM region_dim;"
  region_data <- dbGetQuery(mysql_dbcon, query)
  product_facts_data <- merge(product_facts_data, region_data, by.x = "territoryName", by.y = "regionName")
  product_facts_data$region_key <- product_facts_data$regionID #assign FK
  
  
  # Time_key is added df column set to "ALL-ALL" for years and quarters
  product_facts_data$time_key <- "ALL-ALL"
  
  # Select only the relevant columns
  product_facts_data <- product_facts_data[, c("product_key", "time_key", "region_key", "totalSold")]
  
  # Load data into MySQL
  dbWriteTable(mysql_dbcon, "product_facts", product_facts_data, append = TRUE, row.names = FALSE)
  
}


# Function to pull data from the normalized db and load it to the 
#  product_facts table: total sold entirely 
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_product_facts_total_sold_data <- function(sqlite_dbcon, mysql_dbcon) {
  
  # Extract data from SQLite
  query <- "
    SELECT SUM(s.amount) AS totalSold
    FROM salestxn AS s;"
  product_facts_data <- dbGetQuery(sqlite_dbcon, query)
  
  
  # Pull related data from dimension tables and merge into main df, assign FK columns
  product_facts_data$productName <- "ALL" #product set to all
  query <- "SELECT productID, productName FROM product_dim;"
  product_data <- dbGetQuery(mysql_dbcon, query)
  product_facts_data <- merge(product_facts_data, product_data, by.x = "productName", by.y = "productName")
  product_facts_data$product_key <- product_facts_data$productID #assign FK
  
  product_facts_data$regionName <- "ALL" #region set to all
  query <- "SELECT regionID, regionName FROM region_dim;"
  region_data <- dbGetQuery(mysql_dbcon, query)
  product_facts_data <- merge(product_facts_data, region_data, by.x = "regionName", by.y = "regionName")
  product_facts_data$region_key <- product_facts_data$regionID #assign FK
  
  
  # Time_key is added df column set to "ALL-ALL" for years and quarters
  product_facts_data$time_key <- "ALL-ALL"
  
  # Select only the relevant columns
  product_facts_data <- product_facts_data[, c("product_key", "time_key", "region_key", "totalSold")]
  
  # Load data into MySQL
  dbWriteTable(mysql_dbcon, "product_facts", product_facts_data, append = TRUE, row.names = FALSE)
  
}



# Function to pull sales rep data from the normalized db and load it to 
#  the rep_dim table
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_reps_data <- function(sqlite_dbcon, mysql_dbcon) {
  
  # Extract data from SQLite
  query <- "SELECT firstName, lastName FROM reps;"
  rep_data <- dbGetQuery(sqlite_dbcon, query)
  
  # Create full name for rep with combination of first and last name
  rep_data$repName <- paste(rep_data$firstName, rep_data$lastName, sep = " ")
  
  # Added handling of repID by pre-construction
  rep_data$repID <- seq(nrow(rep_data))
  
  # Select only the relevant columns
  rep_data <- rep_data[, c("repID","repName")]
  
  # Load data into MySQL
  dbWriteTable(mysql_dbcon, "rep_dim", rep_data, append = TRUE, row.names = FALSE)
  
  # Add row for 'ALL' reps with repID of 0
  dbExecute(mysql_dbcon, "INSERT INTO rep_dim (repID, repName) VALUES (0, 'ALL');")
}


# Function to pull data from the normalized db and load it to the 
#  rep_facts table: total sold per rep, quarter, and product
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_rep_facts_quarter_data <- function(sqlite_dbcon, mysql_dbcon) {
  
  # Extract data from SQLite
  query <- "
    SELECT r.firstName, r.lastName, strftime('%Y', s.date) AS year,
           CASE 
             WHEN strftime('%m', s.date) BETWEEN '01' AND '03' THEN 'Q1'
             WHEN strftime('%m', s.date) BETWEEN '04' AND '06' THEN 'Q2'
             WHEN strftime('%m', s.date) BETWEEN '07' AND '09' THEN 'Q3'
             ELSE 'Q4'
           END AS quarter,
           p.productName, SUM(s.amount) AS totalSold
    FROM salestxn AS s
    JOIN products AS p ON s.productID = p.productID
    JOIN reps AS r ON s.repID = r.repID
    GROUP BY r.firstName, r.lastName, year, quarter, p.productName;"
  rep_facts_data <- dbGetQuery(sqlite_dbcon, query)
  
  
  # Pull related data from dimension tables and merge into main df
  query <- "SELECT productID, productName FROM product_dim;"
  rep_data <- dbGetQuery(mysql_dbcon, query)
  rep_facts_data <- merge(rep_facts_data, rep_data, by.x = "productName", by.y = "productName")
  
  rep_facts_data$repName <- paste(rep_facts_data$firstName, rep_facts_data$lastName, sep = " ")
  query <- "SELECT repID, repName FROM rep_dim;"
  rep_data <- dbGetQuery(mysql_dbcon, query)
  rep_facts_data <- merge(rep_facts_data, rep_data, by.x = "repName", by.y = "repName")
  
  
  # Get product_key, time_key, and rep_key for each row
  # Time_key created with concatenation of the year & quarter, as with date_dim table
  rep_facts_data$product_key <- rep_facts_data$productID
  rep_facts_data$time_key <- paste(rep_facts_data$year, rep_facts_data$quarter, sep = "-")
  rep_facts_data$rep_key <- rep_facts_data$repID
  
  # Select only the relevant columns
  rep_facts_data <- rep_facts_data[, c("product_key", "time_key", "rep_key", "totalSold")]
  
  # Load data into MySQL
  dbWriteTable(mysql_dbcon, "rep_facts", rep_facts_data, append = TRUE, row.names = FALSE)
  
}


# Function to pull data from the normalized db and load it to the 
#  rep_facts table: total sold per quarter (all reps, all products)
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_rep_facts_quarter_total_data <- function(sqlite_dbcon, mysql_dbcon) {
  
  # Extract data from SQLite
  query <- "
    SELECT strftime('%Y', s.date) AS year, 
          CASE 
             WHEN strftime('%m', s.date) BETWEEN '01' AND '03' THEN 'Q1'
             WHEN strftime('%m', s.date) BETWEEN '04' AND '06' THEN 'Q2'
             WHEN strftime('%m', s.date) BETWEEN '07' AND '09' THEN 'Q3'
             ELSE 'Q4'
           END AS quarter,
           SUM(s.amount) AS totalSold
    FROM salestxn AS s
    GROUP BY year, quarter;"
  rep_facts_data <- dbGetQuery(sqlite_dbcon, query)
  
  
  # Pull related data from dimension tables and merge into main df, assign FKs
  rep_facts_data$productName <- "ALL" #product set to all
  query <- "SELECT productID, productName FROM product_dim;"
  rep_data <- dbGetQuery(mysql_dbcon, query)
  rep_facts_data <- merge(rep_facts_data, rep_data, by.x = "productName", by.y = "productName")
  rep_facts_data$product_key <- rep_facts_data$productID #assign FK
  
  rep_facts_data$repName <- "ALL" #rep set to all
  query <- "SELECT repID, repName FROM rep_dim;"
  rep_data <- dbGetQuery(mysql_dbcon, query)
  rep_facts_data <- merge(rep_facts_data, rep_data, by.x = "repName", by.y = "repName")
  rep_facts_data$rep_key <- rep_facts_data$repID #assign FK
  
  
  # Time_key assigned; created with concatenation of the year & quarter, as with date_dim table
  rep_facts_data$time_key <- paste(rep_facts_data$year, rep_facts_data$quarter, sep = "-")
  
  # Select only the relevant columns
  rep_facts_data <- rep_facts_data[, c("product_key", "time_key", "rep_key", "totalSold")]
  
  # Load data into MySQL
  dbWriteTable(mysql_dbcon, "rep_facts", rep_facts_data, append = TRUE, row.names = FALSE)
  
}


# Function to pull data from the normalized db and load it to the 
#  rep_facts table: total sold per quarter per rep (all products)
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_rep_facts_quarter_rep_data <- function(sqlite_dbcon, mysql_dbcon) {
  
  # Extract data from SQLite
  query <- "
    SELECT r.firstName, r.lastName, strftime('%Y', s.date) AS year,
           CASE 
             WHEN strftime('%m', s.date) BETWEEN '01' AND '03' THEN 'Q1'
             WHEN strftime('%m', s.date) BETWEEN '04' AND '06' THEN 'Q2'
             WHEN strftime('%m', s.date) BETWEEN '07' AND '09' THEN 'Q3'
             ELSE 'Q4'
           END AS quarter,
           SUM(s.amount) AS totalSold
    FROM salestxn AS s
    JOIN reps AS r ON s.repID = r.repID
    GROUP BY r.firstName, r.lastName, year, quarter;"
  rep_facts_data <- dbGetQuery(sqlite_dbcon, query)
  
  
  # Pull related data from dimension tables and merge into main df, assign FKs
  rep_facts_data$repName <- paste(rep_facts_data$firstName, rep_facts_data$lastName, sep = " ")
  query <- "SELECT repID, repName FROM rep_dim;"
  rep_data <- dbGetQuery(mysql_dbcon, query)
  rep_facts_data <- merge(rep_facts_data, rep_data, by.x = "repName", by.y = "repName")
  rep_facts_data$rep_key <- rep_facts_data$repID #assign FK
  
  rep_facts_data$productName <- "ALL" #product set to all
  query <- "SELECT productID, productName FROM product_dim;"
  rep_data <- dbGetQuery(mysql_dbcon, query)
  rep_facts_data <- merge(rep_facts_data, rep_data, by.x = "productName", by.y = "productName")
  rep_facts_data$product_key <- rep_facts_data$productID #assign FK
  
  
  # Time_key assigned; created with concatenation of the year & quarter, as with date_dim table
  rep_facts_data$time_key <- paste(rep_facts_data$year, rep_facts_data$quarter, sep = "-")
 
  # Select only the relevant columns
  rep_facts_data <- rep_facts_data[, c("product_key", "time_key", "rep_key", "totalSold")]
  
  # Load data into MySQL
  dbWriteTable(mysql_dbcon, "rep_facts", rep_facts_data, append = TRUE, row.names = FALSE)
  
}


# Function to pull data from the normalized db and load it to the 
#  rep_facts table: total sold per rep per year
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_rep_facts_year_rep_data <- function(sqlite_dbcon, mysql_dbcon) {
  # Extract data from SQLite
  query <- "
    SELECT r.firstName, r.lastName, strftime('%Y', s.date) AS year,
           SUM(s.amount) AS totalSold
    FROM salestxn AS s
    JOIN reps AS r ON s.repID = r.repID
    GROUP BY r.firstName, r.lastName, year;"
  rep_facts_data <- dbGetQuery(sqlite_dbcon, query)
  
 
  # Pull related data from dimension tables and merge into main df, assign FKs
  rep_facts_data$repName <- paste(rep_facts_data$firstName, rep_facts_data$lastName, sep = " ")
  query <- "SELECT repID, repName FROM rep_dim;"
  rep_data <- dbGetQuery(mysql_dbcon, query)
  rep_facts_data <- merge(rep_facts_data, rep_data, by.x = "repName", by.y = "repName")
  rep_facts_data$rep_key <- rep_facts_data$repID #assign FK
  
  rep_facts_data$productName <- "ALL" #product set to all
  query <- "SELECT productID, productName FROM product_dim;"
  rep_data <- dbGetQuery(mysql_dbcon, query)
  rep_facts_data <- merge(rep_facts_data, rep_data, by.x = "productName", by.y = "productName")
  rep_facts_data$product_key <- rep_facts_data$productID #assign key
  
  
  # Time_key assigned; created with concatenation of the year & quarter, as with date_dim table
  rep_facts_data$time_key <- paste(rep_facts_data$year, "ALL", sep = "-") #quarter set to all
  
  # Select only the relevant columns
  rep_facts_data <- rep_facts_data[, c("product_key", "time_key", "rep_key", "totalSold")]
  
  # Load data into MySQL
  dbWriteTable(mysql_dbcon, "rep_facts", rep_facts_data, append = TRUE, row.names = FALSE)
  
}

# Function to pull data from the normalized db and load it to the 
#  rep_facts table: total sold per year
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_rep_facts_year_data <- function(sqlite_dbcon, mysql_dbcon) {
  # Extract data from SQLite
  query <- "
    SELECT strftime('%Y', s.date) AS year,
           SUM(s.amount) AS totalSold
    FROM salestxn AS s
    GROUP BY year;"
  rep_facts_data <- dbGetQuery(sqlite_dbcon, query)
  
  
  # Pull related data from dimension tables and merge into main df, assign FKs
  rep_facts_data$repName <- "ALL" # rep set to all
  query <- "SELECT repID, repName FROM rep_dim;"
  rep_data <- dbGetQuery(mysql_dbcon, query)
  rep_facts_data <- merge(rep_facts_data, rep_data, by.x = "repName", by.y = "repName")
  rep_facts_data$rep_key <- rep_facts_data$repID #assign FK
  
  rep_facts_data$productName <- "ALL" #product set to all
  query <- "SELECT productID, productName FROM product_dim;"
  rep_data <- dbGetQuery(mysql_dbcon, query)
  rep_facts_data <- merge(rep_facts_data, rep_data, by.x = "productName", by.y = "productName")
  rep_facts_data$product_key <- rep_facts_data$productID #assign FK
  
  
  # Time_key assigned; created with concatenation of the year & quarter, as with date_dim table
  rep_facts_data$time_key <- paste(rep_facts_data$year, "ALL", sep = "-") #quarter set to all
 
  # Select only the relevant columns
  rep_facts_data <- rep_facts_data[, c("product_key", "time_key", "rep_key", "totalSold")]
  
  # Load data into MySQL
  dbWriteTable(mysql_dbcon, "rep_facts", rep_facts_data, append = TRUE, row.names = FALSE)

}

# Function to pull data from the normalized db and load it to the 
#  rep_facts table: total sold per rep
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data 
etl_rep_facts_per_rep_data <- function(sqlite_dbcon, mysql_dbcon) {
  
  # Extract data from SQLite
  query <- "
    SELECT r.firstName, r.lastName, SUM(s.amount) AS totalSold
    FROM salestxn AS s
    JOIN reps AS r ON s.repID = r.repID
    GROUP BY r.firstName, r.lastName;"
  rep_facts_data <- dbGetQuery(sqlite_dbcon, query)
  
  
  # Pull related data from dimension tables and merge into main df, assign FKs
  rep_facts_data$repName <- paste(rep_facts_data$firstName, rep_facts_data$lastName, sep = " ")
  query <- "SELECT repID, repName FROM rep_dim;"
  rep_data <- dbGetQuery(mysql_dbcon, query)
  rep_facts_data <- merge(rep_facts_data, rep_data, by.x = "repName", by.y = "repName")
  rep_facts_data$rep_key <- rep_facts_data$repID
  
  rep_facts_data$productName <- "ALL" #product set to all
  query <- "SELECT productID, productName FROM product_dim;"
  rep_data <- dbGetQuery(mysql_dbcon, query)
  rep_facts_data <- merge(rep_facts_data, rep_data, by.x = "productName", by.y = "productName")
  rep_facts_data$product_key <- rep_facts_data$productID
  
  
  # Time_key is added df column set to "ALL-ALL" for years and quarters
  rep_facts_data$time_key <- "ALL-ALL"
  
  # Select only the relevant columns
  rep_facts_data <- rep_facts_data[, c("product_key", "time_key", "rep_key", "totalSold")]
  
  # Load data into MySQL
  dbWriteTable(mysql_dbcon, "rep_facts", rep_facts_data, append = TRUE, row.names = FALSE)
  
}


# Function to pull data from the normalized db and load it to the 
#  rep_facts table: total sold per product
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_rep_facts_per_product_data <- function(sqlite_dbcon, mysql_dbcon) {
  
  # Extract data from SQLite
  query <- "
    SELECT p.productName, SUM(s.amount) AS totalSold
    FROM salestxn AS s
    JOIN products AS p ON s.productID = p.productID
    GROUP BY p.productName;"
  rep_facts_data <- dbGetQuery(sqlite_dbcon, query)
  
  
  # Pull related data from dimension tables and merge into main df, assign FKs
  rep_facts_data$repName <- "ALL" #rep set to all
  query <- "SELECT repID, repName FROM rep_dim;"
  rep_data <- dbGetQuery(mysql_dbcon, query)
  rep_facts_data <- merge(rep_facts_data, rep_data, by.x = "repName", by.y = "repName")
  rep_facts_data$rep_key <- rep_facts_data$repID
  
  query <- "SELECT productID, productName FROM product_dim;"
  rep_data <- dbGetQuery(mysql_dbcon, query)
  rep_facts_data <- merge(rep_facts_data, rep_data, by.x = "productName", by.y = "productName")
  rep_facts_data$product_key <- rep_facts_data$productID
  
  
  # Time_key is added df column set to "ALL-ALL" for years and quarters
  rep_facts_data$time_key <- "ALL-ALL"

  # Select only the relevant columns
  rep_facts_data <- rep_facts_data[, c("product_key", "time_key", "rep_key", "totalSold")]
  
  # Load data into MySQL
  dbWriteTable(mysql_dbcon, "rep_facts", rep_facts_data, append = TRUE, row.names = FALSE)
  
}


# Function to pull data from the normalized db and load it to the 
#  rep_facts table: total sold entirely
# inputs: 
#     (1) sqlite_dbcon: the normalized SQLite db from which data will be queried
#     (2) mysql_dbcon: the de-normalized MySQL db to which data will be loaded
# output: no return value, but the table is loaded with [additional] data
etl_rep_facts_total_sold_data <- function(sqlite_dbcon, mysql_dbcon) {
  
  # Extract data from SQLite
  query <- "
    SELECT SUM(s.amount) AS totalSold
    FROM salestxn AS s;"
  rep_facts_data <- dbGetQuery(sqlite_dbcon, query)
  
  
  # Pull related data from dimension tables and merge into main df, assign FKs
  rep_facts_data$repName <- "ALL" #rep set to all
  query <- "SELECT repID, repName FROM rep_dim;"
  rep_data <- dbGetQuery(mysql_dbcon, query)
  rep_facts_data <- merge(rep_facts_data, rep_data, by.x = "repName", by.y = "repName")
  rep_facts_data$rep_key <- rep_facts_data$repID #assign key
  
  rep_facts_data$productName <- "ALL" #product set to all
  query <- "SELECT productID, productName FROM product_dim;"
  rep_data <- dbGetQuery(mysql_dbcon, query)
  rep_facts_data <- merge(rep_facts_data, rep_data, by.x = "productName", by.y = "productName")
  rep_facts_data$product_key <- rep_facts_data$productID #assign key
  
  
  # Time_key is added df column set to "ALL-ALL" for years and quarters
  rep_facts_data$time_key <- "ALL-ALL"
  
  # Select only the relevant columns
  rep_facts_data <- rep_facts_data[, c("product_key", "time_key", "rep_key", "totalSold")]
  
  # Load data into MySQL
  dbWriteTable(mysql_dbcon, "rep_facts", rep_facts_data, append = TRUE, row.names = FALSE)
  
}




# ------------ Disconnect --------------
# Function to disconnect from the database
# inputs: 
#     (1) conn: the name of the database on which the function operates
# output: no return value, but the database is disconnected from
disconnect <- function(conn) {
  dbDisconnect(conn)
}



# Main function to control program flow & test the program
main <- function(){
  
  # ------ DB Connections -------------
  # connect to db & store connection (MySQL Star Schemas)
  db_name <- "sql9626210"
  db_user <- "sql9626210"
  db_password <- "prjFNVd1Xf"
  db_host <- "sql9.freemysqlhosting.net"
  db_port <- 3306
  mydb <- connectToDB_MySQL(db_name, db_user, db_password, db_host, db_port)
  
  # connect to db & store connection (SQLite normalized db)
  fpath = "/Users/sarahgallant/Documents/MSCS_Align/cs5200/Practicum II/CS5200.PracticumII.GallantS/" #where db stored
  db_name <- "realizedDB.sqlite"
  dbcon <- connectToDB_SQLite(db_name, fpath)
  
  

  # ------ Create start schema tables -------
  #fact tables first b/c of drop table component
  create_table_product_facts(mydb)
  create_table_rep_facts(mydb)
  
  #dimension tables and FK links
  create_table_product_dim(mydb)
  create_table_region_dim(mydb)
  create_table_date_dim(mydb)
  
  add_product_key_fk(mydb)
  add_time_key_fk(mydb)
  add_region_key_fk(mydb)
  
  create_table_rep_dim(mydb)
  
  add_product_key_fk_reps(mydb)
  add_time_key_fk_reps(mydb)
  add_rep_key_fk_reps(mydb)
  
  
  
  # ----- ETL load between dbs --------
  etl_territories_data(dbcon, mydb) # dimension table
  etl_product_data(dbcon, mydb) # dimension table
  etl_date_data(dbcon, mydb) # dimension table
  
  #product_facts table loads
  etl_product_facts_quarter_data(dbcon,mydb)
  etl_product_facts_per_product_data(dbcon,mydb)
  etl_product_facts_total_sold_data(dbcon,mydb)
  etl_product_facts_per_region_data(dbcon,mydb)
  etl_product_facts_year_prod_data(dbcon,mydb)
  etl_product_facts_year_data(dbcon,mydb)
  etl_product_facts_quarter_total_data(dbcon,mydb)
  etl_product_facts_quarter_prod_data(dbcon,mydb)
  
  etl_reps_data(dbcon,mydb) # dimension table
  
  #rep_facts table loads
  etl_rep_facts_quarter_data(dbcon,mydb)
  etl_rep_facts_quarter_total_data(dbcon,mydb)
  etl_rep_facts_quarter_rep_data(dbcon,mydb)
  etl_rep_facts_year_rep_data(dbcon,mydb)
  etl_rep_facts_year_data(dbcon,mydb)
  etl_rep_facts_per_rep_data(dbcon,mydb)
  etl_rep_facts_per_product_data(dbcon,mydb)
  etl_rep_facts_total_sold_data(dbcon,mydb)
  
  
  # ----- Disconnect from dbs --------
  disconnect(mydb) # MySQL star schema
  disconnect(dbcon) # SQLite normalized
  
}

main()
