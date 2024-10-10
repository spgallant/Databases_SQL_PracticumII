# title: "Practicum II - Load XML2DB"
# author: "Sarah Gallant"
# date: Summer Full 2023


# Load libraries
library(RSQLite)
library(XML)
library(dplyr)


# -------------- Create DB --------------------
# Function to connect to a SQLite database, creating a new one
#   if it does not yet exist
# inputs: 
#     (1) db_name: the name of the database
#     (2) fpath: the file path where the database will be saved
# output: the variable holding the database connection
connectToDB <- function(db_name, fpath) {
  
  # Connect to/create the database
  dbcon <- dbConnect(RSQLite::SQLite(), paste0(fpath, db_name)) 
  
  return(dbcon)
}


# Function to create the table for salestxn using SQL
# The table is first dropped if it exists, then created with appropriate
#   attributes and constraints
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but a table is created
create_table_salestxn <- function(dbcon) {
  
  # Drop the 'salestxn' table if it already exists
  dbExecute(dbcon, "DROP TABLE IF EXISTS salestxn;")
  
  # Create the 'salestxn' table
  dbExecute(dbcon, "
    CREATE TABLE salestxn (
      txnID TEXT PRIMARY KEY,
      date DATE,
      quantity INTEGER,
      amount INTEGER,
      productID INTEGER,
      customerID INTEGER,
      repID INTEGER,
      FOREIGN KEY (productID) REFERENCES Products(productID),
      FOREIGN KEY (customerID) REFERENCES Customers(customerID),
      FOREIGN KEY (repID) REFERENCES Reps(repID)
    );"
  )
}


# Function to create the table for customers using SQL
# The table is first dropped if it exists, then created with appropriate
#   attributes and constraints
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but a table is created
create_table_customers <- function(dbcon) {
  
  # Drop the 'customers' table if it already exists
  dbExecute(dbcon, "DROP TABLE IF EXISTS customers;")
  
  # Create the 'customers' table
  dbExecute(dbcon, "
    CREATE TABLE customers (
      customerID INTEGER PRIMARY KEY,
      customerName TEXT,
      country INTEGER,
      FOREIGN KEY (country) REFERENCES countries(countryID)
    );"
  )
}


# Function to create the table for products
# The table is first dropped if it exists, then created with appropriate
#   attributes and constraints
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but a table is created
create_table_products <- function(dbcon) {
  
  # Drop the 'products' table if it already exists
  dbExecute(dbcon, "DROP TABLE IF EXISTS products;")
  
  # Create the 'products' table
  dbExecute(dbcon, "
    CREATE TABLE products (
      productID INTEGER PRIMARY KEY,
      productName TEXT
    );"
  )
}

# Function to create the table for reps
# The table is first dropped if it exists, then created with appropriate
#   attributes and constraints
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but a table is created
create_table_reps <- function(dbcon) {
  
  # Drop the 'reps' table if it already exists
  dbExecute(dbcon, "DROP TABLE IF EXISTS reps;")
  
  # Create the 'reps' table
  dbExecute(dbcon, "
    CREATE TABLE reps (
      repID INTEGER PRIMARY KEY,
      firstName TEXT,
      lastName TEXT,
      territory INTEGER,
      FOREIGN KEY (territory) REFERENCES territories(territoryID)
    );"
  )
}


# Function to create the table for countries
# The table is first dropped if it exists, then created with appropriate
#   attributes and constraints
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but a table is created
create_table_countries <- function(dbcon) {
  
  # Drop the 'countries' table if it already exists
  dbExecute(dbcon, "DROP TABLE IF EXISTS countries;")
  
  # Create the 'countries' table
  dbExecute(dbcon, "
    CREATE TABLE countries (
      countryID INTEGER PRIMARY KEY,
      countryName TEXT
    );"
  )
}


# Function to create the table for territories
# The table is first dropped if it exists, then created with appropriate
#   attributes and constraints
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but a table is created
create_table_territories <- function(dbcon) {
  
  # Drop the 'territories' table if it already exists
  dbExecute(dbcon, "DROP TABLE IF EXISTS territories;")
  
  # Create the 'territories' table
  dbExecute(dbcon, "
    CREATE TABLE territories (
      territoryID INTEGER PRIMARY KEY,
      territoryName TEXT
    );"
  )
}


# Function to turn on foreign key support
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no output, but consideration for foreign keys is operational
add_fk_constraints <- function(dbcon) {
  
  dbExecute(dbcon, "PRAGMA foreign_keys = ON;")
}




# -------------- PARSE XMLs --------------------

# Function to load and parse XML data from a file using xmlParse()
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: the variable holding the details of the parsed xml file
load_xml_file <- function(file_path) {
  doc <- xmlParse(file_path) #parse xml file
  return(doc) #return variable holding xml doc detail
}


# Function to pull sales rep data from a parsed xml file and load 
#   it to a data frame
# inputs: 
#     (1) xml_doc: variable holding the parsed xml document
# output: a data frame holding the data from the given xml file
pull_reps_data <- function(xml_doc) {
  
  root <- xmlRoot(xml_doc) #find root
  n <- xmlSize(root) #find number of nodes
  

  # create df to hold data from rep xml, with data types
  rep_df <- data.frame(
    repID = character(),
    firstName = character(),
    lastName = character(),
    territory = character(),
    stringsAsFactors = FALSE
  )
  
  # iterate over the <row> nodes
  for (i in 1:n) {
    
    # set root node location as current iteration
    rep_node <- root[[i]]
    
    # get the value of attributes for current rep node
    rep_id <- xmlAttrs(rep_node)[["rID"]] #required id attribute
    firstName <- xmlValue(getNodeSet(rep_node, "firstName")[[1]])
    lastName <- xmlValue(getNodeSet(rep_node, "lastName")[[1]])
    territory <- xmlValue(getNodeSet(rep_node, "territory")[[1]])
  
    # apply data for this rep to appropriate row in df
    rep_df[i,1] <- rep_id
    rep_df[i,2] <- firstName
    rep_df[i,3] <- lastName
    rep_df[i,4] <- territory
  }
  
  return(rep_df)
}


# Function to pull transaction data from a parsed xml file and load 
#   it to data frame.
# Each new file has a signifier to identify and separate the transactions
#   by application to the txnID field.
# inputs: 
#     (1) xml_doc: the parsed xml document
#     (2) id_tag: the file signifier to apply to txn IDs
# output: a data frame holding the data from the given xml file
pull_txn_data <- function(xml_doc, id_tag) {
  
  root <- xmlRoot(xml_doc) #find root
  n <- xmlSize(root) #find number of nodes
  
  
  # create df to hold data from rep xml, with data types
    txn_df <- data.frame(
      txnID = integer(),
      date = character(),
      customer = character(),
      product = character(),
      quantity = integer(),
      amount = integer(),
      country = character(),
      repID = integer(),
      stringsAsFactors = FALSE
    )

  # iterate over the <row> nodes
  for (i in 1:n) {
    
    # set root node location as current iteration
    txn_node <- root[[i]]
    
    # get the value of attributes for current txn node
    txnID <- paste0(id_tag, "_", xmlValue(getNodeSet(txn_node, "txnID")[[1]]))
    date <- xmlValue(getNodeSet(txn_node, "date")[[1]])
    customer <- xmlValue(getNodeSet(txn_node, "cust")[[1]])
    product <- xmlValue(getNodeSet(txn_node, "prod")[[1]])
    quantity <- xmlValue(getNodeSet(txn_node, "qty")[[1]])
    amount <- xmlValue(getNodeSet(txn_node, "amount")[[1]])
    country <- xmlValue(getNodeSet(txn_node, "country")[[1]])
    repID <- xmlValue(getNodeSet(txn_node, "repID")[[1]])
    
    # apply data for this rep to appropriate row in df
    txn_df[i, 1] <- txnID
    txn_df[i, 2] <- date
    txn_df[i, 3] <- customer
    txn_df[i, 4] <- product
    txn_df[i, 5] <- quantity
    txn_df[i, 6] <- amount
    txn_df[i, 7] <- country
    txn_df[i, 8] <- repID
  }

  return(txn_df)
}




# -------------- LOAD TABLES --------------------

# Function to load the territories table data
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
#     (2) src_df: the data frame holding the data to be loaded into the table
# output: no return value, but the table is loaded with data
load_territories_table <- function(dbcon, src_df) {
 
   # Create the temp df for the territories table from the given df
  terr_data <- data.frame(
    territoryName = src_df$territory
  )
  
  # reduce df to distinct values
  terr_data <- distinct(terr_data, territoryName, .keep_all = TRUE)
  
  # load the table
  dbWriteTable(dbcon, "territories", terr_data, row.names = FALSE, append = TRUE)
}


# Function to load the reps table data
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
#     (2) src_df: the data frame holding the data to be loaded into the table
# output: no return value, but the table is loaded with data
load_reps_table <- function(dbcon, src_df) {
  
  # Get territory data from the territories table and apply to temp df
  terr_data <- dbGetQuery(dbcon, "SELECT territoryID, territoryName FROM territories")
  
  # Merge terr_data with given src_df rep data on the text name of territory
  rep_df <- merge(src_df, terr_data, by.x = "territory", by.y = "territoryName", 
                       all.x = TRUE)
  
  # Create the temp df for the reps table with relevant column info
  rep_data <- data.frame(
    repID = as.integer(gsub("r", "", rep_df$repID)),
    firstName = rep_df$firstName,
    lastName = rep_df$lastName,
    territory = rep_df$territoryID
  )
  
  # load the table
  dbWriteTable(dbcon, "reps", rep_data, row.names = FALSE, append = TRUE)
}


# Function to load the countries table data
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
#     (2) src_df: the data frame holding the data to be loaded into the table
# output: no return value, but the table is loaded with data
load_countries_table <- function(dbcon, src_df) {
  
  # Create the temp df for the countries table from the given df
  country_data <- data.frame(
    countryName = src_df$country
  )
  
  # reduce temp df to unique values
  country_data <- distinct(country_data, countryName, .keep_all = TRUE)
  
  # load the table
  dbWriteTable(dbcon, "countries", country_data, row.names = FALSE, append = TRUE)
}


# Function to load the customers table data
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
#     (2) src_df: the data frame holding the data to be loaded into the table
# output: no return value, but the table is loaded with data
load_customers_table <- function(dbcon, src_df) {
  
  # Get country data from the countries table and apply to temp df
  customer_data <- dbGetQuery(dbcon, "SELECT countryID, countryName FROM countries")
  
  # Merge customer_data with src_df on the text name of country
  txn_df <- merge(src_df, customer_data, by.x = "country", by.y = "countryName", 
                  all.x = TRUE)
 
  # Create the temp df for the customer table w/ relevant columns assigned
  cust_data <- data.frame(
    customerName = txn_df$customer,
    country = txn_df$countryID
  )
  
  # reduce df to unique customers
  cust_data <- distinct(cust_data, customerName, .keep_all = TRUE)
  
  # load the table
  dbWriteTable(dbcon, "customers", cust_data, row.names = FALSE, append = TRUE)
}


# Function to load the products table data
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
#     (2) src_df: the data frame holding the data to be loaded into the table
# output: no return value, but the table is loaded with data
load_products_table <- function(dbcon, src_df) {
  
  # Create the temp df for the products table
  product_data <- data.frame(
    productName = src_df$product
  )
  
  # reduce df to unique products
  product_data <- distinct(product_data, productName, .keep_all = TRUE)
  
  # write to table
  dbWriteTable(dbcon, "products", product_data, row.names = FALSE, append = TRUE)
}


# Function to load salestxn table data
# Date Strategy: xml data is converted to date format, then the date format is 
#   re-specified to allow for easier loading by SQLite. The second formatting
#   effort converts to character type with a specific Y/m/d format, which 
#   allows for implicit loading by SQLite of the date data. This results in 
#   the date column appropriately holding date data in the salestxn table.
#   The date format in the normalized tables allows for extraction of just year
#   and month data (and thus quarter data) as needed for ETL loads to the 
#   tables in the denormalized star schema.
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
#     (2) src_df: the data frame holding the data to be loaded into the table
# output: no return value, but the table is loaded with data
load_salestxn_table <- function(dbcon, src_df) {
  
  # Get lookup data from other FK tables and apply to temp dfs
  customers_data <- dbGetQuery(dbcon, "SELECT customerID, customerName FROM customers")
  products_data <- dbGetQuery(dbcon, "SELECT productID, productName FROM products")
  
  # Merge temp dfs with txn_df/src_df on the text name of each entity tuple
  txn_df <- merge(src_df, customers_data, by.x = "customer", by.y = "customerName", 
                  all.x = TRUE)
  txn_df <- merge(txn_df, products_data, by.x = "product", by.y = "productName", 
                  all.x = TRUE)
  

  # Convert the date column to allow date type in the final table
  txn_df$date <- as.Date(txn_df$date, format = "%m/%d/%Y") # convert to date
  txn_df$date <- format(txn_df$date, "%Y-%m-%d") # set specific format, allowing char
  
  
  # Create the temp df for the salestxn table, specifying columns
  salestxn_data <- data.frame(
    txnID = txn_df$txnID,
    date = txn_df$date,
    quantity = txn_df$quantity,
    amount = txn_df$amount,
    productID = txn_df$productID,
    customerID = txn_df$customerID,
    repID = txn_df$repID
  )
  
  # load the table
  dbWriteTable(dbcon, "salestxn", salestxn_data, row.names = FALSE, append = TRUE)
}




# ------------ Disconnect --------------
# Function to disconnect from the database
# inputs: 
#     (1) dbcon: the name of the database on which the function operates
# output: no return value, but the database is disconnected from
disconnect <- function(dbcon) {
  dbDisconnect(dbcon)
}




# Main function to control program flow 
main <- function(){
  
  # ---------- Create DB ------------------
  # connect to db & store connection for use with functions
  fpath = "/Users/sarahgallant/Documents/MSCS_Align/cs5200/Practicum II/CS5200.PracticumII.GallantS/" #where db stored
  db_name <- "realizedDB.sqlite"
  dbcon <- connectToDB(db_name, fpath)
  
  # create tables & turn on fk constraints
  create_table_salestxn(dbcon)
  create_table_customers(dbcon)
  create_table_products(dbcon)
  create_table_reps(dbcon)
  create_table_countries(dbcon)
  create_table_territories(dbcon)
  add_fk_constraints(dbcon)
  
  
  
  # ---------- Access Current Folder ------------------
  # access txn-xml folder
  project_folder <- getwd() #wd where script is located (shared proj folder)
  txn_xml_folder <- file.path(project_folder, "txn-xml") #find folder
  
  
  # ---------- Process & Load Sales Rep Data ------------------
  reps_xml_path <- file.path(txn_xml_folder, "pharmaReps.xml") #specific file path
  reps_xml_doc <- load_xml_file(reps_xml_path) #apply fn to parse file 
  
  reps_data <- pull_reps_data(reps_xml_doc) #load xml data to df
  load_territories_table(dbcon, reps_data) #load data to territory table
  load_reps_table(dbcon, reps_data) #load data to reps table
  
  
  # ---------- Process & Load Sales Transaction Data ------------------
  # list all XML files in the txn-xml folder accessed via file path
  txn_files <- list.files(txn_xml_folder, pattern = "pharmaSalesTxn.*\\.xml", full.names = TRUE)
  
  # corresponding list to store created dfs from each file
  txn_data_list <- list()

  # iterate through list of files
  count <- 1 # set count variable for id_tag used for unique txn_id
  for (txn_file in txn_files) {
    
    txn_xml_doc <- load_xml_file(txn_file) #apply fn to parse file 
    
    txn_data <- pull_txn_data(txn_xml_doc,count) #load xml data to df
    
    txn_data_list[[count]] <- txn_data #store the data frame in the list

    count <- count + 1 #increment count
  }
  
  # Combine all data frames in the list into a single data frame
  if (length(txn_data_list) > 0) {
    combined_txn_data <- do.call(rbind, txn_data_list)
  } else {
    combined_txn_data <- NULL
  }
  
  # Load tables  
  load_countries_table(dbcon, combined_txn_data) #load data to countries table
  load_customers_table(dbcon, combined_txn_data) #load data to customers table
  load_products_table(dbcon, combined_txn_data) #load data to products table
  load_salestxn_table(dbcon, combined_txn_data) #load data to salestxn table

  
  # ---------- Disconnect from DB ------------------
  disconnect(dbcon)
  
}

main()

