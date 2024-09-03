suppressWarnings(library(DBI))
suppressWarnings(library(odbc))
suppressWarnings(suppressPackageStartupMessages(library(tidyverse)))
suppressWarnings(suppressPackageStartupMessages(library(dbplyr)))

# source("~/Phoon Huat/sql/env.R")
# put it in this directory ~/Phoon Huat/sql/env.R

# read password file ------------------------------------------------------

try({
  
  
  dbtxt <- read_csv("~/Password/database_pwd.txt", show_col_types = FALSE)
  
  sap_db <- dbtxt %>%
    filter(Source=="sap")
  
  sql_db <- dbtxt %>%
    filter(Source=="sql")
  
  dr_db <- dbtxt %>%
    filter(Source=="dr")
  
})

# create connection -------------------------------------------------------

sap <-tryCatch(expr = {
  message("Loading SAP")
  sap <- DBI::dbConnect(odbc::odbc(), dsn = sap_db$Database, uid = sap_db$UID,
                        pwd = sap_db$PWD, CS = "SAPABAP1", timeout = 10)
},

error = function(cond) {
  message("Error: Failed to load *SAP* connection.")
  return(NA)
})

Sys.sleep(1)

con <- sap

sql <- tryCatch(expr = {
  message("Loading SQL")
  sql <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = sql_db$Server, 
                        uid = sql_db$UID, database = sql_db$Database, pwd = sql_db$PWD,
                        timeout = 10)
},

error = function(cond) {
  message("Error: Loaded default SQL *test* connection.")
  sql <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "phredas02.phoonhuat.com", 
                        database = "test",
                        timeout = 10)
})

Sys.sleep(1)

sql_common <- tryCatch(expr = {
  message("Loading SQL common")
  sql_common <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = sql_db$Server, 
                               uid = sql_db$UID, database = "common", pwd = sql_db$PWD, 
                               timeout = 10)
},

error = function(cond) {
  message("Error: Loaded default SQL *common* connection.")
  sql_common <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "phredas02.phoonhuat.com", 
                               database = "common",
                               timeout = 10)
})



Sys.sleep(1)

sql_ecom <- tryCatch(expr = {
  message("Loading SQL ecom")
  sql_ecom <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = sql_db$Server, 
                             uid = sql_db$UID, database = "ecom", pwd = sql_db$PWD, 
                             timeout = 10)
},

error = function(cond) {
  message("Error: Loaded default SQL *ecom* connection.")
  sql_ecom <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "phredas02.phoonhuat.com", 
                             database = "ecom", 
                             timeout = 10)
})


Sys.sleep(1)
dr <- tryCatch(expr = {
  message("Loading dr connection")
  dr <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = dr_db$Server, 
                       database = dr_db$Database, uid = dr_db$UID, port = "1433", pwd = dr_db$PWD, 
                       timeout = 10)
},

error = function(cond) {
  message("Failed to load dr connection")
  message("Original error message:")
  message(cond)
  return(NA)
})
Sys.sleep(1)


flexba <- tryCatch(expr = {
  message("Loading flexba connection")
  flexba <- DBI::dbConnect(odbc::odbc(), 
                           Driver="MySQL ODBC 8.0 Unicode Driver",
                           # user='backup', 
                           user='root',
                           # password='PHp@ssw0rd!', 
                           password='PHred@cc0unt',
                           database='phoonhuat_backup', 
                           server = '170.100.10.94',
                           # server='PHREDSAPUAT2',
                           Port = 3306, timeout = 10)
},

error = function(cond) {
  message("Failed to load flexba connection")
  message("Original error message:")
  message(cond)
  return(NA)
})

# database related functions ----------------------------------------------



writesql <- function(con=NULL, table=NULL, dataframe=NULL, bin=1e6, overwrite=NULL){
  
  # if(is.null(con)){print("No connection")}else{print(con)}
  
  # print(bin)
  for (i in 1:ceiling(nrow(dataframe)/bin)){
    if(i==1){
      start <- 1+bin*(i-1)
      end <- min(bin*i,nrow(dataframe))
      
      DBI::dbWriteTable(con,table,dataframe[start:end,],overwrite=overwrite)
      print(paste0(end, " rows"))
    }else{
      
      start <- 1+bin*(i-1)
      end <- min(bin*i,nrow(dataframe))
      
      DBI::dbWriteTable(con,table,dataframe[start:end,],append=overwrite)
      
      print(paste0(end, " rows"))
      
    }
  }
  
  print(paste0("uploaded to ",table))
  
}


writesql_v2 <- function(con=NULL, table=NULL, dataframe=NULL, bin=1e6, method="overwrite"){
  
  # if(is.null(con)){print("No connection")}else{print(con)}
  
  # print(bin)
  for (i in 1:ceiling(nrow(dataframe)/bin)){
    if(i==1){
      start <- 1+bin*(i-1)
      end <- min(bin*i,nrow(dataframe))
      o_value <- ifelse(method=="overwrite",T,F)
      a_value <- ifelse(method=="append",T,F)
      
      
      DBI::dbWriteTable(con,table,dataframe[start:end,],overwrite=o_value,append=a_value)
      print(paste0(end, " rows"))
    }else{
      
      start <- 1+bin*(i-1)
      end <- min(bin*i,nrow(dataframe))
      
      DBI::dbWriteTable(con,table,dataframe[start:end,],append=T)
      
      print(paste0(end, " rows"))
      
    }
  }
  
  print(paste0("uploaded to ",table))
  
}


# deprecated connection ---------------------------------------------------


# flexba_ecom <- dbConnect(odbc::odbc(),
#                          Driver = "MySQL ODBC 8.0 Unicode Driver",
#                          user = "user",
#                          password = "PHp@ssw0rd123$",
#                          database = "phoonhuat_ecom_production",
#                          server = "172.100.19.3",
#                          Port = 3306,
#                          timeout = 10)
