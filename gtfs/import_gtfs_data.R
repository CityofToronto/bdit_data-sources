library(RPostgreSQL)
library(plyr)
library(broom)
library(ggplot2)
library(chron)

loc = "data/ttc/"

################################
# FUNCTIONS
################################

retrieve_cols <- function(table_name){
  strSQL = paste0('SELECT column_name ',
                 'FROM information_schema.columns ',
                 'WHERE table_name =\'',table_name,'\'',
                 sep = '')
  cols <- dbGetQuery(con, strSQL)
  cols <- cols$column_name
  return(cols)
}

add_missing_cols <- function(cols,tbl){
  for (col in cols){
    if(!(col %in% colnames(tbl))){
      tbl[,col] <- NA
    }
  }
  return(tbl)
}

adj_time <- function(times){
  split <- strsplit(times, split=":")
  hrs <- unlist(split)[3*(1:length(times))-2]
  mins <- unlist(split)[3*(1:length(times))-1]
  secs <- unlist(split)[3*(1:length(times))-0]
  hrs <- sprintf("%02d",as.numeric(hrs) %% 24)
  new_times <- paste(hrs,mins,secs,sep=":")
  return(new_times)
}
################################
# READ DATA
################################
files <- list.files(path = loc, pattern = ".*.txt")

agency_id <- as.character(read.csv(paste0(loc,"agency.txt"),header=T)[,1])

tables <- NULL
for (i in 1:length(files)) {
  tables[i] <- strsplit(files[i],'[.]')[[1]][1]
  temp <- read.csv(paste0(loc,files[i]), header=T)
  temp$agency_id <- agency_id
  assign(tables[i],temp)
  rm(temp)
}

################################
# WRITE DATA TO POSTGRESQL
################################
drv <- dbDriver("PostgreSQL")
source("connect/connect.R")

# AGENCY
if (exists("agency")){
  columns <- retrieve_cols("agency")
  agency <- add_missing_cols(columns,agency)
  dbWriteTable(con, c("gtfs","agency"), agency[columns], append = T, row.names = FALSE)
}

# CALENDAR
if (exists("calendar")){
  columns <- retrieve_cols("calendar")
  calendar <- add_missing_cols(columns,calendar)
  dbWriteTable(con, c("gtfs","calendar"), calendar[columns], append = T, row.names = FALSE)
}

# CALENDAR DATES
if (exists("calendar_dates")){
  calendar_dates$dt <- as.Date(as.character(calendar_dates$date),format = "%Y%m%d")
  calendar_dates$date <- NULL
  columns <- retrieve_cols("calendar_dates")
  calendar_dates <- add_missing_cols(columns,calendar_dates)
  dbWriteTable(con, c("gtfs","calendar_dates"), calendar_dates[columns], append = T, row.names = FALSE)
}

# FARE ATTRIBUTES
if (exists("fare_attributes")){
  columns <- retrieve_cols("fare_attributes")
  fare_attributes <- add_missing_cols(columns,fare_attributes)
  dbWriteTable(con, c("gtfs","fare_attributes"), fare_attributes[columns], append = T, row.names = FALSE)
}

# FARE RULES
if (exists("fare_rules")){
  columns <- retrieve_cols("fare_rules")
  fare_rules <- add_missing_cols(columns,fare_rules)
  dbWriteTable(con, c("gtfs","fare_rules"), fare_rules[columns], append = T, row.names = FALSE)
}

# ROUTES
if (exists("routes")){
  columns <- retrieve_cols("routes")
  routes <- add_missing_cols(columns,routes)
  dbWriteTable(con, c("gtfs","routes"), routes[columns], append = T, row.names = FALSE)
}

# SHAPES
if (exists("shapes")){
  columns <- retrieve_cols("shapes")
  shapes <- add_missing_cols(columns,shapes)
  dbWriteTable(con, c("gtfs","shapes"), shapes[columns], append = T, row.names = FALSE)
}

# STOP TIMES
if (exists("stop_times")){
  stop_times$departure_time <- adj_time(as.character(stop_times$departure_time))
  stop_times$arrival_time <- adj_time(as.character(stop_times$arrival_time))
  columns <- retrieve_cols("stop_times")
  stop_times <- add_missing_cols(columns,stop_times)
  dbWriteTable(con, c("gtfs","stop_times"), stop_times[columns], append = T, row.names = FALSE)
}

# STOPS
if (exists("stops")){
  columns <- retrieve_cols("stops")
  stops <- add_missing_cols(columns,stops)
  dbWriteTable(con, c("gtfs","stops"), stops[columns], append = T, row.names = FALSE)
}

# TRANSFERS
if (exists("transfers")){
  columns <- retrieve_cols("transfers")
  transfers <- add_missing_cols(columns,transfers)
  dbWriteTable(con, c("gtfs","transfers"), transfers[columns], append = T, row.names = FALSE)
}

# TRIPS
if (exists("trips")){
  columns <- retrieve_cols("trips")
  trips <- add_missing_cols(columns,trips)
  dbWriteTable(con, c("gtfs","trips"), trips[columns], append = T, row.names = FALSE)
}
