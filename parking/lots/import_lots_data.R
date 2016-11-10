library(RPostgreSQL)
library(rjson)
library(plyr)

###############################
# FUNCTIONS
###############################
retrieve_cols <- function(table_name){
  strSQL = paste0('SELECT column_name ',
                  'FROM information_schema.columns ',
                  'WHERE table_name =\'',table_name,'\'',
                  sep = '')
  cols <- dbGetQuery(con, strSQL)
  cols <- cols$column_name
  return(cols)
}

###############################
# PULL DATA
###############################
loc <- "http://www1.toronto.ca/City Of Toronto/Information & Technology/Open Data/Data Sets/Assets/Files/greenPParking2015.json"
data <- fromJSON(file=loc)

###############################
# PROCESS DATA
###############################
lots_main <- NULL
lots_payment_methods <- NULL
lots_payment_options <- NULL
lots_periods <- NULL
lots_rates <- NULL
period_id = 1

for (i in 1:length(data$carparks)){
  lot <- data$carparks[[i]]
  
  id <- as.character(lot[1])
  main_item <- NULL
  methods_item <- NULL
  options_item <- NULL
  periods_item <- NULL
  rates_item <- NULL
  
  for (j in 1:length(lot)){
    if (!(names(lot[j]) %in% c("rate_details","payment_methods","payment_options"))){
      main_item <- as.data.frame(cbind(main_item, as.character(lot[j])))
      colnames(main_item)[ncol(main_item)] = names(lot[j])
    }
    if (names(lot[j]) == "payment_methods"){
      methods_item <- as.data.frame(unlist(lot[j]))
      rownames(methods_item) <- NULL
      colnames(methods_item) <- "method"
      methods_item$id <- id
    }
    if (names(lot[j]) == "payment_options"){
      options_item <- as.data.frame(unlist(lot[j]))
      rownames(options_item) <- NULL
      colnames(options_item) <- "opt"
      options_item$id <- id
    }   
    if (names(lot[j]) == "rate_details"){
      item <- lot[[j]]$periods
      for (k in 1:length(item)){
        subitem <- item[[k]]
        for (x in 1:length(subitem)){
          period_item <- NULL
          if (!(names(subitem[x]) == "rates")){
            periods_item <- as.data.frame(cbind(periods_item,as.character(subitem[x])))
          }
          if (names(subitem[x]) == "rates"){
            rates <- subitem[[x]]
            rates_item <- NULL
            if (length(rates) > 0){
              for (y in 1:length(rates)){
              rates_item <- as.data.frame(t(as.data.frame(unlist(rates[[y]]))))
              rownames(rates_item) <- NULL
              rates_item$period_id <- period_id
              lots_rates <- rbind.fill(lots_rates, rates_item)
              }
            }
          }
          periods_item$period_id <- period_id
          periods_item$id <- id
          lots_periods <- rbind.fill(lots_periods, periods_item)
          period_id <- period_id + 1
        }
      }
    } 
  }
  lots_main <- rbind.fill(lots_main,main_item)
  lots_payment_methods <- rbind.fill(lots_payment_methods, methods_item)
  lots_payment_options <- rbind.fill(lots_payment_options, options_item)
}

################################
# CLEAN DATA
################################
lots_main <- lots_main[,1:11]
lots_periods <- lots_periods[,1:3]
colnames(lots_periods)[1] <- "period"
colnames(lots_rates)[1] <- "time_period"
lots_main$max_height <- as.numeric(as.character(lots_main$max_height))
lots_main$rate_half_hour <- as.numeric(as.character(lots_main$rate_half_hour))
################################
# WRITE DATA TO POSTGRESQL
################################
drv <- dbDriver("PostgreSQL")
source("connect/connect.R")

# lots_main
cols <- retrieve_cols("lots_main")
dbWriteTable(con, c("parking","lots_main"), lots_main[cols], append = T, row.names = FALSE)

# lots_payment_methods
cols <- retrieve_cols("lots_payment_methods")
dbWriteTable(con, c("parking","lots_payment_methods"), lots_payment_methods[cols], append = T, row.names = FALSE)

# lots_payment_options
cols <- retrieve_cols("lots_payment_options")
dbWriteTable(con, c("parking","lots_payment_options"), lots_payment_options[cols], append = T, row.names = FALSE)


# lots_periods
cols <- retrieve_cols("lots_periods")
dbWriteTable(con, c("parking","lots_periods"), lots_periods[cols], append = T, row.names = FALSE)

# lots_rates
cols <- retrieve_cols("lots_rates")
dbWriteTable(con, c("parking","lots_rates"), lots_rates[cols], append = T, row.names = FALSE)