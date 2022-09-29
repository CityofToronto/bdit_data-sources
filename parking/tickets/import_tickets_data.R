library(RPostgreSQL)
library(plyr)

drv <- dbDriver("PostgreSQL")
source("connect/connect.R")

###############################
# PULL AND IMPORT DATA
###############################

files = list.files(path = "data/.", pattern = "*.csv")

for (j in 1:length(files)) {
  data <- read.csv(file = paste0('data/',files[j]),
                     sep = ",",
                   stringsAsFactors = FALSE,
                   colClasses=c(rep("character",11)))
  data$source_file <- files[j]
  print(paste0(files[j]," - ",nrow(data)))
  dbWriteTable(con, c('parking','tickets_raw'),data, append = T, row.names = F, overwrite = F)
  
}
