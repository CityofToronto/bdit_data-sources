library(RPostgreSQL)
library(stringr)

drv <- dbDriver("PostgreSQL")
source("connect/connect.R")


files = list.files(path = "data/.", pattern = "*.RPT")


for (j in 1:length(files)) {

  dt <- as.Date(str_extract(files[j], "(?<=15MINVOL_)(.*)(?=.RPT)"), "%d-%b-%Y")
    
  data <- read.csv(paste0("data/",files[j]),stringsAsFactors=FALSE)
  data[2] <- dt
  data <- data[2:1]
  
  colnames(data)[1:2] <- c("dt","raw_info")
    
  dbWriteTable(con, c('rescu','raw_15min'),data, append = T, row.names = F, overwrite = F)
    
}
  