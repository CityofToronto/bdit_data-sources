library(dplyr)
library(plyr)
library(lubridate)

filenames <- list.files(path = "data/", pattern="*.csv", full.names=TRUE)
ldf <- lapply(filenames, read.csv, sep = ";")

for (i in 1:length(ldf)){
  ldf[[i]]$SampleCount <- as.numeric(gsub(",","",ldf[[i]]$SampleCount))
}

df <- ldply(ldf, data.frame)

df$Timestamp <- as.POSIXct(df$Timestamp, format = "%d-%b-%Y %I:%M:%S %p", tz = "GMT")
df$Timestamp <- df$Timestamp - hours(1)
df$StartPointName <- as.character(df$StartPointName)
df$EndPointName <- as.character(df$EndPointName)
# df$SampleCount <- as.numeric(gsub(",","",df$SampleCount))
df$month <- floor_date(df$Timestamp, 'month')

summary <- aggregate(df$SampleCount, by=list(df$StartPointName, df$EndPointName, df$month), FUN = sum)
summary <- summary[order(summary[1], summary[2], summary[3]),]
write.csv(summary, "output.csv")


