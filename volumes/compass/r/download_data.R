library(downloader)

source("connect/one-its.R")

destfile = 'C:\\Users\\aharpal\\Downloads\\400DN0005DSS.csv'
method = 'auto'

download.file(url, destfile, method, quiet = T)
