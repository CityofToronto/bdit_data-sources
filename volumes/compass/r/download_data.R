library(downloader)

source("connect/one-its.R")

url = paste0('http://',user,':',pw,url_base,'year=2017&month=4&reportType=min_30&sensorName=400DN0005DSS')
destfile = 'C:\\Users\\aharpal\\Downloads\\400DN0005DSS.csv'
method = 'auto'

download.file(url, destfile, method, quiet = T)
