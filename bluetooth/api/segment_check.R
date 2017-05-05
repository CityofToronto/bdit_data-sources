library(XML)

data <- xmlRoot(xmlParseDoc("display.xml"))
segs <- vector(mode="character",length=60)
info <- xmlToDataFrame(data)
for (i in seq(2,61)){
  segs[i-1] <- xmlAttrs(xmlRoot(data)[[i]])
}

segs <- as.data.frame(segs)
info <- info[2:nrow(info),]
info <- cbind(segs, info)
routes <- read.csv("routes.csv")
