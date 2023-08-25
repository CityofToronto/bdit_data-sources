library(RPostgreSQL)

drv <- dbDriver("PostgreSQL")
source("connect/connect.R")

strSQL <- paste0("SELECT * FROM activeto.cycling_seasonality_input WHERE day_-type ='Weekday'")
#strSQL <- paste0("SELECT * FROM activeto.cycling_seasonality_input WHERE day_type ='Weekend / Holiday'")

data <- dbGetQuery(con, strSQL)
data$temp_max_capped <- pmin(data$temp_max, 20)
data$sqrt_precip_mm <- sqrt(data$total_precip_mm)

fit <- lm(idx ~ temp_max_capped+num_hours+sqrt_precip_mm, data=data)
summary(fit)

plot(predict(fit), data$idx)
abline(a = 0,
       b = 1,
       col = "red",
       lwd = 2)
