
library(tidyverse)
library(dplyr)
library(dbplyr)
library(ggplot2)
library(config)
library(ggrepel)
library(gridExtra)

setwd('~/../OneDrive - City of Toronto/Documents/R')

#db connection from config.yml located in working directory
dw <- config::get("bigdata")
con <- DBI::dbConnect(RPostgres::Postgres(),
                      host = dw$host,
                      dbname = dw$database,
                      user = dw$user,
                      password = dw$pwd
)

validation_results = tbl(con, sql("
  WITH most_common_counts AS (
    SELECT ecocounter_site_id, count_date, ecocounter_direction, string_agg(ecocounter_bikes || ':' || count, chr(10) ORDER BY count DESC) AS common_counts
    FROM (
      SELECT ecocounter_site_id, count_date, ecocounter_direction, ecocounter_bikes, COUNT(*)
      FROM ecocounter.manual_counts_matched
      WHERE ecocounter_bikes > 0
      GROUP BY ecocounter_site_id, count_date, ecocounter_direction, ecocounter_bikes
    ) AS eco_counts
    GROUP BY ecocounter_site_id, count_date, ecocounter_direction
  )
  
  SELECT
    ecocounter_site_id,
    sites.site_description,
    count_date,
    ecocounter_direction,
    vol_spec_path::numeric,
    vol_ecocounter,
    SUM(round(evci.ecocounter_day_corr_factor * ecocounter_bikes::numeric)) AS total_open_data_method,
    SUM(round(evci.ecocounter_day_corr_factor * ecocounter_bikes::numeric)) - vol_spec_path AS open_data_error,
    ROUND((SUM(round(evci.ecocounter_day_corr_factor * ecocounter_bikes::numeric)) - vol_spec_path) / vol_spec_path, 3) AS open_data_error_percent,
    common_counts
  FROM ecocounter.manual_counts_matched AS vcm --ecocounter_site_id, ecocounter_approach, spectrum_approach, ecocounter_direction, count_date
  JOIN ecocounter.validation_results AS evci USING (ecocounter_site_id, count_date, ecocounter_direction)
  JOIN most_common_counts USING (ecocounter_site_id, count_date, ecocounter_direction)
  JOIN ecocounter.sites ON ecocounter_site_id = site_id
  GROUP BY
    ecocounter_site_id,
    count_date,
    ecocounter_direction,
    vol_spec_path,
    vol_ecocounter,
    sites.site_description,
    common_counts
  ORDER BY ABS(ROUND((vol_spec_path - SUM(round(evci.ecocounter_day_corr_factor * ecocounter_bikes::numeric))) / vol_spec_path, 3)) DESC
")) %>% collect() %>% 
  mutate(title = paste(site_description, '-', ecocounter_direction, '-', count_date)) %>% 
  rename("Spectrum Volume" = vol_spec_path, "Scaling Error" = open_data_error)

summary = validation_results %>% group_by("Spectrum Count < 100" = `Spectrum Volume` < 100) %>% 
  summarise(
    avg_error = round(mean(`Scaling Error`), 1),
    avg_percent_error = scales::label_percent()(sum(`Scaling Error`) / sum(`Spectrum Volume`)),
    count = n()
  )

p <- validation_results %>%
  select(title, `Scaling Error`, `Spectrum Volume`, open_data_error_percent) %>%
  pivot_longer(names_to = "type", values_to = "count",
               cols = c(`Spectrum Volume`, `Scaling Error`)) %>% 
ggplot() +
  geom_col(aes(x=forcats::fct_reorder(title, abs(open_data_error_percent)),
               y=count, fill=type)) + 
theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1),
      text = element_text(size = 10)) + xlab('Study')+
  ggtitle('Ecocounter - Spectrum Scaling Validation') +
  annotation_custom(tableGrob(summary), xmin = 50, xmax = 65, ymin = 1000, ymax = Inf )

p + geom_label_repel(data = validation_results,
                 aes(x=title, y=`Spectrum Volume`,
                     label = scales::label_percent(accuracy = 0.1)(open_data_error_percent)),
                 max.overlaps = Inf, min.segment.length = 0)

p + geom_label_repel(data = validation_results,
                     aes(x=title, y=`Spectrum Volume`,
                         label = `Scaling Error`),
                     max.overlaps = Inf, min.segment.length = 0)
