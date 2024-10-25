library('tidyverse')
library('dplyr')
library('dbplyr')
library('ggplot2')
library('config')
library('gridExtra')


setwd('C:\\Users\\gwolofs\\OneDrive - City of Toronto\\Documents\\R')

dw <- config::get("bigdata")

con <- DBI::dbConnect(RPostgres::Postgres(),
                      host = dw$host,
                      dbname = dw$database,
                      user = dw$user,
                      password = dw$pwd
)

sites = tbl(con, sql("SELECT * FROM ecocounter.sites")) %>% collect()
flows = tbl(con, sql("SELECT * FROM ecocounter.flows")) %>% collect()

volumes = tbl(con, sql("WITH daily_volumes AS (
    SELECT
        site_id,
        f.flow_id,
        datetime_bin::date AS date,
        CASE SUM(lat.corrected_volume) WHEN 0 THEN null ELSE SUM(volume) END AS daily_volume
    FROM ecocounter.counts_unfiltered AS c
    JOIN ecocounter.flows AS f USING (flow_id)
   LEFT JOIN ecocounter.correction_factors cf
     ON c.flow_id = cf.flow_id
     AND c.datetime_bin::date <@ cf.factor_range,
    LATERAL (
      SELECT 
        round(COALESCE(cf.ecocounter_day_corr_factor, 1::numeric) * c.volume::numeric) AS corrected_volume
    ) lat
    GROUP BY
        site_id,
        f.flow_id,
        datetime_bin::date
)

SELECT 
    site_id,
    flow_id,
    date,
    daily_volume,
    AVG(daily_volume) OVER w AS rolling_avg_1_week
FROM daily_volumes
WINDOW w AS (
      PARTITION BY flow_id
      ORDER BY date
      RANGE BETWEEN interval '6 days' PRECEDING AND CURRENT ROW
    )
ORDER BY 
    site_id,
    flow_id,
    date")) %>% collect()

anomalous_ranges = tbl(con, sql("
    SELECT *,
    lower(time_range)::date AS lower,
    upper(time_range)::date AS upper
    FROM ecocounter.anomalous_ranges
")) %>% collect()

anomalous_ranges = anomalous_ranges %>% mutate(
  upper = date(upper), lower = date(lower)
)

correction_factors = tbl(con, sql("
  SELECT
    flow_id,
    site_id,
    flow_direction,
    ecocounter_day_corr_factor AS calibration_factor,
    LOWER(factor_range) AS factor_start,
    UPPER(factor_range) AS factor_end   
  FROM ecocounter.correction_factors
  JOIN ecocounter.flows USING (flow_id)
")) %>% collect()


#s=300028398
p <- lapply(sites$site_id, function(s) {
  site = sites %>% filter(site_id == s)
  vol = volumes %>% filter(site_id == s) %>% left_join(flows, by = 'flow_id')
  ars = anomalous_ranges %>% filter(site_id == s)
  corr = correction_factors %>% filter(site_id == s)
  limits = vol %>% filter(!is.na(daily_volume))
  
  ggplot() +
    theme_bw()+
    geom_rect(data = ars, aes(
      fill=problem_level,
      xmin=coalesce(lower, min(limits$date)),
      xmax=coalesce(upper, max(limits$date)),
      ymin = 0,
      ymax = max(limits$daily_volume)
    ), alpha = 0.5) +
    geom_path(data = vol, aes(
      x=date, y=daily_volume,
      color = paste(flow_direction, '-', flow_id)), linewidth = 0.2, alpha = 0.5) +
    geom_line(data = vol, aes(
      x=date, y=rolling_avg_1_week, linewidth = "7 day avg",
      color = paste(flow_direction, '-', flow_id)), linewidth = 1) +
    geom_vline(data = corr, aes(
        xintercept=factor_start,
        color = paste(flow_direction, '-', flow_id)
      ),
      size = 1.2,
      linetype="dotted") + 
    geom_text(data = ars, aes(
      x = coalesce(upper, max(limits$date)),
      y = max(limits$daily_volume),
      label = stringr::str_wrap(notes, 35),
      hjust = 1,
      vjust = 1), nudge_x = -10) +
    geom_label(data = corr, aes(
      x = coalesce(factor_start, min(limits$date))+3,
      y = max(limits$daily_volume)-50,
      color = paste(flow_direction, '-', flow_id),
      label = paste("CF: ", calibration_factor),
      hjust = 0,
      vjust = 1)) +
    ggtitle(label = paste(site$site_description, "(site_id:", s, ')')) + 
    scale_x_date(date_breaks = "1 month", date_minor_breaks = "1 week",
                 date_labels = "%Y-%m-%d",
                 limits = c(min(limits$date)-10, max(limits$date)+10)) +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))+
    guides(
      alpha=FALSE,
      linewidth=guide_legend(title=NULL),
      color=guide_legend(title="Flow"),
      fill=guide_legend(title="Anomalous Range"))
})

ggsave(
  filename = 'C:\\Users\\gwolofs\\OneDrive - City of Toronto\\SQLs\\Ecocounter\\validation_plots.pdf', 
  plot = marrangeGrob(p, nrow=2, ncol=1), 
  width = 15, height = 9
)
