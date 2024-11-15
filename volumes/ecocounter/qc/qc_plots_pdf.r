library('tidyverse')
library('dplyr')
library('dbplyr')
library('ggplot2')
library('config')
library('gridExtra')
library('ggrepel')

setwd('~/../OneDrive - City of Toronto/Documents/R')

dw <- config::get("bigdata")

con <- DBI::dbConnect(RPostgres::Postgres(),
                      host = dw$host,
                      dbname = dw$database,
                      user = dw$user,
                      password = dw$pwd
)

flows = tbl(con, sql("SELECT * FROM ecocounter.flows")) %>% collect()
sites = tbl(con, sql("SELECT * FROM ecocounter.sites ORDER BY site_description")) %>%
  mutate(site_title = paste0(site_description, " (site_id = ", site_id, ")")) %>% collect()

volumes_valid = tbl(con, sql("
        SELECT
          site_id, direction::text AS flow_color, dt AS date, daily_volume,
          CASE WHEN daily_volume IS NOT NULL THEN AVG(daily_volume) OVER w END AS rolling_avg_1_week
        FROM ecocounter.open_data_daily_counts
        WINDOW w AS (
          PARTITION BY site_id, direction
          ORDER BY dt
          RANGE BETWEEN interval '6 days' PRECEDING AND CURRENT ROW
        )")) %>%
  #filter(site_id %in% !!site_ids()) %>%
  collect() %>% 
  group_by(site_id, flow_color) %>% 
  arrange(site_id, flow_color, date) %>%
  mutate(datedif = as.numeric(date - dplyr::lag(date))-1) %>%
  mutate(groupid = cumsum(ifelse(is.na(datedif), 0, datedif)))

volumes_qc = tbl(con, sql(sprintf("
        SELECT site_id, flow_id, date, daily_volume, rolling_avg_1_week, flow_color
        FROM (VALUES %s) AS sites(s),
        LATERAL ecocounter.qc_graph_volumes(s) AS qc",
                                  paste0('(', sites$site_id, ')', collapse = ', ')))) %>%
  collect() %>% 
  group_by(site_id, flow_color) %>% 
  arrange(date) %>%
  mutate(datedif = as.numeric(date - dplyr::lag(date))-1) %>%
  mutate(groupid = cumsum(ifelse(is.na(datedif), 0, datedif)))

anomalous_ranges = tbl(con, sql("
  SELECT
    site_id,
    lower(time_range)::date AS lower,
    upper(time_range)::date AS upper,
    CASE WHEN flow_id IS NULL THEN 'site_id - ' || site_id
    ELSE 'flow_id - ' || flow_id END || ': ' || notes AS notes,
    problem_level
  FROM ecocounter.anomalous_ranges
")) %>% collect()

calibration_factors = tbl(con, sql("
  SELECT
    flow_id,
    site_id,
    flow_direction,
    ecocounter_day_corr_factor AS calibration_factor,
    LOWER(factor_range) AS factor_start,
    UPPER(factor_range) AS factor_end,
    f.flow_direction || ' - ' || f.flow_id AS flow_color
  FROM ecocounter.calibration_factors
  JOIN ecocounter.flows_unfiltered AS f USING (flow_id)
  JOIN ecocounter.sites USING (site_id)
")) %>% collect()


#s="Bloor St W, between Palmerston & Markham (retired)"
myplots <- list()
i=1
for (s in unique(sites$site_description)) {
  site = sites %>% filter(site_description == s)
  site_ids = (sites %>% filter(site_description == s))$site_id  
  v = volumes_qc %>% filter(site_id %in% site_ids)
  site_titles = (sites %>% filter(site_id %in% site_ids))$site_title
  graph_title <- paste0(s, ' (site_id = ', paste(site_ids, collapse = ', '), ')')
  
  limits = v %>% filter(!is.na(daily_volume))
  min_date <- min(limits$date)
  max_date <- max(limits$date)
  if (max_date - min_date > dyears(3)){
    break_minor = '1 month'
    break_major = '1 year'
  } else {
    break_minor = '1 day'
    break_major = '1 month'
  }

  ars = anomalous_ranges %>% filter(site_id %in% site_ids) %>% rowwise() %>%
    mutate(x_plot = mean.Date(c(coalesce(upper, max_date), coalesce(lower, min_date))))
  cf = calibration_factors %>% filter(site_id %in% site_ids)
  
  volumes_layers <- list(
    geom_path(data = v, aes(
      x=date, y=daily_volume, color = flow_color, group = paste(site_id, flow_color, groupid)
    ), linewidth = 0.2, alpha = 0.5),
    geom_line(data = v, aes(
      x=date, y=rolling_avg_1_week, linewidth = "7 day avg",
      color = flow_color, group = paste(site_id, flow_color, groupid)), linewidth = 1)
  )
  
  v_valid <- volumes_valid %>% filter(site_id %in% site_ids)
  volumes_layers_valid <- list(
    geom_path(data = v_valid, aes(
      x=date, y=daily_volume, color = flow_color, group = paste(site_id, flow_color, groupid)
    ), linewidth = 0.2, alpha = 0.5),
    geom_line(data = v_valid, aes(
      x=date, y=rolling_avg_1_week, linewidth = "7 day avg",
      color = flow_color, group = paste(site_id, flow_color, groupid)), linewidth = 1)
  )
  
  base_plot <- list(
    ggtitle(graph_title),
    theme_bw(),
    scale_x_date(date_breaks = break_major,
                 date_minor_breaks = break_minor,
                 date_labels = "%Y-%m-%d", limits = c(min_date-ddays(20), max_date+ddays(20))),
    theme(axis.text.x = element_text(angle = 45, vjust = 1, hjust=1),
          text = element_text(size = 10)),
    guides(
      alpha="none",
      linewidth=guide_legend(title=NULL),
      color=guide_legend(title="Flow"),
      fill=guide_legend(title="Anomalous Range"))
  )
  
  cf_layers <- list(
    geom_vline(
      #add a jitter to calibration factors since they occur on the same date.
      data = cf, #%>% mutate(factor_start = factor_start + sample(rnorm(1,7), n(), replace = TRUE)),
      aes(xintercept=factor_start, color = flow_color),
      linewidth = 1.2, linetype = 'dotted'),
    geom_label_repel(
      data = cf,
      max.overlaps = Inf,
      aes(x = coalesce(factor_start, min_date)+3, y = max(limits$daily_volume)-50,
          color = flow_color, label = paste("CF: ", calibration_factor)))
  )
  
  ar_layers = list(
    geom_rect(data = ars,
              aes(
                fill=problem_level,
                xmin=coalesce(lower, min_date),
                xmax=coalesce(upper, max_date),
                ymin = 0,
                ymax = max(limits$daily_volume)
              ), alpha = 0.5),
    geom_label_repel(data = ars, aes(
      x = x_plot,
      y = max(limits$daily_volume),
      label = stringr::str_wrap(notes, 35),
      hjust = 0,
      vjust = 1)),
    scale_fill_manual(
        values = c("#00BFC4", "#F8766D"),
        limits = c("valid-caveat", "do-not-use"))
  )
  

  
  gA <- ggplot() + base_plot + ar_layers + volumes_layers + cf_layers
  gB <- ggplot() + base_plot + volumes_layers_valid
  grid::grid.newpage()
  myplots[[i]] <- gridExtra::gtable_rbind(ggplotGrob(gA), ggplotGrob(gB))
  i <- i+1
}


fname = paste0("ecocounter_validation_plots_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".pdf")

ggsave(
  filename = file.path(getwd(), fname), 
  plot = marrangeGrob(myplots, nrow=1, ncol=1), 
  width = 15, height = 9
)
print(file.path(getwd(), fname))