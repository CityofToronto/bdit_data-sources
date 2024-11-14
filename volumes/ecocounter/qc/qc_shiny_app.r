# Install required packages if not already installed
# install.packages("shiny")
# install.packages("ggplot2")

library(shiny)
library(DT)
library(tidyverse)
library(dplyr)
library(dbplyr)
library(ggplot2)
library(config)
library(ggrepel)

#config.yml located here
setwd('~/../OneDrive - City of Toronto/Documents/R')
dw <- config::get("bigdata")

con <- DBI::dbConnect(RPostgres::Postgres(),
                      host = dw$host,
                      dbname = dw$database,
                      user = dw$user,
                      password = dw$pwd
)

dir.create('ecocounter_anomalous_ranges', showWarnings = FALSE)
export_path <- file.path(getwd(), 'ecocounter_anomalous_ranges')

flows = tbl(con, sql("SELECT * FROM ecocounter.flows")) %>% collect()
sites = tbl(con, sql("SELECT * FROM ecocounter.sites ORDER BY site_description")) %>%
  mutate(site_title = paste0(site_description, " (site_id = ", site_id, ")")) %>% collect()

# Define UI for the app
ui <- fluidPage(
  titlePanel("Ecocounter - Interactive Anomalous Range Selection"),
  fluidRow(uiOutput('site_descriptions')),
  actionButton("query", "Query Selected Sites"),
  fluidRow(
    column(3, checkboxInput(inputId = 'validated_only', label = 'Validated Data Only', value = FALSE)),
    column(3, checkboxInput(inputId = 'anomalous_ranges', label = 'Show Anomalous Ranges?', value = TRUE)),
    column(3, checkboxInput(inputId = 'calibration_factors', label = 'Show Calibration Factors?', value = TRUE)),
    column(3, checkboxInput(inputId = 'recent_data', label = 'Only sites with recent data?', value = FALSE))
  ),
  uiOutput("dynamic_title"),  # Dynamic title panel
  fluidRow(
    plotOutput("plot",
               dblclick = "plot_dblclick",
               #click = "plot_click",
               brush = brushOpts(
                 id = "brush",
                 resetOnNew = FALSE,
                 delay = 1000,
                 delayType = "debounce"
               ))),
  fluidRow(
    actionButton("save_range", "Save Range"),
    DTOutput("myTable"),
    actionButton("export_csv", "Export Ranges to File")
  )
)

getRemoveButton <- function(n, idS = "", lab = "Pit") {
  if (stringr::str_length(idS) > 0) idS <- paste0(idS, "-")
  ret <- shinyInput(actionButton, n,
                    'button_', label = "Remove",
                    onclick = sprintf('Shiny.onInputChange(\"%sremove_button_%s\",  this.id)' ,idS, lab))
  return (ret)
}

shinyInput <- function(FUN, n, id, ses, ...) {
  as.character(FUN(paste0(id, n), ...))
}

# Define server logic
server <- function(input, output, session) {
  # Store ranges selected by the user
  values <- reactiveValues()
  values$tab <- data.frame(
    id = integer(),
    site_id = numeric(),
    flow_id = numeric(),
    range_start = as.Date(character()),
    range_end = as.Date(character()),
    notes = character(),
    investigation_level = character(),
    problem_level = character()
  ) %>%
    mutate(Remove = getRemoveButton(id, idS = "", lab = "Tab1"))
  
  buttonCounter <- 0L
  
  proxyTable <- DT::dataTableProxy("tab")
  
  # Render table with delete buttons
  output$myTable <- DT::renderDataTable({
    DT::datatable(values$tab,
                  #options = list(dom = "t"),
                  escape   = FALSE,
                  editable = TRUE)
  })
  
  # reactive values for zoomable plot
  ranges <- reactiveValues(x = NULL, y = NULL)
  
  # Clear brush, reset zoom when site_id changes or range is saved.
  observeEvent(list(input$site_descriptions, input$save_range), priority = -1, {
    session$resetBrush("brush")
    ranges$x <- NULL
    ranges$y <- NULL
  })
  
  #sites_list which can be filtrered for all data or only recent data.
  sites_list <- reactive({
    if (input$recent_data) {
      sites_list <- (sites %>% filter(last_active >= today() - dmonths(2)))$site_description
    } else {
      sites_list <- sites$site_description
    }
    return(unique(sites_list))
  })
  
  site_ids <- eventReactive(input$site_descriptions, {
    temp = sites %>% filter(site_description %in% input$site_descriptions)
    return(temp$site_id)
  })
  
  output$site_descriptions = renderUI({
    selectInput("site_descriptions",
                label = "Select Site ID",
                choices = sites_list(),
                selected = sites_list()[1],
                multiple = TRUE,
                selectize=FALSE,
                width = 400)
  })
  
  # When a double-click happens, check if there's a brush on the plot.
  # If so, zoom to the brush bounds; if not, reset the zoom.
  observeEvent(list(input$plot_dblclick), priority = 1, {
    brush <- input$brush
    if (!is.null(brush)) {
      ranges$x <- c(as.Date(brush$xmin), as.Date(brush$xmax))
      ranges$y <- c(brush$ymin, brush$ymax)
    } else {
      ranges$x <- NULL
      ranges$y <- NULL
    }
  })
  
  #volume data, refreshes on site_id
  vol <- eventReactive(list(input$query, input$validated_only), {
    # Your code to prepare the data for plotting
    
    if(input$validated_only){
      volumes = tbl(con, sql("
        SELECT
          site_id, direction::text AS flow_color, dt AS date, daily_volume,
          CASE WHEN daily_volume IS NOT NULL THEN AVG(daily_volume) OVER w END AS rolling_avg_1_week
        FROM ecocounter.open_data_daily_counts
        WINDOW w AS (
          PARTITION BY site_id, direction
          ORDER BY dt
          RANGE BETWEEN interval '6 days' PRECEDING AND CURRENT ROW
        )")) %>%
        filter(site_id %in% !!site_ids()) %>%
        collect() %>% 
        group_by(site_id, flow_color) %>% 
        arrange(site_id, flow_color, date) %>%
        mutate(datedif = as.numeric(date - dplyr::lag(date))-1) %>%
        mutate(groupid = cumsum(ifelse(is.na(datedif), 0, datedif)))
      
    } else {
      volumes = tbl(con, sql(sprintf("
        SELECT site_id, flow_id, date, daily_volume, rolling_avg_1_week, flow_color
        FROM (VALUES %s) AS sites(s),
        LATERAL ecocounter.qc_graph_volumes(s) AS qc", paste0('(', site_ids(), ')', collapse = ', ')))) %>%
        collect() %>% 
        group_by(site_id, flow_color) %>% 
        arrange(date) %>%
        mutate(datedif = as.numeric(date - dplyr::lag(date))-1) %>%
        mutate(groupid = cumsum(ifelse(is.na(datedif), 0, datedif)))
    }
    return(volumes)
  })
  
  #anomalous ranges data, refreshes on site_id
  ars <- eventReactive(input$site_descriptions, {
    anomalous_ranges = tbl(con, sql("
        SELECT
          site_id,
          lower(time_range)::date AS lower,
          upper(time_range)::date AS upper,
          CASE WHEN flow_id IS NULL THEN 'site_id - ' || site_id
          ELSE 'flow_id - ' || flow_id END || ': ' || notes AS notes,
          problem_level
        FROM ecocounter.anomalous_ranges
    ")) %>% filter(site_id %in% !!site_ids()) %>% collect()
    return(anomalous_ranges)
  })
  
  #calibration factors, refreshes on site_id
  corr <- eventReactive(input$site_descriptions, {
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
      ")) %>% filter(site_id %in% !!site_ids()) %>% collect()
    return(calibration_factors)
  })
  
  output$dynamic_title <- renderUI({
    s = site_ids()
    site_titles = (sites %>% filter(site_id %in% s))$site_title
    tags$h2(
      paste(site_titles, collapse = ', '),
      style = "font-size: 22px; text-align: center;"  # Customize font size, weight, and color here
    )
  })
  
  # Observe "Remove" buttons and removes rows
  observeEvent(input$remove_button_Tab1, {
    myTable <- values$tab
    s <- as.numeric(strsplit(input$remove_button_Tab1, "_")[[1]][2])
    myTable <- filter(myTable, id != s)
    replaceData(proxyTable, myTable, resetPaging = FALSE)
    values$tab <- myTable
  })
  
  # Observe brush input and add ranges to the reactive dataframe
  observeEvent(input$save_range, {
    brush <- input$brush
    
    if (!is.null(brush)) {
      myTable <- isolate(values$tab)
      si = site_ids()
      
      for (s in si){
        fl = (flows %>% filter(site_id == s))$flow_id
        for (f in fl){
          buttonCounter <<- buttonCounter + 1L
          new_range <- data.frame(
            id = buttonCounter,
            site_id = s,
            flow_id = f,
            range_start = as.Date(input$brush$xmin),
            range_end = as.Date(input$brush$xmax),
            notes = "",
            investigation_level = 'confirmed',
            problem_level = 'do-not-use'
          )
          
          myTable <- bind_rows(
            myTable,
            new_range %>%
              mutate(Remove = getRemoveButton(buttonCounter, idS = "", lab = "Tab1")))
        }
      }
      replaceData(proxyTable, myTable, resetPaging = FALSE)
      values$tab <- myTable
    }
  })
  
  
  # Observes table editing and updates data
  observeEvent(input$myTable_cell_edit, {
    
    myTable <- values$tab
    row  <- input$myTable_cell_edit$row
    clmn <- input$myTable_cell_edit$col
    myTable[row, clmn] <- input$myTable_cell_edit$value
    replaceData(proxyTable, myTable, resetPaging = FALSE)
    values$tab <- myTable
    
  })
  
  ar_listen <- reactive({
    list(input$myTable_cell_edit, input$save_range, input$query, input$anomalous_ranges)
  })
  
  ar_data <- eventReactive(ar_listen(), {
    # Your code to prepare the data for plotting
    s=input$site_descriptions
    temp_ars = values$tab %>% filter(site_id == s) %>%
      transmute(site_id, lower = range_start, upper = range_end, notes, problem_level = "Draft")
    
    return(rbind(ars(), temp_ars))
  })
  
  
  output$plot <- renderPlot({
    #this part of the plot only changes when site id changes
    p <- ggplot() + base_plot()
    
    if(input$calibration_factors){
      p <- p + cf_layers()
    }
    
    #changes when brush changes
    if (!is.null(input$brush)) {
      p <- p + brush_labels()
    }
    
    #changes when anomalous ranges change
    if ((ar_data() %>% count() > 0) & input$anomalous_ranges){
      p <- p + ar_layers()
    }
    
    p +
      #reactive to zoom
      coord_cartesian(xlim = ranges$x, ylim = ranges$y, expand = TRUE)
  })
  
  cf_layers <- eventReactive(list(input$calibration_factors, input$query), {
    v = vol()
    limits = v %>% filter(!is.na(daily_volume))
    min_date <- min(limits$date)
    max_date <- max(limits$date)
    cf = corr()
    layers <- list(
      geom_vline(
        #add a jitter to calibration factors since they occur on the same date.
        data = cf, #%>% mutate(factor_start = factor_start + sample(rnorm(1,7), n(), replace = TRUE)),
        aes(xintercept=factor_start, color = flow_color),
        linewidth = 1.2, linetype = 'dotted'),
      geom_label_repel(
        data = cf,
        aes(x = coalesce(factor_start, min_date)+3, y = max(limits$daily_volume)-50,
            color = flow_color, label = paste("CF: ", calibration_factor)))
    )
  })
  
  #a base part of the plot which doesn't change except when site_id changes (improves render time)
  base_plot <- eventReactive(list(input$query, input$validated_only),  {
    v = vol()
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
    
    layers <- list(
      theme_bw(),
      geom_path(data = v, aes(
        x=date, y=daily_volume, color = flow_color, group = paste(site_id, flow_color, groupid)
      ), linewidth = 0.2, alpha = 0.5),
      geom_line(data = v, aes(
        x=date, y=rolling_avg_1_week, linewidth = "7 day avg",
        color = flow_color, group = paste(site_id, flow_color, groupid)), linewidth = 1),
      scale_x_date(date_breaks = break_major,
                   date_minor_breaks = break_minor,
                   date_labels = "%Y-%m-%d", limits = c(min_date-ddays(20), max_date+ddays(20))),
      theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
            text = element_text(size = 20)),
      guides(
        alpha="none",
        linewidth=guide_legend(title=NULL),
        color=guide_legend(title="Flow"),
        fill=guide_legend(title="Anomalous Range"))
    )
    
    return(layers)
  })
  
  #dynamically label brush extents
  brush_labels <- eventReactive(input$brush, {
    brush=input$brush
    if (!is.null(brush)) {
      brush_dates <- c(as.Date(brush$xmin), as.Date(brush$xmax))
      v = vol() %>% filter(date %in% brush_dates)
      layers <- list(
        geom_point(data = v, aes(x=date, y=daily_volume, color = flow_color), size = 2),
        geom_label_repel(data = v, aes(x=date, y=daily_volume, label=daily_volume, color = flow_color))
      )
    }
    return(layers)
  })
  
  #dynamically draw anomalous ranges when they change
  ar_layers <- eventReactive(ar_listen(), {
    ars = ar_data()
    limits = vol() %>% filter(!is.na(daily_volume))
    min_date <- min(limits$date)
    max_date <- max(limits$date)
    ars <- ars %>% rowwise() %>% mutate(x_plot = mean.Date(c(coalesce(upper, max_date), coalesce(lower, min_date))))
    
    layers = list(
      geom_rect(data = ars,
                aes(
                  fill=problem_level,
                  xmin=coalesce(lower, min_date),
                  xmax=coalesce(upper, max_date),
                  ymin = 0,
                  ymax = max(limits$daily_volume)
                ), alpha = 0.5),
      geom_text(data = ars, aes(
        x = x_plot,
        y = max(limits$daily_volume),
        label = stringr::str_wrap(notes, 35),
        hjust = 0,
        vjust = 1))
    )
    return(layers)
  })
  
  # Save ranges to a CSV file when button is clicked
  observeEvent(input$export_csv, {
    fname = paste0("anomalous_ranges_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".txt")
    temp <- values$tab %>%
      mutate(time_range = paste0("[", range_start, ", ", range_end, ")")) %>%
      select(-any_of(c("Remove", "id", "range_start", "range_end")))
    
    comma_sep <- function(x) paste0("('", paste0(x, collapse = "', '"), sep = "')")
    
    lines <- c(
      paste0("INSERT INTO ecocounter.anomalous_ranges (", paste(colnames(temp), collapse = ', '), ") (VALUES "),
      paste0(apply(temp, 1, comma_sep), collapse = ',\n'),
      ') RETURNING ecocounter.anomalous_ranges.*'
    )
    write_lines(lines, file.path(export_path, fname))
    
    showModal(modalDialog(
      title = "Ranges Saved to: ",
      paste0("Your selected ranges have been saved to:", file.path(export_path, fname)),
      easyClose = TRUE
    ))
  })
}

# Run the application 
shinyApp(ui = ui, server = server)