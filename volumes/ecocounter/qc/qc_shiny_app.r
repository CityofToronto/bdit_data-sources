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

setwd('C:\\Users\\gwolofs\\OneDrive - City of Toronto\\Documents\\R')

dw <- config::get("bigdata")

con <- DBI::dbConnect(RPostgres::Postgres(),
                      host = dw$host,
                      dbname = dw$database,
                      user = dw$user,
                      password = dw$pwd
)

sites = tbl(con, sql("SELECT * FROM ecocounter.sites")) %>% collect()

sites_list <- sites$site_id
names(sites_list) <- sites$site_description

# Define UI for the app
ui <- fluidPage(
  titlePanel("Interactive Range Selection"),
  fluidRow(selectInput("site_id", "Select Site ID", choices = sites_list, selected = sites_list[1])),
  fluidRow(
    plotOutput("plot",
              dblclick = "plot_dblclick",
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
      notes = character()
    ) %>%
    mutate(Remove = getRemoveButton(id, idS = "", lab = "Tab1"))
  
  buttonCounter <- 0L
  
  proxyTable <- DT::dataTableProxy("tab")
  
  # Render table with delete buttons
  output$myTable <- DT::renderDataTable({
    DT::datatable(values$tab,
                  options = list(dom = "t"),
                  escape   = FALSE,
                  editable = TRUE)
  })
  
  # Single zoomable plot (on left)
  ranges <- reactiveValues(x = NULL, y = NULL)
  
  # Clear brush, reset zoom when site_id changes or range is saved.
  observeEvent(list(input$site_id, input$save_range), priority = -1, {
    session$resetBrush("brush")
    ranges$x <- NULL
    ranges$y <- NULL
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
  vol <- eventReactive(input$site_id, {
    # Your code to prepare the data for plotting
    s=input$site_id
    volumes = tbl(con, sql(sprintf("
        SELECT site_id, flow_id, date, daily_volume, rolling_avg_1_week, flow_color
        FROM gwolofs.ecocounter_graph_volumes(%s)
                       ", s))) %>% collect()

    return(volumes)
  })
  
  #anomalous ranges data, refreshes on site_id
  ars <- eventReactive(input$site_id, {
    # Your code to prepare the data for plotting
    s=input$site_id
    
    anomalous_ranges = tbl(con, sql("
        SELECT
          site_id,
          lower(time_range)::date AS lower,
          upper(time_range)::date AS upper,
          CASE WHEN flow_id IS NULL THEN 'site_id - ' || site_id
          ELSE 'flow_id - ' || flow_id END || ': ' || notes AS notes,
          problem_level
        FROM ecocounter.anomalous_ranges
    ")) %>% filter(site_id == s) %>% collect()
    return(anomalous_ranges)
  })
  
  #correction factors, refreshes on site_id
  corr <- eventReactive(input$site_id, {
    # Your code to prepare the data for plotting
    s=input$site_id
    
    correction_factors = tbl(con, sql("
        SELECT
          flow_id,
          site_id,
          flow_direction,
          ecocounter_day_corr_factor AS calibration_factor,
          LOWER(factor_range) AS factor_start,
          UPPER(factor_range) AS factor_end,
          f.flow_direction || ' - ' || f.flow_id AS flow_color
        FROM ecocounter.correction_factors
        JOIN ecocounter.flows_unfiltered AS f USING (flow_id)
      ")) %>% filter(site_id == s) %>% collect()
      return(correction_factors)
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
    buttonCounter <<- buttonCounter + 1L
    myTable <- isolate(values$tab)
    
    new_range <- data.frame(
      id = buttonCounter,
      site_id = as.numeric(input$site_id),
      flow_id = 0L,
      range_start = as.Date(input$brush$xmin),
      range_end = as.Date(input$brush$xmax),
      notes = ""
    )
    
    myTable <- bind_rows(
        myTable,
        new_range %>%
          mutate(Remove = getRemoveButton(buttonCounter, idS = "", lab = "Tab1")))
      replaceData(proxyTable, myTable, resetPaging = FALSE)
      values$tab <- myTable
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
    list(input$myTable_cell_edit, input$save_range, input$site_id)
  })
  
  ar_data <- eventReactive(ar_listen(), {
    # Your code to prepare the data for plotting
    s=input$site_id
    temp_ars = values$tab %>% filter(site_id == s) %>%
      transmute(site_id, lower = range_start, upper = range_end, notes, problem_level = "Draft")
    
    return(rbind(ars(), temp_ars))
  })
  
  
  #coords <- eventReactive(list(), {})
  
  output$plot <- renderPlot({
    #this part of the plot only changes when site id changes
    p <- ggplot() + base_plot()
    
    #changes when brush changes
    if (!is.null(input$brush)) {
      p <- p + brush_labels()
    }
    
    #changes when anomalous ranges change
    if (ar_data() %>% count() > 0){
      p <- p + ar_layers()
    }
    
    p +
      #reactive to zoom
      coord_cartesian(xlim = ranges$x, ylim = ranges$y, expand = TRUE)
  })
  
  #a base part of the plot which doesn't change except when site_id changes (improves render time)
  base_plot <- eventReactive(input$site_id, {
    s=input$site_id
    site_name = names(which(sites_list==s))
    v = vol()
    cf = corr()
    limits = v %>% filter(!is.na(daily_volume))
    min_date <- min(limits$date)
    max_date <- max(limits$date)
    
    layers <- list(
      theme_bw(),
      geom_path(data = v, aes(
        x=date, y=daily_volume,
        color = flow_color), linewidth = 0.2, alpha = 0.5),
      geom_line(data = v, aes(
        x=date, y=rolling_avg_1_week, linewidth = "7 day avg",
        color = flow_color), linewidth = 1),
      geom_vline(
        #add a jitter to correction factors since they occur on the same date.
        data = cf, #%>% mutate(factor_start = factor_start + sample(rnorm(1,7), n(), replace = TRUE)),
        aes(xintercept=factor_start, color = flow_color),
        linewidth = 1.2, linetype = 'dotted'),
      geom_label_repel(
        data = cf,
        aes(x = coalesce(factor_start, min_date)+3, y = max(limits$daily_volume)-50,
        color = flow_color, label = paste("CF: ", calibration_factor))),
      ggtitle(label = paste(site_name, "(site_id:", s, ')')),
      scale_x_date(date_breaks = "1 month", date_minor_breaks = "1 week",
                  date_labels = "%Y-%m-%d", limits = c(min_date-20, max_date+20)),
      theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)),
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
        x = mean.Date(c(coalesce(upper, max_date), coalesce(lower, min_date))),
        y = max(limits$daily_volume),
        label = stringr::str_wrap(notes, 35),
        hjust = 0,
        vjust = 1))
    )
    return(layers)
  })
  
  # Save ranges to a CSV file when button is clicked
  observeEvent(input$export_csv, {
    fname = paste0("anomalous_ranges_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv")
    write.csv(values$tab %>% select(-any_of(c("Remove", "id"))), file = fname, row.names = FALSE)
    showModal(modalDialog(
      title = "Ranges Saved to: ",
      paste0("Your selected ranges have been saved to:", fname),
      easyClose = TRUE
    ))
  })
}

# Run the application 
shinyApp(ui = ui, server = server)