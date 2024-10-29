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
flows = tbl(con, sql("SELECT * FROM ecocounter.flows")) %>% collect()

sites_list <- sites$site_id
names(sites_list) <- sites$site_description


# Define UI for the app
ui <- fluidPage(
  titlePanel("Interactive Range Selection"),
  fluidRow(selectInput("site_id", "Select Site ID", choices = sites_list, selected = sites_list[1])),
  fluidRow(
    plotOutput("plot", brush = brushOpts(id = "brush"))
  ),
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
  
  vol <- eventReactive(input$site_id, {
    # Your code to prepare the data for plotting
    s=input$site_id
    volumes = tbl(con, sql(sprintf("
        SELECT site_id, flow_id, date, daily_volume, rolling_avg_1_week, flow_color
        FROM gwolofs.ecocounter_graph_volumes(%s)
                       ", s))) %>% collect()

    return(volumes)
  })
  
  
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
  
  toListen <- reactive({
    list(input$site_id, input$save_range)
  })
    
  # Clear brush when site_id changes
  observeEvent(toListen(), {
    session$resetBrush("brush")
  })
  
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
  
  observeEvent(input$myTable_cell_edit, {
    
    myTable <- values$tab
    row  <- input$myTable_cell_edit$row
    clmn <- input$myTable_cell_edit$col
    myTable[row, clmn] <- input$myTable_cell_edit$value
    replaceData(proxyTable, myTable, resetPaging = FALSE)
    values$tab <- myTable
    
  })
  
  ar_data <- eventReactive(list(input$myTable_cell_edit, input$save_range, input$site_id), {
    # Your code to prepare the data for plotting
    s=input$site_id
    temp_ars = values$tab %>% filter(site_id == s) %>%
      transmute(site_id, lower = range_start, upper = range_end, notes, problem_level = "Draft")
    
    return(rbind(ars(), temp_ars))
  })
  
  # Render plot
  output$plot <- renderPlot({
    s=input$site_id
    site_name = names(which(sites_list==s))
    dt = ar_data()
    v = vol()
    cf = corr()
    limits = v %>% filter(!is.na(daily_volume))
    
    ggplot() +
      theme_bw()+
      geom_rect(data = dt,
        aes(
          fill=problem_level,
          xmin=coalesce(lower, min(limits$date)),
          xmax=coalesce(upper, max(limits$date)),
          ymin = 0,
          ymax = max(limits$daily_volume)
      ), alpha = 0.5) +
      geom_path(data = v, aes(
        x=date, y=daily_volume,
        color = flow_color), linewidth = 0.2, alpha = 0.5) +
      geom_line(data = v, aes(
        x=date, y=rolling_avg_1_week, linewidth = "7 day avg",
        color = flow_color), linewidth = 1) +
      geom_vline(data = cf %>% mutate(factor_start = factor_start + sample(rnorm(1,7), n(), replace = TRUE)), aes(
        xintercept=factor_start,
        color = flow_color
      ),
      linewidth = 1.2,
      linetype = 'dotted') + 
      geom_text(data = dt, aes(
        x = mean.Date(c(coalesce(upper, max(limits$date)), coalesce(lower, min(limits$date)))),
        y = max(limits$daily_volume),
        label = stringr::str_wrap(notes, 35),
        hjust = 0,
        vjust = 1)) +
      geom_label_repel(data = cf, aes(
        x = coalesce(factor_start, min(limits$date))+3,
        y = max(limits$daily_volume)-50,
        color = flow_color,
        label = paste("CF: ", calibration_factor))) +
      ggtitle(label = paste(site_name, "(site_id:", s, ')')) + 
      scale_x_date(date_breaks = "1 month", date_minor_breaks = "1 week",
                   date_labels = "%Y-%m-%d",
                   limits = c(min(limits$date)-20, max(limits$date)+20)) +
      theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))+
      guides(
        alpha=FALSE,
        linewidth=guide_legend(title=NULL),
        color=guide_legend(title="Flow"),
        fill=guide_legend(title="Anomalous Range"))
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
