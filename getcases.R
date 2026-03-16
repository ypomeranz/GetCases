#!/usr/bin/env Rscript

# CourtListener GUI (R / Shiny)
# A lightweight GUI for searching CourtListener opinions and downloading PDFs.
#
# Required packages:
#   install.packages(c("shiny", "httr2", "jsonlite", "DT"))
#
# Run:
#   shiny::runApp("courtlistener_gui.R")

suppressPackageStartupMessages({
  library(shiny)
  library(httr2)
  library(jsonlite)
  library(DT)
})

BASE_URL <- "https://www.courtlistener.com/api/rest/v4/"
CONFIG_PATH <- path.expand("~/.config/courtlistener/config.json")
STORAGE_BASE <- "https://storage.courtlistener.com/"
LOC_CUTOFF <- 542L

COURTS <- c(
  "(any)", "scotus", "ca1", "ca2", "ca3", "ca4", "ca5", "ca6", "ca7",
  "ca8", "ca9", "ca10", "ca11", "cadc", "cafc"
)

load_saved_token <- function() {
  if (!file.exists(CONFIG_PATH)) return("")
  out <- tryCatch(fromJSON(CONFIG_PATH), error = function(e) list())
  out$api_token %||% ""
}

save_token <- function(token) {
  tryCatch({
    dir.create(dirname(CONFIG_PATH), recursive = TRUE, showWarnings = FALSE)
    write_json(list(api_token = token), CONFIG_PATH, auto_unbox = TRUE, pretty = TRUE)
  }, error = function(e) NULL)
}

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0 || is.na(x)) y else x

us_reports_loc_url <- function(citation) {
  if (is.null(citation) || !nzchar(citation)) return(NULL)
  m <- regexec("(\\d+)\\s+U\\.S\\.\\s+(\\d+)", citation)
  hits <- regmatches(citation, m)[[1]]
  if (length(hits) != 3) return(NULL)
  vol <- as.integer(hits[2])
  page <- as.integer(hits[3])
  if (is.na(vol) || is.na(page) || vol > LOC_CUTOFF) return(NULL)
  sprintf(
    "https://cdn.loc.gov/service/ll/usrep/usrep%03d/usrep%03d%03d/usrep%03d%03d.pdf",
    vol, vol, page, vol, page
  )
}

api_get <- function(token, endpoint_or_url, query = list()) {
  url <- if (grepl("^https?://", endpoint_or_url)) endpoint_or_url else paste0(BASE_URL, endpoint_or_url)
  req <- request(url) |> req_headers(Authorization = paste("Token", token)) |> req_url_query(!!!query)
  resp <- req_perform(req)
  if (resp_status(resp) >= 400) {
    stop(resp_body_string(resp))
  }
  resp_body_json(resp, simplifyVector = TRUE)
}

search_cases <- function(token, query, court = NULL, filed_after = NULL, filed_before = NULL, page_size = 20L) {
  params <- list(
    q = query,
    type = "o",
    highlight = "on",
    court = court,
    filed_after = filed_after,
    filed_before = filed_before,
    page_size = as.integer(page_size)
  )
  params <- params[!vapply(params, function(x) is.null(x) || identical(x, ""), logical(1))]
  api_get(token, "search/", params)
}

clean_text <- function(x) {
  if (is.null(x) || !nzchar(x)) return("")
  gsub("<[^>]+>", "", x)
}

pick_main_opinion <- function(opinions) {
  if (is.null(opinions) || length(opinions) == 0) return(NULL)
  if (is.data.frame(opinions)) opinions <- split(opinions, seq_len(nrow(opinions)))
  counts <- vapply(opinions, function(op) length(op$cites %||% list()), numeric(1))
  opinions[[which.max(counts)]]
}

format_row <- function(item) {
  citations <- item$citation %||% character()
  citation_str <- ""
  if (length(citations) > 0) {
    us_idx <- grep(" U\\.S\\. ", citations)
    citation_str <- if (length(us_idx)) citations[us_idx[1]] else citations[1]
  }
  list(
    case_name = item$caseName %||% item$case_name %||% "(unknown)",
    court = item$court %||% item$court_id %||% "",
    date_filed = item$dateFiled %||% item$date_filed %||% "",
    citation = citation_str,
    status = item$status %||% item$precedentialStatus %||% ""
  )
}

resolve_pdf_url <- function(token, item) {
  citations <- item$citation %||% character()
  us_cite <- if (length(citations)) {
    idx <- grep(" U\\.S\\. ", citations)
    if (length(idx)) citations[idx[1]] else NULL
  } else NULL

  if (!is.null(us_cite)) {
    loc_url <- us_reports_loc_url(us_cite)
    if (!is.null(loc_url)) return(loc_url)
  }

  local <- item$local_path %||% item$localPath %||% ""
  if (nzchar(local)) return(paste0(STORAGE_BASE, sub("^/", "", local)))

  fetched_op <- NULL
  op_id <- item$id %||% NULL
  if (!is.null(op_id)) {
    fetched_op <- tryCatch(api_get(token, sprintf("opinions/%s/", op_id)), error = function(e) NULL)
    if (!is.null(fetched_op)) {
      local <- fetched_op$local_path %||% ""
      if (nzchar(local)) return(paste0(STORAGE_BASE, sub("^/", "", local)))
    }
  }

  dl <- item$download_url %||% ""
  if (nzchar(dl)) return(dl)

  if (!is.null(fetched_op)) {
    dl <- fetched_op$download_url %||% ""
    if (nzchar(dl)) return(dl)
  }

  cluster_id <- item$cluster_id %||% item$id %||% NULL
  if (!is.null(cluster_id)) {
    cluster <- tryCatch(api_get(token, sprintf("clusters/%s/", cluster_id)), error = function(e) NULL)
    subs <- cluster$sub_opinions %||% list()
    for (u in subs) {
      op <- tryCatch(api_get(token, u, list(fields = "download_url,local_path")), error = function(e) NULL)
      if (is.null(op)) next
      local <- op$local_path %||% ""
      if (nzchar(local)) return(paste0(STORAGE_BASE, sub("^/", "", local)))
      dl <- op$download_url %||% ""
      if (nzchar(dl)) return(dl)
    }
  }

  NULL
}

ui <- fluidPage(
  titlePanel("CourtListener Case Law Search (R)"),
  sidebarLayout(
    sidebarPanel(
      passwordInput("token", "API Token", value = Sys.getenv("COURTLISTENER_TOKEN", load_saved_token())),
      textInput("query", "Query"),
      selectInput("court", "Court", choices = COURTS, selected = "(any)"),
      dateInput("date_from", "Filed from", value = NA),
      dateInput("date_to", "Filed to", value = NA),
      numericInput("page_size", "Max results", value = 20, min = 5, max = 20, step = 1),
      actionButton("search", "Search", class = "btn-primary"),
      br(), br(),
      downloadButton("download_pdf", "Download PDF")
    ),
    mainPanel(
      tags$h4("Results"),
      DTOutput("main_table"),
      tags$h5("Orders (SCOTUS cases with <= 2 citations)"),
      DTOutput("orders_table"),
      tags$h4("Preview"),
      verbatimTextOutput("preview"),
      tags$hr(),
      textOutput("status")
    )
  )
)

server <- function(input, output, session) {
  rv <- reactiveValues(
    results = list(),
    main_idx = integer(),
    orders_idx = integer(),
    selected_idx = NULL,
    preview = list(),
    status = "Enter a query and click Search."
  )

  observeEvent(input$search, {
    token <- trimws(input$token)
    if (!nzchar(token)) {
      rv$status <- "Missing token."
      showNotification("Please provide a CourtListener API token.", type = "error")
      return()
    }
    save_token(token)

    court <- if (identical(input$court, "(any)")) NULL else input$court
    filed_after <- if (is.na(input$date_from)) NULL else as.character(input$date_from)
    filed_before <- if (is.na(input$date_to)) NULL else as.character(input$date_to)
    q <- trimws(input$query)

    if (!nzchar(q)) {
      rv$status <- "Empty query."
      showNotification("Please enter a search query.", type = "warning")
      return()
    }

    rv$status <- "Searching..."
    data <- tryCatch(
      search_cases(token, q, court, filed_after, filed_before, input$page_size),
      error = function(e) e
    )

    if (inherits(data, "error")) {
      rv$status <- paste("Error:", conditionMessage(data))
      showNotification(rv$status, type = "error")
      return()
    }

    results <- data$results %||% list()
    rv$results <- results
    rv$preview <- list()
    rv$selected_idx <- NULL

    main_rows <- list(); orders_rows <- list()
    main_idx <- integer(); orders_idx <- integer()

    for (i in seq_along(results)) {
      item <- results[[i]]
      op <- pick_main_opinion(item$opinions %||% list())
      snippet <- clean_text(op$snippet %||% "")
      if (nzchar(snippet)) rv$preview[[as.character(i)]] <- snippet

      row <- format_row(item)
      is_scotus <- grepl("scotus", item$court_id %||% "", ignore.case = TRUE)
      cites_count <- length(op$cites %||% list())
      if (is_scotus && cites_count <= 2) {
        orders_rows[[length(orders_rows) + 1]] <- row
        orders_idx <- c(orders_idx, i)
      } else {
        main_rows[[length(main_rows) + 1]] <- row
        main_idx <- c(main_idx, i)
      }
    }

    to_df <- function(rows) {
      if (!length(rows)) {
        data.frame(case_name = character(), court = character(), date_filed = character(), citation = character(), status = character())
      } else {
        do.call(rbind.data.frame, c(rows, stringsAsFactors = FALSE))
      }
    }

    rv$main_idx <- main_idx
    rv$orders_idx <- orders_idx
    output$main_table <- renderDT(datatable(to_df(main_rows), selection = "single", options = list(pageLength = 8)))
    output$orders_table <- renderDT(datatable(to_df(orders_rows), selection = "single", options = list(pageLength = 4)))

    rv$status <- sprintf("Showing %d of %s results.", length(results), format(data$count %||% length(results), big.mark = ","))
  })

  observeEvent(input$main_table_rows_selected, {
    sel <- input$main_table_rows_selected
    if (length(sel)) rv$selected_idx <- rv$main_idx[sel[1]]
  })

  observeEvent(input$orders_table_rows_selected, {
    sel <- input$orders_table_rows_selected
    if (length(sel)) rv$selected_idx <- rv$orders_idx[sel[1]]
  })

  output$preview <- renderText({
    idx <- rv$selected_idx
    if (is.null(idx)) return("(No preview available — select a row)")
    rv$preview[[as.character(idx)]] %||% "(No preview available — download PDF for full opinion)"
  })

  output$status <- renderText(rv$status)

  output$download_pdf <- downloadHandler(
    filename = function() {
      idx <- rv$selected_idx
      if (is.null(idx)) return("opinion.pdf")
      item <- rv$results[[idx]]
      case_name <- item$caseName %||% item$case_name %||% "opinion"
      safe <- gsub("[^A-Za-z0-9 _-]", "_", case_name)
      paste0(substr(trimws(safe), 1, 80), ".pdf")
    },
    content = function(file) {
      idx <- rv$selected_idx
      if (is.null(idx)) stop("No case selected.")
      token <- trimws(input$token)
      item <- rv$results[[idx]]
      url <- resolve_pdf_url(token, item)
      if (is.null(url) || !nzchar(url)) stop("No downloadable PDF found for this opinion.")

      req <- request(url) |> req_headers(Authorization = paste("Token", token))
      resp <- req_perform(req)
      if (resp_status(resp) >= 400) stop(sprintf("Download failed with HTTP %s", resp_status(resp)))
      writeBin(resp_body_raw(resp), file)
      rv$status <- paste("Saved:", file)
    }
  )
}

shinyApp(ui, server)