#' @title InvestingCom Class
#'
#' @description
#' Get data data from investing.com.
#'
#' @export
InvestingCom = R6::R6Class(
  "InvestingCom",
  inherit = DataAbstract,

  public = list(

    #' @description
    #' Create a new FMP object.
    #'
    #' @param azure_storage_endpoint Azure storate endpont
    #' @param context_with_config AWS S3 Tiledb config
    #'
    #' @return A new `FMP` object.
    initialize = function(azure_storage_endpoint = NULL,
                          context_with_config = NULL) {

      # endpoint
      super$initialize(azure_storage_endpoint, context_with_config)
    },

    #' @description Get complete earnings calendar from investing.com.
    #'
    #' @param start_date Start date
    #'
    #' @return Get investing com ea data.
    get_investingcom_earnings_calendar_bulk = function(start_date) {

      # solve No visible binding for global variable
      datetime <- id <- revenue <- revenue_factor <- revenue_forecast <- revenue_forecast_factor <-
        market_cap <- market_cap_factor <- right_time <- symbol <- name <- eps <- eps_forecast <- `.` <- NULL

      # scrap investing.com
      start_dates <- seq.Date(as.Date(start_date), Sys.Date(), 1)
      end_dates <- start_dates + 1

      # scrap all events
      ea_list <- lapply(seq_along(start_dates), function(i) {
        Sys.sleep(1L)
        print(i)
        self$get_investingcom_earnings_calendar(start_dates[i], end_dates[i])
      })
      ea <- rbindlist(ea_list, fill = TRUE)

      # clean table
      DT <- as.data.table(ea)
      setnames(DT, c("id", "name", "eps", "eps_forecast", "revenue", "revenue_forecast", "market_cap", "right_time"))
      DT[, datetime := as.POSIXct(gsub("EarningsCal-\\d+-", "", id), format = "%Y-%m-%d-%H%M%S")]
      DT[, id := str_extract(id, "\\d+")]
      DT <- DT[, lapply(.SD, function(x) str_trim(gsub("/|--", "", x)))]
      DT <- DT[, (c("eps", "eps_forecast")) := lapply(.SD, as.numeric), .SDcols = c("eps", "eps_forecast")]
      setorder(DT, "datetime")
      DT[grep("B", revenue), revenue_factor := 1000000000]
      DT[grep("B", revenue_forecast), revenue_forecast_factor := 1000000000]
      DT[grep("B", market_cap), market_cap_factor := 1000000000]
      DT[grep("M", revenue), revenue_factor := 1000000]
      DT[grep("M", revenue_forecast), revenue_forecast_factor := 1000000]
      DT[grep("M", market_cap), market_cap_factor := 1000000]
      DT[grep("K", revenue), revenue_factor := 1000]
      DT[grep("K", revenue_forecast), revenue_forecast_factor := 1000]
      DT[grep("K", market_cap), market_cap_factor := 1000]
      DT[, revenue := as.numeric(gsub("[A-z]+", "", revenue)) * revenue_factor]
      DT[, revenue_forecast := as.numeric(gsub("[A-z]+", "", revenue_forecast)) * revenue_forecast_factor]
      DT[, market_cap := as.numeric(gsub("[A-z]+", "", market_cap)) * market_cap_factor]
      DT[, `:=`(revenue_factor = NULL, revenue_forecast_factor = NULL, market_cap_factor = NULL)]
      DT[, right_time := gsub("genToolTip oneliner reverseToolTip", "", right_time)]
      DT[right_time == "", right_time := NA]
      DT[, symbol := gsub(".*\\(|\\)", "", name)]
      DT <- unique(DT)
      DT <- DT[, .(id, symbol, datetime, name, eps, eps_forecast, revenue, revenue_forecast, market_cap, right_time)]

      return(DT)
    },


    #' @description Get Earnings Calendar from investing.com for specified dates
    #'
    #' @param date_from Start date
    #' @param date_to End date
    #'
    #' @return Request to investing.com calendar
    get_investingcom_earnings_calendar = function(date_from, date_to) {

      # solve No visible binding for global variable
      . <- NULL

      # make POST request to invest.com earings calendar
      # print("POST request to investing.com")
      p <- RETRY("POST",
        url = "https://www.investing.com/earnings-calendar/Service/getCalendarFilteredData",
        body = list(
          "country[]" = "5",
          "dateFrom" = date_from,
          "dateTo" = date_to,
          "currentTab" = "custom",
          "limit_from" = "0"
        ),
        add_headers(
          "Content-Type" = "application/x-www-form-urlencoded",
          "Referer" = "https://www.investing.com/earnings-calendar/",
          "User-Agent" = "Mozilla/5.0",
          "X-Requested-With" = "XMLHttpRequest"
        ),
        encode = "form",
        times = 5L
      )

      # parse content
      # print("PArse content.")
      json <- content(p, type = "application/json")
      adjusted_html <- paste0("<table>", json$data, "<table>")
      edata <- rvest::read_html(adjusted_html)

      id <- as.vector(fromJSON(json$pairsToRegister))
      id <- gsub(":", "", id[1:(length(id) - 1)]  )

      # next if table is empty
      if (all(id == "domain-1")) {
        return(NULL)
      }

      # extract releasing time
      # print("Extract time")
      right_time <- edata %>%
        html_elements("td[class*='right time']") %>%
        html_element("span") %>%
        html_attr("class")

      # extract table
      # print("Extract table")
      tbl <- edata %>%
        html_element("table") %>%
        html_table()
      tbl[tbl == ""] <- NA
      tbl <- tbl[-which(apply(tbl, 1, function(x) length(unique(x)) == 1)), ]
      tbl <- tbl[, colSums(is.na(tbl)) < nrow(tbl)]

      # merge table and right time
      df <- cbind.data.frame(id = id, tbl, right_time = right_time)

      return(df)
    },

    #' @description Update earnings announcements data from investingcom website
    #'
    #' @param uri TileDB uri argument
    #' @param start_date First date to scrape from.If NULL, takes last date from existing uri.
    #' @param consolidate Consolidate and vacuum at the end.
    #'
    #' @return Get and update ea data from investingcom,
    update_investingcom_earnings = function(uri, start_date = NULL, consolidate = TRUE) {

      # debug
      # library(findata)
      # library(data.table)
      # library(httr)
      # library(RcppQuantuccia)
      # library(tiledb)
      # library(lubridate)
      # library(nanotime)
      # self = InvestingCom$new()
      # uri = "s3://equity-usa-earningsevents-investingcom"

      # check if uri exists
      bucket_check <- tryCatch(tiledb_object_ls(uri), error = function(e) e)
      if (length(bucket_check) < 2 && grepl("bucket does not exist", bucket_check)) {
        stop("Bucket does not exist. Craete s3 bucket with uri name.")
      }

      # define start date
      if (is.null(start_date)) {
        if (tiledb_object_type(uri) == "ARRAY") {
          arr <- tiledb_array(uri,
                              as.data.frame = TRUE,
                              selected_ranges = list(time = cbind(as.POSIXct(Sys.Date() - 10), Sys.time())))
          dt_old <- arr[]
          start_date <- min(dt_old$time, na.rm = TRUE)
        } else {
          start_date <- as.Date("2014-01-01")
        }
      }

      # get new data
      dt <- self$get_investingcom_earnings_calendar_bulk(start_date)

      # check if there are data available for timespan
      if (nrow(dt) == 0) {
        print("No data for earning announcements.")
        return(NULL)
      }

      # clean data
      dt <- unique(dt)
      setorder(dt, "datetime")
      setnames(dt, "datetime", "time")
      dt[, time := as.POSIXct(time, tz = "America/New_York")]
      dt[, time := with_tz(time, tzone = "UTC")]
      dt <- unique(dt, by = c("symbol", "time"))

      # save to AWS S3
      if (tiledb_object_type(uri) != "ARRAY") {
        fromDataFrame(
          obj = dt,
          uri = uri,
          col_index = c("symbol", "time"),
          sparse = TRUE,
          tile_domain=list(time=c(as.POSIXct("1970-01-01 00:00:00", tz = "UTC"),
                                  as.POSIXct("2099-12-31 23:59:59", tz = "UTC"))),
          allows_dups = FALSE
        )
      } else {
        # save to tiledb
        arr <- tiledb_array(uri, as.data.frame = TRUE)
        arr[] <- dt
        tiledb_array_close(arr)
      }

      # consolidate
      if (consolidate) {
        tiledb:::libtiledb_array_consolidate(ctx = self$context_with_config@ptr, uri = uri)
        tiledb:::libtiledb_array_vacuum(ctx = self$context_with_config@ptr, uri = uri)
      }
    }
  )
)
