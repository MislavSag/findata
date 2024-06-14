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
    #' Create a new InvestingCom object.
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
      start_dates <- seq.Date(as.Date(start_date), Sys.Date() + 14, 1)
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
    #' @param path to save output DT.
    #' @param start_date First date to scrape from.If NULL, takes last date from existing uri.
    #' @param update existing uri.
    #'
    #' @return Get and update ea data from investingcom,
    update_investingcom_earnings = function(path, start_date, update = TRUE) {

      # debug
      # library(findata)
      # library(data.table)
      # library(httr)
      # library(RcppQuantuccia)
      # library(arrow)
      # library(lubridate)
      # library(nanotime)
      # self = InvestingCom$new()
      # path = "F:/equity/usa/fundamentals/earning_announcements_investingcom.parquet"

      # help function
      clean_scraped = function(dt) {
        dt <- unique(dt)
        setorder(dt, "datetime")
        setnames(dt, "datetime", "time")
        dt[, time := as.POSIXct(time, tz = "America/New_York")]
        dt[, time := with_tz(time, tzone = "UTC")]
        dt <- unique(dt, by = c("symbol", "time"))
        
      }
      
      # get new data
      dt <- self$get_investingcom_earnings_calendar_bulk(start_date)
      
      # check if there are data available for timespan
      if (nrow(dt) == 0) {
        print("No data for earning announcements.")
        return(NULL)
      }
      
      # clean data
      dt = clean_scraped(dt)
      
      # get data
      if (update) {
        dt_old = read_parquet(path)
        dt = rbind(dt_old, dt)
      }

      # save to uri
      write_parquet(dt, path)
    }
  )
)
