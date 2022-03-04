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
    #' @return A new `InvestingCom` object.
    initialize = function() {
      print("Good")
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
      print("POST request to investing.com")
      p <- POST(
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
        encode = "form"
      )

      # parse content
      print("PArse content.")
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
      print("Extract time")
      right_time <- edata %>%
        html_elements("td[class*='right time']") %>%
        html_element("span") %>%
        html_attr("class")

      # extract table
      print("Extract table")
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
    #' @param days_history how long to the past to look
    #'
    #' @return Get and update ea data from investingcom,
    update_investingcom_earnings = function(days_history = 10) {


      # read old file if exists
      cont <- storage_container(private$azure_storage_endpoint, "investingcom")
      blob_files <- list_blobs(cont)
      # "EarningAnnouncementsInvestingCom.csv" %in% blob_files$name
      if (private$ea_file_name %in% blob_files$name) {
        history <- storage_read_csv(cont, private$ea_file_name) # history <- storage_read_csv(cont, "EarningAnnouncementsInvestingCom.csv")
        history$datetime <- as.POSIXct(history$datetime, tz = "UTC")
        attr(history$datetime, "tzone") <- "EST"
        history <- as.data.table(history)
        history <- history[as.Date(datetime) < (Sys.Date() - days_history)] # there can be changes in the data in the last 10 days? Connservative approach.
        date_ <- max(as.Date(history$datetime)) + 1
      } else {
        date_ <- as.Date("2014-01-01")
      }

      # get new data
      new <- self$get_investingcom_earnings_calendar_bulk(date_) # new <- get_investingcom_earnings_calendar_bulk(date_)

      # check if there are data available for timespan
      if (nrow(new) == 0) {
        print("No data for earning announcements.")
        return(NULL)
      }

      # clean data
      if (private$ea_file_name %in% blob_files$name) {
        new$datetime <- as.POSIXct(new$datetime, tz = "EST")
        results <- rbind(history, new)
      } else {
        results <- copy(new)
      }
      results <- unique(results)
      setorder(results, "datetime")

      # save file to Azure blob if blob_file is not NA
      print(paste0("Data saved to blob file ", private$ea_file_name))
      super$save_blob_files(results, file_name = private$ea_file_name, container = "investingcom")
      print(paste0("Data saved to blob file ", private$ea_file_name))

      # save file to Azure blob if blob_file is not NA
      # if (!is.na(blob_file)) {
      #   save_blob_files(events_new, file_name = blob_file, container = "fundamentals")
      #   print(paste0("Data saved to blob file ", blob_file))
      # }

      return(new)
    }
  ),
  private = list(
    ea_file_name = "EarningAnnouncementsInvestingCom.csv"
  )
)
