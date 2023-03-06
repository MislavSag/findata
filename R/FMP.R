#' @title FMP Class
#'
#' @description
#' Get data data from FMP Cloud.
#'
#' @export
FMP = R6::R6Class(
  "FMP",
  inherit = DataAbstract,

  public = list(
    #' @field api_key API KEY for FMP Cloud
    api_key = NULL,

    #' @description
    #' Create a new FMP object.
    #'
    #' @param api_key API KEY for FMP cloud data.
    #' @param azure_storage_endpoint Azure storate endpont
    #' @param context_with_config AWS S3 Tiledb config
    #'
    #' @return A new `FMP` object.
    initialize = function(api_key = NULL,
                          azure_storage_endpoint = NULL,
                          context_with_config = NULL) {

      # endpoint
      super$initialize(azure_storage_endpoint, context_with_config)

      # check and define variables
      if (is.null(api_key)) {
        self$api_key = assert_character(Sys.getenv("APIKEY-FMPCLOUD"))
      } else {
        self$api_key = api_key
      }
    },

    #' @description
    #' Create a new FMP object.
    #'
    #' @param uri TileDB uri argument
    #' @param start_date First date to scrape from.If NULL, takes last date from existing uri.
    #' @param consolidate Consolidate and vacuum at the end.
    #'
    #' @return Earning announcements data.
    get_earning_announcements = function(uri, start_date = NULL, consolidate = TRUE) {

      # debug
      # library(findata)
      # library(data.table)
      # library(httr)
      # library(RcppQuantuccia)
      # library(tiledb)
      # library(lubridate)
      # library(nanotime)
      # self = FMP$new()
      # symbol = "CA"
      # uri = "s3://equity-usa-earningsevents"
      # save_uri_daily = "s3://equity-usa-daily-fmpcloud"
      # hardcode_start_dates = Sys.Date() - 1
      # hardcode_end_dates = Sys.Date() - 1
      # deep_scan = FALSE

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
                              selected_ranges = list(date = cbind(Sys.Date() - 10, Sys.Date())))
          dt_old <- arr[]
          start_date <- min(dt_old$date)
        } else {
          start_date <- as.Date("2010-01-01")
        }
      }
      end_date <- Sys.Date()

      # define listing dates
      dates_from <- seq.Date(start_date, end_date, by = 3)
      dates_to <- dates_from + 3

      # get data
      ea <- lapply(seq_along(dates_from), function(i) {
        private$fmpv_path("earning_calendar", from = dates_from[i], to = dates_to[i], apikey = self$api_key)
      })
      dt <- rbindlist(ea, fill = TRUE)

      # check if there are data available for timespan
      if (nrow(dt) == 0) {
        print("No data for earning announcements.")
        return(NULL)
      }

      # clean data
      dt <- unique(dt)
      dt[, date := as.Date(date)]
      dt[, fiscalDateEnding := as.Date(fiscalDateEnding)]
      dt[, updatedFromDate := as.Date(updatedFromDate)]

      # save to AWS S3
      if (tiledb_object_type(uri) != "ARRAY") {
        fromDataFrame(
          obj = dt,
          uri = uri,
          col_index = c("symbol", "date"),
          sparse = TRUE,
          tile_domain=list(date=cbind(as.Date("1970-01-01"),
                                      as.Date("2099-12-31"))),
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
    },

    #' @description Get Earning Call Transcript from FMP cloud.
    #'
    #' @param symbols stock symbols. If tiledburi, import symbols from FMP events.
    #' @param years years to extract data from.
    #' @param save_uri Tiledb uri.
    #'
    #' @return Result of GET request
    get_transcripts = function(symbols,
                               years = 1992:2023,
                               save_uri = "s3://equity-usa-transcripts") {

      # debug
      # library(data.table)
      # library(httr)
      # library(findata)
      # library(tiledb)
      # library(lubridate)
      # fmp = FMP$new()
      # private = list(
      #   ea_file_name = "EarningAnnouncements",
      #   transcripts_file_name = "earnings-calendar.rds",
      #   prices_file_name = "prices.csv",
      #   market_cap_file_name = "MarketCap",
      #
      #   fmpv_path = function(path = "earning_calendar", v = "v3", ...) {
      #
      #     # query params
      #     query_params <- list(...)
      #
      #     # define url
      #     url <- paste0("https://financialmodelingprep.com/api/", v, "/", path)
      #
      #     # get data
      #     p <- RETRY("GET", url, query = query_params, times = 5)
      #     result <- suppressWarnings(rbindlist(httr::content(p), fill = TRUE))
      #
      #     return(result)
      #   }
      # )
      # self = list()
      # self$api_key = Sys.getenv("APIKEY-FMPCLOUD")
      # years = 1992:2022
      # save_uri = "s3://equity-usa-transcripts"
      # arr <- tiledb_array("s3://equity-usa-earningsevents", as.data.frame = TRUE)
      # events <- arr[]
      # events <- as.data.table(events)
      # events <- events[date < Sys.Date()]                 # remove announcements for today
      # events <- unique(events, by = c("symbol", "date"))  # remove duplicated symobl / date pair
      # events <- na.omit(events, cols = c("eps"))          # remove rows with NA for earnings
      # url <- modify_url("https://financialmodelingprep.com/", path = "api/v3/available-traded/list",
      #                   query = list(apikey = Sys.getenv("APIKEY-FMPCLOUD") ))
      # stocks <- rbindlist(content(GET(url)))
      # usa_symbols <- stocks[exchangeShortName %in% c("AMEX", "NASDAQ", "NYSE", "OTC")]
      # events <- events[symbol %in% usa_symbols$symbo]
      # symbols <- unique(events$symbol)

      # import transcripts metadata for all symbols
      url <- "https://financialmodelingprep.com/api/v4/earning_call_transcript"
      print("Get all transcripts for symbols")
      all_transcripts_l <- lapply(symbols, function(x) {
        p <- GET(url, query = list(symbol = x, apikey = Sys.getenv("APIKEY-FMPCLOUD")))
        p <- rbindlist(content(p))
        if (nrow(p) > 0) return(cbind.data.frame(symbol = x, p)) else return(NULL)
      })
      all_transcripts <- rbindlist(all_transcripts_l)
      setnames(all_transcripts, c("symbol", "quarter", "year", "date"))
      all_transcripts[, date := as.POSIXct(date, tz = "America/New_York")]
      all_transcripts[, date := with_tz(date, tzone = "UTC")]

      # import existing data on transcripts
      if (tiledb_object_type(save_uri) == "ARRAY") {
        arr <- tiledb_array(save_uri,
                            as.data.frame = TRUE,
                            query_layout = "UNORDERED",
                            attrs = c("year")
        )
        transcripts_old_raw <- arr[]
        tiledb_array_close(arr)
        transcripts_old_raw <- as.data.table(transcripts_old_raw)

        # get new dates
        transcripts_new <- data.table::fsetdiff(all_transcripts[, .(symbol, date)], transcripts_old_raw[, .(symbol, date)])

        if (length(transcripts_new) > 0 && nrow(transcripts_new) > 0) {
          symbols <- unique(transcripts_new$symbol)
        }
      }

      # get data
      s <- Sys.time()
      transcripts_list <- lapply(symbols, function(s) {

        # debug
        print(s)

        # get missing years
        years_ <- transcripts_new[symbol == s, unique(data.table::year(date))]

        # get new
        inner <- lapply(years_, function(y) {
          private$fmpv_path(paste0("batch_earning_call_transcript/", s), v = "v4", year = y, apikey = self$api_key)
        })
        inner <- rbindlist(inner)
        if (nrow(inner) > 0) {
          inner$date <- as.POSIXct(inner$date, tz = "America/New_York")
        } else {
          print("There are no transcripts data.")
        }
        inner
      })
      e <- Sys.time()
      e - s
      transcripts <- rbindlist(transcripts_list)
      transcripts[, date := with_tz(date, tzone = "UTC")]
      transcripts <- na.omit(transcripts)
      transcripts <- unique(transcripts, by = c("symbol", "date"))

      # remove existing transcirpts
      dates_keep <- data.table::fsetdiff(transcripts[, .(symbol, date)], transcripts_old_raw[, .(symbol, date)])
      transcripts <- dates_keep[, index := 1][transcripts, on = c("symbol", "date")]
      transcripts <- transcripts[index == 1]
      transcripts[, index := NULL]

      # check object size
      if (nrow(transcripts) == 0) {
        return(NULL)
      }

      # save to AWS S3
      if (tiledb_object_type(save_uri) != "ARRAY") {
        fromDataFrame(
          obj = transcripts,
          uri = save_uri,
          col_index = c("symbol", "date"),
          sparse = TRUE,
          tile_domain=list(date=c(as.POSIXct("1970-01-01 00:00:00", tz = "UTC"),
                                  as.POSIXct("2099-12-31 23:59:59", tz = "UTC"))),
          allows_dups = FALSE
        )
      } else {
        # save to tiledb
        arr <- tiledb_array(save_uri, as.data.frame = TRUE)
        arr[] <- transcripts
        tiledb_array_close(arr)
      }

      return(NULL)
    },

    #' @description get daily data from FMP cloud for all stocks
    #'
    #' @param uri AWS S3 url.
    #'
    #' @return Scrap all daily data
    get_daily_tiledb = function(uri = "s3://equity-usa-daily-fmp") {

      # debug
      # library(findata)
      # library(data.table)
      # library(httr)
      # library(RcppQuantuccia)
      # library(tiledb)
      # library(lubridate)
      # library(nanotime)
      # setCalendar("UnitedStates/NYSE")
      # self = FMP$new()
      # symbol = "CA"
      # uri = "s3://equity-usa-daily-fmp"
      # save_uri_daily = "s3://equity-usa-daily-fmpcloud"
      # hardcode_start_dates = Sys.Date() - 1
      # hardcode_end_dates = Sys.Date() - 1
      # deep_scan = FALSE

      # create data seq
      seq_date_all <- getBusinessDays(as.Date("2000-01-01"), Sys.Date() - 1)

      # read old data
      if (tiledb_object_type(uri) != "ARRAY") {
        print("Scrap for the first time!")
        seq_date <- seq.Date(as.Date("2000-01-01"), Sys.Date() - 1, by = 1)
      } else {
        # get scraped dates
        arr <- tiledb_array(uri, as.data.frame = TRUE, query_layout = "UNORDERED")
        selected_ranges(arr) <- list(symbol = cbind("SPY", "SPY"))
        old_data <- arr[]
        tiledb_array_close(arr)
        seq_date <- as.Date(setdiff(seq_date_all, old_data$date), origin = "1970-01-01")
      }

      # get daily with batch and save to azure blob
      url_base <- "https://financialmodelingprep.com/api/v4/batch-request-end-of-day-prices"
      lapply(seq_date, function(x) {
        print(x)
        # get data from FMP
        p <- RETRY("GET", url_base, query = list(date = x, apikey = self$api_key), times = 5L)
        data_ <- as.data.table(content(p))
        data_ <- unique(data_, by = c("symbol", "date"))
        if (nrow(data_) == 0) return(NULL)
        cols <- colnames(data_)[3:8]
        data_[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]
        attr(data_, "spec") <- NULL
        attr(data_, "problems") <- NULL

        # Check if the array already exists.
        if (tiledb_object_type(uri) != "ARRAY") {
          fromDataFrame(
            obj = as.data.frame(data_),
            uri = uri,
            col_index = c("symbol", "date"),
            sparse = TRUE,
            tile_domain=list(date=c(as.Date("1970-01-01"),
                                    as.Date("2099-12-31"))),
            allows_dups = FALSE
          )
        } else {
          # save to tiledb
          arr <- tiledb_array(uri, as.data.frame = TRUE)
          arr[] <- as.data.frame(data_)
          tiledb_array_close(arr)
        }

        return(NULL)
      })
    },

    #' @description retrieving intraday market data from FMP cloud.
    #'
    #' @param symbol Symbol of the stock.
    #' @param multiply multiplier.Start date.
    #' @param time Size of the time. (minute, hour, day, week, month, quarter, year)
    #' @param from Start date,
    #' @param to End date.
    #'
    #' @return Data frame with ohlcv data.
    get_intraday_equities = function(symbol,
                                     multiply = 1,
                                     time = 'day',
                                     from = as.character(Sys.Date() - 3),
                                     to = as.character(Sys.Date())) {

      # initial GET request. Don't use RETRY here yet.
      x <- tryCatch({
        GET(paste0('https://financialmodelingprep.com/api/v4/historical-price/',
                   symbol, '/', multiply, '/', time, '/', from, '/', to),
            query = list(apikey = self$api_key),
            timeout(100))
      }, error = function(e) NA)

      # control error
      tries <- 0
      while (all(is.na(x)) & tries < 20) {
        print("There is an error in scraping market data. Sleep and try again!")
        Sys.sleep(60L)
        x <- tryCatch({
          GET(paste0('https://financialmodelingprep.com/api/v4/historical-price/',
                     symbol, '/', multiply, '/', time, '/', from, '/', to),
              query = list(apikey = self$api_key),
              timeout(100))
        }, error = function(e) NA)
        tries <- tries + 1
      }

      # check if status is ok. If not, try to download again
      if (x$status_code == 404) {
        print("There is an 404 error!")
        return(NULL)
      } else if (x$status_code == 200) {
        x <- content(x)
        return(rbindlist(x$results))
      } else {
        x <- RETRY("GET",
                   paste0('https://financialmodelingprep.com/api/v4/historical-price/',
                          symbol, '/', multiply, '/', time, '/', from, '/', to),
                   query = list(apikey = self$api_key),
                   times = 5,
                   timeout(100))
        if (x$status_code == 200) {
          x <- content(x)
          return(rbindlist(x$results))
        } else {
          stop('Error in reposne. Status not 200 and not 404')
        }
      }
    },

    #' @description Help function for calculating start and end dates
    #'
    #' @param start_dates Sequence of start dates.
    #' @param n number of consecutive dates.
    #'
    #' @return list of start and end dates
    create_start_end_dates = function(start_dates, n = 4) {

      # rearange date sequences
      last_date <- start_dates[1]
      start_dates_ <- c(last_date)
      for (s in start_dates) {
        if ((as.integer(s) - as.integer(last_date)) >= n) { # num of trading days to scrap
          start_dates_ <- c(start_dates_, s)
          last_date <- s
        } else {
          next()
        }
      }
      start_dates_ <- as.Date(start_dates_, origin = "1970-01-01")
      end_dates <- c(start_dates_[-1], tail(start_dates, 1) + (n + 1))
      end_dates <- end_dates - 1
      if (tail(end_dates, 1) > (Sys.Date() - 1)) {
        end_dates[length(end_dates)] <- Sys.Date() - 1
      }
      return(list(start_dates = start_dates_, end_dates = end_dates))
    },

    #' @description Get hour data for all history from fmp cloud.
    #'
    #' @param symbols Symbol of the stock.
    #' @param url AWS S3 bucket url.
    #' @param save_uri_hour AWS S3 bucket uri for hour data.
    #' @param save_uri_daily AWS S3 bucket uri for daily data.
    #' @param deep_scan should we test for dates with low number od observation
    #'     and try to scrap again.
    #' @param hardcode_start_dates Start dates to scrape from.
    #' @param hardcode_end_dates end_date to scrap from.
    #'
    #' @return Data saved to Azure blob.
    get_intraday_equities_batch = function(symbols,
                                           url = "s3://equity-usa-minute-fmpcloud",
                                           save_uri_hour = "s3://equity-usa-hour-fmpcloud",
                                           save_uri_daily = "s3://equity-usa-daily-fmpcloud",
                                           deep_scan = FALSE,
                                           hardcode_start_dates = NULL,
                                           hardcode_end_dates = NULL) {

      # debug
      # library(findata)
      # library(data.table)
      # library(httr)
      # library(RcppQuantuccia)
      # library(tiledb)
      # library(lubridate)
      # library(nanotime)
      # library(future.apply)
      # self = FMP$new()
      # symbols = c("AAPL", "SPY")
      # url = "s3://equity-usa-minute-fmpcloud"
      # save_uri_hour = "s3://equity-usa-hour-fmpcloud"
      # save_uri_daily = "s3://equity-usa-daily-fmpcloud"
      # hardcode_start_dates = as.Date(Sys.Date() - 4)
      # hardcode_end_dates =   as.Date(Sys.Date() - 4)
      # deep_scan = FALSE
      # fmp = FMP$new()
      # library(rvest)
      # sp100 <- read_html("https://en.wikipedia.org/wiki/S%26P_100") |>
      #   html_elements(x = _, "table") |>
      #   (`[[`)(3) |>
      #   html_table(x = _, fill = TRUE)
      # sp100_symbols <- c("SPY", "TLT", "GLD", "USO", "SVXY",
      #                    sp100$Symbol)
      # symbols <- fmp$get_sp500_symbols()
      # symbols <- na.omit(symbols)
      # symbols <- symbols[!grepl("/", symbols)]
      # symbols <- unique(union(sp100_symbols, symbols))

      # loop over all symbols
      minute_data_l <- future_lapply(symbols, function(s){

        # debug
        print(s)

        # use start and end dates from arguments if exists
        if (!is.null(hardcode_start_dates) & !is.null(hardcode_end_dates)) {
          start_dates <- hardcode_start_dates
          end_dates <- hardcode_end_dates
          daily_data <- tryCatch({
            self$get_intraday_equities(s, multiply = 1, time = 'day', from = hardcode_start_dates)
          }, error = function(e) NULL)
          if (length(daily_data) == 0) {
            return(NULL)
          }
          data_history <- data.table()
        } else {
          # define start_dates
          start_dates <- seq.Date(as.Date("2004-01-01"), Sys.Date() - 1, by = 1)
          start_dates <- start_dates[isBusinessDay(start_dates)]

          # get trading days from daily data
          print("Get daily data")
          daily_start <- seq.Date(as.Date("2004-01-01"), Sys.Date(), by = 365 * 4)
          daily_end <- c(daily_start[-1], Sys.Date())
          daily_data <- lapply(seq_along(daily_start), function(i) {
            daily_data <- tryCatch({
              self$get_intraday_equities(s,
                                         multiply = 1,
                                         time = 'day',
                                         from = daily_start[i],
                                         to = daily_end[i])
            }, error = function(e) NULL)
          })
          daily_data <- rbindlist(daily_data)
          if (length(daily_data) == 0) return(NULL)
          start_dates <- as.Date(intersect(start_dates, as.Date(daily_data$formated)), origin = "1970-01-01")
          # TODO: check BRK-b (BTK.B) and other symbols with -/.

          # read old data
          if (tiledb_object_type(url) == "ARRAY") {
            # read data for the symbol
            print("Get minute data")
            arr <- tiledb_array(url,
                                as.data.frame = TRUE,
                                query_layout = "UNORDERED",
                                selected_points = list(symbol = s)
            )
            tryCatch({data_history <- arr[]}, error = function(e) NULL)
            tiledb_array_close(arr)

            # try read again if error
            tries <- 0
            while (is.null(data_history) & tries > 10) {
              print("Error in tiledb miunte read")
              tryCatch({data_history <- arr[]}, error = function(e) NULL)
              tries <- tries + 1
              Sys.sleep(10L)
            }

            # cont if there is history data
            if (length(data_history$s) > 0) {
              # basic clean
              data_history <- as.data.table(data_history)
              data_history <- unique(data_history, by = "time")
              setorder(data_history, time)
              # data_history[, time := as.POSIXct(time, origin = as.POSIXct("1970-01-01 00:00:00", tz = "UTC"), tz = "UTC")]

              # missing freq
              if (deep_scan) {

                # create date column
                data_history_tz <- copy(data_history)
                data_history_tz[, time := as.POSIXct(time, origin = as.POSIXct("1970-01-01 00:00:00", tz = "UTC"), tz = "UTC")]
                data_history_tz[, time := with_tz(time, tz = "America/New_York")]
                data_history_tz[, time_ := as.Date(time, tz = "America/New_York")]
                data_history_tz[, time_n := .N, by = time_]

                # define dates we will try to scrap again
                observation_per_day <- 60 * 6
                try_again <- unique(data_history_tz[time_n < observation_per_day, time_])
                start_dates <- as.Date(intersect(try_again, start_dates), origin = "1970-01-01")
                end_dates <- start_dates

              } else {

                # define dates to scrap
                time_utc <- as.POSIXct(data_history$time,
                                       origin = as.POSIXct("1970-01-01 00:00:00",
                                                           tz = "UTC"),
                                       tz = "UTC")
                data_history_date <- unique(as.Date(time_utc, tz = "America/New_York"))

                # get final dates to scrap
                start_dates <- as.Date(setdiff(start_dates, data_history_date), origin = "1970-01-01")
                end_dates <- start_dates
              }

            } else {
              dates <- self$create_start_end_dates(start_dates)
              start_dates <- dates$start_dates
              end_dates <- dates$end_dates
            }
          } else {

            dates <- self$create_start_end_dates(start_dates)
            start_dates <- dates$start_dates
            end_dates <- dates$end_dates
          }
        }

        # if there is no dates next
        if (length(start_dates) == 0) {
          print(paste0("No data for symbol ", s))
          return(NULL)
        }

        # get data
        print("Get new minute data")
        data_slice <- list()
        for (d in seq_along(start_dates)) {
          if (end_dates[d] >= Sys.Date()) end_dates[d] <- Sys.Date() - 1
          data_slice[[d]] <- self$get_intraday_equities(s,
                                                        multiply = 1,
                                                        time = "minute",
                                                        from = start_dates[d],
                                                        to = end_dates[d])
        }
        if (any(unlist(sapply(data_slice, nrow)) > 4999)) {
          stop("More than 4999 rows!")
        }
        data_by_symbol <- rbindlist(data_slice)

        # if there is no data next
        if (nrow(data_by_symbol) == 0) {
          print(paste0("No data for symbol ", s))
          return(NULL)
        }

        # convert to numeric (not sure why I put these, but it have sense I believe).
        cols <- c("o", "h", "l", "c", "v")
        data_by_symbol[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]

        # clean data
        data_by_symbol <- unique(data_by_symbol, by = c("formated"))
        setorder(data_by_symbol, "t")
        data_by_symbol[, symbol := toupper(s)]
        data_by_symbol <- data_by_symbol[, .(symbol, t / 1000, o, h, l, c, v)]
        setnames(data_by_symbol, c("symbol", "time", "open", "high", "low", "close", "volume"))

        return(data_by_symbol)

      })
      minute_data <- rbindlist(minute_data_l)

      # add minute data to tiledb.
      if (tiledb_object_type(url) != "ARRAY") {
        fromDataFrame(
          obj = as.data.frame(minute_data),
          uri = url,
          col_index = c("symbol", "time"),
          sparse = TRUE,
          tile_domain=list(time=c(as.POSIXct("1970-01-01 00:00:00", tz = "UTC"),
                                  as.POSIXct("2099-12-31 23:59:59", tz = "UTC"))),
          allows_dups = FALSE
        )
      } else {
        # save to tiledb
        arr <- tiledb_array(url, as.data.frame = TRUE)
        arr[] <- as.data.frame(minute_data)
        tiledb_array_close(arr)
      }

      # create hour data
      new_data <- copy(minute_data)
      new_data[, date := as.nanotime(time * 1000000000)]
      hour_data <- new_data[, .(open = head(open, 1),
                                high = max(high, na.rm = TRUE),
                                low = min(low, na.rm = TRUE),
                                close = tail(close, 1),
                                volume = sum(volume, na.rm = TRUE)),
                            by = .(symbol,
                                   time = nano_ceiling(date, as.nanoduration("01:00:00")))]
      hour_data[, time := as.POSIXct(time, tz = "UTC")]

      # save hour data
      if (tiledb_object_type(save_uri_hour) != "ARRAY") {
        fromDataFrame(
          as.data.frame(hour_data),
          uri = save_uri_hour,
          col_index = c("symbol", "time"),
          sparse = TRUE,
          allows_dups = FALSE,
          tile_domain=list(time=c(as.POSIXct("1970-01-01 00:00:00", tz = "UTC"),
                                  as.POSIXct("2099-12-31 23:59:59", tz = "UTC"))),
        )
      } else {
        # save to tiledb
        arr <- tiledb_array(save_uri_hour, as.data.frame = TRUE)
        arr[] <- as.data.frame(hour_data)
        tiledb_array_close(arr)
      }

      # daily data
      daily_data <- copy(hour_data)
      daily_data[, time_ny := format.POSIXct(time, tz = "America/New_York")]
      daily_data <- daily_data[substr(time_ny, 12, 19) %between% c("09:30:00", "16:00:00")]
      daily_data <- daily_data[, .(open = head(open, 1),
                                   high = max(high, na.rm = TRUE),
                                   low = min(low, na.rm = TRUE),
                                   close = tail(close, 1),
                                   volume = sum(volume, na.rm = TRUE)),
                               by = .(symbol, date = as.Date(time_ny))]

      # save daily data
      if (tiledb_object_type(save_uri_daily) != "ARRAY") {
        fromDataFrame(
          as.data.frame(daily_data),
          uri = save_uri_daily,
          col_index = c("symbol", "date"),
          sparse = TRUE,
          allows_dups = FALSE,
          tile_domain=list(date=cbind(as.numeric(as.Date("1970-01-01", tz = "UTC")),
                                      as.numeric(as.Date("2099-12-31"), tz = "UTC"))),
        )
      } else {
        # save to tiledb
        arr <- tiledb_array(save_uri_daily, as.data.frame = TRUE)
        arr[] <- as.data.frame(daily_data)
        tiledb_array_close(arr)
      }

      # consolidate and vacum hour data
      system.time({
        tiledb:::libtiledb_array_consolidate(self$context_with_config@ptr,
                                             uri = save_uri_hour)
        tiledb:::libtiledb_array_vacuum(ctx = self$context_with_config@ptr,
                                        uri = save_uri_hour)
      })

      # consolidate and vacum daily data
      system.time({
        tiledb:::libtiledb_array_consolidate(self$context_with_config@ptr,
                                             uri = save_uri_daily)
        tiledb:::libtiledb_array_vacuum(ctx = self$context_with_config@ptr,
                                        uri = save_uri_daily)
      })

      return(NULL)
    },


    #' @description Get SP500 symbols from fmp cloud.
    #'
    #' @return Vector of SP500 symbols.
    get_sp500_symbols = function() {
      # get SP 500 stocks from FMP LCOUD
      SP500 <- content(GET(paste0('https://financialmodelingprep.com/api/v3/sp500_constituent?apikey=', self$api_key)))
      SP500 <- rbindlist(SP500)
      SP500_DELISTED <- GET(paste0('https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey=', self$api_key))
      SP500_DELISTED <- rbindlist(content(SP500_DELISTED))
      SP500_SYMBOLS <- unique(c(SP500$symbol, SP500_DELISTED$symbol))

      # get symbol changes
      utils_changes <- UtilsData$new()
      SP500_CHANGES <- lapply(SP500_SYMBOLS, utils_changes$get_ticker_data)
      SP500_CHANGES <- rbindlist(SP500_CHANGES)
      SP500_SYMBOLS <- unique(c(SP500_SYMBOLS, SP500_CHANGES$ticker_change))

      return(SP500_SYMBOLS)
    },

    #' @description Create factor files for calculating adjusted prices (adjusted for splits and dividends).
    #'
    #' @param save_uri Uri where tiledb saves files
    #' @param prices_uri Uri to import prices from.
    #'
    #' @return Factor files saved to "factor-file" blob.
    get_factor_files = function(save_uri = "s3://equity-usa-factor-files",
                                prices_uri = "s3://equity-usa-daily-fmpcloud") {

      # DEBUG
      # library(httr)
      # library(findata)
      # library(data.table)
      # library(findata)
      # library(tiledb)
      # library(lubridate)
      # library(rvest)
      # self = FMP$new()
      # save_uri = "s3://equity-usa-factor-files"
      # prices_uri = "s3://equity-usa-daily-fmpcloud"
      # DEBUG

      # TODO use dividends from stocksalysis andcheck aIG then. Dividend 3$ wrong

      # read daily unadjusted daily data
      arr <- tiledb_array(prices_uri,
                          as.data.frame = TRUE,
                          query_layout = "UNORDERED")
      system.time(unadj_daily_data <- arr[])

      # delete object
      del_obj <- tryCatch(tiledb_object_rm(save_uri), error = function(e) NA)
      if (is.na(del_obj)) {
        warning("Can't delete object")
      }

      # order by symbol and date
      DT <- as.data.table(unadj_daily_data)
      DT <- unique(DT, by = c("symbol", "date"))
      setorder(DT, symbol, date)
      symbols <- unique(DT$symbol)
      DT <- DT[, .(symbol, date , close)]

      # splits
      utils <- UtilsData$new()
      splits_l <- lapply(symbols, utils$get_split_factor)
      names(splits_l) <- symbols
      splits <- rbindlist(splits_l, fill = TRUE, idcol = "symbol")
      df <- splits[DT, on = c("symbol", "date")]
      df[, ratio := NULL]

      # dividends
      url <- "https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/"
      dividends_l <- lapply(symbols, function(x) {
        y <- RETRY("GET", paste0(url, x), query = list(apikey = self$api_key), times = 5L)
        y <- content(y)
        y <- rbindlist(y$historical, fill = TRUE)
        y <- cbind(symbol = x, y)
        return(y)
      })
      dividends <- rbindlist(dividends_l, fill = TRUE)
      dividends[, date := as.Date(date)]
      dividends <- dividends[date > as.Date("2004-01-01")]
      dividends[is.na(dividend), diviend := adjDividend]
      dividends <- dividends[, .(symbol, date, dividend)]
      df <- dividends[df, on = c("symbol", "date")]

      # create factor file
      df[, `:=`(lag_close = shift(close), date = shift(date)), by = symbol]
      df <- df[!is.na(date)]
      factor_files <- df[(!is.na(dividend) & dividend != 0) | !is.na(split_factor),
                         .(date, dividend, split_factor, lag_close), by = symbol]

      # create empty factor files for equities with no dividends and stock splits
      empty_factor_file_symbols <- setdiff(symbols, unique(factor_files$symbol))
      df_empty <- data.table(date = c(as.Date("2004-01-02"),
                                      as.Date("2050-01-02")),
                             price_factor = c(1, 1),
                             split_factor = c(1, 1),
                             lag_close = c(1, 0))
      factor_empty <- merge(data.frame(symbol = empty_factor_file_symbols),
                            df_empty)
      factor_empty <- as.data.table(factor_empty)
      setorder(factor_empty, symbol, date)

      # create facotr files for othe equities
      ### DEBUG #######
      # df <- splits[DT, on = c("symbol", "date")]
      # df[, ratio := NULL]
      # df <- dividends[df, on = c("symbol", "date")]
      # df[, `:=`(lag_close = shift(close), date = shift(date)), by = symbol]
      # df <- df[!is.na(date)]
      # factor_files <- df[(!is.na(dividend) & dividend != 0) | !is.na(split_factor),
      #                    .(date, dividend, split_factor, lag_close), by = symbol]
      # factor_files <- factor_files[date %between% c("2004-01-01", "2021-04-01")]
      ### DEBUG #######

      # cont creating factor files
      factor_files <- unique(factor_files)
      factor_files[, split_factor := nafill(split_factor, type = "nocb"), by = symbol]
      factor_files[is.na(split_factor), split_factor := as.numeric(1), by = symbol]
      end_row_by_group <- data.table(symbol = unique(factor_files$symbol),
                                     date = as.Date("2050-01-01"),
                                     dividend = NA,
                                     split_factor = 1,
                                     lag_close = 0)
      factor_files <- rbind(factor_files, end_row_by_group)
      setorder(factor_files, symbol, date)

      create_price_factor <- function(lag_close, split_factor, dividend) {
        price_factor <- vector("numeric", length(lag_close))
        for (i in length(lag_close):1) {
          if (i == length(lag_close)) {
            price_factor[i] <- (lag_close[i - 1] - dividend[i -
                                                              1])/lag_close[i - 1] * 1
          }
          else if (i == 1) {
            price_factor[i] <- NA
          }
          else if (is.na(dividend[i - 1])) {
            price_factor[i] <- price_factor[i + 1]
          }
          else {
            price_factor[i] <- ((lag_close[i - 1] - dividend[i -
                                                               1])/lag_close[i - 1]) * price_factor[i + 1]
          }
        }
        price_factor <- c(price_factor[-1], NA)
        price_factor[is.na(price_factor)] <- 1

        return(price_factor)
      }

      factor_files[, price_factor := create_price_factor(lag_close, split_factor, dividend),
                   by = symbol]
      factor_files <- factor_files[, .(symbol, date, price_factor, split_factor,
                                       lag_close)]

      # debug
      # factor_files[symbol == "ABT"]

      # add empty
      factor_files_final <- rbind(factor_empty, factor_files)
      setorder(factor_files_final, symbol, date)

      # save factor file to blob
      if (tiledb_object_type(save_uri) != "ARRAY") {
        fromDataFrame(
          obj = as.data.frame(factor_files_final),
          uri = save_uri,
          col_index = c("symbol", "date"),
          sparse = TRUE,
          allows_dups = TRUE
        )
      }

      return(factor_files)
    },

    #' @description Get IPO date from fmp cloud
    #'
    #' @param ticker Stock ticker
    #'
    #' @return Stock IPO date.
    get_ipo_date = function(ticker) {
      url <- "https://financialmodelingprep.com/api/v4/company-outlook"
      p <- content(GET(url, query = list(symbol = ticker, apikey = self$api_key)))
      if ("error" %in% names(p)) {
        return("2004-01-01")
      } else {
        return(p$profile$ipoDate)
      }
    },

    #' @description Get ipo-calendar-confirmed date from fmp cloud
    #'
    #' @param uri TileDB uri argument
    #'
    #' @return Stock IPO date.
    get_ipo_calendar_confirmed = function(uri = "s3://equity-usa-ipo") {

      # debug
      self = FMP$new()

      # meta
      url <- "https://financialmodelingprep.com/api/v4/ipo-calendar-confirmed"
      seq_date_start <- seq.Date(as.Date("2000-01-01"), Sys.Date() - 1, by = 20)
      seq_date_end <- seq_date_start[2:length(seq_date_start)]
      seq_date_start <- seq_date_start[-length(seq_date_start)]

      # get ipo data for every data span
      ipos <- lapply(seq_along(seq_date_start), function(i) {
        content(GET(url, query = list(from = seq_date_start[i],
                                      to = seq_date_end[i],
                                      apikey = self$api_key)))
      })

      # merge and clean data
      ipo_data <- lapply(ipos, rbindlist)
      ipo_data <- rbindlist(ipo_data)
      ipo_data[, `:=`(filingDate = as.Date(filingDate),
                      acceptedDate = as.POSIXct(acceptedDate, tz = "America/New_York"),
                      effectivenessDate = as.Date(effectivenessDate))]
      ipo_data[, acceptedDate := with_tz(acceptedDate, tzone = "UTC")]
      ipo_data <- unique(ipo_data)

      # add to tiledb
      if (tiledb_object_type(uri) != "ARRAY") {
        fromDataFrame(
          obj = as.data.frame(ipo_data),
          uri = uri,
          col_index = c("symbol", "filingDate"),
          sparse = TRUE,
          allows_dups = FALSE
        )
      } else {
        # save to tiledb
        arr <- tiledb_array(uri, as.data.frame = TRUE)
        arr[] <- as.data.frame(ipo_data)
        tiledb_array_close(arr)
      }
    },

    #' @description Get FI data.
    #'
    #' @param symbol Stock symbol.
    #' @param statement Stock statement.
    #' @param period Stock period.
    #'
    #' @return data.table of financial reports.
    get_fi_statement = function(symbol,
                                statement = c("income-statement", "balance-sheet-statement", "cash-flow-statement"),
                                period = c("annual", "quarter")) {
      url <- "https://financialmodelingprep.com/api/v3"
      url <- paste(url, statement, symbol, sep = "/")
      p <- RETRY("GET", url, query = list(period = period, apikey = self$api_key, limit = 10000))
      res <- content(p)
      res <- rbindlist(res)
      return(res)
    },

    #' @description Get financial ratios, company key metrics.
    #'
    #' @param symbol Stock symbol.
    #' @param type Type of fundamental analysis.
    #' @param period Stock period.
    #'
    #' @return data.table of financial reports.
    get_financial_metrics = function(symbol,
                                     type = c("ratios", "key-metrics", "financial-growth"),
                                     period = c("annual", "quarter")) {
      url <- "https://financialmodelingprep.com/api/v3"
      url <- paste(url, type, symbol, sep = "/")
      p <- RETRY("GET", url, query = list(period = period, apikey = self$api_key, limit = 10000))
      res <- content(p)
      res <- rbindlist(res, fill = TRUE)
      return(res)
    },

    #' @description Get stock splits data from FMP cloud Prep.
    #'
    #' @param start_date Start date
    #' @param end_date End date.
    #'
    #' @return data.table of stock splits
    get_stock_splits = function(start_date = Sys.Date() - 5,
                                end_date = Sys.Date()) {
      url <- "https://financialmodelingprep.com/api/v3/stock_split_calendar"
      p <- RETRY("GET", url, query = list(from = start_date,
                                          to = end_date,
                                          apikey = self$api_key))
      res <- content(p)
      res <- rbindlist(res, fill = TRUE)
      return(res)
    },

    #' @description Get market cap data from FMP cloud Prep.
    #'
    #' @param ticker Ticker.
    #' @param limit Limit.
    #'
    #' @return data.table with market cap data.
    get_market_cap = function(ticker, limit = 10) {
      url <- "https://financialmodelingprep.com/api/v3/historical-market-capitalization/"
      url <- paste0(url, toupper(ticker))
      p <- RETRY("GET", url, query = list(limit = limit, apikey = self$api_key))
      res <- content(p)
      res <- rbindlist(res, fill = TRUE)
      return(res)
    },

    #' @description Get market cap data from FMP cloud Prep - bulk.
    #'
    #' @param uri TileDb uri.
    #'
    #' @return data.table with market cap data.
    get_market_cap_bulk = function(uri = "s3://equity-usa-market-cap") {

      # fmp <- FMP$new()
      securities <- self$get_stock_list()
      securities <- securities[exchangeShortName %in% c("AMES", "NASDAQ", "NYSE", "OTC")]
      securities <- securities[type == "stock"]
      tickers <- unique(securities$symbol)

      # get all existing data
      system.time({
        if (tiledb_object_type(uri) == "ARRAY") {
          arr <- tiledb_array(uri,
                              as.data.frame = TRUE,
                              query_layout = "UNORDERED")
          dt <- arr[]
          tiledb_array_close(arr)
          dt <- as.data.table(dt)
        }
      })

      # get history data if exists
      max_date_dt <- dt[, max(date, na.rm = TRUE), by = symbol]
      limits <- max_date_dt[data.table(symbol = tickers), on = "symbol"]
      limits[, limit := as.integer(Sys.Date() - V1)]
      limits[is.na(V1), limit := 20000]

      # get new data
      print("Get new data")
      system.time({data_ <- future_mapply(self$get_market_cap,
                                          ticker = limits[, symbol][1:1000],
                                          limit = limits[, limit][1:1000],
                                          SIMPLIFY = FALSE)})
      DT <- rbindlist(data_)
      DT[, date := as.Date(date)]

      # save data to array
      if (tiledb_object_type(uri) != "ARRAY") {
        fromDataFrame(
          obj = DT,
          uri = uri,
          col_index = c("symbol"),
          sparse = TRUE,
          allows_dups = TRUE
        )
      } else {
        # save to tiledb
        arr <- tiledb_array(uri, as.data.frame = TRUE)
        arr[] <- DT
        tiledb_array_close(arr)
      }

      return(NULL)
    },

    #' @description Get stock list.
    #'
    #' @return data.table with stock list data.
    get_stock_list = function() {
      url <- "https://financialmodelingprep.com/api/v3/stock/list"
      p <- RETRY("GET", url, query = list(apikey = self$api_key))
      res <- content(p)
      res <- rbindlist(res, fill = TRUE)
      return(res)
    },
    #' @description Get symbol changes.
    #'
    #' @return data.table with symbol changes data.
    get_symbol_changes = function() {
      url <- "https://financialmodelingprep.com/api/v4/symbol_change"
      p <- RETRY("GET", url, query = list(apikey = self$api_key))
      res <- content(p)
      res <- rbindlist(res, fill = TRUE)
      return(res)
    },
    #' @description Get delisted companies.
    #'
    #' @return data.table with delisted companies data.
    get_delisted_companies = function() {
      url <- "https://financialmodelingprep.com/api/v3/delisted-companies"
      p <- RETRY("GET", url, query = list(apikey = self$api_key))
      res <- content(p)
      res <- rbindlist(res, fill = TRUE)
      return(res)
    },

    #' @description Get available traded list.
    #'
    #' @return data.table with available traded list data.
    get_available_traded_list = function() {
      url <- "https://financialmodelingprep.com/api/v3/available-traded/list"
      p <- RETRY("GET", url, query = list(apikey = self$api_key))
      res <- content(p)
      res <- rbindlist(res, fill = TRUE)
      return(res)
    },

    #' @description Get SP500 stocks.
    #'
    #' @return data.table with SP500 tocks.
    get_sp500_constituent = function() {
      url <- "https://financialmodelingprep.com/api/v3/sp500_constituent"
      p <- RETRY("GET", url, query = list(apikey = self$api_key))
      res <- content(p)
      res <- rbindlist(res, fill = TRUE)
      return(res)
    },

    #' @description Get Financial Statements data
    #'
    #' @param years Get statement for specific year.
    #' @param statement quarter or annual.
    #' @param period quarter or annual.
    #'
    #' @return data.table of financial reports.
    get_fi_statement_bulk = function(years = 1999:(data.table::year(Sys.Date())),
                                     statement = c("income-statement-bulk",
                                                   "balance-sheet-statement-bulk",
                                                   "cash-flow-statement-bulk",
                                                   "ratios-bulk",
                                                   "key-metrics-bulk",
                                                   "financial-growth-bulk",
                                                   "income-statement-growth-bulk",
                                                   "balance-sheet-statement-growth-bulk",
                                                   "cash-flow-statement-growth-bulk"),
                                     period = c("annual", "quarter")
    ) {

      # DEBUG
      # library(findata)
      # library(httr)
      # library(data.table)
      # library(tiledb)
      # years = 1990:(data.table::year(Sys.Date()))
      # statement ="ratios-bulk"
      # period = "quarter"
      # self <- FMP$new()

      # define save_uri
      save_uri = paste0("equity-usa-", statement)
      save_uri_s3 <- paste0("s3://", save_uri)
      # tiledb_object_rm(save_uri_s3)

      # prepare donwload data
      url <- "https://financialmodelingprep.com/api/v4"
      url <- paste(url, statement, sep = "/")

      # i all years already scraped download only
      if (tiledb_object_type(save_uri_s3) == "ARRAY") {
        arr <- tiledb_array(save_uri_s3,
                            as.data.frame = TRUE,
                            query_layout = "UNORDERED",
                            attrs = "period")
        old_fs_data <- arr[]
        years_scraped <- unique(data.table::year(old_fs_data$date))
        if (all(years %in% years_scraped)) {
          years_ <- data.table::year(Sys.Date())
        } else {
          years_ <- years
          tiledb_object_rm(paste0("s3://", save_uri))
        }
      } else {
        years_ <- years
      }

      # donwload data
      file_names_l <- lapply(years_, function(year) {
        print(year)
        file_name <- tempfile(fileext = ".csv")
        RETRY("GET",
              url,
              query = list(year = year, period = period, apikey = self$api_key),
              write_disk(file_name, overwrite = TRUE))
        file_name
      })

      # check
      if (length(file_names_l) == 1 && is.null(file_names_l[[1]])) {
        return(NULL)
      }

      # import tables
      fs_data_l <- lapply(
        file_names_l, function(x) {
          df <- fread(x)
          if (any(duplicated(df[, c("symbol", "date")]))) {
            stop("Duplicates in (symbol, date) tuple.")
          }
          return(df)
        })
      fs_data_l <- lapply(fs_data_l, function(df) {
        if ("acceptedDate" %in% colnames(df)) {
          df[, acceptedDate := as.character(acceptedDate)]
        } else {
          df
        }
      })
      fs_data_l <- lapply(fs_data_l, function(df) {
        df[, date := as.Date(date)]
        if ("acceptedDate" %in% colnames(df)) {
          df[, fillingDate := as.Date(fillingDate)]
        }
        df
      })
      fs_data <- rbindlist(fs_data_l, fill = TRUE)
      fs_data <- fs_data[, lapply(fs_data, function(x) {
        if ("IDate" %in% class(x)) {
          x <- as.Date(x)
        }
        return(x)
      })]
      fs_data = na.omit(fs_data, cols = c("symbol", "date"))
      fs_data <- as.data.frame(fs_data)
      fs_data$finalLink <- NULL
      fs_data$link <- NULL

      # save to tiledb
      if (tiledb_object_type(save_uri_s3) != "ARRAY") {
        fromDataFrame(
          obj = fs_data,
          uri = paste0("s3://", save_uri),
          col_index = c("symbol", "date"),
          sparse = TRUE,
          tile_domain=list(date = cbind(as.Date("1970-01-01"),
                                        as.Date("2099-12-31"))),
          allows_dups = FALSE
        )
      } else {
        # save to tiledb
        arr <- tiledb_array(save_uri_s3, as.data.frame = TRUE)
        arr[] <- fs_data
        tiledb_array_close(arr)
        tiledb:::libtiledb_array_consolidate(ctx = self$context_with_config@ptr,
                                             uri = save_uri_s3)
        tiledb:::libtiledb_array_vacuum(ctx = self$context_with_config@ptr,
                                        uri = save_uri_s3)
      }
    },

    #' @description Get Upgrades and Downgrades from FMP cloud.
    #'
    #' @param symbols stock symbols. If tiledburi, import symbols from FMP events.
    #' @param save_uri Tiledb uri.
    #' @param update Should we scrape all data od jut the new one.
    #'
    #' @return Result of GET request
    get_grades = function(symbols,
                          save_uri = "s3://equity-usa-grades",
                          update = FALSE) {

      # get new data if update is FALSE
      if (update == FALSE) {
        system.time({
          all_ud_l <- lapply(symbols, function(s) {
            private$fmpv_path(path = "upgrades-downgrades",
                              v = "v4",
                              symbol = s,
                              apikey = fmp$api_key)
          })
        })
        all_ud <- rbindlist(all_ud_l)
        all_ud[, publishedDate := as.POSIXct(publishedDate,
                                             format = "%Y-%m-%dT%H:%M:%OSZ",
                                             tz = "UTC")]
      } else {

        # import existing data
        arr <- tiledb_array(save_uri,
                            as.data.frame = TRUE,
                            query_layout = "UNORDERED"
        )
        ud_raw <- arr[]
        tiledb_array_close(arr)
        ud_raw <- as.data.table(ud_raw)

        # get new data
        system.time({
          all_udu_l <- lapply(1:20, function(i) {
            private$fmpv_path(path = "upgrades-downgrades-rss-feed",
                              v = "v4",
                              page = i,
                              apikey = fmp$api_key)
          })
        })
        all_ud <- rbindlist(all_udu_l)
        all_ud <- all_ud[symbol %in% symbols]
        all_ud[, publishedDate := as.POSIXct(publishedDate,
                                             format = "%Y-%m-%dT%H:%M:%OSZ",
                                             tz = "UTC")]

        # get new data
        cols_ <- colnames(ud_raw)
        all_ud <- data.table::fsetdiff(all_ud[, ..cols_], ud_raw)

        # check if ther is new data
        if (nrow(all_ud) == 0) {
          return(NULL)
        }
      }

      # clean data
      all_ud <- na.omit(all_ud, c("symbol", "publishedDate"))
      all_ud <- unique(all_ud, by = c("symbol", "publishedDate", "gradingCompany",
                                      "newGrade", "action"))

      # check object size
      if (nrow(all_ud) == 0) {
        return(NULL)
      }

      # save to AWS S3
      if (tiledb_object_type(save_uri) != "ARRAY") {
        fromDataFrame(
          obj = all_ud,
          uri = save_uri,
          col_index = c("symbol", "publishedDate", "gradingCompany",
                        "newGrade", "action"),
          sparse = TRUE,
          tile_domain=list(publishedDate=c(as.POSIXct("1970-01-01 00:00:00", tz = "UTC"),
                                           as.POSIXct("2099-12-31 23:59:59", tz = "UTC"))),
          allows_dups = FALSE
        )
      } else {
        # save to tiledb
        arr <- tiledb_array(save_uri, as.data.frame = TRUE)
        arr[] <- all_ud
        tiledb_array_close(arr)
      }

      return(NULL)
    },

    #' @description Get Price Targest from FMP cloud.
    #'
    #' @param symbols stock symbols. If tiledburi, import symbols from FMP events.
    #' @param save_uri Tiledb uri.
    #' @param update Should we scrape all data od jut the new one.
    #'
    #' @return Result of GET request
    get_targets = function(symbols,
                           save_uri = "s3://equity-usa-targets",
                           update = FALSE) {

      # get new data if update is FALSE
      if (update == FALSE) {
        system.time({
          targets_l <- lapply(symbols, function(s) {
            private$fmpv_path(path = "price-target",
                              v = "v4",
                              symbol = s,
                              apikey = fmp$api_key)
          })
        })
        targets <- rbindlist(targets_l)
        targets[, publishedDate := as.POSIXct(publishedDate,
                                              format = "%Y-%m-%dT%H:%M:%OSZ",
                                              tz = "UTC")]
      } else {

        # import existing data
        arr <- tiledb_array(save_uri,
                            as.data.frame = TRUE,
                            query_layout = "UNORDERED"
        )
        targets_raw <- arr[]
        tiledb_array_close(arr)
        targets_raw <- as.data.table(targets_raw)

        # get new data
        system.time({
          targetsu_l <- lapply(1:20, function(i) {
            private$fmpv_path(path = "price-target-rss-feed",
                              v = "v4",
                              page = i,
                              apikey = fmp$api_key)
          })
        })
        targets <- rbindlist(targetsu_l)
        targets <- targets[symbol %in% symbols]
        targets[, publishedDate := as.POSIXct(publishedDate,
                                              format = "%Y-%m-%dT%H:%M:%OSZ",
                                              tz = "UTC")]

        # get new data
        cols_ <- colnames(targets_raw)
        targets <- data.table::fsetdiff(targets[, ..cols_], targets_raw)

        # check if ther is new data
        if (nrow(targets) == 0) {
          return(NULL)
        }
      }

      # clean data
      targets <- na.omit(targets, c("symbol", "publishedDate"))
      targets <- unique(targets)

      # check object size
      if (nrow(targets) == 0) {
        return(NULL)
      }

      # save to AWS S3
      if (tiledb_object_type(save_uri) != "ARRAY") {
        fromDataFrame(
          obj = targets,
          uri = save_uri,
          col_index = c("symbol", "publishedDate"),
          sparse = TRUE,
          tile_domain=list(publishedDate=c(as.POSIXct("1970-01-01 00:00:00", tz = "UTC"),
                                           as.POSIXct("2099-12-31 23:59:59", tz = "UTC"))),
          allows_dups = TRUE
        )
      } else {
        # save to tiledb
        arr <- tiledb_array(save_uri, as.data.frame = TRUE)
        arr[] <- targets
        tiledb_array_close(arr)
      }

      return(NULL)
    },

    #' @description Get Ratings from FMP cloud.
    #'
    #' @param symbols stock symbols. If tiledburi, import symbols from FMP events.
    #' @param save_uri Tiledb uri.
    #' @param update Should we scrape all data od jut the new one.
    #'
    #' @return Result of GET request
    get_ratings = function(symbols,
                           save_uri = "s3://equity-usa-ratings",
                           update = FALSE) {

      # get new data if update is FALSE
      if (update == FALSE) {
        system.time({
          ratings_l <- lapply(symbols, function(s) {
            private$fmpv_path(path = paste0("historical-rating/", s),
                              v = "v3",
                              limit = 10000,
                              apikey = fmp$api_key)
          })
        }) # 2601
        ratings <- rbindlist(ratings_l)
        ratings[, date := as.Date(date)]

        # keep only numeric columns, without description
        cols_ <- c("symbol", "date", "rating",
                   colnames(ratings)[grep("Score", colnames(ratings))])
        ratings <- ratings[, ..cols_]

      } else {

        # import existing data
        arr <- tiledb_array(save_uri,
                            as.data.frame = TRUE,
                            query_layout = "UNORDERED",
                            # selected_ranges = list(date = cbind(Sys.Date() - 30,
                            #                                     Sys.Date()))
        )
        ratings_raw <- arr[]
        tiledb_array_close(arr)
        ratings_raw <- as.data.table(ratings_raw)

        # get new data
        system.time({
          ratingsu_l <- lapply(symbols, function(s) {
            private$fmpv_path(path = paste0("historical-rating/", s),
                              v = "v3",
                              limit = 30,
                              apikey = fmp$api_key)
          })
        })
        ratings <- rbindlist(ratingsu_l)
        ratings <- ratings[symbol %in% symbols]
        ratings[, date := as.Date(date)]

        # keep only numeric columns, without description
        cols_ <- c("symbol", "date", "rating",
                   colnames(ratings)[grep("Score", colnames(ratings))])
        ratings <- ratings[, ..cols_]

        # get new data
        cols_ <- colnames(ratings_raw)
        ratings <- data.table::fsetdiff(ratings[, ..cols_], ratings_raw)

        # check if ther is new data
        if (nrow(ratings) == 0) {
          return(NULL)
        }
      }

      # clean data
      ratings <- na.omit(ratings, c("symbol", "date"))
      ratings <- unique(ratings)

      # check object size
      if (nrow(ratings) == 0) {
        return(NULL)
      }

      # save to AWS S3
      if (tiledb_object_type(save_uri) != "ARRAY") {
        fromDataFrame(
          obj = ratings,
          uri = save_uri,
          col_index = c("symbol", "date"),
          sparse = TRUE,
          tile_domain=list(date=c(as.Date("1970-01-01", tz = "UTC"),
                                  as.Date("2099-12-31", tz = "UTC"))),
          allows_dups = FALSE
        )
      } else {
        # save to tiledb
        arr <- tiledb_array(save_uri, as.data.frame = TRUE)
        arr[] <- ratings
        tiledb_array_close(arr)
      }

      return(NULL)
    },

    #' @description Get Historical dividends from FMP cloud.
    #'
    #' @param symbols Vector of stock symbols.
    #'
    #' @return Data table with historical dividends for symbol.
    get_historical_dividends = function(symbols) {

      # dividends
      urls <- paste0("https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/",
                     symbols)
      dividends_l <- lapply(urls, function(x) {
        p <- RETRY("GET", x, query = list(apikey = self$api_key))
        res <- httr::content(p)
        div <- rbindlist(res$historical)
        cbind(symbol = res$symbol, div)
      })
      dividends <- rbindlist(dividends_l, fill = TRUE)
      return(dividends)
    }
  ),

  private = list(
    ea_file_name = "EarningAnnouncements",
    transcripts_file_name = "earnings-calendar.rds",
    prices_file_name = "prices.csv",
    market_cap_file_name = "MarketCap",

    fmpv_path = function(path = "earning_calendar", v = "v3", ...) {

      # query params
      query_params <- list(...)

      # define url
      url <- paste0("https://financialmodelingprep.com/api/", v, "/", path)

      # get data
      p <- RETRY("GET", url, query = query_params, times = 5)
      result <- suppressWarnings(rbindlist(httr::content(p), fill = TRUE))

      return(result)
    }
  )
)
