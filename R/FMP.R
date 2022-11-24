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
    #' @param symbols stock symbols
    #' @param years years to extract data from.
    #' @param blob_cont blob container
    #' @param save_files should transcripts be saved.
    #'
    #' @return Result of GET request
    get_transcripts = function(symbols,
                                years = 1992:2021,
                                blob_cont = "transcripts",
                                save_files = FALSE) {
      # overwrite_existing_files = FALSE) {

      # remove downloaded files
      # if (!overwrite_existing_files & save_files) {
      #   existing_files <- gsub("\\.rds", "", get_all_blob_files(blob_cont)[, 1])
      #   symbols <- setdiff(symbols, existing_files)
      # }

      # get data
      transcripts_list <- lapply(symbols, function(s) {
        inner <- lapply(years, function(y) {
          private$fmpv_path(paste0("batch_earning_call_transcript/", s), v = "v4", year = y, apikey = self$api_key)
          # fmpv_path(paste0("batch_earning_call_transcript/", s), v = "v4", year = y, apikey = Sys.getenv("APIKEY-FMPCLOUD"))
        })
        inner <- rbindlist(inner)
        if (nrow(inner) > 0) {
          inner$date <- as.POSIXct(inner$date, tz = "EST")
          # if (save_files) save_blob_files(inner, file_name = paste0(s, ".rds"), container = blob_cont)
        } else {
          print("There are no transcripts data.")
        }
        inner
      })
      transcripts <- rbindlist(transcripts_list)
      return(transcripts)
    },

    #' @description Update earnings announcements data from FMP cloud
    #'
    #' @param symbols Sybmols
    #' @param check_years Which years to check when updatingcheck_years
    #'
    #' @return Return transcripts from FMP cloud.
    update_transcripts = function(symbols, check_years = c(2021, 2022)) {

      # set up container
      cont <- storage_container(self$azure_storage_endpoint, "transcripts")
      # cont <- storage_container(storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY")), "transcripts")

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
      all_transcripts <- all_transcripts[year %in% check_years]

      # main loop
      print("Get missing transcripts")
      for (s in unique(all_transcripts$symbol)) {
        print(s)
        file_name_ <- paste0(s, ".rds")
        # history <- tryCatch({AzureStor::download_blob(cont, file_name_)}, error = function(e) NULL)
        history <- tryCatch({storage_load_rds(cont, file_name_)}, error = function(e) NULL)
        sample_stranscripts <- all_transcripts[symbol == s]
        cols <- c("quarter", "year")
        if (is.null(history) & nrow(sample_stranscripts) > 0) {
          transcript_diff <- sample_stranscripts
        } else {
          transcript_diff <- fsetdiff(sample_stranscripts[, .SD, .SDcols = cols], history[, .SD, .SDcols = cols])
        }

        # get new data if necessary
        if (nrow(transcript_diff) == 0) {
          history_unique <- unique(history)
          print("No transcript data.")
          if (!(identical(history_unique, history))) {
            print(paste0("Data saved to blob file ", file_name_))
            super$save_blob_files(history_unique, file_name = file_name_, "transcripts")
          }
        } else {

          # get new data
          new <- self$get_transcripts(
            s,
            unique(transcript_diff$year),
            save_files = FALSE
          )

          # check if there are data available for timespan
          if (nrow(new) == 0) {
            print("No data for earning announcements.")
            return(NULL)
          }

          # clean data
          new_transcrupts_s <- rbind(new, history)
          setorder(new_transcrupts_s, "date")

          # save to blob file
          print(paste0("Data saved to blob file ", file_name_))
          super$save_blob_files(new_transcrupts_s, file_name = file_name_, "transcripts")
        }
      }
    },

    #' @description get daily data from FMP cloud
    #'
    #' @param symbols Symbols.
    #' @param date_start Start date.
    #' @param date_end End date.
    #'
    #' @return Return transcripts from FMP cloud.
    get_daily = function(symbols, date_start, date_end) {

      # define container
      cont <- storage_container(self$azure_storage_endpoint, "fmpcloud")

      # get daily data from FMP cloud
      result <- lapply(symbols, function(x) {
        url_fmp = paste0("https://financialmodelingprep.com/api/v3/historical-price-full/", x)
        q <- GET(url_fmp, query = list(from = date_start, to = date_end, apikey = Sys.getenv("APIKEY-FMPCLOUD")))
        result <- content(q)
        DT <- rbindlist(result$historical, fill = TRUE)
        DT[, symbol := result$symbol]
      })
      result <- rbindlist(result, fill = TRUE)

      # save file to Azure blob if blob_file is not NA
      super$save_blob_files(results, file_name = private$prices_file_name)
      print(paste0("Data saved to blob file ", private$ea_file_name))

      return(result)
    },

    #' @description get daily data from FMP cloud for all stocks
    #'
    #' @return Scrap all daily data
    get_daily_batch = function() {

      # date sequence
      seq_date <- seq.Date(as.Date("2000-01-01"), Sys.Date() - 1, by = 1)

      # create board for saving files
      board <- board_azure(
        container = storage_container(self$azure_storage_endpoint, "fmpcloud-daily"),
        path = "",
        n_processes = 6L,
        versioned = FALSE,
        cache = NULL
      )

      # # list pins
      blob_files <- pin_list(board)
      if (length(blob_files) == 0) {
        print("scrap for the first time!")
      } else {
        seq_date <- as.Date(setdiff(seq_date, as.Date(blob_files)), origin = "1970-01-01")
      }

      # get daily with batch and save to azure blob
      url_base <- "https://financialmodelingprep.com/api/v4/batch-request-end-of-day-prices"
      lapply(seq_date, function(x) {
        p <- RETRY("GET", url_base, query = list(date = x, apikey = self$api_key), times = 5L)
        data_ <- content(p)
        pin_write(board, data_, type = "csv", name = as.character(x), versioned = FALSE)
        return(NULL)
      })
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
      # setCalendar("UnitedStates::NYSE")
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
        selected_ranges(arr) <- list(symbol = cbind("AAPL", "AAPL"))
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

    #' @description Get hour data for all history from fmp cloud.
    #'
    #' @param symbols Symbol of the stock.
    #' @param freq frequency (hour or minute)
    #' @param deep_scan should we test for dates with low number od observation
    #'     and try to scrap again.
    #'
    #' @return Data saved to Azure blob.
    get_intraday_equities_batch = function(symbols,
                                           freq = c("hour", "minute"),
                                           deep_scan = FALSE) {


      # loop over all symbols
      for (symbol in symbols) {

        # DEBUG
        print(symbol)

        # define start_dates
        start_dates <- seq.Date(as.Date('2004-01-01'), Sys.Date() - 1, by = 1)
        start_dates <- start_dates[isBusinessDay(start_dates)]

        # get trading days from daily data
        print("get daily data")
        # cache_prune(days=0) not sure where to put this !!!
        daily_data <- self$get_intraday_equities(symbol,
                                                 multiply = 1,
                                                 time = 'day',
                                                 from = as.character(as.Date('2004-01-01')),
                                                 to = as.character(Sys.Date() - 1))
        if (is.null(daily_data)) next()
        start_dates <- as.Date(intersect(start_dates, as.Date(daily_data$formated)), origin = "1970-01-01")

        # define board
        storage_name <- paste0("equity-usa-", freq, "-trades-fmplcoud")
        board <- board_azure(
          container = storage_container(self$azure_storage_endpoint, storage_name), # HERE CHANGE WHEN MINUTE DATA !!!
          path = "",
          n_processes = 4,
          versioned = FALSE,
          cache = NULL
        )

        # read old data
        if (pin_exists(board, tolower(symbol))) {
          # read minute data for szmbol and conver to DT with no duplicates
          data_history <- pin_read(board, tolower(symbol))
          if (length(data_history) == 0) next()
          data_history <- as.data.table(data_history)
          data_history <- unique(data_history, by = "formated")
          setorder(data_history, t)

          # check missing freq
          data_history[, date := as.Date(formated)]
          data_history[, date_n := .N, by = date]

          # define dates we will try to scrap again
          if (freq == "minutes") {
            observation_per_day <- 60 * 6
          } else {
            observation_per_day <- 7
          }
          try_again <- unique(data_history[date_n < observation_per_day, date])

          # define dates to scrap
          if (deep_scan) {
            data_history_date <- as.Date(setdiff(as.Date(data_history$formated),
                                                 try_again),
                                         origin = "1970-01-01")
          } else {
            data_history_date <- as.Date(data_history$formated)
          }
          start_dates <- as.Date(setdiff(start_dates, data_history_date), origin = "1970-01-01")

          data_history[, `:=`(date = NULL, date_n = NULL)]
        }

        # create sequence of dates for GET requests
        end_dates <- start_dates + 1
        # if (all(start_dates >= Sys.Date())) {
        #   next()
        # } else if (freq == "hour") {
        #   end_dates <- start_dates + 1
        # } else if (freq == "minute") {
        #   end_dates <- start_dates + 1
        # }

        # get data
        data_slice <- list()
        for (i in seq_along(start_dates)) {
          data_slice[[i]] <- self$get_intraday_equities(symbol,
                                                        multiply = 1,
                                                        time = freq,
                                                        from = start_dates[i],
                                                        to = end_dates[i])
        }
        data_by_symbol <- rbindlist(data_slice)

        # if there is no data next
        if (nrow(data_by_symbol) == 0) {
          print(paste0("No data for symbol ", symbol))
          next
        }

        # convert to numeric (not sure why I put these, but it have sense I believe).
        data_by_symbol[, 1:5] <- lapply(data_by_symbol[, 1:5],
                                        as.numeric)

        # add to old data if there is an old data
        if (pin_exists(board, tolower(symbol))) {
          data_by_symbol <- rbind(data_history, data_by_symbol)
        }

        # clean data
        data_by_symbol <- unique(data_by_symbol, by = c("formated"))
        setorder(data_by_symbol, "t")

        # save locally
        pin_write(board, data_by_symbol, name = tolower(symbol), type = "csv", versioned = FALSE)
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
    get_minute_equities_tiledb = function(symbols,
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
      # self = FMP$new()
      # symbol = "CA"
      # url = "s3://equity-usa-minute-fmpcloud"
      # save_uri_hour = "s3://equity-usa-hour-fmpcloud"
      # save_uri_daily = "s3://equity-usa-daily-fmpcloud"
      # hardcode_start_dates = Sys.Date() - 1
      # hardcode_end_dates = Sys.Date() - 1
      # deep_scan = FALSE

      # loop over all symbols
      for (symbol in symbols) {

        # DEBUG
        print(symbol)

        # use start and end dates from arguments if exists
        if (!is.null(hardcode_start_dates) & !is.null(hardcode_end_dates)) {
          start_dates <- hardcode_start_dates
          end_dates <- hardcode_end_dates
          daily_data <- tryCatch({
            self$get_intraday_equities(symbol, multiply = 1, time = 'day')
          }, error = function(e) NULL)
          if (length(daily_data) == 0) next()
          data_history <- data.table()
        } else {
          # define start_dates
          start_dates <- seq.Date(as.Date("2004-01-01"), Sys.Date() - 1, by = 1)
          start_dates <- start_dates[isBusinessDay(start_dates)]

          # get trading days from daily data
          print("get daily data")
          daily_data <- tryCatch({
            self$get_intraday_equities(symbol,
                                       multiply = 1,
                                       time = 'day',
                                       from = as.character(as.Date("2004-01-01")),
                                       to = as.character(Sys.Date() - 1))
          }, error = function(e) NULL)
          if (length(daily_data) == 0) next()
          start_dates <- as.Date(intersect(start_dates, as.Date(daily_data$formated)), origin = "1970-01-01")
          # TODO: check BRK-b (BTK.B) and other symbols with -/.


          # read old data
          if (tiledb_object_type(url) == "ARRAY") {
            # read data for the symbol
            ranges <- list(
              symbol = cbind(symbol, symbol)
            )
            arr <- tiledb_array(url, as.data.frame = TRUE, selected_ranges = ranges)
            data_history <- arr[]
            tiledb_array_close(arr)

            # cont if there is history data
            if (length(data_history$symbol) > 0) {
              # basic clean
              data_history <- as.data.table(data_history)
              data_history <- unique(data_history, by = "time")
              setorder(data_history, time)
              # data_history[, time := as.POSIXct(time, origin = as.POSIXct("1970-01-01 00:00:00", tz = "UTC"), tz = "UTC")]

              # missing freq
              if (deep_scan) {

                # create date column
                data_history_tz <- copy(data_history)
                data_history_tz[, date := with_tz(date, tz = "America/New_York")]
                data_history_tz[, date_ := as.Date(date, tz = "America/New_York")]
                data_history_tz[, date_n := .N, by = date_]

                # define dates we will try to scrap again
                observation_per_day <- 60 * 6
                try_again <- unique(data_history_tz[date_n < observation_per_day, date_])
                start_dates <- as.Date(intersect(try_again, start_dates), origin = "1970-01-01")
                end_dates <- start_dates + 1

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
          print(paste0("No data for symbol ", symbol))
          next
        }

        # get data
        data_slice <- list()
        for (i in seq_along(start_dates)) {
          if (end_dates[i] >= Sys.Date()) end_dates[i] <- Sys.Date() - 1
          data_slice[[i]] <- self$get_intraday_equities(symbol,
                                                        multiply = 1,
                                                        time = "minute",
                                                        from = start_dates[i],
                                                        to = end_dates[i])
        }
        if (any(unlist(sapply(data_slice, nrow)) > 4999)) {
          stop("More than 4999 rows!")
        }
        data_by_symbol <- rbindlist(data_slice)

        # if there is no data next
        if (nrow(data_by_symbol) == 0) {
          print(paste0("No data for symbol ", symbol))
          next
        }

        # convert to numeric (not sure why I put these, but it have sense I believe).
        cols <- colnames(data_by_symbol)[1:5]
        data_by_symbol[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]

        # clean data
        data_by_symbol <- unique(data_by_symbol, by = c("formated"))
        setorder(data_by_symbol, "t")
        data_by_symbol[, symbol := toupper(symbol)]
        data_by_symbol <- data_by_symbol[, .(symbol, t / 1000, o, h, l, c, v)]
        setnames(data_by_symbol, c("symbol", "time", "open", "high", "low", "close", "volume"))

        # Check if the array already exists.
        if (tiledb_object_type(url) != "ARRAY") {
          fromDataFrame(
            obj = data_by_symbol,
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
          arr[] <- data_by_symbol
          tiledb_array_close(arr)
        }

        # create hour data
        if (nrow(data_history) > 0 & length(start_dates) == 0) {
          new_data <- copy(data_history)
        } else if (nrow(data_history) > 0 & length(start_dates) > 0) {
          new_data <- rbind(data_history, data_by_symbol)
        } else if (nrow(data_history) == 0 & length(start_dates) > 0) {
          new_data <- copy(data_by_symbol)
        } else {
          return(NULL)
        }
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
      }

      # consolidate and vacum hour data
      tiledb:::libtiledb_array_consolidate(self$context_with_config@ptr,
                                           uri = save_uri_hour)
      tiledb:::libtiledb_array_vacuum(ctx = self$context_with_config@ptr,
                                      uri = save_uri_hour)

      # consolidate and vacum daily data
      tiledb:::libtiledb_array_consolidate(self$context_with_config@ptr,
                                           uri = save_uri_daily)
      tiledb:::libtiledb_array_vacuum(ctx = self$context_with_config@ptr,
                                      uri = save_uri_daily)


      return(NULL)
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
    get_minute_equities_tiledb_faster = function(symbols,
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
      # self = FMP$new()
      # symbols = c("AAPL", "SPY")
      # url = "s3://equity-usa-minute-fmpcloud"
      # save_uri_hour = "s3://equity-usa-hour-fmpcloud"
      # save_uri_daily = "s3://equity-usa-daily-fmpcloud"
      # hardcode_start_dates = Sys.Date() - 1
      # hardcode_end_dates = Sys.Date() - 1
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
      minute_data_l <- list()
      for (i in seq_along(symbols)) {

        # create symbol
        symbol = symbols[i]
        print(symbol)

        # use start and end dates from arguments if exists
        if (!is.null(hardcode_start_dates) & !is.null(hardcode_end_dates)) {
          start_dates <- hardcode_start_dates
          end_dates <- hardcode_end_dates
          daily_data <- tryCatch({
            self$get_intraday_equities(symbol, multiply = 1, time = 'day')
          }, error = function(e) NULL)
          if (length(daily_data) == 0) {
            minute_data_l[[i]] <- NULL
            next()
          }
          data_history <- data.table()
        } else {
          # define start_dates
          start_dates <- seq.Date(as.Date("2004-01-01"), Sys.Date() - 1, by = 1)
          start_dates <- start_dates[isBusinessDay(start_dates)]

          # get trading days from daily data
          print("get daily data")
          daily_data <- tryCatch({
            self$get_intraday_equities(symbol,
                                       multiply = 1,
                                       time = 'day',
                                       from = as.character(as.Date("2004-01-01")),
                                       to = as.character(Sys.Date() - 1))
          }, error = function(e) NULL)
          if (length(daily_data) == 0) next()
          start_dates <- as.Date(intersect(start_dates, as.Date(daily_data$formated)), origin = "1970-01-01")
          # TODO: check BRK-b (BTK.B) and other symbols with -/.


          # read old data
          if (tiledb_object_type(url) == "ARRAY") {
            # read data for the symbol
            ranges <- list(
              symbol = cbind(symbol, symbol)
            )
            arr <- tiledb_array(url, as.data.frame = TRUE, selected_ranges = ranges)
            data_history <- arr[]
            tiledb_array_close(arr)

            # cont if there is history data
            if (length(data_history$symbol) > 0) {
              # basic clean
              data_history <- as.data.table(data_history)
              data_history <- unique(data_history, by = "time")
              setorder(data_history, time)
              # data_history[, time := as.POSIXct(time, origin = as.POSIXct("1970-01-01 00:00:00", tz = "UTC"), tz = "UTC")]

              # missing freq
              if (deep_scan) {

                # create date column
                data_history_tz <- copy(data_history)
                data_history_tz[, date := with_tz(date, tz = "America/New_York")]
                data_history_tz[, date_ := as.Date(date, tz = "America/New_York")]
                data_history_tz[, date_n := .N, by = date_]

                # define dates we will try to scrap again
                observation_per_day <- 60 * 6
                try_again <- unique(data_history_tz[date_n < observation_per_day, date_])
                start_dates <- as.Date(intersect(try_again, start_dates), origin = "1970-01-01")
                end_dates <- start_dates + 1

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
          minute_data_l[[i]] <- NULL
          print(paste0("No data for symbol ", symbol))
          next
        }

        # get data
        data_slice <- list()
        for (d in seq_along(start_dates)) {
          if (end_dates[d] >= Sys.Date()) end_dates[d] <- Sys.Date() - 1
          data_slice[[d]] <- self$get_intraday_equities(symbol,
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
          print(paste0("No data for symbol ", symbol))
          next
        }

        # convert to numeric (not sure why I put these, but it have sense I believe).
        cols <- colnames(data_by_symbol)[1:5]
        data_by_symbol[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]

        # clean data
        data_by_symbol <- unique(data_by_symbol, by = c("formated"))
        setorder(data_by_symbol, "t")
        data_by_symbol[, symbol := toupper(symbol)]
        data_by_symbol <- data_by_symbol[, .(symbol, t / 1000, o, h, l, c, v)]
        setnames(data_by_symbol, c("symbol", "time", "open", "high", "low", "close", "volume"))

        # save object to list
        minute_data_l[[i]] <- data_by_symbol
      }
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
      tiledb:::libtiledb_array_consolidate(self$context_with_config@ptr,
                                           uri = save_uri_hour)
      tiledb:::libtiledb_array_vacuum(ctx = self$context_with_config@ptr,
                                      uri = save_uri_hour)

      # consolidate and vacum daily data
      tiledb:::libtiledb_array_consolidate(self$context_with_config@ptr,
                                           uri = save_uri_daily)
      tiledb:::libtiledb_array_vacuum(ctx = self$context_with_config@ptr,
                                      uri = save_uri_daily)

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
    get_factor_file_tiledb = function(save_uri = "s3://equity-usa-factor-files",
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
      arr <- tiledb_array(prices_uri, as.data.frame = TRUE)
      system.time(unadj_daily_data <- arr[])

      # delete object
      del_obj <- tryCatch(tiledb_object_rm(save_uri), error = function(e) NA)
      if (is.na(del_obj)) {
        warning("Can't delete object")
      }

      # order by symbol and date
      DT <- as.data.table(unadj_daily_data)
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

    #' @description Create factor files for calculating adjusted prices (adjusted for splits and dividends).
    #'
    #' @return Factor files saved to "factor-file" blob.
    get_factor_file = function() {

      # DEBUG
      # library(httr)
      # library(findata)
      # library(data.table)
      # library(pins)
      # library(AzureStor)
      # self = list()
      # self$azure_storage_endpoint = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), Sys.getenv("BLOB-KEY"))
      # self$api_key = Sys.getenv("APIKEY-FMPCLOUD")
      # DEBUG

      # define board
      storage_name <- paste0("equity-usa-hour-trades-fmplcoud")
      board <- board_azure(
        container = storage_container(self$azure_storage_endpoint, storage_name),
        path = "",
        n_processes = 10,
        versioned = FALSE,
        cache = NULL
      )
      board_files <- pin_list(board)

      # define board for factor files
      board_factors <- board_azure(
        container = storage_container(self$azure_storage_endpoint, "factor-file"),
        path = "",
        n_processes = 10,
        versioned = FALSE,
        cache = NULL
      )

      # main loop
      for (f in board_files) {

        # DEBUG
        print(f)

        # get data from blob
        market_data_hour <- pin_read(board, f)
        if (length(market_data_hour) == 0) next()
        market_data_hour <- as.data.table(market_data_hour)
        market_data_hour[, formated := as.POSIXct(formated, tz = "America/New_York")]
        market_data_hour <- market_data_hour[format.POSIXct(formated, format = "%H:%D:%M") %between% c("09:30:00", "16:01:00")]
        tail(market_data_hour, 100)

        market_data_daily <- market_data_hour[, .SD[.N], by = .(date = as.Date(formated))]
        market_data_daily[,symbol := toupper(f)]
        setnames(market_data_daily, c("date", "open", "high", "close", "low", "volume", "t", "symbol"))
        df <- market_data_daily[, .(date, close)] # take needed columns

        # splits
        utils <- UtilsData$new()
        splits <- utils$get_split_factor(f)
        splits <- as.data.table(splits)
        if (all(is.na(splits))) {
          df[, `:=`(split_factor, NA)]
        } else {
          df <- splits[df, on = "date"]
          df$ratio <- NULL
        }

        # dividends
        url <- "https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/"
        dividends <- RETRY("GET", url = paste0(url, toupper(f)), query = list(apikey = self$api_key), times = 5L)
        dividends <- content(dividends)
        dividends <- rbindlist(dividends$historical, fill = TRUE)
        # dividends <- fmpc_security_dividends(symbol, startDate = "2004-01-01")
        # dividends <- as.data.table(dividends)
        if (all(is.na(dividends))) {
          df[, `:=`(dividend, NA)]
        } else {
          dividends[, date := as.Date(date)]
          dividends <- dividends[date > as.Date("2004-01-01")]
          if (!("dividend" %in% names(dividends))) {
            dividends[, `:=`(dividend, adjDividend)]
          }
          dividends <- dividends[, `:=`(dividend, ifelse(is.na(dividend) &
                                                           adjDividend > 0, adjDividend, dividend))]
          dividends <- dividends[, .(date, dividend)]
          df <- dividends[df, on = "date"]
        }

        # if not div and splits return factor file
        if (length(dividends) == 0 & nrow(splits) == 0) {
          factor_file <- data.table(date = c("20040102",
                                             "20500101"), price_factor = c(1, 1), split_factor = c(1,
                                                                                                   1), lag_close = c(df$close[1], 0))
          # save factor file to blob
          pin_write(board_factors, factor_file, name = f, type = "csv", versioned = FALSE)

          next()
        }

        # else if there are no dividends or splits after 20040102
        df[, `:=`(lag_close = shift(close), date = shift(date))]
        df <- df[!is.na(date)]
        factor_file <- df[(!is.na(dividend) & dividend != 0) | !is.na(split_factor),
                          .(date, dividend, split_factor, lag_close)]
        if (nrow(factor_file) == 0) {
          factor_file <- data.table(date = c("20040102",
                                             "20500101"), price_factor = c(1, 1), split_factor = c(1,
                                                                                                   1), lag_close = c(df$close[1], 0))
          # save factor file to blob
          pin_write(board_factors, factor_file, name = f, type = "csv", versioned = FALSE)

          next()
        }

        # else create factor file
        factor_file <- unique(factor_file)
        factor_file[, `:=`(split_factor, na.locf(split_factor,
                                                 fromLast = TRUE, na.rm = FALSE))]
        factor_file <- factor_file[is.na(split_factor), `:=`(split_factor, 1)]
        factor_file <- rbind(factor_file, data.table(date = as.Date("2050-01-01"),
                                                     dividend = NA, lag_close = 0, split_factor = 1))
        price_factor <- vector("numeric", nrow(factor_file))
        lag_close <- factor_file$lag_close
        split_factor <- factor_file$split_factor
        dividend <- factor_file$dividend
        for (i in nrow(factor_file):1) {
          if (i == nrow(factor_file)) {
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
        factor_file[, `:=`(price_factor, price_factor)]
        factor_file[, `:=`(date, format.Date(date, "%Y%m%d"))]
        factor_file <- factor_file[, .(date, price_factor, split_factor,
                                       lag_close)]

        # save factor file to blob
        pin_write(board_factors, factor_file, name = f, type = "csv", versioned = FALSE)
      }
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
    #' @return data.table with market cap data.
    get_market_cap_bulk = function() {

      # DEBUG
      # fmp <- FMP$new()
      # tickers <- fmp$get_available_traded_list()
      # tickers <- tickers[exchangeShortName %in% c("AMES", "NASDAQ", "NYSE", "ETF", "OTC")]
      # tickers <- unique(tickers$symbol)
      # limit = 50000

      # get history data if exists
      if (tiledb_object_type(url) != "ARRAY") {
        arr <- tiledb_array("s3://equity-usa-market-cap",
                            as.data.frame = TRUE,
                            selected_ranges = list(symbol = cbind("AAPL", "AAPL")))
        aapl_market_cap <- arr[]
        max_date <- max(aapl_market_cap$date, na.rm = TRUE)
        limit <- as.integer(Sys.Date() - max_date + 2)
      } else {
        limit = 50000
      }

      # get new data
      print("Get new data")
      data_ <- lapply(tickers, self$get_market_cap, limit = limit)
      DT <- rbindlist(data_)
      DT[, date := as.Date(date)]
      DT <- DT[date > max_date]

      # save data to array
      # tiledb_vfs_create_bucket("s3://equity-usa-market-cap")
      if (tiledb_object_type(url) != "ARRAY") {
        fromDataFrame(
          obj = DT,
          uri = "s3://equity-usa-market-cap",
          col_index = c("symbol"),
          sparse = TRUE,
          allows_dups = TRUE
        )
      } else {
        # save to tiledb
        arr <- tiledb_array("s3://equity-usa-market-cap", as.data.frame = TRUE)
        arr[] <- DT
        tiledb_array_close(arr)
      }

      return(DT)
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
    #' @param save_path Path to save csv file.
    #' @param years Get statement for specific year.
    #' @param statement quarter or annual.
    #' @param period quarter or annual.
    #'
    #' @return data.table of financial reports.
    get_fi_statement_bulk = function(save_path,
                                     years = 1999:(data.table::year(Sys.Date())),
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
      # statement = "financial-growth-bulk"
      # period = "quarter"
      # self <- FMP$new()
      # save_path = "D:/fundamental_data/FS"
      # save_uri = "s3://equity-usa-fundamentals"

      # define save_uri
      save_uri = paste0("equity-usa-", statement)

      # prepare donwload data
      url <- "https://financialmodelingprep.com/api/v4"
      url <- paste(url, statement, sep = "/")

      # donwload data
      lapply(years, function(year) {
        print(year)
        file_name <- file.path(save_path, paste0(statement, "-", year, ".csv"))
        RETRY("GET",
              url,
              query = list(year = year, period = period, apikey = self$api_key),
              write_disk(file_name, overwrite = TRUE))
      })

      # import tables
      fs_data_l <- lapply(
        file.path(save_path, paste0(statement, "-", years, ".csv")),
        function(x) {
          df <- fread(x)
          if (any(duplicated(df[, c("symbol", "date")]))) {
            stop("Duplicates in (symbol, date) tuple.")
          }
          return(df)
        })
      fs_data_l <- lapply(fs_data_l, function(df) {
        if ("acceptedDate" %in% colnames(df)) {
          df[, acceptedDate := as.character(acceptedDate)]
        }
      })
      fs_data <- rbindlist(fs_data_l, fill = TRUE)
      fs_data <- fs_data[, lapply(fs_data, function(x) {
        if ("IDate" %in% class(x)) {
          x <- as.Date(x)
        }
        return(x)
        })]
      fs_data <- as.data.frame(fs_data)

      # save to tiledb.
      fromDataFrame(
        obj = fs_data,
        uri = save_uri,
        col_index = c("symbol", "date"),
        sparse = TRUE,
        tile_domain=list(date = cbind(as.Date("1970-01-01"),
                                      as.Date("2099-12-31"))),
        allows_dups = FALSE
        )
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
