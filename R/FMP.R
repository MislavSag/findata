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

    #' @field azure_storage_endpoint Azure storate endpont
    azure_storage_endpoint = NULL,


    #' @description
    #' Create a new FMP object.
    #'
    #' @param api_key API KEY for FMP cloud data.
    #'
    #' @return A new `FMP` object.
    initialize = function(api_key = NULL) {

      # endpoint
      super$initialize(NULL)

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
    #' @param days_history how long to the past to look
    #'
    #' @return Earning announcements data.
    get_earning_announcements = function(days_history = 10) {

      # define board
      board <- board_azure(
        container = storage_container(self$azure_storage_endpoint, "fmpcloud"),
        path = "",
        n_processes = 6L,
        versioned = FALSE,
        cache = NULL
      )

      # define container
      # cont <- storage_container(self$azure_storage_endpoint, "fmpcloud")
      blob_files <- pin_list(board)
      file_exists_on_blob <- private$ea_file_name %in% blob_files
      if (file_exists_on_blob) {
        old_data <- pin_read(board, private$ea_file_name)
        old_data <- as.data.table(old_data)
        cols <- c("symbol", "date", "time", "updatedFromDate", "fiscalDateEnding")
        old_data[, (cols) := lapply(.SD, as.character), .SDcols = cols]
        cols <- "date"
        old_data[, (cols) := lapply(.SD, as.Date), .SDcols = cols]
        start_date <- max(old_data$date, Sys.Date()) - days_history
      } else {
        start_date <- as.Date("2010-01-01")
      }
      end_date <- Sys.Date()

      # check if scrapng ncessary
      if (end_date < start_date) {
        print("No new data.")
        return(old_data)
      }

      # define listing dates
      dates_from <- seq.Date(start_date, end_date, by = 3)
      dates_to <- dates_from + 3

      # get data
      ea <- lapply(seq_along(dates_from), function(i) {
        private$fmpv_path("earning_calendar", from = dates_from[i], to = dates_to[i], apikey = self$api_key)
      })
      new_data <- rbindlist(ea, fill = TRUE)

      # check if there are data available for timespan
      if (nrow(new_data) == 0) {
        print("No data for earning announcements.")
        return(NULL)
      }

      # clean data
      new_data$date <- as.Date(new_data$date)
      results <- unique(new_data)

      # add old data to new data
      if (file_exists_on_blob) {
        results <- rbind(old_data, results)
        results <- unique(results)
      }

      # save file to Azure blob if blob_file is not NA
      pin_write(board, results, type = "csv", name = private$ea_file_name, versioned = FALSE)
      print(paste0("Data saved to blob file ", private$ea_file_name))

      return(results)
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
        p <- RETRY("GET", url_base, query = list(date = x, apikey = self$api_key), times = 5L) # ADD SELF HERE
        data_ <- content(p)
        pin_write(board, data_, type = "csv", name = as.character(x), versioned = FALSE)
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
    #'
    #' @return Data saved to Azure blob.
    get_intraday_equities_batch = function(symbols, freq = c("hour", "minute")) {


      ####### DEBUG #######
      # library(RcppQuantuccia)
      ####### DEBUG #######


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
          data_history <- pin_read(board, tolower(symbol))
          if (length(data_history) == 0) next()
          data_history <- as.data.table(data_history)
          data_history <- unique(data_history, by = "formated")
          setorder(data_history, t)

          # check missing freq
          data_history[, date := as.Date(formated)]
          data_history[, date_n := .N, by = date]

          # define dates
          data_history_date <- as.Date(setdiff(as.Date(data_history$formated),
                                               unique(data_history[date_n < 7, date])),
                                       origin = "1970-01-01")
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
    #' @return Factor files saved to "factor-file" blob.
    get_factor_file = function() {

      ######### DEBUG #########
      # library(httr)
      # library(findata)
      # library(data.table)
      # library(pins)
      # library(AzureStor)
      # self = list()
      # self$azure_storage_endpoint = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), Sys.getenv("BLOB-KEY"))
      # self$api_key = Sys.getenv("APIKEY-FMPCLOUD")
      ######### DEBUG #########

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
        market_data_hour[, datetime_test := as.POSIXct(market_data_hour$t / 1000, origin = "1970-01-01", tz = "UTC")]
        attr(market_data_hour$datetime_test, "tzone") <- "America/New_York"
        market_data_hour[symbol == "aapl"]
        tail(market_data_hour, 100)


        fmp <- FMP$new()
        fmp$get_intraday_equities("AAPL", 1, "hour", as.character(as.Date("2022-05-05")), as.character(as.Date("2022-05-08")))

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
        url <- paste0("https://financialmodelingprep.com/api/v4/company-outlook")
        p <- content(GET(url, query = list(symbol = ticker, apikey = self$api_key)))
        if ("error" %in% names(p)) {
          return("2004-01-01")
        } else {
          return(p$profile$ipoDate)
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

    # get_market_cap = function(ticker, limit = 10) {
    #   url <- "https://financialmodelingprep.com/api/v3/historical-market-capitalization/"
    #   url <- paste0(url, toupper(ticker))
    #   p <- RETRY("GET", url, query = list(limit = limit, apikey = Sys.getenv("APIKEY-FMPCLOUD")))
    #   res <- content(p)
    #   res <- rbindlist(res, fill = TRUE)
    #   return(res)
    # }

    #' @description Get market cap data from FMP cloud Prep - bulk.
    #'
    #' @return data.table with market cap data.
    get_market_cap_bulk = function() {

      # define board
      board <- board_azure(
        container = storage_container(self$azure_storage_endpoint, "fmpcloud"),
        path = "",
        n_processes = 6L,
        versioned = FALSE,
        cache = NULL
      )

      # get history data if exists
      print("Read old data")
      file_exists <- pin_exists(board = board, private$market_cap_file_name)
      if (file_exists) {
        data_history <- pin_read(board, private$market_cap_file_name)
        data_history <- as.data.table(data_history)
        data_history[, `:=`(symbol = as.character(symbol),
                            date = as.Date(as.character(date)))]
        days_history <- max(data_history$date)
        days_history <- Sys.Date() - days_history
        if (days_history <= 1) {
          return(NULL)
        }
        tickers <- unique(as.character(data_history$symbol))
      } else {
        fmp <- FMP$new()
        tickers <- fmp$get_available_traded_list()
        tickers <- tickers[exchangeShortName %in% c("AMES", "NASDAQ", "NYSE", "ETF", "OTC")]
        tickers <- unique(tickers$symbol)
        days_history = 50000
      }

      # get new data
      print("Get new data")
      data_ <- lapply(tickers, self$get_market_cap, limit = days_history + 1)
      DT <- rbindlist(data_)
      DT[, date := as.Date(date)]

      # merge old and new
      if (file_exists) {
        data_new <- rbind(data_history, DT)
        data_new <- unique(data_new)
      } else {
        data_new <- copy(DT)
      }

      # write (save) objeet to azure blob
      pin_write(board, data_new, private$market_cap_file_name, type = "csv")

      return(data_new)
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
