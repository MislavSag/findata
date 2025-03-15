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
    
    #' @field base_url Base URL for the FMP Cloud API.
    base_url = "https://financialmodelingprep.com/api",

    #' @description
    #' Create a new FMP object.
    #'
    #' @param api_key API KEY for FMP cloud data.
    #' @param context_with_config AWS S3 Tiledb config
    #' @param base_url Base URL for the FMP Cloud API.
    #'
    #' @return A new `FMP` object.
    initialize = function(api_key = NULL,
                          context_with_config = NULL,
                          base_url = NULL) {

      # endpoint

      # Set base url
      self$base_url = base_url
      
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
    #' @param path to save reulst data.table
    #' @param start_date First date to scrape from.If NULL, date is 2010-01-01.
    #'
    #' @return Earning announcements data.
    get_earning_announcements = function(path, start_date = NULL) {

      # debug
      # library(findata)
      # library(data.table)
      # library(httr)
      # library(arrow)
      # library(nanotime)
      # self = FMP$new()
      # symbol = "CA"
      # start_date = NULL
      # start_date = as.Date("2023-08-01")
      # path = "F:/equity/usa/fundamentals/earning_announcements.parquet"

      # help function
      get_ea = function(start_date, end_date) {
        # define listing dates
        dates_from = seq.Date(start_date, end_date, by = 3)
        dates_to = dates_from + 14
        
        # get data
        ea <- lapply(seq_along(dates_from), function(i) {
          private$fmpv_path("earning_calendar", from = dates_from[i], to = dates_to[i], apikey = self$api_key)
        })
        dt <- rbindlist(ea, fill = TRUE)
        
        # clean data
        dt <- unique(dt, by = c("symbol", "date"))
        dt[, date := as.Date(date)]
        dt[, fiscalDateEnding := as.Date(fiscalDateEnding)]
        dt[, updatedFromDate := as.Date(updatedFromDate)]
      }
      
      # define start date
      if (is.null(start_date)) {
        start_date = as.Date("2010-01-01")
        dt = get_ea(start_date, Sys.Date())
      } else {
        dt_existing = arrow::read_parquet(path)
        dt_new = get_ea(start_date, Sys.Date())
        if (nrow(dt_new) == 0) {
          print("No data for earning announcements.")
          return(NULL)
        } else {
          dt = unique(rbind(dt_existing, dt_new), by = c("date", "symbol", "updatedFromDate"))
        }
      }

      # save to uri
      arrow::write_parquet(dt, path)
    },
    
    #' @description Get earnings suprises data from FMP cloud Prep.
    #'
    #' @param year Year.
    #'
    #' @return data.table with earnings surprises data.
    get_earnings_suprises = function(year = format(Sys.Date(), "%Y")) {
      url = "https://financialmodelingprep.com/api/v4/earnings-surprises-bulk"
      p = RETRY("GET", url, query = list(apikey = self$api_key, year = year))
      res = content(p)
      setDT(res)
      return(res)
    },

    #' @description Get Earning Call Transcript from FMP cloud.
    #'
    #' @param symbols stock symbols. If tiledburi, import symbols from FMP events.
    #' @param uri to save transcripts data for every symbol.
    #'
    #' @return Result of GET request
    get_transcripts = function(symbols, uri) {

      # debug
      # library(data.table)
      # library(httr)
      # library(findata)
      # library(arrow)
      # library(lubridate)
      # self = list()
      # self$api_key = Sys.getenv("APIKEY-FMPCLOUD")
      # # get symbols for transcripts
      # events = arrow::read_parquet("F:/equity/usa/fundamentals/earning_announcements.parquet")
      # events = as.data.table(events)
      # events <- events[date < Sys.Date()]                 # remove announcements for today
      # events <- na.omit(events, cols = c("eps"))          # remove rows with NA for earnings
      # url <- modify_url("https://financialmodelingprep.com/", path = "api/v3/available-traded/list",
      #                   query = list(apikey = Sys.getenv("APIKEY-FMPCLOUD")))
      # res <- GET(url)
      # stocks <- rbindlist(content(res), fill = TRUE)
      # usa_symbols <- stocks[exchangeShortName %in% c("AMEX", "NASDAQ", "NYSE", "OTC")]
      # events <- events[symbol %in% usa_symbols$symbo]
      # symbols <- unique(events$symbol)
      # uri = "F:/equity/usa/fundamentals/transcripts"
      

      # main loop
      url_meta <- "https://financialmodelingprep.com/api/v4/earning_call_transcript"
      lapply(symbols, function(s) {
        
        # debug
        print(s)
        
        # get transcripts metadata for symbol
        p <- GET(url_meta, query = list(symbol = s, apikey = self$api_key))
        res <- rbindlist(content(p))
        if (nrow(res) > 0) {
          res = cbind.data.frame(symbol = s, res)
        } else {
          return(NULL) 
        }
        setnames(res, c("symbol", "quarter", "year", "date"))
        
        # create dir 
        file_ = file.path(uri, paste0(s, ".parquet"))
        if (file.exists(file_)) {
          old_data = arrow::read_parquet(file_)
          new = fsetdiff(as.data.table(res[, c("quarter", "year")]),
                         as.data.table(old_data[, c("quarter", "year")]))
          years = new[, unique(year)]
          if (length(years) == 0) {
            return(NULL)
          }
        } else {
          years = unique(res$year)
        }

        # get new data
        new_data <- lapply(years, function(y) {
          private$fmpv_path(paste0("batch_earning_call_transcript/", s), v = "v4", 
                            year = y, apikey = self$api_key)
        })
        new_data <- rbindlist(new_data)
        if (nrow(new_data) > 0) {
          new_data$date <- as.POSIXct(new_data$date, tz = "America/New_York")
        } else {
          print("There are no transcripts data.")
        }
        
        # clean data
        new_data[, date := with_tz(date, tzone = "UTC")]
        new_data <- na.omit(new_data)
        new_data <- unique(new_data, by = c("symbol", "date"))

        # rbind old and new data
        if (file.exists(file_)) {
          new_data = unique(rbind(old_data, new_data))
        }
        
        # save
        arrow::write_parquet(new_data, file_)
        
        return(NULL)
      })
    },

    #' @description get daily data from FMP cloud for all stocks
    #'
    #' @param uri to save daily data for every date.
    #'
    #' @return Scrap all daily data
    get_daily_tiledb = function(uri) {

      # debug
      # library(findata)
      # library(data.table)
      # library(httr)
      # library(arrow)
      # library(lubridate)
      # self = FMP$new()
      # uri = "F:/equity/daily_fmp"

      # create data seq
      seq_date_all <- getBusinessDays(as.Date("1990-01-01"), Sys.Date() - 1)

      # # read old data
      files = list.files(uri)
      if (length(files) == 0) {
        seq_date = seq_date_all
      } else {
        seq_date = as.Date(setdiff(as.character(seq_date_all), tools::file_path_sans_ext(files)))
      }

      # get daily with batch and save to azure blob
      url_base <- "https://financialmodelingprep.com/api/v4/batch-request-end-of-day-prices"
      lapply(seq_date, function(x) {
        print(x)
        file_name_ = file.path(uri, paste0(x, ".csv"))
        p <- RETRY("GET", 
                   url_base, 
                   query = list(date = x, apikey = self$api_key), 
                   write_disk(file_name_),
                   times = 5L)
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

    #' #' @description Get tick quote data from FMP cloud.
    #' #'
    #' #' @param symbol Symbol of the stock.
    #' #' @param date multiplier.Start date.
    #' #'
    #' #' @return Data frame with ohlcv data.
    #' get_quotes = function(symbol, date) {
    #' 
    #'   # debug
    #'   # library(httr)
    #'   # library(data.table)
    #'   # self = list()
    #'   # self$api_key = Sys.getenv("APIKEY-FMPCLOUD")
    #'   # symbol = "SPY"
    #'   # date = "2023-08-18"
    #'   
    #'   # init list
    #'   q = list()
    #'   
    #'   # initial GET request. Don't use RETRY here yet.
    #'   x <- tryCatch({
    #'     GET(paste0('https://financialmodelingprep.com/api/v4/historical-price-tick/',
    #'                symbol, '/', date),
    #'         query = list(limit = 30000, apikey = self$api_key),
    #'         timeout(100))
    #'   }, error = function(e) NA)
    #'   x <- content(x)
    #'   q[[1]] = rbindlist(x$results)
    #'   
    #'   # get last time
    #'   last_time = tail(q[[1]]$t, 1)
    #' 
    #'   x <- tryCatch({
    #'     GET(paste0('https://financialmodelingprep.com/api/v4/historical-price-tick/',
    #'                symbol, '/', date),
    #'         query = list(limit = 300, ts = last_time, te = last_time + 1000, apikey = self$api_key),
    #'         timeout(100))
    #'   }, error = function(e) NA)
    #'   x <- content(x)
    #'   rbindlist(x$results)
    #'   q[[2]] = rbindlist(x$results)
    #'   
    #'   
    #' 
    #'   
    #'   # url = "https://financialmodelingprep.com/api/v4/historical-price-tick/SPY/2023-08-04?limit=500&apikey=15cd5d0adf4bc6805a724b4417bbaafc"
    #'   # GET(url)
    #' 
    #'   # control error
    #'   tries <- 0
    #'   while (all(is.na(x)) & tries < 20) {
    #'     print("There is an error in scraping market data. Sleep and try again!")
    #'     Sys.sleep(60L)
    #'     x <- tryCatch({
    #'       GET(paste0('https://financialmodelingprep.com/api/v4/historical-price/',
    #'                  symbol, '/', multiply, '/', time, '/', from, '/', to),
    #'           query = list(apikey = self$api_key),
    #'           timeout(100))
    #'     }, error = function(e) NA)
    #'     tries <- tries + 1
    #'   }
    #' 
    #'   # check if status is ok. If not, try to download again
    #'   if (x$status_code == 404) {
    #'     print("There is an 404 error!")
    #'     return(NULL)
    #'   } else if (x$status_code == 200) {
    #'     x <- content(x)
    #'     return(rbindlist(x$results))
    #'   } else {
    #'     x <- RETRY("GET",
    #'                paste0('https://financialmodelingprep.com/api/v4/historical-price/',
    #'                       symbol, '/', multiply, '/', time, '/', from, '/', to),
    #'                query = list(apikey = self$api_key),
    #'                times = 5,
    #'                timeout(100))
    #'     if (x$status_code == 200) {
    #'       x <- content(x)
    #'       return(rbindlist(x$results))
    #'     } else {
    #'       stop('Error in reposne. Status not 200 and not 404')
    #'     }
    #'   }
    #' },
    
    
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
    #' @param start_date Start date
    #' @param end_date End date.
    #'
    #' @param uri TileDB uri argument
    #'
    #' @return Stock IPO date.
    get_ipo_calendar_confirmed_bulk = function(start_date, 
                                               end_date = Sys.Date()) {
      # start_date = Sys.Date() - 19
      # Meta
      url <- "https://financialmodelingprep.com/api/v4/ipo-calendar-confirmed"
      if ((end_date - start_date) > 20) {
        seq_date_ <- unique(c(seq.Date(start_date, end_date, by = 20), end_date))
        start_date <- seq_date_[1:(length(seq_date_)-1)]
        end_date <- seq_date_[-1]
      }

      # Get ipo data for every data span
      ipos <- lapply(seq_along(start_date), function(i) {
        content(GET(url, query = list(from = start_date[i],
                                      to = end_date[i],
                                      apikey = self$api_key)))
      })

      # Merge and clean data
      ipo_data <- lapply(ipos, rbindlist)
      ipo_data <- rbindlist(ipo_data)
      ipo_data[, `:=`(filingDate = as.Date(filingDate),
                      acceptedDate = as.POSIXct(acceptedDate, tz = "America/New_York"),
                      effectivenessDate = as.Date(effectivenessDate))]
      ipo_data[, acceptedDate := with_tz(acceptedDate, tzone = "UTC")]
      ipo_data <- unique(ipo_data)

      return(ipo_data)
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
    #' @param symbols stock symbols.
    #' @param uri to market cap data.
    #'
    #' @return data.table with market cap data.
    get_market_cap_bulk = function(symbols, uri) {

      
      # main loop 
      lapply(symbols, function(s) {
        # debug
        # library(findata)
        # library(arrow)
        # library(data.table)
        # self = FMP$new()
        # uri = "F:/equity/usa/fundamentals/market_capitalization"
        # [1] "CDXC"
        # [1] "Get new data"
        # [1] "KATE"
        # [1] "Get new data"
        # Error in as.Date.default(date) : 
        #   do not know how to convert 'date' to class “Date”
        # In addition: Warning message:
        #   In `[.data.table`(data_new, , `:=`(symbol, NULL)) :
        #   Column 'symbol' does not exist to remove
        print(s)
        
        # file name
        file_name = file.path(uri, paste0(s, ".parquet"))
        
        # get all existing data
        if (file.exists(file_name)) {
          dt_old = arrow::read_parquet(file_name)
          limit = as.integer(Sys.Date() - dt_old[, max(as.Date(date))])
          if (limit == 0) return(NULL)
        } else {
          limit = 10000
        }
        
        # get new data
        print("Get new data")
        data_new <- mapply(
          self$get_market_cap,
          ticker = s,
          limit = limit,
          SIMPLIFY = FALSE
        )
        data_new = data_new[[1]]
        if (length(data_new) == 0) {
          return(NULL)
        }
        data_new[, symbol := NULL]
        data_new[, date := as.Date(date)]
        
        # merge if necessary
        if (file.exists(file_name)) {
          data_new = unique(rbind(dt_old, data_new))
        }
        setorder(data_new, -date)
        
        # save
        arrow::write_parquet(data_new, file_name)
        return(NULL)
      })
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
    #' @param uri uri path to the directory.
    #' @param years Get statement for specific year.
    #' @param statement quarter or annual.
    #' @param period quarter or annual.
    #'
    #' @return data.table of financial reports.
    get_fi_statement_bulk = function(uri,
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
      # uri = "F:/equity/usa/fundamentals"
      # years = 1990:(data.table::year(Sys.Date()))
      # statement ="rating-bulk"
      # period = "quarter"
      # self <- FMP$new()

      # define save_uri
      save_uri = file.path(uri, statement, period)
      if (!dir.exists(save_uri)) {
        dir.create(save_uri, recursive = TRUE)
      }

      # prepare donwload data
      url <- "https://financialmodelingprep.com/api/v4"
      url <- paste(url, statement, sep = "/")

      # get new years to download
      years_exist = gsub(".csv", "", list.files(save_uri))
      years = c(setdiff(years, years_exist), year(Sys.Date()))
      
      # donwload data
      lapply(years, function(year) {
        print(year)
        file_name = file.path(save_uri, paste0(year, ".csv"))
        RETRY("GET",
              url,
              query = list(year = year, period = period, apikey = self$api_key),
              write_disk(file_name, overwrite = TRUE))
        return(NULL)
      })
    },
    
    #' @description Help function for getting grades and targets
    #'
    #' @param symbols stock symbols.
    #' @param tag url tag.
    #'
    #' @return Result of GET request
    get_daily_v4 = function(symbols, tag = "price-target") {
      # get data for tagfor all symbols
      dt_l <- lapply(symbols, function(s) {
        private$fmpv_path(
          path = tag,
          v = "v4",
          symbol = s,
          apikey = self$api_key
        )
      })
      dt <- rbindlist(dt_l)

      # clean data
      dt <- na.omit(dt, c("symbol"))
      dt <- unique(dt)
      
      return(dt)
    },

    #' @description Get Upgrades and Downgrades from FMP cloud.
    #'
    #' @param symbols stock symbols. If tiledburi, import symbols from FMP events.
    #'
    #' @return Result of GET request
    get_grades = function(symbols) {

      # get all grades for all symbols
      grades = self$get_daily_v4(symbols, "upgrades-downgrades")
      grades[, publishedDate := as.POSIXct(publishedDate,
                                           format = "%Y-%m-%dT%H:%M:%OSZ",
                                           tz = "UTC")]
      return(grades)
    },

    #' @description Get Price Targest from FMP cloud.
    #'
    #' @param symbols stock symbols. If tiledburi, import symbols from FMP events.
    #'
    #' @return Result of GET request
    get_targets = function(symbols) {

      # get all targets for all symbols
      targets = self$get_daily_v4(symbols, "price-target")
      targets[, publishedDate := as.POSIXct(publishedDate,
                                            format = "%Y-%m-%dT%H:%M:%OSZ",
                                            tz = "UTC")]
      return(targets)
    },

    #' @description Get Ratings from FMP cloud.
    #'
    #' @param symbols stock symbols. If tiledburi, import symbols from FMP events.
    #' @param uri uri to save output data.
    #' @param update Should we scrape all data od jut the new one.
    #' @param update_limit how many days to retrieve if update eqtual to TRUE.
    #'
    #' @return Result of GET request
    get_ratings = function(symbols,
                           uri = "ratings.parquet",
                           update = FALSE,
                           update_limit = 10) {

      # debug
      # library(findata)
      # library(httr)
      # library(data.table)
      # library(arrow)
      # self = FMP$new()
      # uri = "F:/equity/usa/fundamentals/ratings.parquet"
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
      # get symbols for transcripts
      # events = arrow::read_parquet("F:/equity/usa/fundamentals/earning_announcements.parquet")
      # events = as.data.table(events)
      # events <- events[date < Sys.Date()]                 # remove announcements for today
      # events <- na.omit(events, cols = c("eps"))          # remove rows with NA for earnings
      # url <- modify_url("https://financialmodelingprep.com/", path = "api/v3/available-traded/list",
      #                   query = list(apikey = Sys.getenv("APIKEY-FMPCLOUD")))
      # res <- GET(url)
      # stocks <- rbindlist(content(res), fill = TRUE)
      # usa_symbols <- stocks[exchangeShortName %in% c("AMEX", "NASDAQ", "NYSE", "OTC")]
      # events <- events[symbol %in% usa_symbols$symbo]
      # symbols <- unique(events$symbol)

      # help function
      ratings_post = function(dt) {
        ratings <- rbindlist(ratings_l)
        ratings[, date := as.Date(date)]
        
        # keep only numeric columns, without description
        cols_ <- c("symbol", "date", "rating",
                   colnames(ratings)[grep("Score", colnames(ratings))])
        ratings <- ratings[, ..cols_]
      }
      
      # get new data if update is FALSE
      if (update == FALSE) {
        # get all ratings for all symbols
        ratings_l <- lapply(symbols, function(s) {
          private$fmpv_path(
            path = paste0("historical-rating/", s),
            v = "v3",
            limit = 10000,
            apikey = self$api_key
          )
        })
        ratings = ratings_post(ratings_l)

      } else {

        # import existing data
        ratings_raw <- as.data.table(arrow::read_parquet(uri))

        # get new data
        ratings_l <- lapply(symbols, function(s) {
          private$fmpv_path(
            path = paste0("historical-rating/", s),
            v = "v3",
            limit = update_limit,
            apikey = self$api_key
          )
        })
        ratings = ratings_post(ratings_l)

        # rbind
        ratings = rbind(ratings_raw, ratings)
      }

      # clean data
      ratings <- na.omit(ratings, c("symbol", "date"))
      ratings <- unique(ratings)

      # save to uri
      arrow::write_parquet(ratings, uri)

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
    },
    
    
    #' @description Get hour data for all history from fmp cloud and save as parquet on AWS.
    #'
    #' @param symbols Symbol of the stock.
    #' @param uri_minute AWS S3 bucket uri or NAS path.
    #' @param save_uri_hour AWS S3 bucket uri for hour data.
    #' @param save_uri_daily AWS S3 bucket uri for daily data.
    #' @param deep_scan should we test for dates with low number od observation
    #'     and try to scrap again.
    #'
    #' @return Data saved to uri.
    get_equities_batch = function(symbols,
                                  uri_minute = "s3://equity-usa-minute",
                                  deep_scan = FALSE) {
      
      
      
      # debug
      # library(findata)
      # library(data.table)
      # library(httr)
      # library(RcppQuantuccia)
      # library(lubridate)
      # library(nanotime)
      # library(arrow)
      # library(checkmate)
      # library(future.apply)
      # self = FMP$new()
      # symbols = c("AAPL", "SPY")
      # uri_minute = "F:/equity/usa/minute" # "s3://equity-usa-minute"
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

      # check if we have all necessary env variables
      assert_choice("AWS_ACCESS_KEY_ID", names(Sys.getenv()))
      assert_choice("AWS_SECRET_ACCESS_KEY", names(Sys.getenv()))
      assert_choice("AWS_DEFAULT_REGION", names(Sys.getenv()))
      
      # # s3 bucket
      # if (grepl("^s3:/", uri_minute)) {
      #   bucket = s3_bucket(uri_minute) 
      #   dir_files = bucket$ls()
      # } else {
      #   dir_files = list.files(uri_minute)
      # }
      
      # parallel execution
      # plan("multisession", workers = 2L)
      # ADD FUTURE LAPPLY BELOW
      
      # main loop to scrap minute data
      lapply(symbols, function(s) {
        
        # debug
        print(s)
        
        # define file name and s3 file name
        file_name = paste0(s, ".parquet")
        file_name_full = file.path(uri_minute, file_name)
        
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

        # OLD DATA ----------------------------------------------------------------
        # read old data
        if (file_name %in% dir_files) {
          # read data for the symbol
          print("Get minute data")
          data_history = arrow::read_parquet(file_name_full)
          
          # cont if there is history data
          if (length(data_history) > 0) {
            # basic clean
            data_history <- unique(data_history, by = "date")
            setorder(data_history, date)

            # missing freq
            if (deep_scan) { # CHECK THIS LATER !!!
              
              # create date column
              data_history_tz <- copy(data_history)
              # data_history_tz[, time := as.POSIXct(time, origin = as.POSIXct("1970-01-01 00:00:00", tz = "UTC"), tz = "UTC")]
              # data_history_tz[, time := with_tz(time, tz = "America/New_York")]
              # data_history_tz[, time_ := as.Date(time, tz = "America/New_York")]
              # data_history_tz[, time_n := .N, by = time_]
              # 
              # # define dates we will try to scrap again
              # observation_per_day <- 60 * 6
              # try_again <- unique(data_history_tz[time_n < observation_per_day, time_])
              # start_dates <- as.Date(intersect(try_again, start_dates), origin = "1970-01-01")
              # end_dates <- start_dates
              
            } else {
              
              # define dates to scrap
              data_history_date = data_history[, unique(as.Date(date, tz = "America/New_York"))]
              
              # get final dates to scrap
              start_dates <- as.Date(setdiff(start_dates, data_history_date), origin = "1970-01-01")
              end_dates <- start_dates
              
              # data_history[as.Date(date) == as.Date("2004-01-16")]
            }
          } else {
            dates <- self$create_start_end_dates(start_dates, 1)
            start_dates <- dates$start_dates
            end_dates <- dates$end_dates
          }
        } else {
          dates <- self$create_start_end_dates(start_dates, 1)
          start_dates <- dates$start_dates
          end_dates <- dates$end_dates
        }
        
        # GET NEW DATA ------------------------------------------------------------
        # if there is no dates next
        if (length(start_dates) == 0) {
          print(paste0("No data for symbol ", s))
          return(NULL)
        }
        
        # get data
        print("Get new minute data")
        
        system.time({
          data_slice = lapply(seq_along(start_dates), function(d) {
          # for (d in seq_along(start_dates[1:10])) {
            if (end_dates[d] >= Sys.Date()) end_dates[d] <- Sys.Date() - 1
            self$get_intraday_equities(
              s,
              multiply = 1,
              time = "minute",
              from = start_dates[d],
              to = end_dates[d]
            )
          })
        })
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
        data_by_symbol[, date := as.POSIXct(t / 1000, 
                                    origin = as.POSIXct("1970-01-01 00:00:00", tz = "UTC"),
                                    tz = "UTC")]
        data_by_symbol = data_by_symbol[, .(date, o, h, l, c, v)]
        setnames(data_by_symbol, c("date", "open", "high", "low", "close", "volume"))
        
        # rbind new an old
        if (file_name %in% dir_files && nrow(data_history) > 0) {
          # rbind new data
          data_by_symbol = rbind(data_history, data_by_symbol)
          setorder(data_by_symbol, "date")
          data_by_symbol = unique(data_by_symbol, by = "date")
          # delete old parquet file
          if (grepl("^s3:/", uri_minute)) {
            bucket$DeleteFile(file_name)
          } else {
            file.remove(file_name_full)
          }
          # save file
          arrow::write_parquet(data_by_symbol, file_name_full)
        } else {
          # save to S3
          arrow::write_parquet(data_by_symbol, file_name_full)
        }
        return(NULL)
      })
    },

    #' @description Get minute data for all history from FMP cloud.
    #'
    #' @param symbols Symbol of the stock.
    #' @param uri_minute AWS S3 bucket uri or NAS path.
    #' @param deep_scan should we test for dates with low number od observation
    #'     and try to scrap again.
    #' @param workers Number of workers.
    #'
    #' @return Data saved to uri.
    get_minute = function(symbols,
                          uri_minute,
                          deep_scan = FALSE,
                          workers = 1L) {
      
      # debug
      # library(findata)
      # library(data.table)
      # library(httr)
      # library(RcppQuantuccia)
      # library(lubridate)
      # library(nanotime)
      # library(arrow)
      # library(checkmate)
      # library(future.apply)
      # self = FMP$new()
      # symbols = c("AAPL", "SPY")
      # uri_minute = "F:/equity/usa/minute"
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
      
      # parallel execution
      # plan("multisession", workers = 2L)
      # ADD FUTURE LAPPLY BELOW
      
      # main loop to scrap minute data
      mclapply(symbols, function(s) {
        
        # debug
        # s = "WBA"
        print(s)
        
        # create folder for symbol if it doesn't exists
        dir_name = file.path(uri_minute, s)
        if (!dir.exists(dir_name)) {
          dir.create(dir_name)
        }
        
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
        dates <- as.Date(daily_data$formated)
        
        # keep only NYSE business days
        dates_business = getBusinessDays(min(dates), max(dates))[]
        dates_business = dates_business[dates_business %in% dates]
        dates = dates_business

        # extract missing dates
        dates_exists = gsub(".parquet", "", list.files(dir_name))
        dates_new = as.Date(setdiff(paste0(dates), dates_exists))
        
        # get new data
        for (i in seq_along(dates_new)) {
          # crate date 
          date = dates_new[i]
          print(date)
          
          # get new data for date
          date_data = self$get_intraday_equities(
            s,
            multiply = 1,
            time = "minute",
            from = date,
            to = date
          )

          # if there is no data next
          if (nrow(date_data) == 0) {
            print(paste0("No data for date ", date, " for symbol ", s))
            date_data = data.table(
              o = NA_real_,
              h = NA_real_,
              l = NA_real_,
              c = NA_real_,
              v = NA_real_,
              t = NA_real_,
              formated = "no minute data"
            )
          }

          # save file
          file_name_full = file.path(dir_name, paste0(date, ".parquet"))
          arrow::write_parquet(date_data, file_name_full)
        }
      }, mc.cores = workers)
    },
    
        
    #' @description Get dividend data from FMP cloud Prep.
    #'
    #' @param ticker Ticker.
    #'
    #' @return data.table with dividend data.
    get_dividends = function(ticker) {
      url <- "https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/"
      url <- paste0(url, toupper(ticker))
      p <- RETRY("GET", url, query = list(apikey = self$api_key))
      res <- content(p)
      res <- rbindlist(res$historical, fill = TRUE)
      return(res)
    },
    
    #' @description Get beneficial ownership from FMP cloud.
    #' 
    #' @param ticker Ticker.
    #' 
    #' @return data.table with beneficial ownership data.
    beneficial_ownership = function(ticker) {
      # ticker = "AAPL"
      # self$api_key <- Sys.getenv("APIKEY-FMPCLOUD")
      url <- "https://financialmodelingprep.com/api/v4/insider/ownership/acquisition_of_beneficial_ownership"
      p <- RETRY("GET", url, query = list(symbol = toupper(ticker), apikey = self$api_key))
      res <- content(p)
      res <- rbindlist(res, fill = TRUE)
      return(res)
    },

    # QUOTE -------------------------------------------------------------------
    #' @description Get the latest bid and ask prices for a stock, as well as the 
    #'      volume and last trade price in real-time.
    #'
    #' @param symbol The stock symbol (e.g., "AAPL").
    #'
    #' @return A data.table containing the full quote for the specified stock.
    get_full_quote = function(symbol) {
      assert_character(symbol, len = 1, null.ok = FALSE)
      url = paste0(self$base_url, "/v3/quote/", symbol)
      p = RETRY("GET", url, query = list(apikey = self$api_key), times = 5)
      res = content(p)
      res = rbindlist(res, fill = TRUE)
      res[, time := as.POSIXct(timestamp, origin = "1970-01-01", tz = "America/New_York")]
      return(res)
    },
    
    #' @description Get a simplified quote for a stock, including the current price, volume, and last trade price.
    #'
    #' @param symbol The stock symbol (e.g., "AAPL").
    #'
    #' @return A data.table containing the quote order for the specified stock.
    get_quote_order = function(symbol) {
      assert_character(symbol, len = 1, null.ok = FALSE)
      url = paste0(self$base_url, "/v3/quote-order/", symbol)
      res = content(RETRY("GET", url, query = list(apikey = self$api_key), times = 5))
      res = rbindlist(res, fill = TRUE)
      return(res)
    },
    
    #' @description Get a simple quote for a stock, including the price, change, and volume.
    #'
    #' @param symbol The stock symbol (e.g., "AAPL").
    #'
    #' @return A data.table containing the simple quote for the specified stock.
    get_simple_quote = function(symbol) {
      assert_character(symbol, len = 1, null.ok = FALSE)
      url = paste0("https://financialmodelingprep.com/api/v3/quote-short/", symbol)
      p = RETRY("GET", url, query = list(apikey = self$api_key), times = 5)
      res = content(p)
      res = rbindlist(res, fill = TRUE)
      return(res)
    },
    
    #' @description Get the latest bid and ask prices for an OTC stock, as well as the volume and last trade price in real-time.
    #'
    #' @param symbol The OTC stock symbol(s) (e.g., "BATRB,FWONB").
    #'
    #' @return A data.table containing the OTC quote for the specified stock(s).
    get_otc_quote = function(symbol) {
      assert_character(symbol, len = 1, null.ok = FALSE)
      url = paste0(self$base_url, "/v3/otc/real-time-price/", symbol)
      res = content(RETRY("GET", url, query = list(apikey = self$api_key), times = 5))
      res = rbindlist(res, fill = TRUE)
      return(res)
    },
    
    #' @description Get real-time prices for all stocks listed on a specific exchange.
    #'
    #' @param exchange The exchange for which to retrieve prices (e.g., "NYSE").
    #'
    #' @return A data.table containing real-time prices for all stocks on the specified exchange.
    get_exchange_prices = function(exchange = "NYSE") {
      assert_character(exchange, len = 1, null.ok = FALSE)
      url = paste0("https://financialmodelingprep.com/api/v3/quotes/", exchange)
      p = RETRY("GET", url, query = list(apikey = self$api_key), times = 5)
      res = content(p)
      res = rbindlist(res, fill = TRUE)
      res[, time := as.POSIXct(timestamp, tz = "America/New_York")]
      return(res)
    },
    
    #' @description Get the change in a stock's price over a given period of time.
    #'
    #' @param symbol The stock symbol (e.g., "AAPL").
    #'
    #' @return A data.table containing the stock price change for the specified stock.
    get_stock_price_change = function(symbol) {
      assert_character(symbol, len = 1, null.ok = FALSE)
      url = paste0(self$base_url, "/v3/stock-price-change/", symbol)
      res = content(RETRY("GET", url, query = list(apikey = self$api_key), times = 5))
      res = rbindlist(res, fill = TRUE)
      return(res)
    },
    
    #' @description Get information on trades that have occurred in the aftermarket.
    #'
    #' @param symbol The stock symbol (e.g., "AAPL").
    #'
    #' @return A data.table containing aftermarket trade information for the specified stock.
    get_aftermarket_trades = function(symbol) {
      assert_character(symbol, len = 1, null.ok = FALSE)
      url = paste0(self$base_url, "/v4/pre-post-market-trade/", symbol)
      res = content(RETRY("GET", url, query = list(apikey = self$api_key), times = 5))
      res = as.data.table(res)
      return(res)
    },
    
    #' @description Get the latest bid and ask prices for a stock in the aftermarket.
    #'
    #' @param symbol The stock symbol (e.g., "AAPL").
    #'
    #' @return A data.table containing aftermarket quote information for the specified stock.
    get_aftermarket_quote = function(symbol) {
      assert_character(symbol, len = 1, null.ok = FALSE)
      url = paste0(self$base_url, "/v4/pre-post-market/", symbol)
      res = content(RETRY("GET", url, query = list(apikey = self$api_key), times = 5))
      res = as.data.table(res)
      return(res)
    },
    
    #' @description Get quotes for multiple stocks at once.
    #'
    #' @param symbols A comma-separated string of stock symbols (e.g., "AAPL,MSFT").
    #'
    #' @return A data.table containing batch quote information for the specified stocks.
    get_batch_quote = function(symbols) {
      assert_character(symbols, len = 1, null.ok = FALSE)
      url = paste0(self$base_url, "/v4/batch-pre-post-market/", symbols)
      res = content(RETRY("GET", url, query = list(apikey = self$api_key), times = 5))
      res = rbindlist(res, fill = TRUE)
      return(res)
    },
    
    #' @description Get trades for multiple stocks at once.
    #'
    #' @param symbols A comma-separated string of stock symbols (e.g., "AAPL,MSFT").
    #'
    #' @return A data.table containing batch trade information for the specified stocks.
    get_batch_trade = function(symbols) {
      assert_character(symbols, len = 1, null.ok = FALSE)
      url = paste0(self$base_url, "/v4/batch-pre-post-market-trade/", symbols)
      res = content(RETRY("GET", url, query = list(apikey = self$api_key), times = 5))
      res = rbindlist(res, fill = TRUE)
      return(res)
    }
  ),

  private = list(
    # PRIVATE -----------------------------------------------------------------
    
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
