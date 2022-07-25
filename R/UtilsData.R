#' @title UtilsData Class
#'
#' @description
#' Various help data.
#'
#' @export
UtilsData = R6::R6Class(
  "UtilsData",
  inherit = DataAbstract,

  public = list(

    #' @field azure_storage_endpoint Azure storate endpont
    azure_storage_endpoint = NULL,

    #' @description
    #' Create a new UtilsData object.
    #'
    #' @return A new `UtilsData` object.
    initialize = function() {

      # endpoint
      super$initialize(NULL)

      print("Good")
    },

    #' @description Get tick data from finam source using QuantTools.
    #'
    #' @param symbol Equity symbol
    #'
    #' @return Get investing com ea data.
    get_ticker_data = function(symbol) {

      p <- RETRY("POST",
                 'https://www.quantumonline.com/search.cfm',
                 body = list(
                   tickersymbol = symbol,
                   sopt = 'symbol',
                   '1.0.1' = 'Search'
                 ),
                 times = 8L)
      changes <- content(p) %>%
        html_elements(xpath = "//*[contains(text(),'Previous Ticker')]") %>%
        html_text() %>%
        gsub('.*Symbol:', '', .) %>%
        trimws(.)
      date <- as.Date(str_extract(changes, '\\d+/\\d+/\\d+'), '%m/%d/%Y')
      tickers <- str_extract(changes, '\\w+')
      changes <- data.table(symbol = symbol, date = date, ticker_change = tickers)
      return(changes)
    },

    #' @description Get split data from the web and calculate split factors.
    #'
    #' @param ticker Stock ticker
    #'
    #' @return Stock splits
    get_split_factor = function(ticker) {
      splits <- content(GET("https://www.splithistory.com/", query = list(symbol = ticker)))
      splits <- html_nodes(splits, xpath = "//table[@width='208' and @style='font-family: Arial; font-size: 12px']")
      splits <- html_table(splits, header = TRUE)[[1]]
      splits$date <- as.Date(splits$Date, "%m/%d/%Y")
      ratio1 <- as.numeric(str_extract(splits$Ratio, "\\d+"))
      ratio2 <- as.numeric(str_extract(splits$Ratio, "\\d+$"))
      splits$ratio <- ratio2 / ratio1
      splits <- splits[splits$date > as.Date("2004-01-01"), ]
      splits <- splits[order(splits$date), ]
      splits <- splits[splits$ratio != 1, ]
      if (length(splits) == 0) {
        return(NA)
      } else {
        splits$split_factor <- rev(cumprod(rev(splits$ratio)))
      }
      return(splits[, c('date', 'ratio', 'split_factor')])
    },

    #' @description Adjusted instraday data for splits and dividends
    #'
    #' @param freq Frequency, can be hour or minute.
    #'
    #' @return Adjusted data saved to Azure blob
    adjust_intraday_data = function(freq = c("hour", "minute")) {

      # delete cached pins
      # cache_prune(days=0)

      # define board for factor files
      board_factors <- board_azure(
        container = storage_container(self$azure_storage_endpoint, "factor-file"),
        path = "",
        n_processes = 2,
        versioned = FALSE,
        cache = NULL
      )
      factor_files <- lapply(pin_list(board_factors), function(x) {
        print(x)
        pin_read(x, board = board_factors)
      })
      names(factor_files) <- toupper(pin_list(board_factors))
      factor_files <- rbindlist(factor_files, idcol = "symbol")
      factor_files[, date := as.Date(as.character(date), "%Y%m%d")]

      # get only data from IPO
      fmp_ipo <- FMP$new()
      ipo_dates <- vapply(unique(factor_files$symbol), function(x) {
        y <- fmp_ipo$get_ipo_date(x)
        if (is.null(y)) {
          return("2004-01-02")
        } else {
          return(y)
        }
      }, character(1))
      ipo_dates_dt <- as.data.table(ipo_dates, keep.rownames = TRUE)
      setnames(ipo_dates_dt, "rn", "symbol")
      ipo_dates_dt[, ipo_dates := as.Date(ipo_dates)]
      ipo_dates_dt[, symbol := toupper(gsub("\\.csv", "", symbol))]

      # board for market data
      storage_name <- paste0("equity-usa-", freq, "-trades-fmplcoud")
      board <- board_azure(
        container = storage_container(self$azure_storage_endpoint, storage_name),
        path = "",
        n_processes = 10,
        versioned = FALSE,
        cache = NULL
      )
      board_files <- pin_list(board)

      # board for market adjusted data
        storage_name_adjusted <- paste0(storage_name, "-adjusted")
        board_adjusted <- board_azure(
          container = storage_container(self$azure_storage_endpoint, storage_name_adjusted),
          path = "",
          n_processes = 10,
          versioned = FALSE,
          cache = NULL
        )

      # market data adjusments
      for (f in board_files) {

        # for debbiging
        print(f)

        # get hourly data from blob
        market_data <- pin_read(board, f)
        if (nrow(market_data) == 0) next()
        market_data <- as.data.table(market_data)
        market_data[, datetime := as.POSIXct(as.character(formated), tz = "America/New_York")]
        if (freq == "hour") {
          market_data <- market_data[format.POSIXct(datetime, format = "%H:%M:%S") %between% c("09:30:00", "16:01:00")]
        } else if(freq == "minute") {
          market_data <- market_data[format.POSIXct(datetime, format = "%H:%M:%S") %between% c("09:30:00", "16:00:00")]
        }
        ##### TEST #####
        min(format.POSIXct(market_data$datetime, format = "%H:%M:%S"))
        max(format.POSIXct(market_data$datetime, format = "%H:%M:%S"))
        ##### TEST #####
        market_data[, symbol := toupper(f)]
        market_data <- market_data[, .(symbol, datetime, o, h, l, c, v)]
        setnames(market_data, c("symbol", "datetime", "open", "high", "low", "close", "volume"))

        # extract only data after IPO
        market_data_new <- ipo_dates_dt[market_data, on = "symbol"]
        market_data_new <- market_data_new[as.Date(datetime) >= ipo_dates]
        market_data_new_daily <- market_data_new[, .SD[.N], by = .(symbol, date = as.Date(datetime))]
        market_data_new_daily$ipo_dates <- NULL

        # adjust
        df <- market_data_new[toupper(f) %in% unique(factor_files$symbol)]
        df[, date:= as.Date(datetime)]
        df <- merge(df, factor_files, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
        df[, `:=`(split_factor = na.locf(split_factor, na.rm = FALSE, rev = TRUE),
                  price_factor = na.locf(price_factor, na.rm = FALSE, rev = TRUE)), by = symbol]
        df[, `:=`(split_factor = ifelse(is.na(split_factor), 1, split_factor),
                  price_factor = ifelse(is.na(price_factor), 1, price_factor))]
        cols_change <- c("open", "high", "low", "close")
        df[, (cols_change) := lapply(.SD, function(x) {x * price_factor * split_factor}), .SDcols = cols_change]
        df <- df[, .(datetime, open, high, low, close, volume)]
        setorder(df, datetime)
        df <- unique(df)

        # save factor file to blob
        pin_write(board_adjusted, df, name = f, type = "csv", versioned = FALSE)
      }
    },

    #' @description Get SP500 stocks for every date.
    #'
    #' @return Data table of Sp500 tickers
    sp500_history = function() {

      # get SP500 stocks by date
      url <- "https://raw.githubusercontent.com/fja05680/sp500/master/S%26P%20500%20Historical%20Components%20%26%20Changes(03-14-2022).csv"
      sp500_symbols <- fread(url)
      symbols_long <- tstrsplit(sp500_symbols$tickers, split = ",")
      symbols_long <- as.data.table(do.call(cbind, symbols_long))
      sp500_symbols <- cbind(date = setDT(sp500_symbols)[, .(date)], symbols_long)
      setnames(sp500_symbols, "date.date", "date")
      sp500_symbols <- melt(sp500_symbols, id.vars = "date")
      sp500_symbols <- sp500_symbols[, .(date, value)]
      setnames(sp500_symbols, c("date", "symbol"))
      sp500_symbols <- na.omit(sp500_symbols)
      setorder(sp500_symbols, "date")

      return(sp500_symbols)
    },

    #' @description Help function to import clean data from Azureblob.
    #'
    #' @param symbols Symbols
    #' @param frequency Frequency can be hour and minute.
    #'
    #' @return Data table with market prices
    get_market_data = function(symbols = c("aapl", "aal"),
                               frequency = "hour") {


      ##### DEBUG #####
      # library(pins)
      # library(data.table)
      # library(fasttime)
      # symbols = c("aapl", "aal", "fgsdf")
      # frequency = "hour"
      # self <- list()
      # self$azure_storage_endpoint = AzureStor::storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), Sys.getenv("BLOB-KEY"))
      ##### DEBUG #####

      # board
      sc <- storage_container(self$azure_storage_endpoint,
                              paste0("equity-usa-",
                                     frequency,
                                     "-trades-fmplcoud-adjusted"))
      board_market_data <- board_azure(
        container = sc,
        path = "",
        n_processes = 6L,
        versioned = NULL,
        cache = NULL
      )

      # check if symbols on blob azure
      symbols_exists <- tolower(symbols) %in% pin_list(board_market_data)
      print(paste0("This symbols are not on blob: ",
                   unlist(symbols[symbols_exists == FALSE])))
      symbols <- symbols[symbols_exists]

      # import minute market data
      market_data_l <- lapply(tolower(symbols), pin_read, board = board_market_data)
      names(market_data_l) <- tolower(symbols)
      market_data <- rbindlist(market_data_l, idcol = "symbol")
      market_data[, datetime := fastPOSIXct(as.character(datetime), tz = "GMT")]
      market_data[, datetime := as.POSIXct(as.numeric(datetime),
                                           origin=as.POSIXct("1970-01-01", tz="EST"),
                                           tz="EST")]
      market_data <- market_data[format.POSIXct(datetime, format = "%H:%M:%S") %between% c("09:30:00", "16:00:00")]
      market_data <- unique(market_data, by = c("symbol", "datetime"))
      market_data[, returns := close / shift(close, 1L) - 1, by = "symbol"]
      market_data <- na.omit(market_data)

      return(market_data)
    }

  )
)
