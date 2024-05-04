#' @title Import Class
#'
#' @description
#' Help class that imports all relevant data from sources (for now onlz FMP).
#'
#' @export
Import = R6::R6Class(
  "Import",
  inherit = DataAbstract,

  public = list(
    #' @field fmp Help field to hold FMP class.
    fmp = NULL,

    #' @description
    #' Create a new Import object.
    #'
    #' @return A new `Import` object.
    initialize = function() {
      # initiate classes
      self$fmp = FMP$new()
    },

    #' @description
    #' Function imports all relevant data for USA stocks from FMP Prep.
    #'
    #' @param symbols Securities symbols.
    #' @param uri_market_cap Tiledb uri for market capitalization.
    #' @param uri_earning_announcements Tiledb uri for earning announcements.
    #' @param uri_fundamentals to fundamentals data.
    #' @param uri_prices Tiledb uri for daily OHLCV prices.
    #' @param uri_dividends Tiledb uri for dividends data.
    #' @param first_date if NA, keep all. Otherwise, keep only data from
    #'     first_date.
    #'
    #' @return Data.table with factors.
    get_data_fmp = function(symbols,
                            uri_market_cap = "F:/equity/usa/fundamentals/market_cap.parquet",
                            uri_earning_announcements = "F:/data/equity/us/fundamentals/earning_announcements.parquet",
                            uri_fundamentals = "F:/data/equity/us/fundamentals/fundamentals.parquet",
                            uri_prices = "F:/data/equity/daily_fmp_all.csv",
                            uri_dividends = "F:/equity/usa/fundamentals/dividends.parquet",
                            first_date = NA) {

      # Debug
      # library(data.table)
      # library(findata)
      # library(arrow)
      # library(httr)
      # library(duckdb)
      # self = list()
      # self$fmp = FMP$new()
      # securities <- self$fmp$get_stock_list()
      # exchanges = c("AMEX", "NASDAQ", "NYSE", "OTC")
      # stocks_us <- securities[type == "stock" & exchangeShortName %in% exchanges]
      # symbols_us <- stocks_us[, unique(symbol)]
      # events = arrow::read_parquet(uri_earning_announcements)
      # events = as.data.table(events)
      # events <- events[date < Sys.Date()]                 # remove announcements for today
      # events <- na.omit(events, cols = c("eps"))          # remove rows with NA for earnings
      # securities = self$fmp$get_stock_list()
      # stocks <- securities[type == "stock" & exchangeShortName %in% c("AMEX", "NASDAQ", "NYSE", "OTC")]
      # events <- events[symbol %in% stocks$symbol]
      # symbols_events <- unique(events$symbol)
      # symbols = unique(c(symbols_us, symbols_events))

       
      # PROFILES ----------------------------------------------------------------
      print("Profiles")
      tmp_file <- tempfile(fileext = ".csv")
      GET("https://financialmodelingprep.com//api/v4/profile/all",
          query = list(apikey = Sys.getenv("APIKEY-FMPCLOUD")),
          write_disk(tmp_file, overwrite = TRUE))
      profiles <- as.data.table(read.csv(tmp_file))
      profiles <- profiles[country == "US"]
      setnames(profiles, tolower(colnames(profiles)))

      # MARKET CAP --------------------------------------------------------------
      # get market cap data
      print("Market cap data.")
      market_cap = arrow::read_parquet(uri_market_cap)
      setorder(market_cap, symbol, date)
      market_cap = market_cap[symbol %in% symbols]
      
      # EARNING ANNOUNCEMENTS ---------------------------------------------------
      # get earning announcmenet evetns data from FMP
      print("Earnings announcements")
      events = arrow::read_parquet(uri_earning_announcements)
      setorder(events, date)
      events = events[date < Sys.Date()]
      events = unique(events, by = c("symbol", "date"))
      events = events[symbol %in% symbols]

      # FUNDAMENTAL DATA --------------------------------------------------------
      # get fundmanetals data
      print("Fundamental data.")
      fundamentals = arrow::read_parquet(uri_fundamentals)
      fundamentals = fundamentals[symbol %in% symbols]
      setorder(market_cap, symbol, date)
      
      # MARKET DATA -------------------------------------------------------------
      # import daily market data using duckdb
      con <- dbConnect(duckdb::duckdb())
      symbols_string <- paste(symbols, collapse = "', '")
      symbols_string <- paste0("'", symbols_string, "'")
      query <- sprintf("
      SELECT *
      FROM read_csv_auto('%s', sample_size=-1)
      WHERE Symbol IN (%s)
      ", uri_prices, symbols_string)
      prices <- as.data.table(dbGetQuery(con, query))
      dbDisconnect(con)
      
      # remove duplicated values an non business days
      prices = unique(prices, by = c("symbol", "date"))
      prices = prices[date %in% qlcal::getBusinessDays(min(date, na.rm = TRUE),
                                                       max(date, na.rm = TRUE))]
      setorder(prices, "symbol", "date")
      prices <- prices[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0]
      prices[, returns := adjClose / data.table::shift(adjClose) - 1, by = symbol]
      adjust_cols <- c("open", "high", "low")
      prices[, (adjust_cols) := lapply(.SD, function(x) 
        x * (adjClose / close)), .SDcols = adjust_cols]
      prices[, close_raw := close]
      prices[, close := adjClose]
      prices = na.omit(prices[, .(symbol, date, open, high, low, close, volume, 
                                  close_raw, returns)])
      
      # add profiles data
      prices = profiles[, .(symbol, industry, sector)][prices, on = "symbol"]

      # add market cap data
      prices = market_cap[prices, on = c("symbol", "date")]

      # add number of shares
      fund_merge <- fundamentals[, .(symbol, 
                                     date = acceptedDate, 
                                     weightedAverageShsOut, 
                                     weightedAverageShsOutDil)]
      prices = fund_merge[prices, on = c("symbol", "date"), roll = Inf]
      setorder(prices, symbol, date)

      # DIVIDENDS ---------------------------------------------------------------
      # read dividends data
      print("Import dividends data.")
      dividends = arrow::read_parquet(uri_dividends)
      dividends[, lapply(.SD, as.Date), 
                .SDcols = c("date", "recordDate", "paymentDate", "declarationDate")]
      dividends[, label := NULL]
      dividends = dividends[symbol %in% symbols]
      setorder(dividends, symbol, date)
      
      # RETURN ------------------------------------------------------------------
      return(list(
        events = events,
        fundamentals = fundamentals,
        prices = prices,
        dividends = dividends
      ))
    }
  )
)
