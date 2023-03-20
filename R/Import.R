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
    #' @param uri_market_cap Tiledb uri for market capitalization.
    #' @param uri_earning_announcements Tiledb uri for earning announcements.
    #' @param uri_pl Tiledb uri for p&l.
    #' @param uri_bs Tiledb uri for balance sheet.
    #' @param uri_fg Tiledb uri for financial growth.
    #' @param uri_metrics Tiledb uri for key metrics.
    #' @param uri_prices Tiledb uri for daily OHLCV prices.
    #' @param first_date if NA, keep all. Otherwise, keep only data from
    #'     first_date.
    #'
    #' @return Data.table with factors.
    get_data_fmp = function(uri_market_cap = "s3://equity-usa-market-cap",
                            uri_earning_announcements = "s3://equity-usa-earningsevents",
                            uri_pl = "s3://equity-usa-income-statement-bulk",
                            uri_bs = "s3://equity-usa-balance-sheet-statement-bulk",
                            uri_fg = "s3://equity-usa-financial-growth-bulk",
                            uri_metrics = "s3://equity-usa-key-metrics-bulk",
                            uri_prices = "D:/equity-usa-daily-fmp",
                            first_date = NA) {

        # UNIVERSE ----------------------------------------------------------------
        # universe consists of US stocks
        securities <- self$fmp$get_stock_list()
        stocks_us <- securities[type == "stock" &
                                  exchangeShortName %in% c("AMEX", "NASDAQ", "NYSE", "OTC")]
        symbols_list <- stocks_us[, unique(symbol)]


        # PROFILES ----------------------------------------------------------------
        tmp_file <- tempfile(fileext = ".csv")
        p <- GET("https://financialmodelingprep.com//api/v4/profile/all",
                 query = list(apikey = Sys.getenv("APIKEY-FMPCLOUD")),
                 write_disk(tmp_file, overwrite = TRUE))
        profiles <- fread(tmp_file)
        profiles <- profiles[country == "US"]
        setnames(profiles, tolower(colnames(profiles)))


        # MARKET CAP --------------------------------------------------------------
        # market cap data
        arr <- tiledb_array(uri_market_cap,
                            as.data.frame = TRUE,
                            query_layout = "UNORDERED")
        system.time(market_cap <- arr[])
        tiledb_array_close(arr)
        market_cap <- as.data.table(market_cap)
        market_cap <- market_cap[date %in% qlcal::getBusinessDays(min(date, na.rm = TRUE),
                                                                  max(date, na.rm = TRUE))]
        setorder(market_cap, symbol, date)


        # EARNING ANNOUNCEMENTS ---------------------------------------------------
        # get earning announcmenet evetns data from FMP
        arr <- tiledb_array(uri_earning_announcements,
                            as.data.frame = TRUE,
                            query_layout = "UNORDERED")
        events <- arr[]
        events <- as.data.table(events)
        setorder(events)
        events <- events[date < Sys.Date()]                 # remove announcements for today
        events <- unique(events, by = c("symbol", "date"))  # remove duplicated symbol / date pair
        events <- events[symbol %in% symbols_list]          # keep only relevant symbols


        # FUNDAMENTAL DATA --------------------------------------------------------
        # income statement data
        arr <- tiledb_array(uri_pl, as.data.frame = TRUE, query_layout = "UNORDERED",)
        system.time(pl <- arr[])
        tiledb_array_close(arr)
        pl <- as.data.table(pl)
        pl[, `:=`(acceptedDateTime = as.POSIXct(acceptedDate, format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York"),
                  acceptedDate = as.Date(acceptedDate, format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York"))]

        # balance sheet data
        arr <- tiledb_array(uri_bs, as.data.frame = TRUE, query_layout = "UNORDERED")
        system.time(bs <- arr[])
        tiledb_array_close(arr)
        bs <- as.data.table(bs)
        bs[, `:=`(acceptedDateTime = as.POSIXct(acceptedDate, format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York"),
                  acceptedDate = as.Date(acceptedDate, format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York"))]

        # financial growth
        arr <- tiledb_array(uri_fg, as.data.frame = TRUE, query_layout = "UNORDERED")
        system.time(fin_growth <- arr[])
        tiledb_array_close(arr)
        fin_growth <- as.data.table(fin_growth)

        # financial ratios
        arr <- tiledb_array(uri_metrics, as.data.frame = TRUE, query_layout = "UNORDERED")
        system.time(fin_ratios <- arr[])
        tiledb_array_close(arr)
        fin_ratios <- as.data.table(fin_ratios)

        # merge all fundamental data
        columns_diff_pl <- c("symbol", "date", setdiff(colnames(pl), colnames(bs)))
        columns_diff_fg <- c("symbol", "date", setdiff(colnames(fin_growth), colnames(pl)))
        columns_diff_fr <- c("symbol", "date", setdiff(colnames(fin_ratios), colnames(pl)))
        fundamentals <- Reduce(function(x, y) merge(x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
                               list(bs,
                                    pl[, ..columns_diff_pl],
                                    fin_growth[, ..columns_diff_fg],
                                    fin_ratios[, ..columns_diff_fr]))


        # MARKET DATA -------------------------------------------------------------
        # market daily data
        if (is.na(first_date)) {
          arr <- tiledb_array(uri_prices, as.data.frame = TRUE, query_layout = "UNORDERED")
        } else {
          arr <- tiledb_array(uri_prices, as.data.frame = TRUE, query_layout = "UNORDERED",
                              selected_ranges = list(date = cbind(first_date, Sys.Date())))
        }
        system.time(prices <- arr[])
        tiledb_array_close(arr)
        prices <- as.data.table(prices)

        # keep only US securites
        prices <- prices[symbol %in% c("SPY", symbols_list)]

        # remove duplicated values an non business days
        prices_dt <- unique(prices, by = c("symbol", "date")) # remove duplicates if they exists
        prices_dt <- prices_dt[date %in% qlcal::getBusinessDays(min(date, na.rm = TRUE),
                                                                max(date, na.rm = TRUE))]
        setorder(prices_dt, "symbol", "date")
        prices_dt <- prices_dt[open > 0 & high > 0 & low > 0 & close > 0 & adjClose > 0] # remove rows with zero and negative prices
        prices_dt[, returns := adjClose   / data.table::shift(adjClose) - 1, by = symbol] # calculate returns
        adjust_cols <- c("open", "high", "low")
        prices_dt[, (adjust_cols) := lapply(.SD, function(x) x * (adjClose / close)), .SDcols = adjust_cols] # adjust open, high and low prices
        prices_dt[, close := adjClose]
        prices_dt <- na.omit(prices_dt[, .(symbol, date, open, high, low, close, volume, returns)])

        # # remove observations with extreme prices
        # prices_dt <- prices_dt[returns < 0.8 & returns > -0.8]
        # prices_dt[, high_return := high / shift(high) - 1, by = symbol]
        # prices_dt <- prices_dt[high_return < 0.8 & high_return > -0.8]
        # prices_dt[, low_return := low / shift(low) - 1, by = symbol]
        # prices_dt <- prices_dt[low_return < 0.8 & low_return > -0.8]
        # prices_dt[, `:=`(low_return = NULL, high_return = NULL)]

        # # remove observation with less than 3 years of data
        # prices_n <- prices_dt[, .N, by = symbol]
        # prices_n <- prices_n[N > (trading_year * 3)]  # remove prices with only 700 or less observations
        # prices_dt <- prices_dt[symbol %in% prices_n[, symbol]]

        # add profiles data
        prices_dt = profiles[, .(symbol, industry, sector)][prices_dt, on = "symbol"]

        # add market cap data
        prices_dt <- market_cap[prices_dt, on = c("symbol", "date")]

        # add number of shares
        fund_merge <- fundamentals[, .(symbol, date = acceptedDate, weightedAverageShsOut,
                                       weightedAverageShsOutDil)]
        prices_dt <- fund_merge[prices_dt, on = c("symbol", "date"), roll = Inf]
        setorder(prices_dt, symbol, date)


        # RETURN ------------------------------------------------------------------
        return(list(
          events = events,
          fundamentals = fundamentals,
          prices = prices_dt
        ))
      }
  )
)
