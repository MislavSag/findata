#' @title Macros Class
#'
#' @description
#' Function calculates macro variables potentially important for
#' trading/investment strategies. Some are ussualy used in asset pricing
#' literature, while others are not.
#'
#' @export
Macros = R6::R6Class(
  "Macros",
  inherit = DataAbstract,

  public = list(

    #' @field source Data source for calculating macro predictors.
    source = NULL,

    #' @field fmp Help field to hold FMP class.
    fmp = NULL,

    #' @field import Help field to hold Import class.
    import = NULL,

    # DEBUG
    # self = list()
    # self$fmp = FMP$new()
    # self$import = Import$new()

    #' @description
    #' Create a new Macros object.
    #'
    #' @param source Data source for calculating macro predictors.
    #'
    #' @return A new `Macros` object.
    initialize = function(source = NULL) {
      self$source = source

      # initiate findata classes
      self$fmp = FMP$new()
      self$import = Import$new()
    },

    #' @description
    #' Main function that calculates factors.
    #'
    #' @return Data.table with factors.
    get_macros = function() {

      # help functions and variables
      future_return <- function(x, n) {
        (data.table::shift(x, n, type = 'lead') - x) / x
      }
      trading_year <- 256
      trading_halfyear <- trading_year / 2
      trading_quarter <- trading_year / 4

      # help function for weigthed mean
      # source: https://stackoverflow.com/questions/40269022/weighted-average-using-na-weights
      weighted_mean = function(x, w, ..., na.rm=FALSE){
        if(na.rm){
          keep = !is.na(x)&!is.na(w)
          w = w[keep]
          x = x[keep]
        }
        weighted.mean(x, w, ..., na.rm=FALSE)
      }

      # fred help function
      get_fred <- function(id = "VIXCLS", name = "vix", calculate_returns = FALSE) {
        x <- fredr_series_observations(
          series_id = id,
          observation_start = as.Date("2000-01-01"),
          observation_end = Sys.Date()
        )
        x <- as.data.table(x)
        x <- x[, .(date, value)]
        x <- unique(x)
        setnames(x, c("date", name))

        # calcualte returns
        if (calculate_returns) {
          x <- na.omit(x)
          x[, paste0(name, "_ret_month") := get(name) / shift(get(name), 22) - 1]
          x[, paste0(name, "_ret_year") := get(name) / shift(get(name), 252) - 1]
          x[, paste0(name, "_ret_week") := get(name) / shift(get(name), 5) - 1]
        }
        x
      }

      # fred help function for vintage days
      get_fred_vintage <- function(id = "TB3MS", name = "tbl") {
        start_dates <- seq.Date(as.Date("2000-01-01"), Sys.Date(), by = 365)
        end_dates <- c(start_dates[-1], Sys.Date())
        map_fun <- function(start_date, end_date) {
          x <- tryCatch({
            fredr_series_observations(
              series_id = id,
              observation_start = start_date,
              observation_end = end_date,
              realtime_start = start_date,
              realtime_end = end_date
            )
          }, error = function(e) NULL)
          x
        }
        x_l <- mapply(map_fun, start_dates, end_dates, SIMPLIFY = FALSE)
        x <- rbindlist(x_l)
        x <- unique(x)
        x <- x[, .(realtime_start, value)]
        setnames(x, c("date", name))
        x
      }

      # FMP Prep
      if (self$source == "fmp") {

        # FMP DATA ----------------------------------------------------------------
        # import fmp data
        system.time({
          fmp_data = self$import$get_data_fmp(
            uri_market_cap = "s3://equity-usa-market-cap",
            uri_earning_announcements = "s3://equity-usa-earningsevents",
            uri_pl = "s3://equity-usa-income-statement-bulk",
            uri_bs = "s3://equity-usa-balance-sheet-statement-bulk",
            uri_fg = "s3://equity-usa-financial-growth-bulk",
            uri_metrics = "s3://equity-usa-key-metrics-bulk",
            uri_prices = "D:/equity-usa-daily-fmp")
        })
        events <- fmp_data$events
        fmp_data$events <- NULL
        fundamentals <- fmp_data$fundamentals
        fmp_data$fundamentals <- NULL
        prices <- fmp_data$prices
        fmp_data$prices <- NULL

        # free memory
        rm(fmp_data)
        gc()

        # sp500 historical universe cleaned
        fmp = FMP$new()
        sp500_symbols = fmp$get_sp500_symbols()


        # CLEAN PRICES ------------------------------------------------------------
        # remove observations with extreme prices
        prices_dt <- prices[returns < 0.8 & returns > -0.8]
        prices_dt[, high_return := high / shift(high) - 1, by = symbol]
        prices_dt <- prices_dt[high_return < 0.8 & high_return > -0.8]
        prices_dt[, low_return := low / shift(low) - 1, by = symbol]
        prices_dt <- prices_dt[low_return < 0.8 & low_return > -0.8]
        prices_dt[, `:=`(low_return = NULL, high_return = NULL)]

        # remove observation with less than 3 years of data
        prices_n <- prices_dt[, .N, by = symbol]
        prices_n <- prices_n[N > (trading_year * 2)]  # remove prices with only 700 or less observations
        prices_dt <- prices_dt[symbol %in% prices_n[, symbol]]

        # spy
        spy <- prices_dt[symbol == "SPY"]


        # NEBOJSA -----------------------------------------------------------------
        # volume of growth / decline stocks
        vol_52 <- prices_dt[, .(symbol, date, high, low, close, volume)]
        vol_52[, return_52w := close / shift(close, n = 52 * 5) - 1, by = "symbol"]
        vol_52[return_52w > 0, return_52w_dummy := "up", by = "symbol"]
        vol_52[return_52w <= 0, return_52w_dummy := "down", by = "symbol"]
        vol_52 <- vol_52[!is.na(return_52w_dummy)]
        vol_52_indicators <- vol_52[, .(volume = sum(volume, na.rm = TRUE)), by = c("return_52w_dummy", "date")]
        setorderv(vol_52_indicators, c("return_52w_dummy", "date"))
        vol_52_indicators <- dcast(vol_52_indicators, date ~ return_52w_dummy, value.var = "volume")
        vol_52_indicators[, volume_down_up_ratio := down / up]
        vol_52_indicators <- vol_52_indicators[, .(date, volume_down_up_ratio)]



        # WELCH GOYAL --------------------------------------------------------
        # TODO: ADD SP500 stocks after Matej finish his task!
        # net expansion
        welch_goyal <- prices_dt[, .(symbol, date, marketCap, returns)]
        welch_goyal <- welch_goyal[, .(mcap = sum(marketCap, na.rm = TRUE),
                                       vwretx = weighted_mean(returns, marketCap, na.rm = TRUE)), by = date]
        setorder(welch_goyal, date)
        welch_goyal[, ntis := mcap - (shift(mcap, 22) *
                                        (1 + frollapply(vwretx, 22, function(x) prod(1+x)-1)))]
        welch_goyal[, ntis3m := mcap - (shift(mcap, 22 * 3) *
                                          (1 + frollapply(vwretx, 22 * 3, function(x) prod(1+x)-1)))]
        welch_goyal[, ntis_halfyear := mcap - (shift(mcap, trading_halfyear) *
                                                 (1 + frollapply(vwretx, trading_halfyear, function(x) prod(1+x)-1)))]
        welch_goyal[, ntis_year := mcap - (shift(mcap, trading_year) *
                                             (1 + frollapply(vwretx, trading_year, function(x) prod(1+x)-1)))]
        welch_goyal[, ntis_2year := mcap - (shift(mcap, trading_year * 2) *
                                              (1 + frollapply(vwretx, trading_year*2, function(x) prod(1+x)-1)))]
        welch_goyal[, ntis_3year := mcap - (shift(mcap, trading_year * 3) *
                                              (1 + frollapply(vwretx, trading_year*3, function(x) prod(1+x)-1)))]

        # dividend to price for all market
        url <- "https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/"
        dividends_l <- lapply(sp500_symbols, function(s){
          p <- GET(paste0(url, s), query = list(apikey = Sys.getenv("APIKEY-FMPCLOUD")))
          res <- content(p)
          dividends_ <- rbindlist(res$historical, fill = TRUE)
          cbind(symbol = res$symbol, dividends_)
        })
        dividends <- rbindlist(dividends_l)
        dividends[, date := as.Date(date)]
        dividends <- na.omit(dividends, cols = c("adjDividend", "date"))
        # dividends <- unique(dividends, by = c("symbol", "date"))
        dividends[order(adjDividend)]

        # dividend sp500
        dividends_sp500 <- dividends[, .(div = sum(adjDividend , na.rm = TRUE)), by = date]
        setorder(dividends_sp500, date)
        dividends_sp500[, div_year := frollsum(div, trading_year, na.rm = TRUE)]
        dividends_sp500 <- dividends_sp500[spy, on = "date"]
        dividends_sp500[, div_year := nafill(div_year, "locf")]
        dividends_sp500[, dp := div_year / close]
        dividends_sp500[, dy := div_year / shift(close)]
        dividends_sp500 <- na.omit(dividends_sp500[, .(date, dp, dy)])

        # add to welch_goyal
        welch_goyal <- dividends_sp500[welch_goyal, on = "date"]
        plot(as.xts.data.table(welch_goyal[, .(date, dp)]))

        # series without vintage days
        sp500 <- get_fred("SP500", "sp500", TRUE)
        oil <- get_fred("DCOILWTICO", "oil", TRUE)
        vix <- get_fred(id = "VIXCLS", name = "vix")
        t10y2y <- get_fred("T10Y2Y", "t10y2y")

        # series with vintage days
        tbl <- get_fred_vintage("TB3MS", "tbl")
        welch_goyal <- tbl[welch_goyal, on = c("date"), roll = Inf]

        # MERGE PREDICTORS --------------------------------------------------------
        # merge prices, earnings accouncements, fundamentals and  macro vars
        predictors <- Reduce(function(x, y) merge(x, y, by = "date", all.x = TRUE, all.y = FALSE),
                               list(welch_goyal, vol_52_indicators,
                                    vix, sp500, oil, t10y2y))

        return(predictors)
      }
    }
  )
)
