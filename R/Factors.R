#' @title Factors Class
#'
#' @description
#' Function calculates factor often (and less often) used in asset pricing
#' factor investing literature.
#'
#' @export
Factors = R6::R6Class(
  "Factors",
  inherit = DataAbstract,

  public = list(

    #' @field source Data source for calculating factors.
    source = NULL,

    #' @field fmp Help field to hold FMP class.
    fmp = NULL,

    #' @field import Help field to hold Import class.
    import = NULL,

    #' @field utilsdata Help field to hold Import class.
    utilsdata = NULL,
    
    #' @field sp500_symbols Path to QuantConnect SPY file.
    sp500_symbols = NULL,
    
    #' @description
    #' Create a new Factors object.
    #'
    #' @param sp500_symbols Path to QuantConnect SPY file.
    #' @param source Data source for calculating factors.
    #'
    #' @return A new `Factors` object.
    initialize = function(sp500_symbols, source = "fmp") {
      self$source = source

      # initiate findata classes
      self$fmp = FMP$new()
      self$import = Import$new()
      self$utilsdata = UtilsData$new()
      self$sp500_symbols = sp500_symbols
    },

    #' @description
    #' Main function that calculates factors.
    #'
    #' @param ... Arguemnts of get_data_fmp method of Import class.
    #'
    #' @return Data.table with factors.
    get_factors = function(...) {
      
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

      # FMP Prep
      if (self$source == "fmp") {

        # FMP DATA ----------------------------------------------------------------
        # debug
        # library(data.table)
        # library(findata)
        # library(arrow)
        # library(httr)
        # self = list()
        # self$fmp = FMP$new()
        # securities <- self$fmp$get_stock_list()
        # exchanges = c("AMEX", "NASDAQ", "NYSE", "OTC")
        # stocks_us <- securities[type == "stock" & exchangeShortName %in% exchanges]
        # symbols_us <- stocks_us[, unique(symbol)]
        # events = read_parquet("F:/equity/usa/fundamentals/earning_announcements.parquet")
        # events = as.data.table(events)
        # events <- events[date < Sys.Date()]                 # remove announcements for today
        # events <- na.omit(events, cols = c("eps"))          # remove rows with NA for earnings
        # securities = self$fmp$get_stock_list()
        # stocks <- securities[type == "stock" & exchangeShortName %in% c("AMEX", "NASDAQ", "NYSE", "OTC")]
        # events <- events[symbol %in% stocks$symbol]
        # symbols_events <- unique(events$symbol)
        # symbols = unique(c(symbols_us, symbols_events))
        # symbols = c("SPY", symbols)
        # self = Factors$new("F:/lean_root/data/equity/usa/universes/etf/spy")
        # fmp_data = self$import$get_data_fmp(
        #   symbols = symbols,
        #   uri_market_cap = "F:/equity/usa/fundamentals/market_cap.parquet",
        #   uri_earning_announcements = "F:/equity/usa/fundamentals/earning_announcements.parquet",
        #   uri_fundamentals = "F:/equity/usa/fundamentals/fundamentals.parquet",
        #   uri_prices = "F:/equity/daily_fmp_all.csv",
        #   uri_dividends = "F:/equity/usa/fundamentals/dividends.parquet"
        # )
        # sp500_symbols = self$utilsdata$sp500_history("F:/lean_root/data/equity/usa/universes/etf/spy")
        # sp500_symbols = sp500_symbols$symbols
        # indmom3year          indmom3year_lag              sec_returns                   secmom               secmom_lag                 secmom6m 
        # TRUE                     TRUE                     TRUE                     TRUE                     TRUE                     TRUE 
        # secmom6m_lag               secmomyear           secmomyear_lag              secmom2year          secmom2year_lag              secmom3year 
        # TRUE                     TRUE                     TRUE                     TRUE                     TRUE                     TRUE 
        # secmom3year_lag    weightedAverageShsOut weightedAverageShsOutDil           share_turnover                     turn           turn_half_year 
        # TRUE                     TRUE                     TRUE                     TRUE                     TRUE                     TRUE 
        # turn_year               turn_2year               turn_3year                 std_turn              std_turn_6m              std_turn_1y 
        # TRUE                     TRUE                     TRUE                     TRUE                     TRUE                     TRUE 
        
        # add SPY to symbols
        symbols = c("SPY", symbols)
        
        # import fmp data
        fmp_data = self$import$get_data_fmp(...)
        
        # divide and concuer
        events <- fmp_data$events
        fmp_data$events <- NULL
        fundamentals <- fmp_data$fundamentals
        fmp_data$fundamentals <- NULL
        prices <- fmp_data$prices
        fmp_data$prices <- NULL
        dividends = fmp_data$dividends

        # free memory
        rm(fmp_data)
        gc()

        # CLEAN PRICES ------------------------------------------------------------
        # remove observations with extreme prices
        prices <- unique(prices, by = c("symbol", "date"))
        setorder(prices, symbol, date)
        prices <- prices[returns < 0.9 & returns > -0.9]
        prices[, high_return := high / shift(high) - 1, by = symbol]
        prices <- prices[high_return < 0.9 & high_return > -0.9]
        prices[, low_return := low / shift(low) - 1, by = symbol]
        prices <- prices[low_return < 0.9 & low_return > -0.9]
        prices[, `:=`(low_return = NULL, high_return = NULL)]

        # remove observation with less than 2 years of data
        prices_n <- prices[, .N, by = symbol]
        prices_n <- prices_n[N > (trading_year * 2)]  # remove prices with only 700 or less observations
        prices <- prices[symbol %in% prices_n[, symbol]]
        setorder(prices, symbol, date)

        # spy
        spy <- prices[symbol == "SPY"]

        # PREDICTORS EARNING ANNOUNCEMENTS ----------------------------------------
        # create predictors from earnings announcements
        events[, `:=`(
          nincr = frollsum(eps > epsEstimated, 4, na.rm = TRUE),
          nincr_half = frollsum(eps > epsEstimated, 2, na.rm = TRUE),
          nincr_2y = frollsum(eps > epsEstimated, 8, na.rm = TRUE),
          nincr_3y = frollsum(eps > epsEstimated, 12, na.rm = TRUE),
          eps_diff = (eps - epsEstimated + 0.00001) / (epsEstimated + 0.00001)
        ), by = symbol]

        # OHLCV PREDICTORS --------------------------------------------------------
        # momentum
        setorder(prices, "symbol", "date") # order, to be sure
        prices[, mom1w := close / shift(close, 5) - 1, by = symbol]
        prices[, mom1w_lag := shift(close, 10) / shift(close, 5) - 1, by = symbol]
        prices[, mom1m := close / shift(close, 22) - 1, by = symbol]
        prices[, mom1m_lag := shift(close, 22) / shift(close, 44) - 1, by = symbol]
        prices[, mom6m := close / shift(close, 22 * 6) - 1, by = symbol]
        prices[, mom6m_lag := shift(close, 22) / shift(close, 22 * 7) - 1, by = symbol]
        prices[, mom12m := close / shift(close, 22 * 12) - 1, by = symbol]
        prices[, mom12m_lag := shift(close, 22) / shift(close, 22 * 13) - 1, by = symbol]
        prices[, mom36m := close / shift(close, 22 * 36) - 1, by = symbol]
        prices[, mom36m_lag := shift(close, 22) / shift(close, 22 * 37) - 1, by = symbol]
        prices[, chmom := mom6m / shift(mom6m, 22) - 1, by = symbol]
        prices[, chmom_lag := shift(mom6m, 22) / shift(mom6m, 44) - 1, by = symbol]

        # maximum and minimum return
        prices[, maxret_3y := roll::roll_max(returns, trading_year * 3), by = symbol]
        prices[, maxret_3y := roll::roll_max(returns, trading_year * 3), by = symbol]
        prices[, maxret_2y := roll::roll_max(returns, trading_year * 2), by = symbol]
        prices[, maxret := roll::roll_max(returns, trading_year), by = symbol]
        prices[, maxret_half := roll::roll_max(returns, trading_halfyear), by = symbol]
        prices[, maxret_quarter := roll::roll_max(returns, trading_quarter), by = symbol]
        prices[, minret_3y := roll::roll_min(returns, trading_year * 3), by = symbol]
        prices[, minret_2y := roll::roll_min(returns, trading_year * 2), by = symbol]
        prices[, minret := roll::roll_min(returns, trading_year), by = symbol]
        prices[, minret_half := roll::roll_min(returns, trading_halfyear), by = symbol]
        prices[, minret_quarter := roll::roll_min(returns, trading_quarter), by = symbol]

        # sector momentum
        sector_returns <- prices[, .(sec_returns = weighted_mean(returns, marketCap, na.rm = TRUE)), by = .(sector, date)]
        sector_returns[, secmom := frollapply(sec_returns, 22, function(x) prod(1 + x) - 1), by = sector]
        sector_returns[, secmom_lag := shift(secmom, 22), by = sector]
        sector_returns[, secmom6m := frollapply(sec_returns, 22, function(x) prod(1 + x) - 1), by = sector]
        sector_returns[, secmom6m_lag := shift(secmom6m, 22), by = sector]
        sector_returns[, secmomyear := frollapply(sec_returns, 22 * 12, function(x) prod(1 + x) - 1), by = sector]
        sector_returns[, secmomyear_lag := shift(secmomyear, 22), by = sector]
        sector_returns[, secmom2year := frollapply(sec_returns, 22 * 24, function(x) prod(1 + x) - 1), by = sector]
        sector_returns[, secmom2year_lag := shift(secmom2year, 22), by = sector]
        sector_returns[, secmom3year := frollapply(sec_returns, 22 * 36, function(x) prod(1 + x) - 1), by = sector]
        sector_returns[, secmom3year_lag := shift(secmom3year, 22), by = sector]

        # industry momentum
        ind_returns = prices[, .(indret = weighted_mean(returns, marketCap, na.rm = TRUE)), by = .(industry, date)]
        ind_returns[, indmom := frollapply(indret, 22, function(x) prod(1 + x) - 1), by = industry]
        ind_returns[, indmom_lag := shift(indmom, 22), by = industry]
        ind_returns[, indmom6m := frollapply(indret, 22, function(x) prod(1 + x) - 1), by = industry]
        ind_returns[, indmom6m_lag := shift(indmom6m, 22), by = industry]
        ind_returns[, indmomyear := frollapply(indret, 22 * 12, function(x) prod(1 + x) - 1), by = industry]
        ind_returns[, indmomyear_lag := shift(indmomyear, 22), by = industry]
        ind_returns[, indmom2year := frollapply(indret, 22 * 24, function(x) prod(1 + x) - 1), by = industry]
        ind_returns[, indmom2year_lag := shift(indmom2year, 22), by = industry]
        ind_returns[, indmom3year := frollapply(indret, 22 * 36, function(x) prod(1 + x) - 1), by = industry]
        ind_returns[, indmom3year_lag := shift(indmom3year, 22), by = industry]

        # merge sector and industry predictors to prices
        prices <- sector_returns[prices, on = c("sector", "date")]
        prices <- ind_returns[prices, on = c("industry", "date")]
        
        # volatility
        prices[, dvolume := volume * close]
        prices[, `:=`(
          dolvol = frollsum(dvolume, 22),
          dolvol_halfyear = frollsum(dvolume, trading_halfyear),
          dolvol_year = frollsum(dvolume, trading_year),
          dolvol_2year = frollsum(dvolume, trading_year * 2),
          dolvol_3year = frollsum(dvolume, trading_year * 3)
        ), by = symbol]
        prices[, `:=`(
          dolvol_lag = shift(dolvol, 22),
          dolvol_halfyear_lag = frollsum(dolvol_halfyear, 22),
          dolvol_year_lag = frollsum(dolvol_year, 22)
        ), by = symbol]

        # illiquidity
        prices[, `:=`(
          illiquidity = abs(returns) / volume, #  Illiquidity (Amihud's illiquidity)
          illiquidity_month = frollsum(abs(returns), 22) / frollsum(volume, 22),
          illiquidity_half_year = frollsum(abs(returns), trading_halfyear) /
            frollsum(volume, trading_halfyear),
          illiquidity_year = frollsum(abs(returns), trading_year) / frollsum(volume, trading_year),
          illiquidity_2year = frollsum(abs(returns), trading_year * 2) / frollsum(volume, trading_year * 2),
          illiquidity_3year = frollsum(abs(returns), trading_year * 3) / frollsum(volume, trading_year * 3)
        ), by = symbol]

        # Share turnover
        prices[, `:=`(
          share_turnover = volume / weightedAverageShsOut,
          turn = frollsum(volume, 22) / weightedAverageShsOut,
          turn_half_year = frollsum(volume, trading_halfyear) / weightedAverageShsOut,
          turn_year = frollsum(volume, trading_year) / weightedAverageShsOut,
          turn_2year = frollsum(volume, trading_year * 2) / weightedAverageShsOut,
          turn_3year = frollsum(volume, trading_year * 2) / weightedAverageShsOut
        ), by = symbol]

        # Std of share turnover
        prices[, `:=`(
          std_turn = roll::roll_sd(share_turnover, 22),
          std_turn_6m = roll::roll_sd(share_turnover, trading_halfyear),
          std_turn_1y = roll::roll_sd(share_turnover, trading_year)
        ), by = symbol]

        # return volatility
        prices[, `:=`(
          retvol = roll::roll_sd(returns, width = 22),
          retvol3m = roll::roll_sd(returns, width = 22 * 3),
          retvol6m = roll::roll_sd(returns, width = 22 * 6),
          retvol1y = roll::roll_sd(returns, width = 22 * 12),
          retvol2y = roll::roll_sd(returns, width = 22 * 12 * 2),
          retvol3y = roll::roll_sd(returns, width = 22 * 12 * 3)
        ), by= symbol]
        prices[, `:=`(
          retvol_lag = shift(retvol, 22),
          retvol3m_lag = shift(retvol3m, 22),
          retvol6m_lag = shift(retvol6m, 22),
          retvol1y_lag = shift(retvol1y, 22)
        ), by = symbol]

        # idiosyncratic volatility
        weekly_market_returns <- prices[, .(market_returns = mean(returns, na.rm = TRUE)),
                                           by = date]
        weekly_market_returns[, market_returns := frollapply(market_returns, 22, function(x) prod(1+x)-1)]
        prices <- weekly_market_returns[prices, on = "date"]
        id_vol <- na.omit(prices, cols = c("market_returns", "mom1w"))
        id_vol <- id_vol[, .(date, e = QuantTools::roll_lm(market_returns, mom1w, trading_year * 3)[, 3][[1]]),
                         by = symbol]
        id_vol[, `:=`(
          idvol = roll::roll_sd(e, 22),
          idvol3m = roll::roll_sd(e, 22 * 3),
          idvol6m = roll::roll_sd(e, 22 * 6),
          idvol1y = roll::roll_sd(e, trading_year),
          idvol2y = roll::roll_sd(e, trading_year * 2),
          idvol3y = roll::roll_sd(e, trading_year * 3)
        )]
        prices <- id_vol[prices, on = c("symbol", "date")]

        # GU AT ALL ---------------------------------------------------------------
        # help for finding columns in fundamentals
        colnames(fundamentals)[grep("book", colnames(fundamentals), ignore.case = TRUE)]
        
        # create Gu et al predictors
        fundamentals[, `:=`(
          # open source AP
          am = totalAssets / marketCap,
          # Gu st all
          agr = assetGrowth,          # asset growth
          bm = 1 / pbRatio,
          cash = cashAndCashEquivalents / totalAssets, # cash holdings
          chcsho = weightedAverageSharesGrowth,        # change in shares outstanding
          currat = currentRatio,      # current ratio
          dy = dividendYield,         # dividend to price
          ep = 1 / peRatio,           # earnings to price,
          lgr = longTermDebt / shift(longTermDebt) - 1, # growth in long term debt
          mvel1 = marketCap,          # size
          pchcurrat = currentRatio / shift(currentRatio) - 1,  # % change in current ratio
          pchdepr = depreciationAndAmortization / shift(depreciationAndAmortization) - 1, # % change in depreciation
          rd = ResearchAndDevelopmentExpenses / shift(ResearchAndDevelopmentExpenses) - 1, # R&D increase
          rd_mve = ResearchAndDevelopmentExpenses / marketCap, # R&D to market capitalization
          rd_sale = ResearchAndDevelopmentExpenses / revenue,  # R&D to sales
          salecash = revenue / cashAndCashEquivalents,  # sales to cash
          saleinv = revenue / inventory,  # sales to inventory
          salerec = revenue / averageReceivables,  # sales to receivables
          sgr = revenueGrowth,        # sales growth
          sp = 1 / priceToSalesRatio  # sales to price,
        ), by = symbol]
        cols <- colnames(fundamentals)[which(colnames(fundamentals) == "agr"):ncol(fundamentals)]
        fundamentals[, (cols) := lapply(.SD, function(x) ifelse(is.nan(x), 0, x)), .SDcols = cols]

        # MACRO -------------------------------------------------------------------
        # fred help function
        print("Macro data")
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

        # volume of growth / decline stocks
        vol_52 <- prices[, .(symbol, date, high, low, close, volume)]
        vol_52[, return_52w := close / shift(close, n = 52 * 5) - 1, by = "symbol"]
        vol_52[return_52w > 0, return_52w_dummy := "up", by = "symbol"]
        vol_52[return_52w <= 0, return_52w_dummy := "down", by = "symbol"]
        vol_52 <- vol_52[!is.na(return_52w_dummy)]
        vol_52_indicators <- vol_52[, .(volume = sum(volume, na.rm = TRUE)), by = c("return_52w_dummy", "date")]
        setorderv(vol_52_indicators, c("return_52w_dummy", "date"))
        vol_52_indicators <- dcast(vol_52_indicators, date ~ return_52w_dummy, value.var = "volume")
        vol_52_indicators[, volume_down_up_ratio := down / up]
        vol_52_indicators <- vol_52_indicators[, .(date, volume_down_up_ratio)]

        # TODO: ADD SP500 stocks after Matej finish his task!
        # net expansion
        welch_goyal <- prices[, .(symbol, date, marketCap, returns)]
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
        print("Dividend data")
        date_cols = c("date", "recordDate", "paymentDate", "declarationDate")
        dividends[, (date_cols) := lapply(.SD, as.Date), .SDcols = date_cols]
        dividends <- na.omit(dividends, cols = c("adjDividend", "date"))
        dividends = dividends[symbol %in% sp500_symbols]

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

        # series without vintage days
        print("FRED data")
        sp500 <- get_fred("SP500", "sp500", TRUE)
        oil <- get_fred("DCOILWTICO", "oil", TRUE)
        vix <- get_fred(id = "VIXCLS", name = "vix")
        t10y2y <- get_fred("T10Y2Y", "t10y2y")

        # series with vintage days
        tbl <- get_fred_vintage("TB3MS", "tbl")
        welch_goyal <- tbl[welch_goyal, on = c("date"), roll = Inf]


        # MERGE PREDICTORS --------------------------------------------------------
        # merge prices, earnings announcements, fundamentals and  macro vars
        macro <- Reduce(function(x, y) merge(x, y, by = "date", all.x = TRUE, all.y = FALSE),
                        list(welch_goyal, vol_52_indicators,
                             vix, sp500, oil, t10y2y))

        return(list(prices_factos = prices,
                    fundamental_factors = fundamentals,
                    macro = macro))
      }
    }
  )
)
