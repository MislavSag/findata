.fundamentals_shares_outstanding = function(quarterly_fundamentals) {
  q = data.table::as.data.table(data.table::copy(quarterly_fundamentals))
  q[, fund_available_date := .findata_as_date(fund_available_date)]
  q[, shares_outstanding := .findata_coalesce_numeric(
    .SD,
    c(
      "shares_outstanding",
      "commonStockSharesOutstanding",
      "commonStockSharesIssued",
      "ordinarySharesNumber",
      "shareIssued",
      "weightedAverageShsOut",
      "weightedAverageSharesOutstandingBasic",
      "weightedAverageSharesOutstandingDiluted"
    )
  )]
  q = q[!is.na(fund_available_date) & !is.na(shares_outstanding) &
          shares_outstanding > 0]
  q[, market_cap_source_date := fund_available_date]
  q = q[, .(symbol, fund_available_date, market_cap_source_date,
            shares_outstanding)]
  data.table::setorder(q, symbol, fund_available_date)
  q = q[, .SD[.N], by = .(symbol, fund_available_date)]
  q
}

#' Build daily market capitalization from prices and shares outstanding
#'
#' @param prices_dir Directory containing EODHD daily price files.
#' @param quarterly_fundamentals Quarterly fundamentals panel.
#' @param security_master Optional security master used to restrict symbols.
#'
#' @return A `data.table` with daily market cap.
#' @export
build_daily_market_cap = function(prices_dir,
                                  quarterly_fundamentals,
                                  security_master = NULL) {
  symbols = NULL
  if (!is.null(security_master)) {
    symbols = unique(as.character(security_master$symbol))
  }
  prices = read_eodhd_prices(prices_dir, symbols = symbols)
  prices_join = data.table::copy(prices)
  data.table::setnames(prices_join, "date", "fund_available_date")

  shares = .fundamentals_shares_outstanding(quarterly_fundamentals)
  data.table::setkey(shares, symbol, fund_available_date)
  data.table::setkey(prices_join, symbol, fund_available_date)

  out = shares[prices_join, roll = Inf]
  data.table::setnames(out, "fund_available_date", "date")
  out[, market_cap := adjusted_close * shares_outstanding]
  out = out[, .(
    symbol,
    date,
    adjusted_close,
    shares_outstanding,
    market_cap,
    market_cap_source_date
  )]
  out = out[!is.na(date)]
  data.table::setorder(out, symbol, date)
  validate_unique_keys(out, c("symbol", "date"))
  if (any(!is.na(out$market_cap_source_date) & out$market_cap_source_date > out$date)) {
    stop("Daily market cap contains shares outstanding from the future.",
         call. = FALSE)
  }
  out
}

#' Flag extreme daily market cap jumps
#'
#' @param daily_market_cap Daily market cap table.
#' @param max_abs_log_change Maximum absolute log market cap change allowed.
#'
#' @return Input table with outlier flag columns.
#' @export
flag_market_cap_outliers = function(daily_market_cap,
                                    max_abs_log_change = log(10)) {
  dt = data.table::as.data.table(data.table::copy(daily_market_cap))
  data.table::setorder(dt, symbol, date)
  dt[, market_cap_log_change := log(market_cap / data.table::shift(market_cap)),
     by = symbol]
  dt[, market_cap_outlier := is.finite(market_cap_log_change) &
       abs(market_cap_log_change) > max_abs_log_change]
  dt[, market_cap_outlier_reason := data.table::fifelse(
    market_cap_outlier,
    paste0("abs_log_change_gt_", signif(max_abs_log_change, 4L)),
    NA_character_
  )]
  dt
}

#' Clean flagged market cap outliers
#'
#' This function is explicit by design. The pipeline should write the audit table
#' returned by [flag_market_cap_outliers()] before cleaning.
#'
#' @param daily_market_cap Daily market cap table.
#' @param outliers Optional flagged outlier table.
#' @param method Cleaning method. `"set_na"` keeps rows and blanks market cap;
#'   `"remove"` removes flagged rows.
#'
#' @return A cleaned `data.table`.
#' @export
clean_market_cap_outliers = function(daily_market_cap,
                                     outliers = NULL,
                                     method = c("set_na", "remove")) {
  method = match.arg(method)
  dt = data.table::as.data.table(data.table::copy(daily_market_cap))
  if (is.null(outliers)) {
    outliers = flag_market_cap_outliers(dt)
  }
  flags = data.table::as.data.table(outliers)[
    market_cap_outlier == TRUE,
    .(symbol, date, market_cap_outlier)
  ]
  if (nrow(flags) == 0L) {
    return(dt)
  }
  dt = flags[dt, on = c("symbol", "date")]
  dt[is.na(market_cap_outlier), market_cap_outlier := FALSE]
  if (method == "remove") {
    dt = dt[market_cap_outlier == FALSE]
  } else {
    dt[market_cap_outlier == TRUE, market_cap := NA_real_]
  }
  dt[, market_cap_outlier := NULL]
  dt
}

