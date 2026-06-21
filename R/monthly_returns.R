#' Build monthly returns from adjusted daily prices
#'
#' @param prices_dir Directory containing EODHD daily price files.
#'
#' @return A `data.table` with `symbol`, `month`, `ret`, and
#'   `price_month_end`.
#' @export
build_monthly_returns = function(prices_dir) {
  prices = read_eodhd_prices(prices_dir)
  prices[, month := .findata_month_start(date)]
  data.table::setorder(prices, symbol, month, date)
  monthly = prices[, .SD[.N], by = .(symbol, month)]
  monthly = monthly[, .(
    symbol,
    month,
    ret = adjusted_close / data.table::shift(adjusted_close) - 1,
    price_month_end = adjusted_close,
    price_month_end_date = date
  ), by = symbol]
  data.table::setorder(monthly, symbol, month)
  validate_unique_keys(monthly, c("symbol", "month"))
  monthly
}

