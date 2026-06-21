#' Validate uniqueness of key columns
#'
#' @param data Data frame or data table.
#' @param keys Character vector of key columns.
#'
#' @return Invisibly returns `TRUE`.
#' @export
validate_unique_keys = function(data, keys) {
  dt = data.table::as.data.table(data)
  missing_keys = setdiff(keys, names(dt))
  if (length(missing_keys) > 0L) {
    stop("Missing key columns: ", paste(missing_keys, collapse = ", "),
         call. = FALSE)
  }
  dup = dt[, .N, by = keys][N > 1L]
  if (nrow(dup) > 0L) {
    stop("Duplicate keys found for: ", paste(keys, collapse = ", "),
         call. = FALSE)
  }
  invisible(TRUE)
}

#' Validate that fundamentals do not look ahead
#'
#' @param characteristics Monthly characteristics table.
#'
#' @return Invisibly returns `TRUE`.
#' @export
validate_no_lookahead = function(characteristics) {
  dt = data.table::as.data.table(characteristics)
  required = c("fund_available_date", "signal_date")
  missing = setdiff(required, names(dt))
  if (length(missing) > 0L) {
    stop("Missing look-ahead validation columns: ", paste(missing, collapse = ", "),
         call. = FALSE)
  }
  dt[, fund_available_date := .findata_as_date(fund_available_date)]
  dt[, signal_date := .findata_as_date(signal_date)]
  violations = dt[!is.na(fund_available_date) & !is.na(signal_date) &
                    fund_available_date > signal_date]
  if (nrow(violations) > 0L) {
    stop(
      "Look-ahead validation failed: ", nrow(violations),
      " rows have fund_available_date > signal_date.",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

#' Validate daily market cap coverage
#'
#' @param daily_market_cap Daily market cap table.
#' @param min_coverage Minimum non-missing market cap coverage.
#'
#' @return Invisibly returns `TRUE`.
#' @export
validate_market_cap_coverage = function(daily_market_cap, min_coverage = 0) {
  dt = data.table::as.data.table(daily_market_cap)
  required = c("symbol", "date", "market_cap", "market_cap_source_date")
  missing = setdiff(required, names(dt))
  if (length(missing) > 0L) {
    stop("Missing market cap columns: ", paste(missing, collapse = ", "),
         call. = FALSE)
  }
  dt[, date := .findata_as_date(date)]
  dt[, market_cap_source_date := .findata_as_date(market_cap_source_date)]
  future = dt[!is.na(market_cap_source_date) & market_cap_source_date > date]
  if (nrow(future) > 0L) {
    stop("Market cap validation failed: shares outstanding source date is after price date.",
         call. = FALSE)
  }
  coverage = mean(!is.na(dt$market_cap) & is.finite(dt$market_cap))
  if (is.finite(min_coverage) && coverage < min_coverage) {
    stop(
      "Market cap coverage is below threshold: ",
      signif(coverage, 4L), " < ", signif(min_coverage, 4L),
      call. = FALSE
    )
  }
  invisible(TRUE)
}

#' Validate factor returns
#'
#' @param factor_returns Factor return table.
#'
#' @return Invisibly returns `TRUE`.
#' @export
validate_factor_returns = function(factor_returns) {
  dt = data.table::as.data.table(factor_returns)
  required = c("month", "factor", "ret", "long_ret", "short_ret", "n_long", "n_short")
  missing = setdiff(required, names(dt))
  if (length(missing) > 0L) {
    stop("Missing factor return columns: ", paste(missing, collapse = ", "),
         call. = FALSE)
  }
  validate_unique_keys(dt, c("month", "factor"))
  if (any(dt$n_long < 0L, na.rm = TRUE) || any(dt$n_short < 0L, na.rm = TRUE)) {
    stop("Factor return validation failed: negative side counts.", call. = FALSE)
  }
  invisible(TRUE)
}

