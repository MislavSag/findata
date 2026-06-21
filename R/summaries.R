#' Summarize factor returns
#'
#' @param factor_returns Factor return table.
#' @param periods_per_year Annualization frequency.
#'
#' @return A summary `data.table`.
#' @export
summarize_factor_returns = function(factor_returns, periods_per_year = 12) {
  dt = data.table::as.data.table(data.table::copy(factor_returns))
  dt[, .(
    n = sum(!is.na(ret)),
    mean = mean(ret, na.rm = TRUE),
    sd = stats::sd(ret, na.rm = TRUE),
    t_stat = {
      n_non_na = sum(!is.na(ret))
      sd_ret = stats::sd(ret, na.rm = TRUE)
      if (n_non_na > 1L && is.finite(sd_ret) && sd_ret > 0) {
        mean(ret, na.rm = TRUE) / sd_ret * sqrt(n_non_na)
      } else {
        NA_real_
      }
    },
    annualized_mean = mean(ret, na.rm = TRUE) * periods_per_year,
    annualized_vol = stats::sd(ret, na.rm = TRUE) * sqrt(periods_per_year),
    annualized_sharpe = {
      sd_ret = stats::sd(ret, na.rm = TRUE)
      if (is.finite(sd_ret) && sd_ret > 0) {
        mean(ret, na.rm = TRUE) / sd_ret * sqrt(periods_per_year)
      } else {
        NA_real_
      }
    }
  ), by = factor][order(factor)]
}

#' Build a factor correlation matrix
#'
#' @param factor_returns Factor return table.
#' @param start_date Optional first month.
#' @param use Missing-data rule passed to [stats::cor()].
#'
#' @return A correlation matrix.
#' @export
factor_correlation_matrix = function(factor_returns,
                                     start_date = NULL,
                                     use = "pairwise.complete.obs") {
  dt = data.table::as.data.table(data.table::copy(factor_returns))
  dt[, month := .findata_month_start(month)]
  if (!is.null(start_date)) {
    dt = dt[month >= .findata_month_start(start_date)]
  }
  wide = data.table::dcast(dt, month ~ factor, value.var = "ret")
  months = wide$month
  wide[, month := NULL]
  mat = as.matrix(wide)
  rownames(mat) = as.character(months)
  stats::cor(mat, use = use)
}

