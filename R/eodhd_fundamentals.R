#' EODHD fundamentals pipeline coordinator
#'
#' The class coordinates the EODHD pipeline and writes stable package tables.
#' Heavy logic lives in standalone functions.
#'
#' @export
EodhdFundamentals = R6::R6Class(
  "EodhdFundamentals",
  public = list(
    fundamentals_dir = NULL,
    prices_dir = NULL,
    output_dir = NULL,

    initialize = function(fundamentals_dir, prices_dir, output_dir) {
      self$fundamentals_dir = fundamentals_dir
      self$prices_dir = prices_dir
      self$output_dir = output_dir
      invisible(self)
    },

    build_security_master = function(filter_common_us = FALSE, ...) {
      sm = get("build_security_master", envir = asNamespace("findata"))(
        self$fundamentals_dir,
        ...
      )
      if (isTRUE(filter_common_us)) {
        sm = filter_common_us_stocks(sm)
      }
      .write_findata_parquet(sm, self$output_dir, "security_master.parquet")
    },

    build_quarterly_fundamentals = function(security_master = NULL, ...) {
      if (is.null(security_master)) {
        security_master = private$read_if_exists("security_master.parquet")
      }
      q = get("build_quarterly_fundamentals", envir = asNamespace("findata"))(
        self$fundamentals_dir,
        security_master = security_master,
        ...
      )
      .write_findata_parquet(q, self$output_dir, "quarterly_fundamentals.parquet")
    },

    build_daily_market_cap = function(quarterly_fundamentals = NULL,
                                      security_master = NULL,
                                      write_outlier_audit = TRUE,
                                      clean_outliers = FALSE,
                                      ...) {
      if (is.null(quarterly_fundamentals)) {
        quarterly_fundamentals = private$read_if_exists("quarterly_fundamentals.parquet")
      }
      if (is.null(security_master)) {
        security_master = private$read_if_exists("security_master.parquet")
      }
      dmc = get("build_daily_market_cap", envir = asNamespace("findata"))(
        self$prices_dir,
        quarterly_fundamentals = quarterly_fundamentals,
        security_master = security_master
      )
      outliers = flag_market_cap_outliers(dmc, ...)
      if (isTRUE(write_outlier_audit)) {
        dir.create(self$output_dir, recursive = TRUE, showWarnings = FALSE)
        data.table::fwrite(
          outliers[market_cap_outlier == TRUE],
          file.path(self$output_dir, "market_cap_outliers.csv")
        )
      }
      if (isTRUE(clean_outliers)) {
        dmc = clean_market_cap_outliers(dmc, outliers = outliers)
      }
      validate_market_cap_coverage(dmc)
      .write_findata_parquet(dmc, self$output_dir, "daily_market_cap.parquet")
    },

    build_monthly_returns = function() {
      returns = get("build_monthly_returns", envir = asNamespace("findata"))(
        self$prices_dir
      )
      .write_findata_parquet(returns, self$output_dir, "monthly_returns.parquet")
    },

    build_monthly_characteristics = function(quarterly_fundamentals = NULL,
                                             daily_market_cap = NULL,
                                             monthly_returns = NULL,
                                             security_master = NULL) {
      if (is.null(quarterly_fundamentals)) {
        quarterly_fundamentals = private$read_if_exists("quarterly_fundamentals.parquet")
      }
      if (is.null(daily_market_cap)) {
        daily_market_cap = private$read_if_exists("daily_market_cap.parquet")
      }
      if (is.null(monthly_returns)) {
        monthly_returns = private$read_if_exists("monthly_returns.parquet")
      }
      if (is.null(security_master)) {
        security_master = private$read_if_exists("security_master.parquet")
      }
      chars = get("build_monthly_characteristics", envir = asNamespace("findata"))(
        quarterly_fundamentals = quarterly_fundamentals,
        daily_market_cap = daily_market_cap,
        monthly_returns = monthly_returns,
        security_master = security_master
      )
      .write_findata_parquet(chars, self$output_dir, "monthly_characteristics.parquet")
    },

    build_all = function(filter_common_us = FALSE, clean_market_cap_outliers = FALSE) {
      sm = self$build_security_master(filter_common_us = filter_common_us)
      q = self$build_quarterly_fundamentals(security_master = sm)
      dmc = self$build_daily_market_cap(
        quarterly_fundamentals = q,
        security_master = sm,
        clean_outliers = clean_market_cap_outliers
      )
      returns = self$build_monthly_returns()
      self$build_monthly_characteristics(
        quarterly_fundamentals = q,
        daily_market_cap = dmc,
        monthly_returns = returns,
        security_master = sm
      )
    }
  ),
  private = list(
    read_if_exists = function(file_name) {
      path = file.path(self$output_dir, file_name)
      if (!file.exists(path)) {
        return(NULL)
      }
      data.table::as.data.table(arrow::read_parquet(path))
    }
  )
)

