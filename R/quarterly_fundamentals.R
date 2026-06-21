.eodhd_scalar_row = function(row) {
  keep = vapply(row, function(value) {
    is.null(value) || (!is.list(value) && length(value) <= 1L)
  }, logical(1L))
  row = row[keep]
  if (length(row) == 0L) {
    return(NULL)
  }
  row = lapply(row, function(value) {
    if (is.null(value) || length(value) == 0L) {
      NA
    } else {
      value[[1L]]
    }
  })
  data.table::as.data.table(row)
}

.eodhd_statement_quarterly = function(x, symbol, statement_name) {
  quarterly = .eodhd_get(x, c("Financials", statement_name, "quarterly"), NULL)
  if (is.null(quarterly)) {
    quarterly = .eodhd_get(x, c(statement_name, "quarterly"), NULL)
  }
  if (is.null(quarterly) || length(quarterly) == 0L) {
    return(NULL)
  }
  rows = lapply(seq_along(quarterly), function(i) {
    row = quarterly[[i]]
    if (!is.list(row)) {
      return(NULL)
    }
    dt = .eodhd_scalar_row(row)
    if (is.null(dt)) {
      return(NULL)
    }
    if (!"date" %in% names(dt) && !is.null(names(quarterly)[i]) &&
        nzchar(names(quarterly)[i])) {
      dt[, date := names(quarterly)[i]]
    }
    dt[, symbol := symbol]
    dt[, statement := statement_name]
    dt
  })
  data.table::rbindlist(rows, fill = TRUE)
}

.eodhd_normalize_statement = function(dt, default_reporting_lag_days) {
  if (is.null(dt) || nrow(dt) == 0L) {
    return(NULL)
  }
  dt = data.table::copy(dt)
  data.table::setnames(dt, names(dt), make.names(names(dt), unique = TRUE))
  dt[, fiscal_date_ending := .findata_coalesce_date(
    .SD,
    c("fiscal_date_ending", "fiscalDateEnding", "date", "Date")
  )]
  dt[, filing_date := .findata_coalesce_date(
    .SD,
    c("filing_date", "filingDate", "fillingDate", "acceptedDate", "accepted_date")
  )]
  dt[, report_date := .findata_coalesce_date(
    .SD,
    c("report_date", "reportDate", "date", "Date")
  )]
  dt[, period := .findata_coalesce_character(
    .SD,
    c("period", "Period", "type")
  )]
  dt[, currency := .findata_coalesce_character(
    .SD,
    c("currency", "currency_symbol", "currencySymbol", "reportedCurrency")
  )]
  dt[, fund_available_date := data.table::fcoalesce(
    filing_date,
    report_date + default_reporting_lag_days
  )]

  meta_cols = c(
    "symbol", "statement", "fiscal_date_ending", "fund_available_date",
    "filing_date", "report_date", "period", "currency"
  )
  source_meta = c(
    "date", "Date", "fiscalDateEnding", "fiscal_date_ending",
    "filing_date", "filingDate", "fillingDate", "acceptedDate",
    "accepted_date", "report_date", "reportDate", "period", "Period",
    "type", "currency", "currency_symbol", "currencySymbol",
    "reportedCurrency"
  )
  measure_cols = setdiff(names(dt), unique(c(meta_cols, source_meta)))
  for (col in measure_cols) {
    numeric_col = .findata_numeric(dt[[col]])
    non_missing_original = sum(!is.na(dt[[col]]))
    non_missing_numeric = sum(!is.na(numeric_col))
    if (non_missing_original == 0L || non_missing_numeric >= non_missing_original * 0.8) {
      data.table::set(dt, j = col, value = numeric_col)
    }
  }
  dt
}

.eodhd_one_quarterly_panel = function(file, default_reporting_lag_days) {
  x = .eodhd_read_json(file)
  if (is.null(x)) {
    return(NULL)
  }
  symbol = .eodhd_file_symbol(file)
  statements = c("Balance_Sheet", "Income_Statement", "Cash_Flow")
  statement_data = lapply(statements, function(statement) {
    .eodhd_statement_quarterly(x, symbol, statement)
  })
  statement_data = lapply(statement_data, .eodhd_normalize_statement,
                          default_reporting_lag_days = default_reporting_lag_days)
  statement_data = Filter(Negate(is.null), statement_data)
  if (length(statement_data) == 0L) {
    return(NULL)
  }

  meta = data.table::rbindlist(lapply(statement_data, function(dt) {
    dt[, .(
      symbol,
      fiscal_date_ending,
      fund_available_date,
      filing_date,
      report_date,
      period,
      currency
    )]
  }), fill = TRUE)
  meta = meta[!is.na(fiscal_date_ending)]
  data.table::setorder(meta, symbol, fiscal_date_ending, fund_available_date, filing_date)
  meta = meta[, .SD[.N], by = .(symbol, fiscal_date_ending)]

  measure_tables = lapply(statement_data, function(dt) {
    common_cols = c(
      "statement", "filing_date", "report_date",
      "period", "currency"
    )
    source_meta = c(
      "date", "Date", "fiscalDateEnding", "fiscal_date_ending",
      "filing_date", "filingDate", "fillingDate", "acceptedDate",
      "accepted_date", "report_date", "reportDate", "period", "Period",
      "type", "currency", "currency_symbol", "currencySymbol",
      "reportedCurrency"
    )
    measure_cols = setdiff(names(dt), unique(c(common_cols, source_meta)))
    measure_cols = unique(c(
      "symbol", "fiscal_date_ending", "fund_available_date",
      setdiff(measure_cols, c("symbol", "fiscal_date_ending", "fund_available_date"))
    ))
    out = dt[, ..measure_cols]
    out = out[!is.na(fiscal_date_ending)]
    data.table::setorder(out, symbol, fiscal_date_ending, fund_available_date)
    out = out[, .SD[.N], by = .(symbol, fiscal_date_ending)]
    out[, fund_available_date := NULL]
    out
  })

  measures = Reduce(function(x, y) {
    merge(x, y, by = c("symbol", "fiscal_date_ending"), all = TRUE,
          suffixes = c("", ".dup"))
  }, measure_tables)
  duplicate_cols = grep("\\.dup$", names(measures), value = TRUE)
  if (length(duplicate_cols) > 0L) {
    duplicate_cols = unique(duplicate_cols)
    measures[, (duplicate_cols) := NULL]
  }
  merge(meta, measures, by = c("symbol", "fiscal_date_ending"), all.x = TRUE)
}

#' Build a quarterly fundamentals panel from EODHD JSON files
#'
#' @param fundamentals_dir Directory containing raw EODHD fundamentals JSON files.
#' @param security_master Optional security master used to restrict symbols.
#' @param default_reporting_lag_days Lag used when a filing date is missing.
#'
#' @return A `data.table` keyed by `symbol` and `fiscal_date_ending`.
#' @export
build_quarterly_fundamentals = function(fundamentals_dir,
                                        security_master = NULL,
                                        default_reporting_lag_days = 90L) {
  files = .eodhd_fundamental_files(fundamentals_dir)
  if (!is.null(security_master)) {
    keep = unique(as.character(security_master$symbol))
    files = files[.eodhd_file_symbol(files) %in% keep]
  }
  if (length(files) == 0L) {
    stop("No fundamentals JSON files left after applying security master filter.",
         call. = FALSE)
  }
  panel = data.table::rbindlist(
    lapply(files, .eodhd_one_quarterly_panel,
           default_reporting_lag_days = default_reporting_lag_days),
    fill = TRUE
  )
  if (nrow(panel) == 0L) {
    stop("No quarterly fundamentals could be parsed.", call. = FALSE)
  }
  panel = panel[!is.na(fiscal_date_ending)]
  data.table::setorder(panel, symbol, fiscal_date_ending, fund_available_date)
  panel = panel[, .SD[.N], by = .(symbol, fiscal_date_ending)]
  validate_unique_keys(panel, c("symbol", "fiscal_date_ending"))
  panel
}
