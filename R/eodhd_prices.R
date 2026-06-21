.eodhd_price_files = function(prices_dir) {
  if (!dir.exists(prices_dir)) {
    stop("Prices directory does not exist: ", prices_dir, call. = FALSE)
  }
  files = list.files(
    prices_dir,
    pattern = "\\.(parquet|csv|csv\\.gz)$",
    recursive = TRUE,
    full.names = TRUE,
    ignore.case = TRUE
  )
  if (length(files) == 0L) {
    stop("No parquet or CSV price files found in: ", prices_dir, call. = FALSE)
  }
  files
}

.eodhd_price_file_symbol = function(file) {
  x = basename(file)
  x = sub("\\.csv\\.gz$", "", x, ignore.case = TRUE)
  x = sub("\\.parquet$", "", x, ignore.case = TRUE)
  x = sub("\\.csv$", "", x, ignore.case = TRUE)
  x
}

.canonical_price_names = function(names_x) {
  tolower(gsub("[^A-Za-z0-9]+", "_", names_x))
}

.read_one_eodhd_price_file = function(file) {
  dt = tryCatch({
    if (grepl("\\.parquet$", file, ignore.case = TRUE)) {
      data.table::as.data.table(arrow::read_parquet(file))
    } else {
      data.table::fread(file)
    }
  }, error = function(e) {
    warning("Cannot read price file: ", file, " (", conditionMessage(e), ")",
            call. = FALSE)
    NULL
  })
  if (is.null(dt) || nrow(dt) == 0L) {
    return(NULL)
  }

  original_names = names(dt)
  canonical = .canonical_price_names(original_names)
  data.table::setnames(dt, original_names, canonical)

  date_col = .findata_first_existing_name(names(dt), c("date", "datetime", "timestamp"))
  if (is.na(date_col)) {
    warning("Skipping price file without a date column: ", file, call. = FALSE)
    return(NULL)
  }
  symbol_col = .findata_first_existing_name(names(dt), c("symbol", "ticker", "code"))
  adjusted_col = .findata_first_existing_name(
    names(dt),
    c("adjusted_close", "adjustedclose", "adj_close", "adjclose", "adjusted")
  )
  close_col = .findata_first_existing_name(names(dt), c("close", "last"))
  if (is.na(adjusted_col) && is.na(close_col)) {
    warning("Skipping price file without adjusted close or close: ", file,
            call. = FALSE)
    return(NULL)
  }
  if (is.na(adjusted_col)) {
    adjusted_col = close_col
  }

  dt[, date := .findata_as_date(get(date_col))]
  if (is.na(symbol_col)) {
    dt[, symbol := .eodhd_price_file_symbol(file)]
  } else {
    dt[, symbol := as.character(get(symbol_col))]
  }
  dt[, adjusted_close := .findata_numeric(get(adjusted_col))]
  if (!is.na(close_col)) {
    dt[, close := .findata_numeric(get(close_col))]
  } else {
    dt[, close := adjusted_close]
  }
  for (col in c("open", "high", "low", "volume")) {
    if (col %in% names(dt)) {
      data.table::set(dt, j = col, value = .findata_numeric(dt[[col]]))
    } else {
      data.table::set(dt, j = col, value = NA_real_)
    }
  }
  dt = dt[!is.na(date) & !is.na(symbol)]
  dt[, .(symbol, date, open, high, low, close, adjusted_close, volume)]
}

#' Read EODHD adjusted price files
#'
#' @param prices_dir Directory containing EODHD daily price CSV or parquet files.
#' @param symbols Optional symbol vector used to filter the output.
#'
#' @return A normalized `data.table`.
#' @export
read_eodhd_prices = function(prices_dir, symbols = NULL) {
  files = .eodhd_price_files(prices_dir)
  prices = data.table::rbindlist(lapply(files, .read_one_eodhd_price_file),
                                 fill = TRUE)
  if (nrow(prices) == 0L) {
    stop("No price rows could be read from: ", prices_dir, call. = FALSE)
  }
  if (!is.null(symbols)) {
    prices = prices[symbol %in% symbols]
  }
  prices = prices[!is.na(adjusted_close) & adjusted_close > 0]
  prices = unique(prices, by = c("symbol", "date"))
  data.table::setorder(prices, symbol, date)
  prices
}

