#' Build a security master from EODHD fundamentals JSON files
#'
#' @param fundamentals_dir Directory containing raw EODHD fundamentals JSON files.
#'
#' @return A `data.table` with one row per symbol.
#' @export
build_security_master = function(fundamentals_dir) {
  files = .eodhd_fundamental_files(fundamentals_dir)

  rows = lapply(files, function(file) {
    x = .eodhd_read_json(file)
    if (is.null(x)) {
      return(NULL)
    }
    general = x[["General"]]
    if (is.null(general)) {
      general = list()
    }
    file_symbol = .eodhd_file_symbol(file)
    base_symbol = as.character(.eodhd_get(x, c("General", "Code"), NA_character_))
    symbol = file_symbol
    if (is.na(symbol) || !nzchar(symbol)) {
      symbol = base_symbol
    }
    exchange = as.character(data.table::fcoalesce(
      .eodhd_get(x, c("General", "ExchangeShortName"), NA_character_),
      .eodhd_get(x, c("General", "Exchange"), NA_character_)
    ))
    country = as.character(data.table::fcoalesce(
      .eodhd_get(x, c("General", "CountryName"), NA_character_),
      .eodhd_get(x, c("General", "CountryISO"), NA_character_)
    ))
    country[country %in% c("US", "United States", "United States of America")] = "USA"
    data.table::data.table(
      symbol = symbol,
      base_symbol = data.table::fcoalesce(base_symbol, sub("\\..*$", "", symbol)),
      exchange = exchange,
      country = country,
      currency = as.character(data.table::fcoalesce(
        .eodhd_get(x, c("General", "CurrencyCode"), NA_character_),
        .eodhd_get(x, c("General", "CurrencyName"), NA_character_)
      )),
      security_type = as.character(.eodhd_get(x, c("General", "Type"), NA_character_)),
      sector = as.character(.eodhd_get(x, c("General", "Sector"), NA_character_)),
      industry = as.character(.eodhd_get(x, c("General", "Industry"), NA_character_)),
      name = as.character(.eodhd_get(x, c("General", "Name"), NA_character_)),
      isin = as.character(.eodhd_get(x, c("General", "ISIN"), NA_character_)),
      cusip = as.character(.eodhd_get(x, c("General", "CUSIP"), NA_character_))
    )
  })

  security_master = data.table::rbindlist(rows, fill = TRUE)
  if (nrow(security_master) == 0L) {
    stop("No security metadata could be parsed from fundamentals JSON files.",
         call. = FALSE)
  }
  security_master = unique(security_master, by = "symbol")
  data.table::setorder(security_master, symbol)
  security_master
}

#' Filter a security master to common US stocks
#'
#' @param security_master Security master returned by [build_security_master()].
#' @param keep_security_types Security types to keep.
#' @param keep_country Country values to keep.
#' @param keep_exchanges Exchange values to keep.
#'
#' @return A filtered `data.table`.
#' @export
filter_common_us_stocks = function(security_master,
                                   keep_security_types = "Common Stock",
                                   keep_country = "USA",
                                   keep_exchanges = c("NYSE", "NASDAQ", "AMEX")) {
  dt = data.table::as.data.table(data.table::copy(security_master))
  if ("security_type" %in% names(dt)) {
    dt = dt[security_type %in% keep_security_types]
  }
  if ("country" %in% names(dt)) {
    dt = dt[country %in% keep_country]
  }
  if ("exchange" %in% names(dt)) {
    dt = dt[exchange %in% keep_exchanges]
  }
  dt
}
