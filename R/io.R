.findata_output_path = function(output_dir, file_name) {
  file.path(output_dir, file_name)
}

.write_findata_parquet = function(x, output_dir, file_name) {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(x, .findata_output_path(output_dir, file_name))
  invisible(x)
}

.read_findata_parquet = function(output_dir, file_name) {
  path = .findata_output_path(output_dir, file_name)
  if (!file.exists(path)) {
    stop("File does not exist: ", path, call. = FALSE)
  }
  data.table::as.data.table(arrow::read_parquet(path))
}

#' Read a findata security master table
#'
#' @param output_dir Directory containing `security_master.parquet`.
#'
#' @return A `data.table`.
#' @export
read_security_master = function(output_dir) {
  .read_findata_parquet(output_dir, "security_master.parquet")
}

#' Read a findata quarterly fundamentals table
#'
#' @param output_dir Directory containing `quarterly_fundamentals.parquet`.
#'
#' @return A `data.table`.
#' @export
read_quarterly_fundamentals = function(output_dir) {
  .read_findata_parquet(output_dir, "quarterly_fundamentals.parquet")
}

#' Read a findata daily market cap table
#'
#' @param output_dir Directory containing `daily_market_cap.parquet`.
#'
#' @return A `data.table`.
#' @export
read_daily_market_cap = function(output_dir) {
  .read_findata_parquet(output_dir, "daily_market_cap.parquet")
}

#' Read a findata monthly returns table
#'
#' @param output_dir Directory containing `monthly_returns.parquet`.
#'
#' @return A `data.table`.
#' @export
read_monthly_returns = function(output_dir) {
  .read_findata_parquet(output_dir, "monthly_returns.parquet")
}

#' Read a findata monthly characteristics table
#'
#' @param output_dir Directory containing `monthly_characteristics.parquet`.
#'
#' @return A `data.table`.
#' @export
read_monthly_characteristics = function(output_dir) {
  .read_findata_parquet(output_dir, "monthly_characteristics.parquet")
}

#' Read findata factor returns
#'
#' @param output_dir Directory containing `factor_returns.parquet` or
#'   `factor_returns.csv`.
#'
#' @return A `data.table`.
#' @export
read_factor_returns = function(output_dir) {
  parquet_path = .findata_output_path(output_dir, "factor_returns.parquet")
  csv_path = .findata_output_path(output_dir, "factor_returns.csv")
  if (file.exists(parquet_path)) {
    return(data.table::as.data.table(arrow::read_parquet(parquet_path)))
  }
  if (file.exists(csv_path)) {
    return(data.table::fread(csv_path))
  }
  stop("Cannot find factor_returns.parquet or factor_returns.csv in: ", output_dir,
       call. = FALSE)
}

