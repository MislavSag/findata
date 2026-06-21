.findata_as_date = function(x) {
  if (inherits(x, "Date")) {
    return(x)
  }
  if (inherits(x, c("POSIXct", "POSIXlt"))) {
    return(as.Date(x))
  }
  if (is.numeric(x)) {
    return(as.Date(x, origin = "1970-01-01"))
  }
  x = trimws(as.character(x))
  x[x %in% c("", "NA", "N/A", "null", "NULL", "0000-00-00")] = NA_character_
  suppressWarnings(as.Date(x))
}

.findata_month_start = function(date) {
  date = .findata_as_date(date)
  as.Date(format(date, "%Y-%m-01"))
}

.findata_month_end = function(date) {
  month = .findata_month_start(date)
  as.Date(seq(month, by = "month", length.out = 2L)[2L]) - 1L
}

.findata_previous_month_end = function(month) {
  .findata_month_start(month) - 1L
}

