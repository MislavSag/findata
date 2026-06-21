.factor_portfolio_return = function(dt, weighting) {
  if (nrow(dt) == 0L) {
    return(NA_real_)
  }
  if (weighting == "value") {
    return(.findata_weighted_mean(dt$ret, dt$market_cap))
  }
  .findata_mean(dt$ret)
}

.factor_month_result = function(d, characteristic, direction, breakpoints,
                                weighting, breakpoint_universe,
                                min_stocks_side, factor) {
  if (!characteristic %in% names(d)) {
    stop("Characteristic column not found: ", characteristic, call. = FALSE)
  }
  month_value = d$month[1L]
  d = d[is.finite(ret) & is.finite(get(characteristic))]
  if (weighting == "value") {
    d = d[is.finite(market_cap) & market_cap > 0]
  }
  if (nrow(d) == 0L) {
    return(data.table::data.table(
      month = month_value, factor = factor, ret = NA_real_,
      long_ret = NA_real_, short_ret = NA_real_, n_long = 0L, n_short = 0L
    ))
  }
  bp = d
  if (!is.null(breakpoint_universe) && "exchange" %in% names(d)) {
    bp_candidate = d[exchange %in% breakpoint_universe]
    if (nrow(bp_candidate) > 0L) {
      bp = bp_candidate
    }
  }
  qs = stats::quantile(bp[[characteristic]], probs = breakpoints,
                       na.rm = TRUE, names = FALSE, type = 7)
  if (length(qs) != 2L || any(!is.finite(qs))) {
    long = d[0]
    short = d[0]
  } else if (direction == "high_minus_low") {
    long = d[get(characteristic) >= qs[2L]]
    short = d[get(characteristic) <= qs[1L]]
  } else {
    long = d[get(characteristic) <= qs[1L]]
    short = d[get(characteristic) >= qs[2L]]
  }
  n_long = nrow(long)
  n_short = nrow(short)
  if (n_long < min_stocks_side || n_short < min_stocks_side) {
    long_ret = NA_real_
    short_ret = NA_real_
    ret = NA_real_
  } else {
    long_ret = .factor_portfolio_return(long, weighting)
    short_ret = .factor_portfolio_return(short, weighting)
    ret = long_ret - short_ret
  }
  data.table::data.table(
    month = d$month[1L],
    factor = factor,
    ret = ret,
    long_ret = long_ret,
    short_ret = short_ret,
    n_long = n_long,
    n_short = n_short
  )
}

#' Build a long-short factor return series
#'
#' @param characteristics Monthly characteristics table.
#' @param characteristic Characteristic column used for sorting.
#' @param direction `"high_minus_low"` or `"low_minus_high"`.
#' @param breakpoint_universe Exchange values used to compute breakpoints.
#' @param breakpoints Two probabilities used as low/high cutoffs.
#' @param weighting `"value"` or `"equal"`.
#' @param min_stocks_side Minimum number of stocks per side.
#' @param start_date Optional first month.
#' @param factor Optional factor name. Defaults to `characteristic`.
#'
#' @return A factor return `data.table`.
#' @export
build_long_short_factor = function(characteristics,
                                   characteristic,
                                   direction = c("high_minus_low", "low_minus_high"),
                                   breakpoint_universe = "NYSE",
                                   breakpoints = c(0.3, 0.7),
                                   weighting = c("value", "equal"),
                                   min_stocks_side = 10,
                                   start_date = NULL,
                                   factor = characteristic) {
  direction = match.arg(direction)
  weighting = match.arg(weighting)
  if (length(breakpoints) != 2L || any(breakpoints <= 0 | breakpoints >= 1) ||
      breakpoints[1L] >= breakpoints[2L]) {
    stop("breakpoints must be two increasing probabilities between 0 and 1.",
         call. = FALSE)
  }
  dt = data.table::as.data.table(data.table::copy(characteristics))
  dt[, month := .findata_month_start(month)]
  if (!is.null(start_date)) {
    dt = dt[month >= .findata_month_start(start_date)]
  }
  if (!"market_cap" %in% names(dt)) {
    dt[, market_cap := NA_real_]
  }
  data.table::setorder(dt, month, symbol)
  out = data.table::rbindlist(lapply(split(dt, dt$month), function(d) {
    .factor_month_result(
      d = data.table::as.data.table(d),
      characteristic = characteristic,
      direction = direction,
      breakpoints = breakpoints,
      weighting = weighting,
      breakpoint_universe = breakpoint_universe,
      min_stocks_side = min_stocks_side,
      factor = factor
    )
  }))
  validate_factor_returns(out)
  out
}

.sector_month_result = function(d, characteristic, direction, breakpoints,
                                weighting, min_stocks_side, factor) {
  month_value = d$month[1L]
  d = d[!is.na(sector) & is.finite(ret) & is.finite(get(characteristic))]
  if (weighting == "value") {
    d = d[is.finite(market_cap) & market_cap > 0]
  }
  if (nrow(d) == 0L) {
    return(data.table::data.table(
      month = month_value, factor = factor, ret = NA_real_,
      long_ret = NA_real_, short_ret = NA_real_, n_long = 0L, n_short = 0L
    ))
  }
  sector_returns = d[, {
    qs = stats::quantile(get(characteristic), probs = breakpoints,
                         na.rm = TRUE, names = FALSE, type = 7)
    if (length(qs) != 2L || any(!is.finite(qs))) {
      long = .SD[0]
      short = .SD[0]
    } else if (direction == "high_minus_low") {
      long = .SD[get(characteristic) >= qs[2L]]
      short = .SD[get(characteristic) <= qs[1L]]
    } else {
      long = .SD[get(characteristic) <= qs[1L]]
      short = .SD[get(characteristic) >= qs[2L]]
    }
    data.table::data.table(
      sector_long_ret = .factor_portfolio_return(long, weighting),
      sector_short_ret = .factor_portfolio_return(short, weighting),
      n_long = nrow(long),
      n_short = nrow(short)
    )
  }, by = sector]
  n_long = sum(sector_returns$n_long)
  n_short = sum(sector_returns$n_short)
  if (n_long < min_stocks_side || n_short < min_stocks_side) {
    long_ret = NA_real_
    short_ret = NA_real_
    ret = NA_real_
  } else {
    long_ret = .findata_mean(sector_returns$sector_long_ret)
    short_ret = .findata_mean(sector_returns$sector_short_ret)
    ret = long_ret - short_ret
  }
  data.table::data.table(
    month = d$month[1L],
    factor = factor,
    ret = ret,
    long_ret = long_ret,
    short_ret = short_ret,
    n_long = n_long,
    n_short = n_short
  )
}

#' Build a sector-neutral long-short factor
#'
#' @param characteristics Monthly characteristics table.
#' @param characteristic Characteristic column used for sorting.
#' @param direction `"high_minus_low"` or `"low_minus_high"`.
#' @param breakpoints Two probabilities used as low/high cutoffs.
#' @param weighting `"value"` or `"equal"` within each sector.
#' @param min_stocks_side Minimum total number of stocks per side.
#' @param start_date Optional first month.
#' @param factor Optional factor name. Defaults to `characteristic`.
#'
#' @return A factor return `data.table`.
#' @export
build_sector_neutral_factor = function(characteristics,
                                       characteristic,
                                       direction = c("high_minus_low", "low_minus_high"),
                                       breakpoints = c(0.3, 0.7),
                                       weighting = c("value", "equal"),
                                       min_stocks_side = 10,
                                       start_date = NULL,
                                       factor = characteristic) {
  direction = match.arg(direction)
  weighting = match.arg(weighting)
  dt = data.table::as.data.table(data.table::copy(characteristics))
  if (!"sector" %in% names(dt)) {
    stop("Sector-neutral factors require a sector column.", call. = FALSE)
  }
  dt[, month := .findata_month_start(month)]
  if (!is.null(start_date)) {
    dt = dt[month >= .findata_month_start(start_date)]
  }
  if (!"market_cap" %in% names(dt)) {
    dt[, market_cap := NA_real_]
  }
  data.table::setorder(dt, month, sector, symbol)
  out = data.table::rbindlist(lapply(split(dt, dt$month), function(d) {
    .sector_month_result(
      d = data.table::as.data.table(d),
      characteristic = characteristic,
      direction = direction,
      breakpoints = breakpoints,
      weighting = weighting,
      min_stocks_side = min_stocks_side,
      factor = factor
    )
  }))
  validate_factor_returns(out)
  out
}

#' Build a table of factor returns from factor specifications
#'
#' @param characteristics Monthly characteristics table.
#' @param specs Factor specification table.
#' @param start_date Optional first month.
#' @param ... Additional arguments passed to factor builders.
#'
#' @return A factor return `data.table`.
#' @export
build_factor_zoo = function(characteristics,
                            specs = factor_zoo_specs(),
                            start_date = NULL,
                            ...) {
  specs = data.table::as.data.table(data.table::copy(specs))
  required = c("factor", "characteristic", "direction")
  missing = setdiff(required, names(specs))
  if (length(missing) > 0L) {
    stop("Missing factor spec columns: ", paste(missing, collapse = ", "),
         call. = FALSE)
  }
  if (!"sector_neutral" %in% names(specs)) {
    specs[, sector_neutral := FALSE]
  }
  out = data.table::rbindlist(lapply(seq_len(nrow(specs)), function(i) {
    spec = specs[i]
    if (isTRUE(spec$sector_neutral)) {
      build_sector_neutral_factor(
        characteristics = characteristics,
        characteristic = spec$characteristic,
        direction = spec$direction,
        start_date = start_date,
        factor = spec$factor,
        ...
      )
    } else {
      build_long_short_factor(
        characteristics = characteristics,
        characteristic = spec$characteristic,
        direction = spec$direction,
        start_date = start_date,
        factor = spec$factor,
        ...
      )
    }
  }), fill = TRUE)
  validate_factor_returns(out)
  out
}
