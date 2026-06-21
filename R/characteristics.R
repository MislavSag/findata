#' Safely divide numeric vectors
#'
#' @param x Numerator.
#' @param y Denominator.
#' @param min_abs_denominator Denominators with smaller absolute value are
#'   treated as missing.
#'
#' @return Numeric vector.
#' @export
safe_divide = function(x, y, min_abs_denominator = 1e-8) {
  x = as.numeric(x)
  y = as.numeric(y)
  out = x / y
  out[is.na(y) | abs(y) < min_abs_denominator] = NA_real_
  out[!is.finite(out)] = NA_real_
  out
}

.roll_source_to_target = function(source, target, source_date, target_date,
                                  by = "symbol") {
  source = data.table::as.data.table(data.table::copy(source))
  target = data.table::as.data.table(data.table::copy(target))
  source[, .join_date := .findata_as_date(get(source_date))]
  target[, .join_date := .findata_as_date(get(target_date))]
  source = source[!is.na(.join_date)]
  data.table::setorderv(source, c(by, ".join_date"))
  source = source[, .SD[.N], by = c(by, ".join_date")]
  data.table::setkeyv(source, c(by, ".join_date"))
  data.table::setkeyv(target, c(by, ".join_date"))
  out = source[target, roll = Inf]
  out[, .join_date := NULL]
  out
}

.quarterly_characteristics = function(quarterly_fundamentals) {
  q = data.table::as.data.table(data.table::copy(quarterly_fundamentals))
  for (col in c("fiscal_date_ending", "fund_available_date", "filing_date",
                "report_date", "period", "currency")) {
    if (!col %in% names(q)) {
      q[, (col) := NA]
    }
  }
  q[, fiscal_date_ending := .findata_as_date(fiscal_date_ending)]
  q[, fund_available_date := .findata_as_date(fund_available_date)]
  q[, filing_date := .findata_as_date(filing_date)]
  q[, report_date := .findata_as_date(report_date)]
  data.table::setorder(q, symbol, fiscal_date_ending, fund_available_date)
  q = q[, .SD[.N], by = .(symbol, fiscal_date_ending)]

  q[, assets := .findata_coalesce_numeric(
    .SD,
    c("totalAssets", "total_assets", "assets")
  )]
  q[, equity := .findata_coalesce_numeric(
    .SD,
    c("totalStockholderEquity", "totalShareholderEquity",
      "stockholdersEquity", "commonStockTotalEquity", "total_equity",
      "shareholders_equity")
  )]
  q[, liabilities := .findata_coalesce_numeric(
    .SD,
    c("totalLiab", "totalLiabilities", "total_liabilities", "liabilities")
  )]
  q[, revenue := .findata_coalesce_numeric(
    .SD,
    c("totalRevenue", "revenue", "sales")
  )]
  q[, cost_of_revenue := .findata_coalesce_numeric(
    .SD,
    c("costOfRevenue", "cost_of_revenue", "costOfGoodsSold", "cogs")
  )]
  q[, gross_profit := .findata_coalesce_numeric(
    .SD,
    c("grossProfit", "gross_profit")
  )]
  q[, selling_general_admin := .findata_coalesce_numeric(
    .SD,
    c("sellingGeneralAdministrative", "sellingGeneralAndAdministrative",
      "selling_general_administrative", "sellingAndMarketingExpenses")
  )]
  q[, interest_expense := .findata_coalesce_numeric(
    .SD,
    c("interestExpense", "interest_expense")
  )]
  q[, operating_income := .findata_coalesce_numeric(
    .SD,
    c("operatingIncome", "operating_income")
  )]
  q[, net_income := .findata_coalesce_numeric(
    .SD,
    c("netIncome", "net_income", "netIncomeCommonStockholders")
  )]
  q[, operating_cash_flow := .findata_coalesce_numeric(
    .SD,
    c("totalCashFromOperatingActivities", "cashFromOperatingActivities",
      "netCashProvidedByOperatingActivities", "operatingCashFlow",
      "operating_cash_flow")
  )]
  q[, capital_expenditures := .findata_coalesce_numeric(
    .SD,
    c("capitalExpenditures", "capital_expenditures", "capitalExpenditure")
  )]
  q[, free_cash_flow := .findata_coalesce_numeric(
    .SD,
    c("freeCashFlow", "free_cash_flow")
  )]
  q[is.na(free_cash_flow), free_cash_flow := operating_cash_flow + capital_expenditures]
  q[, cash := .findata_coalesce_numeric(
    .SD,
    c("cash", "cashAndCashEquivalents", "cashAndShortTermInvestments")
  )]
  q[, current_assets := .findata_coalesce_numeric(
    .SD,
    c("totalCurrentAssets", "current_assets")
  )]
  q[, current_liabilities := .findata_coalesce_numeric(
    .SD,
    c("totalCurrentLiabilities", "current_liabilities")
  )]
  q[, inventory := .findata_coalesce_numeric(.SD, c("inventory", "Inventory"))]
  q[, debt := .findata_coalesce_numeric(
    .SD,
    c("shortLongTermDebtTotal", "totalDebt", "shortLongTermDebt",
      "longTermDebtTotal", "longTermDebt", "shortTermDebt")
  )]
  q[, shares_outstanding := .findata_coalesce_numeric(
    .SD,
    c("shares_outstanding", "commonStockSharesOutstanding",
      "commonStockSharesIssued", "ordinarySharesNumber", "shareIssued",
      "weightedAverageShsOut", "weightedAverageSharesOutstandingBasic",
      "weightedAverageSharesOutstandingDiluted")
  )]
  q[, stock_repurchases := abs(.findata_coalesce_numeric(
    .SD,
    c("repurchaseOfStock", "repurchaseOfCommonStock",
      "paymentsForRepurchaseOfCommonStock", "stockRepurchase")
  ))]
  q[, stock_issuance := .findata_coalesce_numeric(
    .SD,
    c("issuanceOfStock", "issuanceOfCommonStock",
      "proceedsFromIssuanceOfCommonStock", "commonStockIssued")
  )]

  data.table::setorder(q, symbol, fiscal_date_ending, fund_available_date)
  q[, assets_lag := data.table::shift(assets), by = symbol]
  q[, equity_lag := data.table::shift(equity), by = symbol]
  q[, liabilities_lag := data.table::shift(liabilities), by = symbol]
  q[, current_ratio_lag := data.table::shift(safe_divide(current_assets, current_liabilities)),
    by = symbol]
  q[, shares_lag := data.table::shift(shares_outstanding), by = symbol]
  q[, gross_margin := safe_divide(gross_profit, revenue)]
  q[, gross_margin_lag := data.table::shift(gross_margin), by = symbol]
  q[, asset_turnover_base := safe_divide(revenue, assets)]
  q[, asset_turnover_lag := data.table::shift(asset_turnover_base), by = symbol]
  q[, roa_lag := data.table::shift(safe_divide(net_income, assets)), by = symbol]

  operating_profit = q$operating_income
  fallback_operating_profit = q$revenue - q$cost_of_revenue -
    q$selling_general_admin - q$interest_expense
  q[, operating_profit := data.table::fcoalesce(operating_profit,
                                                fallback_operating_profit)]
  q[, operating_assets := assets - cash]
  q[, operating_liabilities := liabilities - debt]
  q[, net_operating_assets := operating_assets - operating_liabilities]
  q[, net_operating_assets_lag := data.table::shift(net_operating_assets),
    by = symbol]

  q[, gross_profitability := safe_divide(gross_profit, assets)]
  q[, operating_profitability := safe_divide(operating_profit, equity)]
  q[, roa := safe_divide(net_income, assets)]
  q[, roe := safe_divide(net_income, equity)]
  q[, cfo_to_assets := safe_divide(operating_cash_flow, assets)]
  q[, fcf_to_assets := safe_divide(free_cash_flow, assets)]
  q[, accruals := safe_divide(net_income - operating_cash_flow, assets)]
  q[, noa := safe_divide(net_operating_assets, assets)]
  q[, asset_growth := safe_divide(assets, assets_lag) - 1]
  q[, investment_to_assets := safe_divide(assets - assets_lag, assets_lag)]
  q[, sales_to_assets := safe_divide(revenue, assets)]
  q[, asset_turnover := safe_divide(revenue, assets_lag)]
  q[, leverage := safe_divide(liabilities, assets)]
  q[, leverage_lag := data.table::shift(leverage), by = symbol]
  q[, debt_to_equity := safe_divide(debt, equity)]
  q[, current_ratio := safe_divide(current_assets, current_liabilities)]
  q[, net_margin := safe_divide(net_income, revenue)]
  q[, operating_margin := safe_divide(operating_income, revenue)]
  q[, share_issuance := safe_divide(shares_outstanding, shares_lag) - 1]
  q[, net_share_repurchases := safe_divide(stock_repurchases - stock_issuance, assets)]
  q[is.na(net_share_repurchases), net_share_repurchases := -share_issuance]

  q[, piotroski_f_score :=
      .findata_score_signal(roa > 0) +
      .findata_score_signal(operating_cash_flow > 0) +
      .findata_score_signal(roa > roa_lag) +
      .findata_score_signal(operating_cash_flow > net_income) +
      .findata_score_signal(leverage < leverage_lag) +
      .findata_score_signal(current_ratio > current_ratio_lag) +
      .findata_score_signal(is.na(share_issuance) | share_issuance <= 0) +
      .findata_score_signal(gross_margin > gross_margin_lag) +
      .findata_score_signal(asset_turnover_base > asset_turnover_lag),
    by = symbol]

  keep_cols = c(
    "symbol", "fiscal_date_ending", "fund_available_date", "filing_date",
    "report_date", "period", "currency", "assets", "equity",
    "shares_outstanding", "gross_profitability", "operating_profitability",
    "roa", "roe", "cfo_to_assets", "fcf_to_assets", "accruals", "noa",
    "asset_growth", "investment_to_assets", "sales_to_assets",
    "asset_turnover", "leverage", "debt_to_equity", "current_ratio",
    "net_margin", "operating_margin", "share_issuance",
    "net_share_repurchases", "piotroski_f_score", "free_cash_flow",
    "operating_cash_flow"
  )
  q[, ..keep_cols]
}

.add_cross_sectional_scores = function(dt) {
  dt = data.table::as.data.table(dt)
  z_cols = c(
    "gross_profitability", "operating_profitability", "roa", "roe",
    "cfo_to_assets", "fcf_to_assets", "accruals", "leverage",
    "fcf_yield", "cashflow_to_price"
  )
  z_cols = z_cols[z_cols %in% names(dt)]
  for (col in z_cols) {
    z_col = paste0(".z_", col)
    dt[, (z_col) := {
      x = as.numeric(get(col))
      s = stats::sd(x, na.rm = TRUE)
      if (!is.finite(s) || s == 0) {
        rep(NA_real_, .N)
      } else {
        (x - mean(x, na.rm = TRUE)) / s
      }
    }, by = month]
  }
  qmj_inputs = c(
    ".z_gross_profitability", ".z_operating_profitability", ".z_roa",
    ".z_roe", ".z_cfo_to_assets", ".z_fcf_to_assets"
  )
  if (".z_accruals" %in% names(dt)) {
    dt[, .z_negative_accruals := -.z_accruals]
    qmj_inputs = c(qmj_inputs, ".z_negative_accruals")
  }
  if (".z_leverage" %in% names(dt)) {
    dt[, .z_negative_leverage := -.z_leverage]
    qmj_inputs = c(qmj_inputs, ".z_negative_leverage")
  }
  cash_inputs = c(".z_cfo_to_assets", ".z_fcf_to_assets",
                  ".z_fcf_yield", ".z_cashflow_to_price")
  if (".z_accruals" %in% names(dt)) {
    cash_inputs = c(cash_inputs, ".z_negative_accruals")
  }
  dt[, qmj_score := .findata_row_mean(.SD, qmj_inputs), .SDcols = names(dt)]
  dt[, cash_generation_score := .findata_row_mean(.SD, cash_inputs),
     .SDcols = names(dt)]
  remove_cols = grep("^\\.z_", names(dt), value = TRUE)
  dt[, (remove_cols) := NULL]
  dt
}

#' Build monthly characteristics without look-ahead bias
#'
#' @param quarterly_fundamentals Quarterly fundamentals panel.
#' @param daily_market_cap Daily market cap table.
#' @param monthly_returns Monthly return table.
#' @param security_master Optional security master.
#'
#' @return A `data.table` keyed by `symbol` and `month`.
#' @export
build_monthly_characteristics = function(quarterly_fundamentals,
                                         daily_market_cap,
                                         monthly_returns,
                                         security_master = NULL) {
  q = .quarterly_characteristics(quarterly_fundamentals)
  data.table::setorder(q, symbol, fund_available_date, fiscal_date_ending)
  q = q[!is.na(fund_available_date)]
  q = q[, .SD[.N], by = .(symbol, fund_available_date)]

  returns = data.table::as.data.table(data.table::copy(monthly_returns))
  if (!"price_month_end" %in% names(returns)) {
    returns[, price_month_end := NA_real_]
  }
  if (!"price_month_end_date" %in% names(returns)) {
    returns[, price_month_end_date := as.Date(NA)]
  }
  returns[, month := .findata_month_start(month)]
  returns[, signal_date := .findata_previous_month_end(month)]
  returns = returns[, .(
    symbol,
    month,
    signal_date,
    ret,
    price_month_end,
    price_month_end_date
  )]
  validate_unique_keys(returns, c("symbol", "month"))

  chars = .roll_source_to_target(
    source = q,
    target = returns,
    source_date = "fund_available_date",
    target_date = "signal_date"
  )

  market_cap = data.table::as.data.table(data.table::copy(daily_market_cap))
  market_cap[, market_cap_date := .findata_as_date(date)]
  market_cap = market_cap[, .(
    symbol,
    market_cap_date,
    market_cap,
    market_cap_source_date,
    market_cap_shares_outstanding = shares_outstanding
  )]
  chars = .roll_source_to_target(
    source = market_cap,
    target = chars,
    source_date = "market_cap_date",
    target_date = "signal_date"
  )

  chars[, book_to_market := safe_divide(equity, market_cap)]
  chars[, fcf_yield := safe_divide(free_cash_flow, market_cap)]
  chars[, cashflow_to_price := safe_divide(operating_cash_flow, market_cap)]
  chars = .add_cross_sectional_scores(chars)

  if (!is.null(security_master)) {
    sm = data.table::as.data.table(data.table::copy(security_master))
    add_cols = intersect(
      c("symbol", "base_symbol", "exchange", "country", "security_type",
        "sector", "industry", "name", "isin", "cusip"),
      names(sm)
    )
    add_cols = unique(c("symbol", setdiff(add_cols, names(chars))))
    sm = unique(sm[, ..add_cols], by = "symbol")
    chars = sm[chars, on = "symbol"]
  }

  first_cols = c(
    "symbol", "month", "signal_date", "ret", "price_month_end",
    "price_month_end_date", "fiscal_date_ending", "fund_available_date",
    "filing_date", "report_date", "period", "currency",
    "market_cap_source_date", "market_cap_date", "exchange", "sector",
    "industry", "country", "security_type"
  )
  characteristic_cols = c(
    "book_to_market", "market_cap", "gross_profitability",
    "operating_profitability", "roa", "roe", "cfo_to_assets",
    "fcf_to_assets", "fcf_yield", "cashflow_to_price", "accruals", "noa",
    "asset_growth", "investment_to_assets", "sales_to_assets",
    "asset_turnover", "leverage", "debt_to_equity", "current_ratio",
    "net_margin", "operating_margin", "share_issuance",
    "net_share_repurchases", "piotroski_f_score", "qmj_score",
    "cash_generation_score"
  )
  ordered_cols = unique(c(first_cols, characteristic_cols))
  ordered_cols = ordered_cols[ordered_cols %in% names(chars)]
  other_cols = setdiff(names(chars), ordered_cols)
  chars = chars[, c(ordered_cols, other_cols), with = FALSE]
  data.table::setorder(chars, symbol, month)
  validate_unique_keys(chars, c("symbol", "month"))
  validate_no_lookahead(chars)
  chars
}
