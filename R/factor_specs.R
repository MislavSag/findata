.default_factor_zoo_specs = function() {
  data.table::data.table(
    factor = c(
      "book_to_market", "market_cap", "gross_profitability",
      "operating_profitability", "roa", "roe", "cfo_to_assets",
      "fcf_to_assets", "fcf_yield", "cashflow_to_price", "accruals",
      "noa", "asset_growth", "investment_to_assets", "sales_to_assets",
      "asset_turnover", "leverage", "debt_to_equity", "current_ratio",
      "net_margin", "operating_margin", "share_issuance",
      "net_share_repurchases", "piotroski_f_score", "qmj_score",
      "cash_generation_score"
    ),
    characteristic = c(
      "book_to_market", "market_cap", "gross_profitability",
      "operating_profitability", "roa", "roe", "cfo_to_assets",
      "fcf_to_assets", "fcf_yield", "cashflow_to_price", "accruals",
      "noa", "asset_growth", "investment_to_assets", "sales_to_assets",
      "asset_turnover", "leverage", "debt_to_equity", "current_ratio",
      "net_margin", "operating_margin", "share_issuance",
      "net_share_repurchases", "piotroski_f_score", "qmj_score",
      "cash_generation_score"
    ),
    direction = c(
      "high_minus_low", "low_minus_high", "high_minus_low",
      "high_minus_low", "high_minus_low", "high_minus_low",
      "high_minus_low", "high_minus_low", "high_minus_low",
      "high_minus_low", "low_minus_high", "low_minus_high",
      "low_minus_high", "low_minus_high", "high_minus_low",
      "high_minus_low", "low_minus_high", "low_minus_high",
      "high_minus_low", "high_minus_low", "high_minus_low",
      "low_minus_high", "high_minus_low", "high_minus_low",
      "high_minus_low", "high_minus_low"
    ),
    category = c(
      "value", "size", "profitability", "profitability", "profitability",
      "profitability", "cash_flow", "cash_flow", "cash_flow", "cash_flow",
      "accruals", "investment", "investment", "investment", "efficiency",
      "efficiency", "leverage", "leverage", "liquidity", "profitability",
      "profitability", "issuance", "issuance", "quality", "quality",
      "cash_flow"
    ),
    sector_neutral = FALSE
  )
}

#' Return factor zoo specifications
#'
#' @return A `data.table` with factor specifications.
#' @export
factor_zoo_specs = function() {
  path = system.file("factor_specs", "factor_zoo.csv", package = "findata")
  if (nzchar(path) && file.exists(path)) {
    specs = data.table::fread(path)
    if ("sector_neutral" %in% names(specs)) {
      specs[, sector_neutral := as.logical(sector_neutral)]
    }
    return(specs)
  }
  .default_factor_zoo_specs()
}

#' Return a small Fama-French style spec table
#'
#' @return A `data.table`.
#' @export
fama_french_specs = function() {
  path = system.file("factor_specs", "fama_french.csv", package = "findata")
  if (nzchar(path) && file.exists(path)) {
    return(data.table::fread(path))
  }
  data.table::data.table(
    factor = c("hml", "smb", "rmw", "cma"),
    characteristic = c("book_to_market", "market_cap",
                       "operating_profitability", "asset_growth"),
    direction = c("high_minus_low", "low_minus_high",
                  "high_minus_low", "low_minus_high"),
    category = c("value", "size", "profitability", "investment"),
    sector_neutral = FALSE
  )
}

