test_that("validate_no_lookahead stops on future fundamentals", {
  bad = data.table::data.table(
    symbol = "AAA.US",
    month = as.Date("2021-02-01"),
    fund_available_date = as.Date("2021-02-15"),
    signal_date = as.Date("2021-01-31")
  )
  expect_error(validate_no_lookahead(bad), "Look-ahead validation failed")
})

test_that("monthly characteristics use only data available before signal date", {
  qf = data.table::data.table(
    symbol = "AAA.US",
    fiscal_date_ending = as.Date("2020-12-31"),
    fund_available_date = as.Date("2021-02-15"),
    filing_date = as.Date("2021-02-15"),
    report_date = as.Date("2020-12-31"),
    period = "Q4",
    currency = "USD",
    totalAssets = 1000,
    totalStockholderEquity = 500,
    totalLiab = 500,
    totalRevenue = 200,
    grossProfit = 100,
    operatingIncome = 60,
    netIncome = 40,
    totalCashFromOperatingActivities = 50,
    capitalExpenditures = -10,
    totalCurrentAssets = 300,
    totalCurrentLiabilities = 150,
    shortLongTermDebtTotal = 200,
    cash = 100,
    commonStockSharesOutstanding = 10
  )
  dmc = data.table::data.table(
    symbol = "AAA.US",
    date = as.Date(c("2021-01-29", "2021-02-26")),
    adjusted_close = c(100, 200),
    shares_outstanding = 10,
    market_cap = c(1000, 2000),
    market_cap_source_date = as.Date("2021-02-15")
  )
  returns = data.table::data.table(
    symbol = "AAA.US",
    month = as.Date(c("2021-02-01", "2021-03-01")),
    ret = c(0.01, 0.02),
    price_month_end = c(100, 102),
    price_month_end_date = as.Date(c("2021-02-26", "2021-03-31"))
  )
  chars = build_monthly_characteristics(qf, dmc, returns)

  expect_true(is.na(chars[month == as.Date("2021-02-01"), fund_available_date]))
  march = chars[month == as.Date("2021-03-01")]
  expect_equal(march$fund_available_date, as.Date("2021-02-15"))
  expect_equal(march$signal_date, as.Date("2021-02-28"))
  expect_equal(march$book_to_market, 500 / 2000)
  expect_true(validate_no_lookahead(chars))
})

