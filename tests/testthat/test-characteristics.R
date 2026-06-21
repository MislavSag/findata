test_that("security master and quarterly fundamentals parse EODHD JSON", {
  fundamentals_dir = withr::local_tempdir()
  jsonlite::write_json(
    list(
      General = list(
        Code = "AAA",
        Exchange = "NYSE",
        CountryISO = "USA",
        CurrencyCode = "USD",
        Type = "Common Stock",
        Sector = "Industrials",
        Industry = "Machinery",
        Name = "AAA Inc",
        ISIN = "US0000000001",
        CUSIP = "000000001"
      ),
      Financials = list(
        Balance_Sheet = list(
          quarterly = list(
            "2020-12-31" = list(
              date = "2020-12-31",
              filing_date = "2021-02-15",
              currency_symbol = "USD",
              totalAssets = "1000",
              totalStockholderEquity = "500",
              commonStockSharesOutstanding = "10"
            )
          )
        ),
        Income_Statement = list(
          quarterly = list(
            "2020-12-31" = list(
              date = "2020-12-31",
              filing_date = "2021-02-15",
              totalRevenue = "200",
              grossProfit = "100",
              netIncome = "40"
            )
          )
        ),
        Cash_Flow = list(
          quarterly = list(
            "2020-12-31" = list(
              date = "2020-12-31",
              filing_date = "2021-02-15",
              totalCashFromOperatingActivities = "50",
              capitalExpenditures = "-10"
            )
          )
        )
      )
    ),
    file.path(fundamentals_dir, "AAA.US.json"),
    auto_unbox = TRUE
  )

  sm = build_security_master(fundamentals_dir)
  expect_equal(sm$symbol, "AAA.US")
  expect_equal(filter_common_us_stocks(sm)$symbol, "AAA.US")

  qf = build_quarterly_fundamentals(fundamentals_dir, sm)
  expect_equal(qf$fiscal_date_ending, as.Date("2020-12-31"))
  expect_equal(qf$fund_available_date, as.Date("2021-02-15"))
  expect_equal(qf$totalAssets, 1000)
  expect_equal(qf$totalRevenue, 200)
  expect_equal(qf$totalCashFromOperatingActivities, 50)
})

test_that("safe_divide protects tiny denominators", {
  expect_equal(safe_divide(c(1, 1), c(2, 0)), c(0.5, NA_real_))
  expect_equal(safe_divide(1, 1e-10), NA_real_)
})

