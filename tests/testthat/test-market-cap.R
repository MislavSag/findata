test_that("daily market cap rolls shares outstanding without looking ahead", {
  prices_dir = withr::local_tempdir()
  data.table::fwrite(
    data.table::data.table(
      date = as.Date(c("2021-01-10", "2021-01-20", "2021-03-05")),
      adjusted_close = c(10, 11, 12)
    ),
    file.path(prices_dir, "AAA.US.csv")
  )
  qf = data.table::data.table(
    symbol = "AAA.US",
    fiscal_date_ending = as.Date(c("2020-12-31", "2021-03-31")),
    fund_available_date = as.Date(c("2021-01-15", "2021-03-01")),
    commonStockSharesOutstanding = c(10, 20)
  )

  dmc = build_daily_market_cap(prices_dir, qf)

  expect_true(is.na(dmc[date == as.Date("2021-01-10"), shares_outstanding]))
  expect_equal(dmc[date == as.Date("2021-01-20"), shares_outstanding], 10)
  expect_equal(dmc[date == as.Date("2021-01-20"), market_cap], 110)
  expect_equal(dmc[date == as.Date("2021-03-05"), shares_outstanding], 20)
  expect_true(all(is.na(dmc$market_cap_source_date) |
                    dmc$market_cap_source_date <= dmc$date))
  expect_true(validate_market_cap_coverage(dmc))
})

test_that("market cap outlier audit is explicit", {
  dmc = data.table::data.table(
    symbol = "AAA.US",
    date = as.Date("2021-01-01") + 0:2,
    adjusted_close = c(10, 1000, 11),
    shares_outstanding = 10,
    market_cap = c(100, 10000, 110),
    market_cap_source_date = as.Date("2020-12-31")
  )
  flagged = flag_market_cap_outliers(dmc, max_abs_log_change = log(20))
  expect_true(flagged[date == as.Date("2021-01-02"), market_cap_outlier])

  cleaned = clean_market_cap_outliers(dmc, flagged)
  expect_true(is.na(cleaned[date == as.Date("2021-01-02"), market_cap]))
})

