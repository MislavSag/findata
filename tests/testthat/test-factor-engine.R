test_that("long-short factor uses requested breakpoints and direction", {
  chars = data.table::data.table(
    symbol = sprintf("S%02d", 1:10),
    month = as.Date("2021-01-01"),
    exchange = "NYSE",
    sector = rep(c("A", "B"), each = 5),
    market_cap = 1,
    quality = 1:10,
    ret = (1:10) / 100
  )

  factor = build_long_short_factor(
    chars,
    characteristic = "quality",
    direction = "high_minus_low",
    breakpoints = c(0.3, 0.7),
    weighting = "equal",
    min_stocks_side = 1
  )

  expect_equal(factor$n_long, 3)
  expect_equal(factor$n_short, 3)
  expect_equal(factor$long_ret, mean(c(0.08, 0.09, 0.10)))
  expect_equal(factor$short_ret, mean(c(0.01, 0.02, 0.03)))
  expect_equal(factor$ret, 0.07)
})

test_that("sector-neutral factor ranks inside sectors and averages sectors", {
  chars = data.table::data.table(
    symbol = sprintf("S%02d", 1:10),
    month = as.Date("2021-01-01"),
    sector = rep(c("A", "B"), each = 5),
    market_cap = 1,
    quality = rep(1:5, 2),
    ret = c(0.01, 0.02, 0.03, 0.04, 0.05,
            0.11, 0.12, 0.13, 0.14, 0.15)
  )

  factor = build_sector_neutral_factor(
    chars,
    characteristic = "quality",
    direction = "high_minus_low",
    breakpoints = c(0.3, 0.7),
    weighting = "equal",
    min_stocks_side = 1
  )

  expect_equal(factor$n_long, 4)
  expect_equal(factor$n_short, 4)
  expect_equal(factor$long_ret, mean(c(mean(c(0.04, 0.05)), mean(c(0.14, 0.15)))))
  expect_equal(factor$short_ret, mean(c(mean(c(0.01, 0.02)), mean(c(0.11, 0.12)))))
})

