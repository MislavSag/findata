
test_that("construction", {
  fmp <- FMP$new()
  expect_class(fmp, "FMP")
})

test_that("historical dividends works", {
  fmp <- FMP$new()
  x <- fmp$get_historical_dividends("AAPL")
  expect_s3_class(x, "data.frame")
  expect_length(x, 8)
  cols <- c("symbol", "date", "label", "adjDividend", "dividend", "recordDate",
            "paymentDate", "declarationDate")
  expect_true(all(colnames(x) == cols))
})
