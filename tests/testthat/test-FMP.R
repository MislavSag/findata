
test_that("construction", {
  fmp <- FMP$new()
  expect_class(fmp, "FMP")
})

test_that("historical dividends works", {
  skip_on_cran()
  skip_if(Sys.getenv("APIKEY-FMPCLOUD") == "", "No API key set in APIKEY-FMPCLOUD")

  fmp <- FMP$new(api_key = Sys.getenv("APIKEY-FMPCLOUD"))
  x <- fmp$get_historical_dividends("AAPL")
  expect_s3_class(x, "data.frame")
  cols <- c("symbol", "date", "label", "adjDividend", "dividend", "recordDate",
            "paymentDate", "declarationDate")
  expect_true(nrow(x) > 0)
  expect_identical(colnames(x), cols)
})
