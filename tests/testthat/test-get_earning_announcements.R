# tests/testthat/test-get_earning_announcements.R
library(testthat)
library(data.table)
library(arrow)
library(withr)

# Factory that builds an FMP subclass instance with a mocked private$get
make_fmp_with_get <- function(payload_rows) {
  force(payload_rows)
  R6::R6Class(
    "FMP_Mock",
    inherit = FMP,
    private = list(
      get = function(tag, params = list()) {
        testthat::expect_equal(tag, "earnings-calendar")
        # Return a copy so callers don't mutate shared state
        data.table::copy(payload_rows)
      }
    )
  )$new(api_key = "TEST")
}

test_that("fresh run (start_date = NULL) writes parquet with cleaned/unique dates", {
  pth <- local_tempfile(fileext = ".parquet")
  
  mock_rows <- data.table(
    symbol = c("AAPL", "AAPL", "MSFT"),
    date = c("2024-01-10", "2024-01-10", "2024-01-11"), # duplicate AAPL/day to test unique()
    lastUpdated = c("2024-01-12", "2024-01-12", "2024-01-13"),
    updatedFromDate = c("2024-01-09", "2024-01-09", "2024-01-10")
  )
  
  fmp <- make_fmp_with_get(mock_rows)
  
  # No date mocking; mock get returns deterministic rows
  expect_invisible(
    fmp$get_earning_announcements(path = pth, start_date = NULL)
  )
  
  expect_true(file.exists(pth))
  got <- as.data.table(arrow::read_parquet(pth))
  
  # Dates coerced to Date
  expect_s3_class(got$date, "Date")
  expect_s3_class(got$lastUpdated, "Date")
  
  # Duplicates removed by (symbol, date)
  setorder(got, symbol, date)
  expect_equal(nrow(got), 2L)
  expect_equal(got$symbol, c("AAPL","MSFT"))
  expect_equal(got$date, as.Date(c("2024-01-10","2024-01-11")))
})

test_that("incremental update merges existing + new by (date, symbol, updatedFromDate)", {
  pth <- local_tempfile(fileext = ".parquet")
  
  # Seed existing parquet (simulate prior run)
  existing <- data.table(
    symbol = "AAPL",
    date = as.Date("2024-01-08"),
    lastUpdated = as.Date("2024-01-09"),
    updatedFromDate = "2024-01-07"
  )
  arrow::write_parquet(existing, pth)
  
  # Prepare new rows: one duplicate same (date,symbol,updatedFromDate), one new
  new_rows <- data.table(
    symbol = c("AAPL", "MSFT"),
    date = c("2024-01-08", "2024-01-12"),
    lastUpdated = c("2024-01-10", "2024-01-13"),
    updatedFromDate = c("2024-01-07", "2024-01-11")
  )
  
  fmp <- make_fmp_with_get(new_rows)
  
  expect_invisible(
    fmp$get_earning_announcements(path = pth, start_date = as.Date("2024-01-01"))
  )
  
  got <- as.data.table(arrow::read_parquet(pth))
  setorder(got, symbol, date, updatedFromDate)
  
  # We should have 2 rows total: existing AAPL 2024-01-08 (dup eliminated)
  # and new MSFT 2024-01-12
  expect_equal(nrow(got), 2L)
  expect_true(any(got$symbol == "AAPL" & got$date == as.Date("2024-01-08") & got$updatedFromDate == "2024-01-07"))
  expect_true(any(got$symbol == "MSFT" & got$date == as.Date("2024-01-12") & got$updatedFromDate == "2024-01-11"))
  
  # lastUpdated coerced
  expect_s3_class(got$lastUpdated, "Date")
})

test_that("incremental update with no new data returns NULL and does not overwrite", {
  pth <- local_tempfile(fileext = ".parquet")
  
  # Seed existing parquet
  existing <- data.table(
    symbol = "AAPL",
    date = as.Date("2024-01-08"),
    lastUpdated = as.Date("2024-01-09"),
    updatedFromDate = "2024-01-07"
  )
  arrow::write_parquet(existing, pth)
  
  # Mock returns empty result
  empty_rows <- data.table(
    symbol = character(),
    date = character(),
    lastUpdated = character(),
    updatedFromDate = character()
  )
  
  fmp <- make_fmp_with_get(empty_rows)
  
  expect_null(
    fmp$get_earning_announcements(path = pth, start_date = as.Date("2024-01-10"))
  )
  
  got <- as.data.table(arrow::read_parquet(pth))
  expect_equal(got, existing)
})

test_that("helper get_ea coercion & duplicate drop via the public method", {
  pth <- local_tempfile(fileext = ".parquet")
  
  rows <- data.table(
    symbol = c("AAPL","AAPL"),
    date = c("2024-01-10","2024-01-10"),
    lastUpdated = c("2024-01-12","2024-01-12"),
    updatedFromDate = c("2024-01-09","2024-01-09")
  )
  
  fmp <- make_fmp_with_get(rows)
  
  expect_invisible(
    fmp$get_earning_announcements(path = pth, start_date = NULL)
  )
  
  got <- as.data.table(arrow::read_parquet(pth))
  expect_equal(nrow(got), 1L)
  expect_s3_class(got$date, "Date")
  expect_s3_class(got$lastUpdated, "Date")
})
