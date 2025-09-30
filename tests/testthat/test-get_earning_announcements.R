# tests/testthat/test-get_earning_announcements_integration.R
library(testthat)
library(data.table)
library(arrow)
library(withr)

test_that("get_earning_announcements runs and writes parquet (live smoke test)", {
  skip_on_cran()
  skip_if(Sys.getenv("APIKEY-FMPCLOUD") == "", "No API key set in APIKEY-FMPCLOUD")
  
  # Use a tiny window to avoid heavy API usage
  start_date <- Sys.Date() - 3
  
  # Temp parquet path
  pth <- local_tempfile(fileext = ".parquet")
  
  # Seed an empty parquet so the function's incremental branch can read it safely
  empty_dt <- data.table(
    symbol = character(),
    date = as.Date(character()),
    lastUpdated = as.Date(character()),
    updatedFromDate = character()
  )
  arrow::write_parquet(empty_dt, pth)
  
  # Initialize FMP (pass base_url explicitly to avoid NULL override in initialize)
  fmp <- FMP$new(
    api_key = Sys.getenv("APIKEY-FMPCLOUD"),
    base_url = "https://financialmodelingprep.com/stable/"
  )
  
  # Call the function (incremental branch)
  expect_invisible(
    fmp$get_earning_announcements(path = pth, start_date = start_date)
  )
  
  # Verify parquet exists and has expected core columns
  expect_true(file.exists(pth))
  got <- as.data.table(arrow::read_parquet(pth))
  expect_true(all(c("symbol", "date") %in% names(got)))
  
  # Types/coercions
  expect_s3_class(got$date, "Date")
  if ("lastUpdated" %in% names(got)) {
    expect_s3_class(got$lastUpdated, "Date")
  }
  
  # Basic sanity: no duplicate (symbol,date) if API cooperates
  if (all(c("symbol", "date") %in% names(got)) && nrow(got) > 0) {
    dup_check <- got[, .N, by = .(symbol, date)][N > 1L]
    expect_equal(nrow(dup_check), 0L)
  }
})

test_that("second incremental call does not error and preserves data", {
  skip_on_cran()
  skip_if(Sys.getenv("APIKEY-FMPCLOUD") == "", "No API key set in APIKEY-FMPCLOUD")
  
  start_date <- Sys.Date() - 3
  pth <- local_tempfile(fileext = ".parquet")
  
  # Seed empty parquet with required columns
  empty_dt <- data.table(
    symbol = character(),
    date = as.Date(character()),
    lastUpdated = as.Date(character()),
    updatedFromDate = character()
  )
  arrow::write_parquet(empty_dt, pth)
  
  fmp <- FMP$new(
    api_key = Sys.getenv("APIKEY-FMPCLOUD"),
    base_url = "https://financialmodelingprep.com/stable/"
  )
  
  # First run
  expect_invisible(
    fmp$get_earning_announcements(path = pth, start_date = start_date)
  )
  before <- as.data.table(arrow::read_parquet(pth))
  
  # Second run with same small window (should be idempotent or append non-duplicates)
  expect_invisible(
    fmp$get_earning_announcements(path = pth, start_date = start_date)
  )
  after <- as.data.table(arrow::read_parquet(pth))
  
  # Should not shrink; usually stays the same or grows slightly
  expect_gte(nrow(after), nrow(before))
  
  # Core columns still present
  expect_true(all(c("symbol", "date") %in% names(after)))
})
