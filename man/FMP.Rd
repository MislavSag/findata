% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/FMP.R
\name{FMP}
\alias{FMP}
\title{FMP Class}
\description{
Get data data from FMP Cloud.
}
\section{Super class}{
\code{\link[findata:DataAbstract]{findata::DataAbstract}} -> \code{FMP}
}
\section{Public fields}{
\if{html}{\out{<div class="r6-fields">}}
\describe{
\item{\code{api_key}}{API KEY for FMP Cloud}
}
\if{html}{\out{</div>}}
}
\section{Methods}{
\subsection{Public methods}{
\itemize{
\item \href{#method-FMP-new}{\code{FMP$new()}}
\item \href{#method-FMP-get_earning_announcements}{\code{FMP$get_earning_announcements()}}
\item \href{#method-FMP-get_transcripts}{\code{FMP$get_transcripts()}}
\item \href{#method-FMP-get_daily_tiledb}{\code{FMP$get_daily_tiledb()}}
\item \href{#method-FMP-get_intraday_equities}{\code{FMP$get_intraday_equities()}}
\item \href{#method-FMP-create_start_end_dates}{\code{FMP$create_start_end_dates()}}
\item \href{#method-FMP-get_sp500_symbols}{\code{FMP$get_sp500_symbols()}}
\item \href{#method-FMP-get_factor_files}{\code{FMP$get_factor_files()}}
\item \href{#method-FMP-get_ipo_date}{\code{FMP$get_ipo_date()}}
\item \href{#method-FMP-get_ipo_calendar_confirmed_bulk}{\code{FMP$get_ipo_calendar_confirmed_bulk()}}
\item \href{#method-FMP-get_fi_statement}{\code{FMP$get_fi_statement()}}
\item \href{#method-FMP-get_financial_metrics}{\code{FMP$get_financial_metrics()}}
\item \href{#method-FMP-get_stock_splits}{\code{FMP$get_stock_splits()}}
\item \href{#method-FMP-get_market_cap}{\code{FMP$get_market_cap()}}
\item \href{#method-FMP-get_market_cap_bulk}{\code{FMP$get_market_cap_bulk()}}
\item \href{#method-FMP-get_stock_list}{\code{FMP$get_stock_list()}}
\item \href{#method-FMP-get_symbol_changes}{\code{FMP$get_symbol_changes()}}
\item \href{#method-FMP-get_delisted_companies}{\code{FMP$get_delisted_companies()}}
\item \href{#method-FMP-get_available_traded_list}{\code{FMP$get_available_traded_list()}}
\item \href{#method-FMP-get_sp500_constituent}{\code{FMP$get_sp500_constituent()}}
\item \href{#method-FMP-get_fi_statement_bulk}{\code{FMP$get_fi_statement_bulk()}}
\item \href{#method-FMP-get_daily_v4}{\code{FMP$get_daily_v4()}}
\item \href{#method-FMP-get_grades}{\code{FMP$get_grades()}}
\item \href{#method-FMP-get_targets}{\code{FMP$get_targets()}}
\item \href{#method-FMP-get_ratings}{\code{FMP$get_ratings()}}
\item \href{#method-FMP-get_historical_dividends}{\code{FMP$get_historical_dividends()}}
\item \href{#method-FMP-get_equities_batch}{\code{FMP$get_equities_batch()}}
\item \href{#method-FMP-get_minute}{\code{FMP$get_minute()}}
\item \href{#method-FMP-get_dividends}{\code{FMP$get_dividends()}}
\item \href{#method-FMP-beneficial_ownership}{\code{FMP$beneficial_ownership()}}
\item \href{#method-FMP-clone}{\code{FMP$clone()}}
}
}
\if{html}{\out{
<details open><summary>Inherited methods</summary>
<ul>
<li><span class="pkg-link" data-pkg="findata" data-topic="DataAbstract" data-id="save_blob_files"><a href='../../findata/html/DataAbstract.html#method-DataAbstract-save_blob_files'><code>findata::DataAbstract$save_blob_files()</code></a></span></li>
</ul>
</details>
}}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-new"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-new}{}}}
\subsection{Method \code{new()}}{
Create a new FMP object.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$new(
  api_key = NULL,
  azure_storage_endpoint = NULL,
  context_with_config = NULL
)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{api_key}}{API KEY for FMP cloud data.}

\item{\code{azure_storage_endpoint}}{Azure storate endpont}

\item{\code{context_with_config}}{AWS S3 Tiledb config}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
A new `FMP` object.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_earning_announcements"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_earning_announcements}{}}}
\subsection{Method \code{get_earning_announcements()}}{
Create a new FMP object.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_earning_announcements(path, start_date = NULL)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{path}}{to save reulst data.table}

\item{\code{start_date}}{First date to scrape from.If NULL, date is 2010-01-01.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Earning announcements data.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_transcripts"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_transcripts}{}}}
\subsection{Method \code{get_transcripts()}}{
Get Earning Call Transcript from FMP cloud.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_transcripts(symbols, uri)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{symbols}}{stock symbols. If tiledburi, import symbols from FMP events.}

\item{\code{uri}}{to save transcripts data for every symbol.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Result of GET request
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_daily_tiledb"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_daily_tiledb}{}}}
\subsection{Method \code{get_daily_tiledb()}}{
get daily data from FMP cloud for all stocks
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_daily_tiledb(uri)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{uri}}{to save daily data for every date.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Scrap all daily data
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_intraday_equities"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_intraday_equities}{}}}
\subsection{Method \code{get_intraday_equities()}}{
retrieving intraday market data from FMP cloud.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_intraday_equities(
  symbol,
  multiply = 1,
  time = "day",
  from = as.character(Sys.Date() - 3),
  to = as.character(Sys.Date())
)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{symbol}}{Symbol of the stock.}

\item{\code{multiply}}{multiplier.Start date.}

\item{\code{time}}{Size of the time. (minute, hour, day, week, month, quarter, year)}

\item{\code{from}}{Start date,}

\item{\code{to}}{End date.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Data frame with ohlcv data.
#' @description Get tick quote data from FMP cloud.
#'
#' @param symbol Symbol of the stock.
#' @param date multiplier.Start date.
#'
#' @return Data frame with ohlcv data.
get_quotes = function(symbol, date) {

  # debug
  # library(httr)
  # library(data.table)
  # self = list()
  # self$api_key = Sys.getenv("APIKEY-FMPCLOUD")
  # symbol = "SPY"
  # date = "2023-08-18"
  
  # init list
  q = list()
  
  # initial GET request. Don't use RETRY here yet.
  x <- tryCatch({
    GET(paste0('https://financialmodelingprep.com/api/v4/historical-price-tick/',
               symbol, '/', date),
        query = list(limit = 30000, apikey = self$api_key),
        timeout(100))
  }, error = function(e) NA)
  x <- content(x)
  q[[1]] = rbindlist(x$results)
  
  # get last time
  last_time = tail(q[[1]]$t, 1)

  x <- tryCatch({
    GET(paste0('https://financialmodelingprep.com/api/v4/historical-price-tick/',
               symbol, '/', date),
        query = list(limit = 300, ts = last_time, te = last_time + 1000, apikey = self$api_key),
        timeout(100))
  }, error = function(e) NA)
  x <- content(x)
  rbindlist(x$results)
  q[[2]] = rbindlist(x$results)
  
  

  
  # url = "https://financialmodelingprep.com/api/v4/historical-price-tick/SPY/2023-08-04?limit=500&apikey=15cd5d0adf4bc6805a724b4417bbaafc"
  # GET(url)

  # control error
  tries <- 0
  while (all(is.na(x)) & tries < 20) {
    print("There is an error in scraping market data. Sleep and try again!")
    Sys.sleep(60L)
    x <- tryCatch({
      GET(paste0('https://financialmodelingprep.com/api/v4/historical-price/',
                 symbol, '/', multiply, '/', time, '/', from, '/', to),
          query = list(apikey = self$api_key),
          timeout(100))
    }, error = function(e) NA)
    tries <- tries + 1
  }

  # check if status is ok. If not, try to download again
  if (x$status_code == 404) {
    print("There is an 404 error!")
    return(NULL)
  } else if (x$status_code == 200) {
    x <- content(x)
    return(rbindlist(x$results))
  } else {
    x <- RETRY("GET",
               paste0('https://financialmodelingprep.com/api/v4/historical-price/',
                      symbol, '/', multiply, '/', time, '/', from, '/', to),
               query = list(apikey = self$api_key),
               times = 5,
               timeout(100))
    if (x$status_code == 200) {
      x <- content(x)
      return(rbindlist(x$results))
    } else {
      stop('Error in reposne. Status not 200 and not 404')
    }
  }
},
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-create_start_end_dates"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-create_start_end_dates}{}}}
\subsection{Method \code{create_start_end_dates()}}{
Help function for calculating start and end dates
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$create_start_end_dates(start_dates, n = 4)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{start_dates}}{Sequence of start dates.}

\item{\code{n}}{number of consecutive dates.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
list of start and end dates
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_sp500_symbols"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_sp500_symbols}{}}}
\subsection{Method \code{get_sp500_symbols()}}{
Get SP500 symbols from fmp cloud.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_sp500_symbols()}\if{html}{\out{</div>}}
}

\subsection{Returns}{
Vector of SP500 symbols.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_factor_files"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_factor_files}{}}}
\subsection{Method \code{get_factor_files()}}{
Create factor files for calculating adjusted prices (adjusted for splits and dividends).
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_factor_files(
  save_uri = "s3://equity-usa-factor-files",
  prices_uri = "s3://equity-usa-daily-fmpcloud"
)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{save_uri}}{Uri where tiledb saves files}

\item{\code{prices_uri}}{Uri to import prices from.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Factor files saved to "factor-file" blob.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_ipo_date"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_ipo_date}{}}}
\subsection{Method \code{get_ipo_date()}}{
Get IPO date from fmp cloud
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_ipo_date(ticker)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{ticker}}{Stock ticker}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Stock IPO date.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_ipo_calendar_confirmed_bulk"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_ipo_calendar_confirmed_bulk}{}}}
\subsection{Method \code{get_ipo_calendar_confirmed_bulk()}}{
Get ipo-calendar-confirmed date from fmp cloud
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_ipo_calendar_confirmed_bulk(start_date, end_date = Sys.Date())}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{start_date}}{Start date}

\item{\code{end_date}}{End date.}

\item{\code{uri}}{TileDB uri argument}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Stock IPO date.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_fi_statement"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_fi_statement}{}}}
\subsection{Method \code{get_fi_statement()}}{
Get FI data.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_fi_statement(
  symbol,
  statement = c("income-statement", "balance-sheet-statement", "cash-flow-statement"),
  period = c("annual", "quarter")
)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{symbol}}{Stock symbol.}

\item{\code{statement}}{Stock statement.}

\item{\code{period}}{Stock period.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
data.table of financial reports.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_financial_metrics"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_financial_metrics}{}}}
\subsection{Method \code{get_financial_metrics()}}{
Get financial ratios, company key metrics.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_financial_metrics(
  symbol,
  type = c("ratios", "key-metrics", "financial-growth"),
  period = c("annual", "quarter")
)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{symbol}}{Stock symbol.}

\item{\code{type}}{Type of fundamental analysis.}

\item{\code{period}}{Stock period.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
data.table of financial reports.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_stock_splits"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_stock_splits}{}}}
\subsection{Method \code{get_stock_splits()}}{
Get stock splits data from FMP cloud Prep.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_stock_splits(start_date = Sys.Date() - 5, end_date = Sys.Date())}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{start_date}}{Start date}

\item{\code{end_date}}{End date.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
data.table of stock splits
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_market_cap"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_market_cap}{}}}
\subsection{Method \code{get_market_cap()}}{
Get market cap data from FMP cloud Prep.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_market_cap(ticker, limit = 10)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{ticker}}{Ticker.}

\item{\code{limit}}{Limit.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
data.table with market cap data.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_market_cap_bulk"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_market_cap_bulk}{}}}
\subsection{Method \code{get_market_cap_bulk()}}{
Get market cap data from FMP cloud Prep - bulk.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_market_cap_bulk(symbols, uri)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{symbols}}{stock symbols.}

\item{\code{uri}}{to market cap data.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
data.table with market cap data.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_stock_list"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_stock_list}{}}}
\subsection{Method \code{get_stock_list()}}{
Get stock list.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_stock_list()}\if{html}{\out{</div>}}
}

\subsection{Returns}{
data.table with stock list data.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_symbol_changes"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_symbol_changes}{}}}
\subsection{Method \code{get_symbol_changes()}}{
Get symbol changes.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_symbol_changes()}\if{html}{\out{</div>}}
}

\subsection{Returns}{
data.table with symbol changes data.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_delisted_companies"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_delisted_companies}{}}}
\subsection{Method \code{get_delisted_companies()}}{
Get delisted companies.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_delisted_companies()}\if{html}{\out{</div>}}
}

\subsection{Returns}{
data.table with delisted companies data.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_available_traded_list"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_available_traded_list}{}}}
\subsection{Method \code{get_available_traded_list()}}{
Get available traded list.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_available_traded_list()}\if{html}{\out{</div>}}
}

\subsection{Returns}{
data.table with available traded list data.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_sp500_constituent"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_sp500_constituent}{}}}
\subsection{Method \code{get_sp500_constituent()}}{
Get SP500 stocks.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_sp500_constituent()}\if{html}{\out{</div>}}
}

\subsection{Returns}{
data.table with SP500 tocks.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_fi_statement_bulk"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_fi_statement_bulk}{}}}
\subsection{Method \code{get_fi_statement_bulk()}}{
Get Financial Statements data
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_fi_statement_bulk(
  uri,
  years = 1999:(data.table::year(Sys.Date())),
  statement = c("income-statement-bulk", "balance-sheet-statement-bulk",
    "cash-flow-statement-bulk", "ratios-bulk", "key-metrics-bulk",
    "financial-growth-bulk", "income-statement-growth-bulk",
    "balance-sheet-statement-growth-bulk", "cash-flow-statement-growth-bulk"),
  period = c("annual", "quarter")
)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{uri}}{uri path to the directory.}

\item{\code{years}}{Get statement for specific year.}

\item{\code{statement}}{quarter or annual.}

\item{\code{period}}{quarter or annual.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
data.table of financial reports.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_daily_v4"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_daily_v4}{}}}
\subsection{Method \code{get_daily_v4()}}{
Help function for getting grades and targets
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_daily_v4(symbols, tag = "price-target")}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{symbols}}{stock symbols.}

\item{\code{tag}}{url tag.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Result of GET request
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_grades"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_grades}{}}}
\subsection{Method \code{get_grades()}}{
Get Upgrades and Downgrades from FMP cloud.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_grades(symbols)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{symbols}}{stock symbols. If tiledburi, import symbols from FMP events.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Result of GET request
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_targets"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_targets}{}}}
\subsection{Method \code{get_targets()}}{
Get Price Targest from FMP cloud.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_targets(symbols)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{symbols}}{stock symbols. If tiledburi, import symbols from FMP events.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Result of GET request
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_ratings"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_ratings}{}}}
\subsection{Method \code{get_ratings()}}{
Get Ratings from FMP cloud.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_ratings(
  symbols,
  uri = "ratings.parquet",
  update = FALSE,
  update_limit = 10
)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{symbols}}{stock symbols. If tiledburi, import symbols from FMP events.}

\item{\code{uri}}{uri to save output data.}

\item{\code{update}}{Should we scrape all data od jut the new one.}

\item{\code{update_limit}}{how many days to retrieve if update eqtual to TRUE.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Result of GET request
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_historical_dividends"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_historical_dividends}{}}}
\subsection{Method \code{get_historical_dividends()}}{
Get Historical dividends from FMP cloud.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_historical_dividends(symbols)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{symbols}}{Vector of stock symbols.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Data table with historical dividends for symbol.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_equities_batch"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_equities_batch}{}}}
\subsection{Method \code{get_equities_batch()}}{
Get hour data for all history from fmp cloud and save as parquet on AWS.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_equities_batch(
  symbols,
  uri_minute = "s3://equity-usa-minute",
  deep_scan = FALSE
)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{symbols}}{Symbol of the stock.}

\item{\code{uri_minute}}{AWS S3 bucket uri or NAS path.}

\item{\code{deep_scan}}{should we test for dates with low number od observation
and try to scrap again.}

\item{\code{save_uri_hour}}{AWS S3 bucket uri for hour data.}

\item{\code{save_uri_daily}}{AWS S3 bucket uri for daily data.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Data saved to uri.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_minute"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_minute}{}}}
\subsection{Method \code{get_minute()}}{
Get minute data for all history from FMP cloud.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_minute(symbols, uri_minute, deep_scan = FALSE, workers = 1L)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{symbols}}{Symbol of the stock.}

\item{\code{uri_minute}}{AWS S3 bucket uri or NAS path.}

\item{\code{deep_scan}}{should we test for dates with low number od observation
and try to scrap again.}

\item{\code{workers}}{Number of workers.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
Data saved to uri.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-get_dividends"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-get_dividends}{}}}
\subsection{Method \code{get_dividends()}}{
Get dividend data from FMP cloud Prep.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$get_dividends(ticker)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{ticker}}{Ticker.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
data.table with dividend data.
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-beneficial_ownership"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-beneficial_ownership}{}}}
\subsection{Method \code{beneficial_ownership()}}{
Get beneficial ownership from FMP cloud.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$beneficial_ownership(ticker)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{ticker}}{Ticker.}
}
\if{html}{\out{</div>}}
}
\subsection{Returns}{
data.table with beneficial ownership data.
#' @description Get hour data for all history from fmp cloud and save as parquet on AWS.
#'
#' @param symbols Symbol of the stock.
#' @param uri_minute AWS S3 bucket uri or NAS path.
#' @param save_uri_hour AWS S3 bucket uri for hour data.
#' @param save_uri_daily AWS S3 bucket uri for daily data.
#' @param deep_scan should we test for dates with low number od observation
#'     and try to scrap again.
#'
#' @return Data saved to Azure blob.
get_equities_taq = function(symbols,
                            # uri_minute = "s3://equity-usa-minute",
                            # save_uri_hour = "s3://equity-usa-hour-fmpcloud",
                            # save_uri_daily = "s3://equity-usa-daily-fmpcloud",
                            deep_scan = FALSE) {
  
  
  
  # debug
  # library(findata)
  # library(data.table)
  # library(httr)
  # library(RcppQuantuccia)
  # library(lubridate)
  # library(nanotime)
  # library(arrow)
  # library(checkmate)
  # library(future.apply)
  # self = FMP$new()
  # symbols = c("AAPL", "SPY")
  # uri_minute = "C:/Users/Mislav/SynologyDrive/equity/usa/minute" # "s3://equity-usa-minute"
  # deep_scan = FALSE
  # fmp = FMP$new()
  # library(rvest)
  # sp100 <- read_html("https://en.wikipedia.org/wiki/S%26P_100") |>
  #   html_elements(x = _, "table") |>
  #   (`[[`)(3) |>
  #   html_table(x = _, fill = TRUE)
  # sp100_symbols <- c("SPY", "TLT", "GLD", "USO", "SVXY",
  #                    sp100$Symbol)
  # symbols <- fmp$get_sp500_symbols()
  # symbols <- na.omit(symbols)
  # symbols <- symbols[!grepl("/", symbols)]
  # symbols <- unique(union(sp100_symbols, symbols))
  
  # check if we have all necessary env variables
  assert_choice("AWS_ACCESS_KEY_ID", names(Sys.getenv()))
  assert_choice("AWS_SECRET_ACCESS_KEY", names(Sys.getenv()))
  assert_choice("AWS_DEFAULT_REGION", names(Sys.getenv()))
  
  self$get_intraday_equities(symbol,
                             multiply = 1)
  
  
  
  get_intraday_equities = function(symbol,
                                   multiply = 1,
                                   time = 'day',
                                   from = as.character(Sys.Date() - 3),
                                   to = as.character(Sys.Date())) {
    
    # initial GET request. Don't use RETRY here yet.
    x <- tryCatch({
      GET(paste0('https://financialmodelingprep.com/api/v4/historical-price/',
                 symbol, '/', multiply, '/', time, '/', from, '/', to),
          query = list(apikey = self$api_key),
          timeout(100))
    }, error = function(e) NA)
    
    # control error
    tries <- 0
    while (all(is.na(x)) & tries < 20) {
      print("There is an error in scraping market data. Sleep and try again!")
      Sys.sleep(60L)
      x <- tryCatch({
        GET(paste0('https://financialmodelingprep.com/api/v4/historical-price/',
                   symbol, '/', multiply, '/', time, '/', from, '/', to),
            query = list(apikey = self$api_key),
            timeout(100))
      }, error = function(e) NA)
      tries <- tries + 1
    }
    
    # check if status is ok. If not, try to download again
    if (x$status_code == 404) {
      print("There is an 404 error!")
      return(NULL)
    } else if (x$status_code == 200) {
      x <- content(x)
      return(rbindlist(x$results))
    } else {
      x <- RETRY("GET",
                 paste0('https://financialmodelingprep.com/api/v4/historical-price/',
                        symbol, '/', multiply, '/', time, '/', from, '/', to),
                 query = list(apikey = self$api_key),
                 times = 5,
                 timeout(100))
      if (x$status_code == 200) {
        x <- content(x)
        return(rbindlist(x$results))
      } else {
        stop('Error in reposne. Status not 200 and not 404')
      }
    }
  },
  
  
  
  # s3 bucket
  if (grepl("^s3:/", uri_minute)) {
    bucket = s3_bucket(uri_minute) 
    dir_files = bucket$ls()
  } else {
    dir_files = list.files(uri_minute)
  }
  
  # parallel execution
  # plan("multisession", workers = 2L)
  # ADD FUTURE LAPPLY BELOW
  
  # main loop to scrap minute data
  lapply(symbols, function(s) {
    
    # debug
    print(s)
    
    # define file name and s3 file name
    file_name = paste0(s, ".parquet")
    file_name_full = file.path(uri_minute, file_name)
    
    # define start_dates
    start_dates <- seq.Date(as.Date("2004-01-01"), Sys.Date() - 1, by = 1)
    start_dates <- start_dates[isBusinessDay(start_dates)]
    
    # get trading days from daily data
    print("Get daily data")
    daily_start <- seq.Date(as.Date("2004-01-01"), Sys.Date(), by = 365 * 4)
    daily_end <- c(daily_start[-1], Sys.Date())
    daily_data <- lapply(seq_along(daily_start), function(i) {
      daily_data <- tryCatch({
        self$get_intraday_equities(s,
                                   multiply = 1,
                                   time = 'day',
                                   from = daily_start[i],
                                   to = daily_end[i])
      }, error = function(e) NULL)
    })
    daily_data <- rbindlist(daily_data)
    if (length(daily_data) == 0) return(NULL)
    start_dates <- as.Date(intersect(start_dates, as.Date(daily_data$formated)), origin = "1970-01-01")
    # TODO: check BRK-b (BTK.B) and other symbols with -/.
    
    # OLD DATA ----------------------------------------------------------------
    # read old data
    if (file_name %in% dir_files) {
      # read data for the symbol
      print("Get minute data")
      data_history = arrow::read_parquet(file_name_full)
      
      # cont if there is history data
      if (length(data_history) > 0) {
        # basic clean
        data_history <- unique(data_history, by = "date")
        setorder(data_history, date)
        
        # missing freq
        if (deep_scan) { # CHECK THIS LATER !!!
          
          # create date column
          data_history_tz <- copy(data_history)
          # data_history_tz[, time := as.POSIXct(time, origin = as.POSIXct("1970-01-01 00:00:00", tz = "UTC"), tz = "UTC")]
          # data_history_tz[, time := with_tz(time, tz = "America/New_York")]
          # data_history_tz[, time_ := as.Date(time, tz = "America/New_York")]
          # data_history_tz[, time_n := .N, by = time_]
          # 
          # # define dates we will try to scrap again
          # observation_per_day <- 60 * 6
          # try_again <- unique(data_history_tz[time_n < observation_per_day, time_])
          # start_dates <- as.Date(intersect(try_again, start_dates), origin = "1970-01-01")
          # end_dates <- start_dates
          
        } else {
          
          # define dates to scrap
          data_history_date = data_history[, unique(as.Date(date, tz = "America/New_York"))]
          
          # get final dates to scrap
          start_dates <- as.Date(setdiff(start_dates, data_history_date), origin = "1970-01-01")
          end_dates <- start_dates
          
          # data_history[as.Date(date) == as.Date("2004-01-16")]
        }
      } else {
        dates <- self$create_start_end_dates(start_dates, 1)
        start_dates <- dates$start_dates
        end_dates <- dates$end_dates
      }
    } else {
      dates <- self$create_start_end_dates(start_dates, 1)
      start_dates <- dates$start_dates
      end_dates <- dates$end_dates
    }
    
    # GET NEW DATA ------------------------------------------------------------
    # if there is no dates next
    if (length(start_dates) == 0) {
      print(paste0("No data for symbol ", s))
      return(NULL)
    }
    
    # get data
    print("Get new minute data")
    
    system.time({
      data_slice = lapply(seq_along(start_dates), function(d) {
        # for (d in seq_along(start_dates[1:10])) {
        if (end_dates[d] >= Sys.Date()) end_dates[d] <- Sys.Date() - 1
        self$get_intraday_equities(
          s,
          multiply = 1,
          time = "minute",
          from = start_dates[d],
          to = end_dates[d]
        )
      })
    })
    if (any(unlist(sapply(data_slice, nrow)) > 4999)) {
      stop("More than 4999 rows!")
    }
    data_by_symbol <- rbindlist(data_slice)
    
    # if there is no data next
    if (nrow(data_by_symbol) == 0) {
      print(paste0("No data for symbol ", s))
      return(NULL)
    }
    
    # convert to numeric (not sure why I put these, but it have sense I believe).
    cols <- c("o", "h", "l", "c", "v")
    data_by_symbol[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]
    
    # clean data
    data_by_symbol <- unique(data_by_symbol, by = c("formated"))
    data_by_symbol[, date := as.POSIXct(t / 1000, 
                                        origin = as.POSIXct("1970-01-01 00:00:00", tz = "UTC"),
                                        tz = "UTC")]
    data_by_symbol = data_by_symbol[, .(date, o, h, l, c, v)]
    setnames(data_by_symbol, c("date", "open", "high", "low", "close", "volume"))
    
    # rbind new an old
    if (file_name %in% dir_files && nrow(data_history) > 0) {
      # rbind new data
      data_by_symbol = rbind(data_history, data_by_symbol)
      setorder(data_by_symbol, "date")
      data_by_symbol = unique(data_by_symbol, by = "date")
      # delete old parquet file
      if (grepl("^s3:/", uri_minute)) {
        bucket$DeleteFile(file_name)
      } else {
        file.remove(file_name_full)
      }
      # save file
      arrow::write_parquet(data_by_symbol, file_name_full)
    } else {
      # save to S3
      arrow::write_parquet(data_by_symbol, file_name_full)
    }
    return(NULL)
  })
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-FMP-clone"></a>}}
\if{latex}{\out{\hypertarget{method-FMP-clone}{}}}
\subsection{Method \code{clone()}}{
The objects of this class are cloneable with this method.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{FMP$clone(deep = FALSE)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{deep}}{Whether to make a deep clone.}
}
\if{html}{\out{</div>}}
}
}
}
