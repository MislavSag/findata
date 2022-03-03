#' @title UtilsData Class
#'
#' @description
#' Various help data.
#'
#' @export
UtilsData = R6::R6Class(
  "UtilsData",
  inherit = DataAbstract,

  public = list(

    #' @description
    #' Create a new UtilsData object.
    #'
    #' @return A new `UtilsData` object.
    initialize = function() {
      print("Good")
    },

    #' @description Get tick data from finam source using QuantTools.
    #'
    #' @param symbol Equity symbol
    #'
    #' @return Get investing com ea data.
    get_ticker_data = function(symbol) {

      p <- RETRY("POST",
                 'https://www.quantumonline.com/search.cfm',
                 body = list(
                   tickersymbol = symbol,
                   sopt = 'symbol',
                   '1.0.1' = 'Search'
                 ),
                 times = 8L)
      changes <- content(p) %>%
        html_elements(xpath = "//*[contains(text(),'Previous Ticker')]") %>%
        html_text() %>%
        gsub('.*Symbol:', '', .) %>%
        trimws(.)
      date <- as.Date(str_extract(changes, '\\d+/\\d+/\\d+'), '%m/%d/%Y')
      tickers <- str_extract(changes, '\\w+')
      changes <- data.table(symbol = symbol, date = date, ticker_change = tickers)
      return(changes)
    }
  )
)
