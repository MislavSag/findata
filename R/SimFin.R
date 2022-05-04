#' #' @title SimFin Class
#' #'
#' #' @description
#' #' Get data data from Simfin API.
#' #'
#' #' @export
#' SimFin = R6::R6Class(
#'   "SimFin",
#'   inherit = DataAbstract,
#'
#'   public = list(
#'
#'     #' @field api_key API KEY for FMP Cloud
#'     api_key = NULL,
#'
#'     #' @description
#'     #' Create a new FMP object.
#'     #'
#'     #' @param api_key API KEY for FMP cloud data.
#'     #'
#'     #' @return A new `FMP` object.
#'     initialize = function(api_key = NULL) {
#'
#'       # check and define variables
#'       if (is.null(api_key)) {
#'         self$api_key = assert_character(Sys.getenv("APIKEY-SIMFIN"))
#'       } else {
#'         self$api_key = api_key
#'       }
#'     },
#'
#'     #' @description
#'     #' Create a new FMP object.
#'     #'
#'     #' @param days_history how long to the past to look
#'     #'
#'     #' @return Earning announcements data.
#'     get_prices = function(id, ticker, ratios = '&ratios', asreported = '&asreported', start = "2020-03-01", end = "2020-06-30") {
#'
#'       # get data
#'       data_ <- private$simfin_path(path = "companies/prices", v = "v2", "api-key" = self$api_key,
#'                                    ticker = paste("AAPL", "TSLA", sep = ","))
#'       lapply(data_, function(x) {
#'         found <- x$found
#'         cols <- unlist(x$columns)
#'         currency <- x$currency
#'         prices_ <- rbindlist(x$data)
#'         setnames(prices_, cols)
#'         prices_[, curency := currency]
#'       })
#'
#'       data_[[1]]$
#'
#'       data_[[1]]$currency
#'       data_ <- rbindlist(data_[[1]], fill = TRUE)
#'     }
#'   ),
#'
#'   private = list(
#'     ea_file_name = "EarningAnnouncements.csv",
#'
#'     simfin_path = function(path = "earning_calendar", v = "v2", ...) {
#'
#'       # query params
#'       query_params <- list(...)
#'
#'       # define url
#'       url <- paste0("https://simfin.com/api/", v, "/", path)
#'       print(url)
#'
#'       # get data
#'       p <- RETRY("GET", url, query = query_params, times = 2)
#'       result <- httr::content(p)
#'
#'       return(result)
#'     }
#'   )
#' )
#'
#' p <- GET(paste0("https://simfin.com/api/v2/companies/prices?api-key=", self$api_key))
#' content(p)
