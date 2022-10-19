#' @title Finam Class
#'
#' @description
#' Get data data from finam using Quanttools package.
#'
#' @export
Finam = R6::R6Class(
  "Finam",
  inherit = DataAbstract,

  public = list(

    #' @description
    #' Create a new Finam object.
    #'
    #' @param azure_storage_endpoint Azure storate endpont
    #' @param context_with_config AWS S3 Tiledb config
    #'
    #' @return A new `Finam` object.
    initialize = function(azure_storage_endpoint = NULL,
                          context_with_config = NULL) {

      # endpoint
      super$initialize(azure_storage_endpoint, context_with_config)
    },

    #' @description Get tick data from finam source using QuantTools.
    #'
    #' @param symbols Start date
    #' @param days Days to scrap data for
    #'
    #' @return Get investing com ea data.
    get_ticker_tiledb = function(symbols,
                                 days = getBusinessDays(as.Date("2011-01-01"), Sys.Date() - 1)) {

      # debug
      # library(RcppQuantuccia)
      # library(QuantTools)
      # library(lubridate)

      # get data for every symbol
      for (s in symbols) {

        # debug
        print(s)

        # main loop
        lapply(days, function(d) {

          # DEBUG and fair scraping
          Sys.sleep(1L)

          # get data
          tick_data <- get_finam_data(s, d, d, period = "tick")
        })

        # clean tick data
        tick_data[, time := force_tz(time, tzone = "EST")]
        tick_data[, time := with_tz(time, tzone = "UTC")]

      }
    }
  )
)
