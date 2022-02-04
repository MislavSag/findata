#' @title Data Class
#'
#' @description
#' Get or update all data from the package.
#'
#' @export
Data = R6::R6Class(
  "Data",

  public = list(

    #' @field fmp FMP object
    fmp = NULL,

    #' @description
    #' Create a new Data object.
    #'
    #' @param fmp FMP object.
    #'
    #' @return A new `Data` object.
    initialize = function(fmp = NULL) {

      # FMP cloud data
      self$fmp = fmp


    },

    #' @description
    #' Scrap (download) all data
    #'
    #' @return Earning announcements data.
    get_all_data = function() {

      # get ernings announcements
      ea = self$fmp$get_earning_announcements()
    }

  )
)
