#' @title DataAbstract Class
#'
#' @description
#' Abstract class for all other data classes.
#'
#' @export
DataAbstract = R6::R6Class(
  "DataAbstract",

  public = list(
    #' @field fredr_apikey FRED api key.
    fredr_apikey = NULL,

    #' @description
    #' Create a new DataAbstract object.
    #'
    #' @param fredr_apikey FRED api key
    #'
    #' @return A new `DataAbstract` object.
    initialize = function(fredr_apikey = NULL) {

      # set calendar for qlcal package
      qlcal::setCalendar("UnitedStates/NYSE")

      # set fred apikey
      if (Sys.getenv("FRED-KEY") != "") {
        fredr::fredr_set_key(Sys.getenv("FRED-KEY"))
      } else if (Sys.getenv("FRED-KEY") != "") {
        fredr::fredr_set_key(fredr_apikey)
      } else {
        warning("Fred API key is not set.")
      }
    }
  )
)
