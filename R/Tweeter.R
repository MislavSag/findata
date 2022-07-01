#' @title Tweeter Class
#'
#' @description
#' Get data data from Tweeter.
#'
#' @export
Tweeter = R6::R6Class(
  "Tweeter",
  # inherit = DataAbstract,

  public = list(
    #' @field api_key API KEY for FMP Cloud
    api_key = NULL,

    #' @description
    #' Create a new Tweeter object.
    #'
    #' @return A new `Tweeter` object.
    initialize = function() {

    }
  ),

  private = list(

  )
)


