#' #' @title Zorro Class
#' #'
#' #' @description
#' #' Help functions for transforming market data to Zorro data.
#' #'
#' #' @export
#' Zorro = R6::R6Class(
#'   "Zorro",
#'   inherit = DataAbstract,
#'
#'   public = list(
#'
#'     #' @description
#'     #' Create a new Zorro object.
#'     #'
#'     #' @param azure_storage_endpoint Azure storate endpont
#'     #' @param context_with_config Azure storate endpont
#'     #'
#'     #' @return A new `Lean` object.
#'     initialize = function(azure_storage_endpoint = NULL,
#'                           context_with_config = NULL) {
#'
#'       # endpoint
#'       super$initialize(azure_storage_endpoint, context_with_config)
#'     },
#'
#'     #' @description Convert FMP Cloud minute data to zorro t6 data.
#'     #'
#'     #' @param save_path Zorro History path.
#'     #'
#'     #' @return No value returned.
#'     equity_minute_from_tiledb = function(symbol,
#'                                          save_path = "D:/findata") {
#'
#'       # debug
#'
#'
#'
#'     }
#'   )
#' )
