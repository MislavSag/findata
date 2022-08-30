#' @title IBTWS Class
#'
#' @description
#' Get data data from IB.
#'
#' @export
IBTWS = R6::R6Class(
  "IBTWS",
  inherit = DataAbstract,

  public = list(
    #' @field port Port.
    port = NULL,

    #' @field clientId Client ID.
    clientId = NULL,

    #' @field ic Client ID.
    ic = NULL,


    #' @description
    #' Create a new FMP object.
    #'
    #' @param port Port.
    #' @param clientId client ID.
    #'
    #' @return A new `IBTWS` object.
    initialize = function(port = 4002, clientId = 1) {

      # endpoint
      super$initialize(NULL)

      # arguments
      self$port = port
      self$clientId = clientId

      # init IBWrap
      # Define a customized callbacks wrapper
      IBWrapCustom <- R6::R6Class("IBWrapCustom",
                                  class=      FALSE,
                                  cloneable=  FALSE,
                                  lock_class= TRUE,

                                  inherit= IBWrap,

                                  public= list(
                                    # Customized methods go here
                                    error=            function(id, errorCode, errorString, advancedOrderRejectJson)
                                      cat("Error:", id, errorCode, errorString, advancedOrderRejectJson, "\n"),

                                    nextValidId=      function(orderId)
                                      cat("Next OrderId:", orderId, "\n"),

                                    managedAccounts=  function(accountsList)
                                      cat("Managed Accounts:", accountsList, "\n")

                                    # more method overrides can go here...
                                  )
      )

      # Instantiate wrapper and client
      wrap <- IBWrapCustom$new()
      self$ic   <- IBClient$new(wrap)

      # Connect to the server with clientId = 1
      self$ic$connect(port = self$port, clientId = self$clientId)

    },

    #' @description
    #' Get market data.
    #'
    #' @param days_history how long to the past to look
    #'
    #' @return Earning announcements data.
    get_data = function(days_history = 10) {

    }
  ),

  private = list(
    ea_file_name = "EarningAnnouncements"
  )
)
