#' @title IBREST Class
#'
#' @description
#' Get data data from IB Client Portal API.
#'
#' @export
IBREST2 = R6::R6Class(
  "IBREST2",
  inherit = DataAbstract,

  public = list(

    #' @field domain Domain, by default localhost
    domain = NULL,

    #' @field port Port, by default 5000
    port = NULL,

    #' @field baseurl Base url
    baseurl = NULL,

    #' @description
    #' Create a new IBREST object.
    #'
    #' @param azure_storage_endpoint Azure storage endpoint
    #' @param domain Domain, by default localhost
    #' @param port Port, by default 5000
    #'
    #' @return A new `IBREST` object.
    initialize = function(domain = "localhost",
                          port = 5000L,
                          azure_storage_endpoint = NULL) {

      # endpoint
      super$initialize(azure_storage_endpoint)

      # base url
      self$baseurl <- paste(paste0("https://", domain), port, sep = ":")
    },

    #' @description
    #' IB GET object
    #'
    #' @param url Url of GET endpoint.
    #' @param query Query of GET request.
    #'
    #' @references \url{https://www.interactivebrokers.com/api/doc.html}
    #' @return GET response.
    ib_get = function(url = modify_url(self$baseurl, path = "v1/api/sso/validate"),
                      query = NULL) {
      p <- RETRY("GET",
                 url,
                 config = httr::config(ssl_verifypeer = FALSE, ssl_verifyhost = FALSE),
                 query = query,
                 times = 5L)
      x <- content(p)
      return(x)
    },

    #' @description
    #' IB POST object
    #'
    #' @param url Url of POST endpoint.
    #' @param body Body of POST request.
    #'
    #' @references \url{https://www.interactivebrokers.com/api/doc.html}
    #' @return GET response.
    ib_post = function(url = modify_url(self$baseurl, path = "v1/api/tickle"),
                       body = NULL) {
      p <- RETRY("POST",
                 url,
                 config = httr::config(ssl_verifypeer = FALSE, ssl_verifyhost = FALSE),
                 body = body,
                 add_headers('User-Agent' = 'Console',
                             'content-type' = 'application/json'),
                 encode = "json",
                 times = 5L)
      x <- content(p)
      return(x)
    },

    #' @description
    #' Get IB REST API description.
    #'
    #' @return JSON object with description.
    get_api_spec = function() {
      ib_spec <- jsonlite::fromJSON("https://gdcdyn.interactivebrokers.com/portal.proxy/v1/portal/swagger/swagger?format=json")
      return(ib_spec)
    },

    #' @description
    #' Get unadjusted market data.
    #'
    #' @param conid Contract ID.
    #' @param exchange Exchange.
    #' @param period Period.
    #' @param bar Bar.
    #' @param outsideRth Data outside trading hours.
    #' @param keep_nytime_10_16 If TRUE, timezone is changed to NY time and only
    #'     trading hours kept.
    #'
    #' @references \url{https://www.interactivebrokers.com/api/doc.html#tag/Market-Data/paths/~1iserver~1marketdata~1history/get}
    #' @return Data table with unadjusted market data.
    get_unadjusted_market = function(conid,
                                     exchange = NULL,
                                     period = "5d",
                                     bar = "1h",
                                     outsideRth = TRUE,
                                     keep_nytime_10_16 = TRUE) {

      # send GET request fro market data
      # print("Get unadjusted data from IB...")
      md <- self$ib_get(modify_url(self$baseurl, path = "v1/api/iserver/marketdata/history"),
                        list(conid = conid,
                             exchange = exchange,
                             period = period,
                             bar = bar,
                             outsideRth = outsideRth))
      md <- rbindlist(md$data)

      # convert timezone to New york time and keep trading hours
      if (keep_nytime_10_16) {
        # print("Change timezone to NY time.")
        # change timesone to NY
        print(class(md$t))
        print(md$t)
        md$t <- as.numeric(md$t)
        md[, datetime := as.POSIXct(t / 1000,
                                    origin = "1970-01-01",
                                    tz = Sys.timezone())]
        # print("Debug")
        attr(md$datetime, "tzone") <- "America/New_York"

        # keep trading hours
        # print("Keep trading hours.")
        md$datetime <- md$datetime + 60 * 60
        md <- md[format.POSIXct(datetime, format = "%H:%M:%S") %between% c("09:30:00", "16:00:00")]
      }

      return(md)
    },

    #' @description
    #' Get portfolio positions.
    #'
    #' @param account_id Account ID.
    #' @param con_id Contract id.
    #'
    #' @return list object with info on positions.
    get_position = function(account_id, con_id) {
      url <- paste0(modify_url(self$baseurl, path = "/v1/api/portfolio/"),
                    account_id,
                    "/position/",
                    con_id)
      positions <- self$ib_get(url)
      return(positions)
    },

    #' @description
    #' Place order.
    #'
    #' @param account_id Account ID.
    #' @param order_body Body of POST request which place order.
    #'
    #' @references \url{https://www.interactivebrokers.com/api/doc.html#tag/Order/paths/~1iserver~1account~1\%7Bacc€ountId\%7D~1orders/post}
    #' @return list object with info on positions.
    buy_and_confirm = function(account_id, order_body) {
      # place order
      url <- paste0(modify_url(self$baseurl, path = "/v1/api/iserver/account/"),
                    account_id,
                    "/orders")
      body_json = toJSON(list(orders = list(order_body)), auto_unbox = TRUE)
      order_message <- self$ib_post(url, body = body_json)

      # wait for 1 sec for order to be sent. Probably not necessary
      Sys.sleep(1L)

      # confirm order
      url <- paste0(modify_url(self$baseurl, path = "/v1/api/iserver/reply/"),
                    order_message[[1]]$id)
      confirmed <- self$ib_post(url,
                                body = toJSON(list(confirmed = TRUE),
                                              auto_unbox = TRUE))
      return(list(order_message = order_message, confirmed = confirmed))
    },

    #' @description
    #' Cancel order.
    #'
    #' @param account_id Account ID.
    #' @param order_id Order id from Place orders endpoint result.
    #'
    #' @return list object with info on positions.
    cancel_order =  function(account_id, order_id) {
      url <- paste0("https://localhost:5000/v1/api/iserver/account/",
                    account_id,
                    "/order/",
                    order_id)
      p <- DELETE(url, config = httr::config(ssl_verifypeer = FALSE,  ssl_verifyhost = FALSE))
      return(content(p))
    },

    #' @description
    #' Portfolio summary.
    #'
    #' @param account_id Account ID.
    #'
    #' @return list object with info on podrtfolio summary.
    get_portfolio_summary = function(account_id) {
      url <- paste0(modify_url(self$baseurl, path = "v1/api/portfolio/"),
                    account_id,
                    "/summary")
      positions <- self$ib_get(url)
      return(positions)
    }
  )
)
