#' @title StockAnalysis Class
#'
#' @description
#' Get data data from StockAnalysis website.
#'
#' @export
StockAnalysis = R6::R6Class(
  "StockAnalysis",
  inherit = DataAbstract,

  public = list(

    #' @field base_url Base url
    base_url = NULL,

    #' @description
    #' Create a new FMP object.
    #'
    #' @param base_url API KEY for FMP cloud data.
    #' @param azure_storage_endpoint Azure storate endpont
    #' @param context_with_config AWS S3 Tiledb config
    #'
    #' @return A new `FMP` object.
    initialize = function(base_url = "https://stockanalysis.com",
                          azure_storage_endpoint = NULL,
                          context_with_config = NULL) {

      # endpoint
      super$initialize(azure_storage_endpoint, context_with_config)

      # base url
      self$base_url = base_url
    },

    #' @description
    #' Get corporate action for symbol.
    #'
    #' @param k Url id
    #' @param query Type of corporate action
    #'
    #' @references \url{https://stockanalysis.com/actions/}
    #' @return Data table of corporate action.
    get_corporate_actions = function(k = "AAAAB3NzaC1yc2EAAAADAQABAAABAQDNK7jARJUOF5HofsvEkZi6T80FW4Shxx1k6tGnyw1bMyrGXOuMg7xx",
                                     query = c("listed", "delisted","splits",
                                               "changes", "spinoffs",
                                               "bankruptcies", "acquisitions")) {


      # match
      query <- match.arg(query)

      # # create urls
      url_ <- paste0(self$base_url, "/api/actions/fetch/", query)
      url_ <- paste(url_, 1998:(year(Sys.Date())), sep = "/")
      url_ <- vapply(url_, modify_url, query = list(k = k), character(1))

      # get data
      corp_actions <- lapply(url_, read_json)
      corp_actions <- lapply(corp_actions, `[[`, 1)
      corp_actions <- lapply(corp_actions, rbindlist)
      corp_actions <- rbindlist(corp_actions)
      corp_actions[, query := query]

      return(corp_actions)
    },

    #' @description
    #' Get corporate actions and save it to TileDB..
    #'
    #' @param url TileDB uri
    #'
    #' @return Null
    get_corporate_tiledb = function(url = "s3://equity-usa-corporateactions") {

      # get
      queries <- c("listed", "delisted","splits", "changes", "spinoffs",
                   "bankruptcies", "acquisitions")
      cadt_l <- lapply(queries, function(x) self$get_corporate_actions(query = x))
      cadt <- rbindlist(cadt_l, fill = TRUE)
      cadt[, date := as.Date(date, format = "%b %d, %Y")]

      # delete and create again array
      del_obj <- tryCatch({tiledb_object_rm(url)}, error = function(e) NA)

      # save to tiledb
      fromDataFrame(
        as.data.frame(cadt),
        url,
        col_index = "query",
        sparse = TRUE,
        allows_dups = TRUE
      )
    }
  )
)
