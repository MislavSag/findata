#' @title DataAbstract Class
#'
#' @description
#' Abstract class for all other data classes.
#'
#' @export
DataAbstract = R6::R6Class(
  "DataAbstract",

  public = list(
    #' @field context_with_config AWS S3 Tiledb config
    context_with_config = NULL,
    
    #' @field fredr_apikey FRED api key.
    fredr_apikey = NULL,

    #' @description
    #' Create a new DataAbstract object.
    #'
    #' @param context_with_config AWS S3 Tiledb config
    #' @param fredr_apikey FRED api key
    #'
    #' @return A new `DataAbstract` object.
    initialize = function(context_with_config = NULL,
                          fredr_apikey = NULL) {

      # set calendar for RcppQuantuccia package
      qlcal::setCalendar("UnitedStates/NYSE")

      # Azure storage endpoint
      # if (is.null(azure_storage_endpoint) & Sys.getenv("BLOB-ENDPOINT") != "") {
      #   self$azure_storage_endpoint = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), Sys.getenv("BLOB-KEY"))
      # } else {
      #   self$azure_storage_endpoint <- azure_storage_endpoint
      # }

      # configure s3
      config <- tiledb_config()
      config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
      config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
      config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
      self$context_with_config <- tiledb_ctx(config)

      # set fred apikey
      if (is.null(azure_storage_endpoint) & Sys.getenv("FRED-KEY") != "") {
        fredr::fredr_set_key(Sys.getenv("FRED-KEY"))
      } else if (!is.null(azure_storage_endpoint)) {
        fredr::fredr_set_key(fredr_apikey)
      } else {
        warning("Fred API key is not set.")
      }
    }
  )
)
