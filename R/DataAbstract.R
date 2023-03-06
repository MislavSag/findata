#' @title DataAbstract Class
#'
#' @description
#' Abstract class for all other data classes.
#'
#' @export
DataAbstract = R6::R6Class(
  "DataAbstract",

  public = list(
    #' @field azure_storage_endpoint Azure storate endpont
    azure_storage_endpoint = NULL,

    #' @field context_with_config AWS S3 Tiledb config
    context_with_config = NULL,

    #' @description
    #' Create a new DataAbstract object.
    #'
    #' @param azure_storage_endpoint Azure endpoint
    #' @param context_with_config AWS S3 Tiledb config
    #'
    #' @return A new `DataAbstract` object.
    initialize = function(azure_storage_endpoint = NULL,
                          context_with_config = NULL) {

      # set calendar for RcppQuantuccia package
      qlcal::setCalendar("UnitedStates/NYSE")

      # Azure storage endpoint
      if (is.null(azure_storage_endpoint) & Sys.getenv("BLOB-ENDPOINT") != "") {
        self$azure_storage_endpoint = storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), Sys.getenv("BLOB-KEY"))
      } else {
        self$azure_storage_endpoint <- azure_storage_endpoint
      }

      # configure s3
      config <- tiledb_config()
      config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
      config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
      config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
      self$context_with_config <- tiledb_ctx(config)

      # set fred apikey
      fredr::fredr_set_key(Sys.getenv("FRED-KEY"))
    },

    #' @description Save files bob. It automaticly saves files as csv and rds objects
    #'
    #' @param object Object to save
    #' @param file_name File name (with extensions)
    #' @param container Blob container
    save_blob_files = function(object, file_name, container = "fmpcloud") {
      cont <- storage_container(self$azure_storage_endpoint, container)
      if (grepl("csv", file_name)) {
        storage_write_csv(object, cont, file = file_name)
      } else if (grepl("rds", file_name)) {
        storage_save_rds(object, cont, file = file_name)
      }
    }
  )
)
