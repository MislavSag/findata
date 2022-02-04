#' @title DataAbstract Class
#'
#' @description
#' Abstract class for all other data classes.
#'
#' @export
DataAbstract = R6::R6Class(
  "DataAbstract",

  public = list(

    #' @description
    #' Create a new DataAbstract object.
    #'
    #' @param azure_storage_endpoint Azure endpoint
    #'
    #' @return A new `DataAbstract` object.
    initialize = function(azure_storage_endpoint) {

      # set calendar for RcppQuantuccia package
      setCalendar("UnitedStates::NYSE")
    },

    #' @description Save files bob. It automaticly saves files as csv and rds objects
    #'
    #' @param object Object to save
    #' @param file_name File name (with extensions)
    #' @param container Blob container
    save_blob_files = function(object, file_name, container = "fmpcloud") {
      cont <- storage_container(private$azure_storage_endpoint, container)
      if (grepl("csv", file_name)) {
        storage_write_csv(object, cont, file = file_name)
      } else if (grepl("rds", file_name)) {
        storage_save_rds(object, cont, file = file_name)
      }
    }
  ),
  private = list(
    azure_storage_endpoint = AzureStor::storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
  )
)
