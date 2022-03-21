#' @title Lean Class
#'
#' @description
#' Help functions for transforming raw market data to Quantconnect data.
#'
#' @export
Lean = R6::R6Class(
  "Lean",
  inherit = DataAbstract,

  public = list(

    #' @description
    #' Create a new Lean object.
    #'
    #' @return A new `Lean` object.
    initialize = function() {
      print("Good")
    },

    #' @description Convert FMP Cloud hour data to Quantconnect hour data.
    #'
    #' @param save_path Quantconnect data equity hour folder path.
    #'
    #' @return No valuie returned.
    equity_hour_from_fmpcloud = function(save_path = "D:/lean_projects/data/equity/usa/hour") {

      # crate pin board
      board <- board_azure(
        container = storage_container(private$azure_storage_endpoint, "equity-usa-hour-trades-fmplcoud"),
        path = "",
        n_processes = 4L,
        versioned = FALSE,
        cache = NULL
      )
      blob_dir <- pin_list(board)

      # change every file to quantconnect like file and add to destination
      for (symbol in blob_dir) {
        # debug
        print(symbol)

        # load data
        data_ <- pin_read(board, tolower(symbol))
        data_ <- as.data.table(data_)

        # convert to lean format
        data_[, `:=`(
          DateTime = format.POSIXct(as.POSIXct(formated, format = "%Y-%m-%d %H:%M:%S", tz = "EST"),
                                    format = "%Y-%m-%d %H:%M"),
          Open = o * 1000,
          High = h * 1000,
          Low = l * 1000,
          Close = c * 1000,
          Volume = v
        )]
        data_qc <- data_[, .(DateTime, Open, High, Low, Close, Volume)]

        # save to destination
        file_name_csv <- paste0(tolower(symbol), ".csv")
        fwrite(data_qc, file.path(save_path, file_name_csv), col.names = FALSE)
        zip_file <- file.path(save_path, paste0(tolower(symbol), ".zip"))
        zipr(zip_file, file.path(save_path, file_name_csv))
        file.remove(file.path(save_path, file_name_csv))
      }
    }
  )
)
