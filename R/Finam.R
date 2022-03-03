#' @title Finam Class
#'
#' @description
#' Get data data from finam using Quanttools package.
#'
#' @export
Finam = R6::R6Class(
  "Finam",
  inherit = DataAbstract,

  public = list(

    #' @description
    #' Create a new Finam object.
    #'
    #' @return A new `Finam` object.
    initialize = function() {
      print("Good")
    },

    #' @description Get tick data from finam source using QuantTools.
    #'
    #' @param symbols Start date
    #' @param days Days to scrap data for
    #'
    #' @return Get investing com ea data.
    get_ticker_data = function(symbols,
                               days = getBusinessDays(as.Date("2011-01-01"), Sys.Date() - 1)) {

      # get data for every symbol
      for (s in symbols) {

        # DEBUG
        print(s)

        # crate pin board
        board <- board_azure(
          container = storage_container(private$azure_storage_endpoint, "equity-usa-tick-finam"),
          path = tolower(s),
          n_processes = 4L,
          versioned = FALSE,
          cache = FALSE
        )
        blob_dir <- pin_list(board)

        # blob_dir <- AzureStor::list_blobs(storage_container(private$azure_storage_endpoint, "equity-usa-tick-finam"))
        # lapply(blob_dir$name, AzureStor::delete_blob, container = cont, confirm = FALSE)
        # remove already scraped dates (days)
        if (length(blob_dir) > 0) {
          dates_scraped <- as.Date(gsub(".*/|_.*", "", blob_dir), format = "%Y%m%d")
          days_ <- as.Date(setdiff(days, dates_scraped), origin = "1970-01-01")
          if (sum(days_ > (Sys.Date() - 126)) < 5) days_ <- days_[days_ > Sys.Date() - 120]
        } else {
          days_ <- days
        }

        # test if there are days
        if (length(days_) == 0) next()

        # main loop
        lapply(days_, function(d) {

          # file name
          file_name_csv <- paste(format.Date(d, "%Y%m%d"), tolower(s), "Trade", "Tick", sep = "_")

          # if file exists next
          if (pin_exists(board, file_name_csv)) return(NULL)

          # DEBUG and fair scraping
          print(d)
          Sys.sleep(1L)

          # get data
          tick_data <- get_finam_data(s, d, d, period = "tick")

          # if ther eis no data nex, else save to blob
          if (is.null(tick_data)) {
            return(NULL)
          } else {

            # add symbol
            tick_data <- cbind(symbol = s, tick_data)

            # create zip file data
            pin_write(board, tick_data, name = file_name_csv, type = "csv")
            return(NULL)
          }
        })
      }
    }
  )
)
