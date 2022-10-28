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
    #' @param azure_storage_endpoint Azure storate endpont
    #' @param context_with_config AWS S3 Tiledb config
    #'
    #' @return A new `Finam` object.
    initialize = function(azure_storage_endpoint = NULL,
                          context_with_config = NULL) {

      # endpoint
      super$initialize(azure_storage_endpoint, context_with_config)
    },

    #' @description Get tick data from finam source using QuantTools.
    #'
    #' @param symbols Start date
    #' @param days Days to scrap data for
    #' @param save_uri Tiledb uri to save.
    #'
    #' @return Get investing com ea data.
    finam_tick_tiledb = function(symbols,
                                 days = getBusinessDays(as.Date("2011-01-01"), Sys.Date() - 1),
                                 save_uri = "s3://equity-usa-tick-finam") {

      # debug
      # library(RcppQuantuccia)
      # library(QuantTools)
      # library(lubridate)
      # library(tiledb)
      # config <- tiledb_config()
      # config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
      # config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
      # config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
      # context_with_config <- tiledb_ctx(config)
      # symbols <- c("SPY")
      # days = getBusinessDays(as.Date("2011-01-01"), Sys.Date() - 1)

      # get data for every symbol
      for (s in symbols) {

        # debug
        print(s)

        # main loop
        tick_data_l <- lapply(days, function(d) {

          # DEBUG and fair scraping
          Sys.sleep(1L)

          # get data
          tries <- 1
          repeat {

            tick_data <- tryCatch({get_finam_data(s, d, d, period = "tick")},
                                  error = function(e) NULL)

            # test for errors
            if (!is.null(tick_data) | tries > 3) {
              break()
            } else if (is.null(tick_data)){
              tick_data <- tryCatch({get_finam_data(s, d, d, period = "tick")},
                                    error = function(e) NULL)
              }

            tries <- tries + 1
            print(paste0("Repeat ", tries))
            Sys.sleep(1L)
          }

          return(tick_data)
        })

        # merge and clean data
        tick_data_symbol <- rbindlist(tick_data_l)
        tick_data_symbol[, time := force_tz(time, tzone = "America/New_York")]
        tick_data_symbol[, time := with_tz(time, tzone = "UTC")]
        tick_data_symbol[, symbol := s]

        # save to tiledb
        if (tiledb_object_type(save_uri) != "ARRAY") {
          fromDataFrame(
            obj = tick_data_symbol,
            uri = save_uri,
            col_index = c("symbol", "time"),
            sparse = TRUE,
            tile_domain = list(time=c(as.POSIXct("1970-01-01 00:00:00", tz = "UTC"),
                                      as.POSIXct("2099-12-31 23:59:59", tz = "UTC"))),
            allows_dups = TRUE
          )
        } else {
          # save to tiledb
          arr <- tiledb_array(save_uri, as.data.frame = TRUE)
          arr[] <- tick_data_symbol
          tiledb_array_close(arr)
        }
      }
    }
  )
)
