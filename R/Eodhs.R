#' @title EODHD Class
#'
#' @description
#' Get data data from EODHD.
#'
#' @export
EODHD = R6::R6Class(
  "EODHD",
  inherit = DataAbstract,

  public = list(
    #' @field api_key API KEY for EODHD.
    api_key = NULL,

    #' @description
    #' Create a new FMP object.
    #'
    #' @param api_key API KEY for FMP cloud data.
    #'
    #' @return A new `FMP` object.
    initialize = function(api_key = NULL) {

      # check and define variables
      if (is.null(api_key)) {
        self$api_key = assert_character(Sys.getenv("APIKEY-FMPCLOUD"))
      } else {
        self$api_key = api_key
      }
    },

    #' @description
    #' Create a new FMP object.
    #'
    #' @param symbols Symbol of the stock.
    #' @param uri to save data to.
    #' @param workers Number of workers.
    #'
    #' @return Earning announcements data.
    get_intraday_bulk = function(symbols, uri, workers = 1L) {

      # DEBUG
      # uri = "F:/eodhd"

      # main loop to scrap minute data
      mclapply(symbols, function(s) {

        # debug
        # s = "SPY.US"
        print(s)

        # create folder for symbol if it doesn't exists
        dir_name = path(uri, s)
        if (!dir.exists(dir_name)) {
          dir.create(dir_name)
        }

        # existing dates
        dates_exists = dir_ls(dir_name)

        # define dates
        if (length(dates_exists) == 0) {
          seq_date_start = seq.Date(as.Date("2004-01-01"), Sys.Date(), by = 100)
          seq_date_start[seq_date_start != Sys.Date()]
          seq_date_end = c(seq_date_start[2:length(seq_date_start)], Sys.Date() - 1)
          seq_date_start = as.POSIXct(paste0(seq_date_start, " 00:00:00"), tz = "America/New_York")
          seq_date_end = as.POSIXct(paste0(seq_date_end, " 00:00:00"), tz = "America/New_York")
        } else {
          start_date = max(as.Date(path_ext_remove(path_file(dates_exists))))
          seq_date_start = seq.Date(min(start_date+1, Sys.Date()-1), Sys.Date()-1, by = 100)
          if (length(seq_date_start) == 1) {
            seq_date_end = Sys.Date() - 1
          }
        }

        # get data
        dt_l = lapply(seq_along(seq_date_start), function(i) {
          dt_l = get_intraday_historical_data(
            api_token = self$api_key,
            symbol = s,
            from_unix_time = as.numeric(seq_date_start[i]),
            to_unix_time = as.numeric(seq_date_end[i]),
            interval = "1m"
          )
          rbindlist(dt_l)
        })
        dt = rbindlist(dt_l)
        
        # if empty cont
        if (length(dt) == 0) return(NULL)

        # remove duplicates
        dt = unique(dt)

        # set timezone to UTC
        setattr(dt$timestamp, "tz", "UTC")
        
        # crate date column in America/New_York timezone
        dt[, date := as.Date(as.POSIXct(timestamp, origin = "1970-01-01", tz = "UTC"))]
        dt[, date := with_tz(date, "America/New_York")]
        dt[, date := as.Date(date, tz = "America/New_York")]
        dt[, date := as.character(date)]

        # save for every date
        dt[, write_parquet(.SD, path(dir_name, paste0(.BY, ".parquet"))), by = date]
        return(NULL)
      }, mc.cores = workers)
      return(NULL)
    }
  ),
  private = list()
)
