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
    #' @param azure_storage_endpoint Azure storate endpont
    #' @param context_with_config AWS S3 Tiledb config
    #'
    #' @return A new `Lean` object.
    initialize = function(azure_storage_endpoint = NULL,
                          context_with_config = NULL) {

      # endpoint
      super$initialize(azure_storage_endpoint, context_with_config)
    },

    #' @description Convert FMP Cloud daily data to Quantconnect daily data.
    #'
    #' @param save_path Quantconnect data equity daily folder path.
    #'
    #' @return No valuie returned.
    equity_daily_from_fmpcloud = function(save_path = "D:/findata") {

      # crate pin board
      board <- board_azure(
        container = storage_container(azure_storage_endpoint, "fmpcloud-daily"),
        path = "",
        n_processes = 2L,
        versioned = FALSE,
        cache = save_path
      )
      blob_dir <- pin_list(board)

      # downlaod new data if available
      files_new <- setdiff(blob_dir, list.files(save_path))
      lapply(files_new, pin_download, board = board)
      files_local <- vapply(file.path(CACHEDIR, blob_dir),
                            list.files, recursive = TRUE, pattern = "\\.csv", full.names = TRUE,
                            FUN.VALUE = character(1))
      prices_l <- lapply(files_local, fread)
      prices <- prices_l[vapply(prices_l, function(x) nrow(x) > 0, FUN.VALUE = logical(1))]
      prices <- rbindlist(prices)

      # 19980102 00:00	136300	162500	135000	162500	6315000
      # 19980105 00:00	165000	165600	151900	160000	5677300
      # 19980106 00:00	159400	205000	147500	190000	16064600
      # 19980107 00:00	188100	192500	173100	174400	9122300

      # change every file to quantconnect like file and add to destination
      for (symbol in blob_dir) {
        # debug
        print(symbol)

        # load data
        data_ <- pin_read(board, tolower(symbol))
        data_ <- as.data.table(data_)

        # convert to lean format
        data_[, `:=`(
          DateTime = as.POSIXct(formated, format = "%Y-%m-%d %H:%M:%S", tz = "EST"),
          Open = o * 1000,
          High = h * 1000,
          Low = l * 1000,
          Close = c * 1000,
          Volume = v
        )]
        data_ <- data_[format(DateTime, "%H:%M:%S") %between% c("10:00:00", "16:01:00")]
        data_[, DateTime := format.POSIXct(DateTime, format = "%Y%m%d %H:%M")]
        data_qc <- data_[, .(DateTime, Open, High, Low, Close, Volume)]
        data_qc[, DateTime := as.character(DateTime)]
        cols <- colnames(data_qc)[2:ncol(data_qc)]
        data_qc <- data_qc[, (cols) := lapply(.SD, format, scientific = FALSE), .SDcols = cols]

        # save to destination
        file_name_csv <- paste0(tolower(symbol), ".csv")
        fwrite(data_qc, file.path(save_path, file_name_csv), col.names = FALSE)
        zip_file <- file.path(save_path, paste0(tolower(symbol), ".zip"))
        zipr(zip_file, file.path(save_path, file_name_csv))
        file.remove(file.path(save_path, file_name_csv))
      }
    },


    #' @description Convert FMP Cloud hour data to Quantconnect hour data.
    #'
    #' @param save_path Quantconnect data equity hour folder path.
    #'
    #' @return No valuie returned.
    equity_hour_from_fmpcloud = function(save_path = "D:/lean_projects/data/equity/usa/hour") {

      # crate pin board
      board <- board_azure(
        container = storage_container(self$azure_storage_endpoint, "equity-usa-hour-trades-fmplcoud"),
        path = "",
        n_processes = 2L,
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
          DateTime = as.POSIXct(formated, format = "%Y-%m-%d %H:%M:%S", tz = "EST"),
          Open = o * 1000,
          High = h * 1000,
          Low = l * 1000,
          Close = c * 1000,
          Volume = v
        )]
        data_ <- data_[format(DateTime, "%H:%M:%S") %between% c("10:00:00", "16:01:00")]
        data_[, DateTime := format.POSIXct(DateTime, format = "%Y%m%d %H:%M")]
        data_qc <- data_[, .(DateTime, Open, High, Low, Close, Volume)]
        data_qc[, DateTime := as.character(DateTime)]
        cols <- colnames(data_qc)[2:ncol(data_qc)]
        data_qc <- data_qc[, (cols) := lapply(.SD, format, scientific = FALSE), .SDcols = cols]

        # save to destination
        file_name_csv <- paste0(tolower(symbol), ".csv")
        fwrite(data_qc, file.path(save_path, file_name_csv), col.names = FALSE)
        zip_file <- file.path(save_path, paste0(tolower(symbol), ".zip"))
        zipr(zip_file, file.path(save_path, file_name_csv))
        file.remove(file.path(save_path, file_name_csv))
      }
    },

    #' @description Convert FMP Cloud minute data to Quantconnect minute data.
    #'
    #' @param lean_data_path Quantconnect data equity minute folder path.
    #' @param url S3 url.
    #'
    #' @return No value returned.
    equity_minute_from_fmpcloud = function(lean_data_path = "D:/lean_projects/data/equity/usa/minute",
                                           url = "s3://equity-usa-minute-fmp") {

      # debug
      # library(data.table)
      # library(findata)
      # self = Lean$new()

      options(scipen=999)

      # read factor files
      # factor files
      arr_ff <- tiledb_array("s3://equity-usa-factor-files",
                             as.data.frame = TRUE,
                             ctx = self$context_with_config)
      factor_files <- arr_ff[]
      tiledb_array_close(arr_ff)
      factor_files <- as.data.table(factor_files)
      factor_files[, date := as.Date(as.character(date), "%Y%m%d")]
      factor_files <- setorder(factor_files, symbol, date)

      # add factor files to Lean folder
      for (ff in unique(factor_files$symbol)) {
        sample_ <- factor_files[symbol == ff]
        folder <- file.path("D:/lean_projects/data/equity/usa", "factor_files")
        file_name <- file.path(folder, paste0(tolower(ff), ".csv"))
        fwrite(sample_[, -1], file_name, col.names = FALSE)
      }

        # add data to lean folder
      for (s in factor_files[, unique(symbol)]) {

        # debug
        print(s)

        # sample
        arr <- tiledb_array(url,
                            as.data.frame = TRUE,
                            selected_ranges = list(symbol = cbind(s, s)))
        sample_ <- arr[]
        sample_ <- as.data.table(sample_)
        sample_[, date := with_tz(date, "America/New_York")]
        sample_ <- unique(sample_, by = c("symbol", "date"))

        # convert to QC format
        sample_[, `:=`(DateTime = (hour(date) * 3600 + minute(date) * 60 + second(date)) * 1000,
                       Open = open * 10000,
                       High = high * 10000,
                       Low = low * 10000,
                       Close = close * 10000,
                       Volume = volume)]
        sample_ <- sample_[, .(symbol, date, DateTime, Open, High, Low, Close, Volume)]

        # remove scientific notation
        # cols <- colnames(sample_)[3:ncol(sample_)]
        # sample_ <- sample_[, (cols) := lapply(.SD, format, scientific = FALSE), .SDcols = cols]
        # sample_ <- sample_[, lapply(.SD, sprintf, "%10d"), .SDcols = cols]

        # extract dates
        dates_ <- unique(as.Date(sample_$date))

        # path
        symbol_path <- file.path(lean_data_path, tolower(s))

        # create path if it doesnt exist, instead get already scraped
        if (!dir.exists(symbol_path)) {
          dir.create(symbol_path)
        } else {
          scraped_dates <- list.files(symbol_path)
          scraped_dates <- gsub("_.*", "", scraped_dates)
          scraped_dates <- as.Date(scraped_dates, "%Y%m%d")
          dates_ <- as.Date(setdiff(dates_, scraped_dates), origin = "1970-01-01")
        }

        # saving loop
        for (d in dates_) {
          day_minute <- sample_[as.Date(date) == d]
          if (is.null(day_minute)) {
            next()
          }
          setorderv(day_minute, "date")
          date_ <- gsub("-", "", as.character(as.Date(day_minute$date[1])))
          day_minute <- day_minute[, .(DateTime, Open, High, Low, Close, Volume)]
          # day_minute[, DateTime := as.character(DateTime)]

          # save
          file_name <- file.path(symbol_path, paste0(date_, "_", tolower(s), "_minute_trade.csv"))
          zip_file_name <- file.path(symbol_path, paste0(date_, "_trade.zip"))
          fwrite(day_minute, file_name, col.names = FALSE)
          zipr(zip_file_name, file_name)
          file.remove(file_name)
          }
      }
      # allow again scientific notation
      options(scipen=0)
    }
  )
)
