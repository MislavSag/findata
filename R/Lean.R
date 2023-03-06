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
    #' @param lean_data_path Quantconnect data equity minute folder path.
    #' @param uri TileDB uri argument.
    #' @param fast If TRUE, all daily data is readed in RAM.
    #'     You should have at least 32 GB RAM to use this option for all US.
    #'
    #' @return No value returned.
    equity_daily_from_fmpcloud = function(lean_data_path = "D:/lean/data/equity/usa/daily",
                                          uri = "D:/equity-usa-daily-fmp",
                                          fast = TRUE) {

      # debug
      # library(data.table)
      # library(findata)
      # library(tiledb)
      # library(lubridate)
      # library(zip)
      # self = Lean$new()
      # lean_data_path = "D:/lean/data/equity/usa/daily"
      # uri = "D:/equity-usa-daily-fmp"

      # remove scientific notation
      options(scipen=999)

      # get all symbols from FMP cloud
      fmp = FMP$new()
      stock_list <- fmp$get_stock_list()
      exchanges <- c("AMEX", "NASDAQ", "NYSE", "ETF", "OTC", "BATS")
      stock_list <- stock_list[exchangeShortName %in% exchanges]

      # import all daily data
      arr <- tiledb_array(uri, as.data.frame = TRUE, query_layout = "UNORDERED")
      system.time(daily_data <- arr[])
      tiledb_array_close(arr)
      daily_data <- as.data.table(daily_data)
      daily_data <- daily_data[symbol %in% unique(stock_list$symbol)]
      setorder(daily_data, symbol, date)

      # example for data
      # 19980102 00:00	136300	162500	135000	162500	6315000
      # 19980105 00:00	165000	165600	151900	160000	5677300
      # 19980106 00:00	159400	205000	147500	190000	16064600
      # 19980107 00:00	188100	192500	173100	174400	9122300

      # change every file to quantconnect like file and add to destination
      symbols_ <- unique(daily_data$symbol)
      for (s in symbols_) {

        # error
        if (s == "PRN") {
          next()
        }

        # sample data by symbol
        print(s)
        data_ <- daily_data[symbol == s]

        # format date
        data_[, date_ := paste0(format.Date(date, format = "%Y%m%d"), " 00:00")]

        # convert to lean format
        data_[, `:=`(
          DateTime = date_,
          Open = open * 1000,
          High = high * 1000,
          Low = low * 1000,
          Close = close * 1000,
          Volume = volume
        )]
        data_qc <- data_[, .(DateTime, Open, High, Low, Close, Volume)]
        cols <- colnames(data_qc)[2:ncol(data_qc)]
        data_qc <- data_qc[, (cols) := lapply(.SD, format, scientific = FALSE), .SDcols = cols]

        # save to destination
        file_name_csv <- paste0(tolower(s), ".csv")
        fwrite(data_qc, file.path(lean_data_path, file_name_csv), col.names = FALSE, )
        zip_file <- file.path(lean_data_path, paste0(tolower(s), ".zip"))
        zipr(zip_file, file.path(lean_data_path, file_name_csv))
        file.remove(file.path(lean_data_path, file_name_csv))
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
    #' @param uri TileDB uri argument.
    #' @param uri_factor_files TileDB uri argument for factor files.
    #'
    #' @return No value returned.
    equity_minute_from_fmpcloud = function(lean_data_path = "D:/lean_projects/data/equity/usa/minute",
                                           uri = "D:/equity-usa-minute-fmpcloud",
                                           uri_factor_files = "s3://equity-usa-factor-files") {

      # debug
      # library(data.table)
      # library(findata)
      # library(tiledb)
      # library(lubridate)
      # library(zip)
      # self = Lean$new()
      # lean_data_path = "D:/lean_projects/data/equity/usa/minute"
      # uri = "D:/equity-usa-minute-fmpcloud"
      # uri_factor_files = "s3://equity-usa-factor-files"

      options(scipen=999)

      # read factor files
      arr_ff <- tiledb_array(uri_factor_files,
                             as.data.frame = TRUE,
                             query_layout = "UNORDERED")
      factor_files <- arr_ff[]
      tiledb_array_close(arr_ff)
      factor_files <- as.data.table(factor_files)
      factor_files <- setorder(factor_files, symbol, date)
      factor_files[, date := format.Date(date, format = "%Y%m%d")]

      # add factor files to Lean folder
      folder <- file.path(gsub("/minute", "", lean_data_path), "factor_files")
      for (ff in unique(factor_files$symbol)) {
        sample_ <- factor_files[symbol == ff]
        file_name <- file.path(folder, paste0(tolower(ff), ".csv"))
        fwrite(sample_[, -1], file_name, col.names = FALSE)
      }

      # define tiledb array object
      arr <- tiledb_array(uri, as.data.frame = TRUE, query_layout = "UNORDERED")

      # add data to lean folder

      for (s in factor_files[, unique(symbol)]) {

        # debug
        print(s)

        # sample
        sample_ <- arr[s]
        sample_ <- as.data.table(sample_)

        # set timezone and change posix to nanotime
        sample_[, time := as.POSIXct(time,
                                     origin = as.POSIXct("1970-01-01 00:00:00",
                                                         tz = "UTC"),
                                     tz = "UTC")]

        # Dt, timezone, unique
        sample_ <- unique(sample_, by = c("symbol", "time"))
        sample_[, time := with_tz(time, tz = "America/New_York")]

        # keep trading hours
        sample_ <- sample_[as.ITime(time) %between% c(as.ITime("09:30:00"), as.ITime("16:00:00"))]

        # convert to QC format
        sample_[, `:=`(DateTime = (hour(time) * 3600 + minute(time) * 60 + second(time)) * 1000,
                       Open = open * 10000,
                       High = high * 10000,
                       Low = low * 10000,
                       Close = close * 10000,
                       Volume = volume)]
        sample_ <- sample_[, .(symbol, time, DateTime, Open, High, Low, Close, Volume)]

        # remove scientific notation
        # cols <- colnames(sample_)[3:ncol(sample_)]
        # sample_ <- sample_[, (cols) := lapply(.SD, format, scientific = FALSE), .SDcols = cols]
        # sample_ <- sample_[, lapply(.SD, sprintf, "%10d"), .SDcols = cols]

        # extract dates
        dates_ <- unique(as.Date(sample_$time))

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
          day_minute <- sample_[as.Date(time) == d]
          if (is.null(day_minute)) {
            next()
          }
          setorderv(day_minute, "time")
          date_ <- gsub("-", "", as.character(as.Date(day_minute$time[1])))
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

      # allow again scientific notation and close tiledb array
      tiledb_array_close(arr)
      options(scipen=0)
    }
  )
)
