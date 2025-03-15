#' @title FirstRate Class
#'
#' @description
#' Get data data from FirstRate.
#'
#' @export
FirstRate = R6::R6Class(
  "FirstRate",
  inherit = DataAbstract,

  public = list(
    #' @field userid FirstRate user ID.
    userid = NULL,

    #' @description
    #' Create a new FirstRate object.
    #'
    #' @param userid FirstRate user ID.
    #' @param context_with_config AWS S3 Tiledb config
    #'
    #' @return A new `FirstRate` object.
    initialize = function(userid = NULL,
                          context_with_config = NULL) {

      # endpoint
      super$initialize(context_with_config)

      # check and define variables
      if (is.null(userid)) {
        self$userid = assert_character(Sys.getenv("FIRSTRATE-USERID"))
      } else {
        self$userid = userid
      }
    },

    #' @description
    #' Download stock data from FirstRate.
    #'
    #' @param save_dest Save path
    #' @param type Security type
    #' @param period Period.
    #' @param ticker_range First letter of symbols.
    #' @param timeframe Time frequency.
    #' @param adjustment Adjustment type.
    #'
    #' @return NULL.
    get_historical_data = function(save_dest,
                                   type = c("stock"),
                                   period = c("full", "month", "week", "day"),
                                   ticker_range = toupper(letters),
                                   timeframe = c("1min", "5min", "30min", "1hour", "1day"),
                                   adjustment = c("adj_split", "adj_splitdiv", "UNADJUSTED")) {

      # debug
      # library(httr)
      # library(checkmate)
      # save_dest = "D:/FirstRate"
      # type = "stock"
      # period = "full"
      # ticker_range = toupper(letters)
      # timeframe = "1hour"
      # adjustment = "adjsplitdiv"
      # self = list()
      # self$userid = Sys.getenv("FIRSTRATE-USERID")

      # checks
      assert_choice(timeframe, c("1min", "5min", "30min", "1hour", "1day"))
      assert_choice(adjustment, c("adj_split", "adj_splitdiv", "UNADJUSTED"))

      # url endpoint
      url_endpoint <- "https://firstratedata.com/api/data_file"

      # modifyi url so it includes all parameters
      url_req <- vapply(ticker_range, function(x) {
        modify_url(url_endpoint,
                   query = list(type = type,
                                period = period,
                                ticker_range = x,
                                timeframe = timeframe,
                                adjustment = adjustment,
                                userid = self$userid))
      }, FUN.VALUE = character(1))

      # download file
      options(timeout=60*100)
      lapply(url_req, function(url_) {
        file_name <- unlist(parse_url(url_)$query)
        file_name <- paste0(file_name[-length(file_name)], collapse = "_")
        file_name <- file.path(save_dest, paste0(file_name, ".zip"))
        download.file(url_, file_name, mode = "wb")
      })
      options(timeout=60)

      return(NULL)
    },

    #' @description
    #' Save data to TileDB Uri.
    #'
    #' @param zip_files Zip files downloaded from FirstRate site.
    #' @param save_uri TileDB URI to save data to.
    #'
    #' @return NULL
    firstrate_to_tledb = function(zip_files,
                                  save_uri = "s3://equity-usa-hour-firstrate-adjusted") {

      # debug
      # library(data.table)
      # library(lubridate)
      # library(tiledb)
      # zip_files = list.files(path = "D:/FirstRate",
      #                    pattern = "1hour.*adj_",
      #                    full.names = TRUE)
      # config <- tiledb_config()
      # config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
      # config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
      # config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
      # tiledb_ctx(config)

      #
      for (z in zip_files[1:length(zip_files)]) {

        # debug
        # zip_files <- "C:/Users/Mislav/Downloads/hour_adjusted.zip"
        # z <- "C:/Users/Mislav/Downloads/hour_adjusted.zip"
        print(z)

        # unzip , clean and merge al files
        zip::unzip(z, exdir = dirname(zip_files)[1])
        files_txt <- list.files(dirname(zip_files)[1],
                                pattern = "txt",
                                full.names = TRUE)

        # parse and save to S3 loop
        data_ <- list()
        for (i in seq_along(files_txt)) {

          # debug
          # f <- "C:/Users/Mislav/Downloads/BVXV_1-hour.txt"
          f <- files_txt[i]
          print(f)

          #
          DT <- fread(f)
          if (length(DT) > 6) {
            print(paste0("Problem with file ", f))
            next()
          } else if (length(DT) == 0) {
            print(paste0("No data in file ", f))
            next()
          }
          setnames(DT, c("time", "open", "high", "low", "close", "volume"))
          DT[, time := force_tz(time, "America/New_York")]
          DT[, time := with_tz(time, "UTC")]
          DT[, symbol := gsub("_.*", "", basename(f))]
          data_[[i]] <- unique(DT, by = c("symbol", "time"))
        }

        # merge all data
        data_merged <- rbindlist(data_)

        # save to AWS S3
        if (tiledb_object_type(save_uri) != "ARRAY") {
          fromDataFrame(
            obj = as.data.frame(data_merged),
            uri = save_uri,
            col_index = c("symbol", "time"),
            sparse = TRUE,
            tile_domain=list(time=c(as.POSIXct("1970-01-01 00:00:00", tz = "UTC"),
                                    as.POSIXct("2099-12-31 23:59:59", tz = "UTC"))),
            allows_dups = FALSE
          )
        } else {
          # save to tiledb
          arr <- tiledb_array(save_uri, as.data.frame = TRUE)
          arr[] <- as.data.frame(data_merged)
          tiledb_array_close(arr)
        }

        # delete unzipped files
        file.remove(files_txt)
      }
      return(NULL)
    }
  )
)
