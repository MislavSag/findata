#' @title Dukascopy Class
#'
#' @description
#' Get data data from Dukascopy.
#'
#' @export
Dukascopy = R6::R6Class(
  "Dukascopy",
  inherit = DataAbstract,

  public = list(

    #' @description
    #' Create a new Dukascopy object.
    #'
    #' @param azure_storage_endpoint Azure storate endpont
    #' @param context_with_config AWS S3 Tiledb config
    #'
    #' @return A new `Dukascopy` object.
    initialize = function(azure_storage_endpoint = NULL,
                          context_with_config = NULL) {

      # endpoint
      super$initialize(azure_storage_endpoint, context_with_config)
    },

    #' @description
    #' Get all symbols from Dukascopy
    #'
    #' @return Vector of symbols.
    get_symbols = function() {
      quotes <- fromJSON("https://www.dukascopy.com/plugins/quotes-list/base.json.php")
      etfs <- read_html("https://www.dukascopy.com/swiss/english/cfd/range-of-markets/") |>
        html_elements("#ETFtable") |>
        html_table() |>
        (`[[`)(1) |>
        as.data.frame()
      colnames(etfs) <- c("instr", "desc", "market", "point")
      quotes <- rbindlist(list(quotes, etfs), fill = TRUE)
      quotes$symbols <- gsub("/|\\.", "", quotes$instr)

      return(quotes)
    },

    #' @description
    #' IB GET object
    #'
    #' @param save_path Saving directory.
    #' @param symbols Sybmols to scrap.
    #' @param start_date Start date.
    #' @param end_date End date.
    #'
    #' @references \url{https://www.interactivebrokers.com/api/doc.html}
    #' @return GET response.
    download_raw = function(save_path = "D:/dukascopy",
                            symbols = "SPYUSUSD",
                            start_date = as.Date("2017-01-01"),
                            end_date = Sys.Date() - 1) {



      # checks
      if (!(dir.exists(save_path))) {
        stop("Directory doesn-t exists!")
      }

      # metadata
      url_base <- "https://datafeed.dukascopy.com/datafeed"
      seq_dates <- seq.Date(start_date, end_date, by = 1)
      seq_dates <- format.Date(seq_dates, format = "%Y/%m/%d")
      file_name <- paste0(formatC(0:23, width = 2, format = "d", flag = "0"),
                          "h_ticks.bi5")
      metadata <- expand.grid(file_name, seq_dates, symbols, url_base,
                              stringsAsFactors = FALSE)
      urls <- paste(metadata$Var4, metadata$Var3, metadata$Var2, metadata$Var1,
                    sep = "/")
      save_paths <- paste(save_path, metadata$Var3, gsub("/", "-", metadata$Var2), metadata$Var1,
                          sep = "/")
      save_paths <- gsub("\\.bi5", "\\.lzma", save_paths)

      # remove urls and paths already scraped
      remove_indecies <- which(file.exists(save_paths))
      if (length(remove_indecies) > 0) {
        save_paths <- save_paths[-remove_indecies]
        urls <- urls[-remove_indecies]
        }

      # create directories
      lapply(unique(dirname(save_paths)), dir.create, recursive = TRUE)

      # scraping loop
      for (i in seq_along(urls)) {

        # get dukascopy file and save localy
        p <- GET(urls[i],
                 add_headers("user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36"),
                 write_disk(save_paths[i], overwrite = TRUE))


        # check status
        if (!(status_code(p) %in% c(200, 404))) {
          stop(paste0("Check url ", urls[i]))
        }
      }

      return(NULL)
    },

    #' @description
    #' Parse Dukascopy data and convert to data.table format
    #'
    #' @param files Bin files (lzma files) downlaoded from Dukascopy and saved
    #'     localy.
    #'
    #' @return Data.table with ask and bid prices and volumes.
    raw_to_dt = function(files) {

      # debug
      # files <- list.files("D:/dukascopy",
      #                     full.names = TRUE,
      #                     recursive  = TRUE)
      # library(nanotime)

      # keep only files with positive size
      files_sizes <- file.size(files)
      files <- files[files_sizes > 0]
      data_by_hour <- lapply(files, function(f) {

        print(f)

        # decompress file and save it temporarily
        decompresed <- tryCatch({system(paste("lzma -d", f, " --keep --force"),
                                        intern = TRUE)},
                                error = function(e) NA)
        if (!(is.null(attributes(decompresed)))) {
          return(NULL)
        }

        # open new filw
        new_file_name <- gsub("\\.lzma", "", f)
        con <- file(new_file_name, "rb")

        # create dataframe
        seq_along_ <- 0:(file.size(new_file_name)/20-1)
        TIME <- vector("numeric", length(seq_along_))
        ASKP <- vector("numeric", length(seq_along_))
        BIDP <- vector("numeric", length(seq_along_))
        ASKV <- vector("numeric", length(seq_along_))
        BIDV <- vector("numeric", length(seq_along_))
        for (i in 0:(file.size(new_file_name)/20-1)) {
          data <- readBin(con = con, "raw", 20)
          TIME[i] <- data[4:1] |> rawToBits() |> as.logical() |> which() |> (\(x) 2^(x - 1))() |> sum()
          ASKP[i] <- data[8:5] |> rawToBits() |> as.logical() |> which() |> (\(x) 2^(x - 1))() |> sum()
          BIDP[i] <- data[12:9]  |> rawToBits() |> as.logical() |> which() |> (\(x) 2^(x - 1))() |> sum()
          ASKV[i] <- data[16:13] |> rawToBits() |> as.logical() |> which() |> (\(x) 2^(x - 1))() |> sum()
          BIDV[i] <- data[20:17] |> rawToBits() |> as.logical() |> which() |> (\(x) 2^(x - 1))() |> sum()
        }

        # orginize data
        split_path <- strsplit(new_file_name, "/", fixed = TRUE)[[1]]
        datetime_ <- as.nanotime(paste0(split_path[length(split_path)-1],
                                        " ", substr(split_path[length(split_path)], 1, 2),
                                        ":00:00"), tz = "UTC") # tz = "UTC" imlicitly
        data_by_hour <- data.frame(symbol = split_path[length(split_path)-2],
                                   date = datetime_+TIME*1000000,
                                   askp = ASKP / 1000,
                                   bidp = BIDP / 1000,
                                   askv = ASKV / 1000,
                                   bidv = BIDV / 1000)

        # remove new file
        close(con)

        return(data_by_hour)
      })
      # merge list elements
      quotes_data <- rbindlist(data_by_hour)

      # remove observations where all double values are 0
      quotes_data_clean <- quotes_data[askp > 0 & bidp > 0 & askv > 0 & bidv > 0]

      # remove tmep files
      file.remove(gsub("\\.lzma", "", files))

      return(quotes_data_clean)
    }
  )
)

# library(httr)
# library(jsonlite)
# library(nanotime)
# library(stringr)
# library(tiledb)
#
#
# # setup
# dir_ <- "C:/Users/Mislav/Downloads"
# binF <- file.path(dir_, "ticks.lzma")
#
# # metadata
# url_base <- "https://datafeed.dukascopy.com/datafeed"
# symbols <- jsonlite::fromJSON("https://www.dukascopy.com/plugins/quotes-list/base.json.php")
# symbols_us <- symbols[symbols$market == "US", "instr"]
# symbols_us <- gsub("/|\\.", "", symbols_us)
# seq_dates <- seq.Date(as.Date("2020-01-01"), Sys.Date() - 1, by = 1)
# seq_dates <- format.Date(seq_dates, format = "%Y/%m/%d")
# file_name <- paste0(str_pad(0:23, width = 2, side = "left", pad = "0"), "h_ticks.bi5")
# metadata <- expand.grid(file_name, seq_dates, symbols_us, url_base, stringsAsFactors = FALSE)
# metadata$url <- paste(metadata$Var4, metadata$Var3, metadata$Var2, metadata$Var1, sep = "/")
# metadata$Var4 <- NULL
# colnames(metadata) <- c("file_name", "date", "symbol", "url")
# metadata$posix <- as.POSIXct(paste0(metadata$date, " ", substr(metadata$file_name, 1, 2), ":00:00"),
#                              format = "%Y/%m/%d %H:%M:%S", tz = "UTC")
# metadata$ticker <- gsub("USUSD", "", metadata$symbol)
# metadata <- metadata[metadata$ticker == "AAPL", ]
# head(metadata)
# dim(metadata)
#
# # configure s3
# config <- tiledb_config()
# config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
# config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
# config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
# context_with_config <- tiledb_ctx(config)
#
# # scraping loop
# # i = 63
# for (i in 1:nrow(metadata)) {
#   print(i)
#
#   # meta
#   url <- metadata[i, "url"]
#
#   # get dukascopy file and save localy
#   p <- GET(url,
#            add_headers("user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36"),
#            write_disk(binF, overwrite = TRUE))
#
#   # test if there is data for url
#   if (length(content(p, encoding = "UTF-8")) == 0 | status_code(p) == 404) {
#     next()
#   }
#
#   # save file temporarily
#   system(paste("lzma -d", binF))
#   new_file_name <- gsub("\\.lzma", "", binF)
#   new_file_name_renamed <- paste0(new_file_name, i)
#   file.rename(new_file_name, new_file_name_renamed)
#   con <- file(new_file_name_renamed, "rb")
#
#   # create dataframe
#   seq_along_ <- 0:(file.size(new_file_name_renamed)/20-1)
#   TIME <- vector("numeric", length(seq_along_))
#   ASKP <- vector("numeric", length(seq_along_))
#   BIDP <- vector("numeric", length(seq_along_))
#   ASKV <- vector("numeric", length(seq_along_))
#   BIDV <- vector("numeric", length(seq_along_))
#   for (i in 0:(file.size(new_file_name_renamed)/20-1)) {
#     data <- readBin(con = con, "raw", 20)
#     TIME[i] <- data[4:1] %>% rawToBits %>% as.logical %>% which %>% {2^(. - 1)} %>% sum
#     ASKP[i] <- data[8:5] %>% rawToBits %>% as.logical %>% which %>% {2^(. - 1)} %>% sum
#     BIDP[i] <- data[12:9] %>% rawToBits %>% as.logical %>% which %>% {2^(. - 1)} %>% sum
#     ASKV[i] <- data[16:13] %>% rawToBits %>% as.logical %>% which %>% {2^(. - 1)} %>% sum
#     BIDV[i] <- data[20:17] %>% rawToBits %>% as.logical %>% which %>% {2^(. - 1)} %>% sum
#   }
#
#   # orginize data
#   data_by_hour <- data.frame(symbol = metadata[i, "ticker"],
#                              date = as.nanotime(metadata[i, "posix"])+TIME*1000000,
#                              askp = ASKP / 1000,
#                              bidp = BIDP / 1000,
#                              askv = ASKV / 1000,
#                              bidv = BIDV / 1000
#                              )
#
#   # save to AWS S3 TileDB
#   uri <- "s3://equity-usa-quote-dukascopy"
#   if (tiledb_object_type(uri) != "ARRAY") {
#     fromDataFrame(
#       obj = data_by_hour,
#       uri = uri,
#       col_index = c("symbol", "date"),
#       sparse = TRUE,
#       tile_domain=list(date=c(as.nanotime("2015-06-17T00:00:00"),
#                               as.nanotime("2099-06-17T00:00:00"))),
#       allows_dups = FALSE
#     )
#   } else {
#     # save to tiledb
#     arr <- tiledb_array(uri, as.data.frame = TRUE)
#     arr[] <- data_by_hour
#     tiledb_array_close(arr)
#   }
#
#   # remove new file
#   close(con)
#   file.remove(new_file_name_renamed)
# }
