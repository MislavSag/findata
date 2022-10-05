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
