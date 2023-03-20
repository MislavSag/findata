# library(findata)
# library(tiledb)
# library(nanotime)
# library(bit64)
# library(lubridate)
#
#
# # init Dukascopy (dc)
# dc = Dukascopy$new()
#
# # get dc symbols
# dc_symbols <- dc$get_symbols()
# dc_symbols[grep("SPY", dc_symbols$symbols), ]
#
# # get raw data
# dc$download_raw(save_path = "D:/dukascopy",
#                 symbols = c("AAPLUSUSD", "SPYUSUSD"),
#                 start_date = as.Date("2017-01-01"),
#                 end_date = Sys.Date() - 1)
#
# # raw data to data.table
# files <- list.files("D:/dukascopy", full.names = TRUE, recursive  = TRUE)
# quotes_data_clean <- dc$raw_to_dt(files)
#
# # configure s3
# config <- tiledb_config()
# config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
# config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
# config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
# context_with_config <- tiledb_ctx(config)
#
# # save to S3Tiledb
# save_uri <- "s3://equity-usa-quotes-dukascopy"
# if (tiledb_object_type(save_uri) != "ARRAY") {
#   fromDataFrame(
#     obj = quotes_data_clean,
#     uri = save_uri,
#     col_index = c("symbol", "date"),
#     sparse = TRUE,
#     tile_domain=list(date=c(bit64::as.integer64(as.nanotime("1970-01-01 00:00:00", tz = "UTC")),
#                             bit64::as.integer64(as.nanotime("2099-12-31 23:59:59", tz = "UTC")))),
#     allows_dups = FALSE
#   )
# } else {
#   # save to tiledb
#   arr <- tiledb_array(save_uri, as.data.frame = TRUE)
#   arr[] <- quotes_data_clean
#   tiledb_array_close(arr)
# }
#
# # create data.table
