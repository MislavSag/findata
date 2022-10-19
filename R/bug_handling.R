# if (FALSE) {
#   library(data.table)
#   library(tiledb)
#   library(nanotime)
#   library(ggplot2)
#
#
#   # configure s3
#   config <- tiledb_config()
#   config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
#   config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
#   config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
#   context_with_config <- tiledb_ctx(config)
#   url <- "s3://equity-usa-minute-fmpcloud"
#
#   # factor files
#   arr_ff <- tiledb_array("s3://equity-usa-factor-files",
#                          as.data.frame = TRUE)
#   factor_files <- arr_ff[]
#   tiledb_array_close(arr_ff)
#   factor_files <- as.data.table(factor_files)
#   factor_files[, date := as.Date(as.character(date), "%Y%m%d")]
#   factor_files <- setorder(factor_files, symbol, date)
#   symbols <- unique(factor_files$symbol)
#
#   # change timezones!
#   for (s in symbols) {
#
#     # debug
#     print(s)
#
#     # import data
#     arr <- tiledb_array("D:/equity-usa-minute-fmp",
#                         as.data.frame = TRUE,
#                         selected_ranges = list(symbol = cbind(s, s)))
#     df <- arr[]
#     dt = as.data.table(df)
#
#     # change timezone
#     dt[, date := force_tz(date, "UTC")]
#     dt[, date := with_tz(date, "America/New_York")]
#     dt[, date := force_tz(date, "EST")]
#     dt[, date := with_tz(date, "UTC")]
#
#     # Check if the array already exists.
#     if (tiledb_object_type(url) != "ARRAY") {
#       fromDataFrame(
#         obj = dt,
#         uri = url,
#         col_index = c("symbol", "date"),
#         sparse = TRUE,
#         tile_domain=list(date=c(as.POSIXct("1970-01-01 00:00:00"),
#                                 as.POSIXct("2099-12-31 23:59:59"))),
#         allows_dups = FALSE
#       )
#     } else {
#       # save to tiledb
#       arr <- tiledb_array(url, as.data.frame = TRUE)
#       arr[] <- dt
#       tiledb_array_close(arr)
#     }
#   }
#
# }
#
