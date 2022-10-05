# library(tiledb)
# library(findata)
# library(data.table)
#
#
# #
# fmp = FMP$new()
#
#
# sa <- StockAnalysis$new()
# sa$get_corporate_tiledb()
#
# # configure s3
# config <- tiledb_config()
# config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
# config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
# config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
# context_with_config <- tiledb_ctx(config)
#
# arr <- tiledb_array("s3://equity-usa-corporateactions",
#                     as.data.frame = TRUE)
# ca <- arr[]
# ca <- as.data.table(ca)
# changes <- ca[query == "changes"]
# changes <- changes[, which(unlist(lapply(changes, function(x) !all(is.na(x))))), with=FALSE]
# setorder(changes, date)
#
# delisted <- ca[query == "delisted"]
# delisted <- delisted[, which(unlist(lapply(delisted, function(x) !all(is.na(x))))), with=FALSE]
# setorder(delisted, date)
#
# spinoffs <- ca[query == "spinoffs"]
# spinoffs <- spinoffs[, which(unlist(lapply(spinoffs, function(x) !all(is.na(x))))), with=FALSE]
# setorder(spinoffs, date)
#
# acquisitions <- ca[query == "acquisitions"]
# acquisitions <- acquisitions[, which(unlist(lapply(acquisitions, function(x) !all(is.na(x))))), with=FALSE]
# setorder(acquisitions, date)
#
# symbol_ = "WM"
# changes[symbol_ == oldsymbol | symbol_ == newsymbol]
# delisted[symbol == symbol_]
# spinoffs[symbol == symbol_ | old == symbol_]
# acquisitions[symbol == symbol_ | newsymbol == symbol_]
#
# fmp$get_ipo_date("AAA")
#
# # fmp_ipo <- FMP$new()
# # ipo_dates <- vapply(unique(factor_files$symbol), function(x) {
# #   y <- fmp_ipo$get_ipo_date(x)
#
# x <- fmp$get_intraday_equities(symbol = "FB",
#                                multiply = 1,
#                                time = "minute",
#                                from = "2003-01-01",
#                                to = "2003-01-10")
