# library(httr)
# library(jsonlite)
# library(stringr)
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
#                              format = "%Y/%m/%d %H:%M:%S")
# metadata$ticker <- gsub("USUSD", "", metadata$symbol)
# head(metadata)
# dim(metadata)
#
# # scraping loop
# i = 91
# for (i in 1:nrow(metadata)) {
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
#   if (length(content(p)) == 0) {
#     next()
#   }
#
#   # save file temporarily
#   system(paste("lzma -d", binF))
#   new_file_name <- gsub("\\.lzma", "", binF)
#   con <- file(new_file_name, "rb")
#
#   # create dataframe
#   seq_along_ <- 0:(file.size(new_file_name)/20-1)
#   TIME <- vector("numeric", length(seq_along_))
#   ASKP <- vector("numeric", length(seq_along_))
#   BIDP <- vector("numeric", length(seq_along_))
#   ASKV <- vector("numeric", length(seq_along_))
#   BIDV <- vector("numeric", length(seq_along_))
#   for (i in 0:(file.size(new_file_name)/20-1)) {
#     data <- readBin(con = con, "raw", 20)
#     TIME[i] <- data[4:1] %>% rawToBits %>% as.logical %>% which %>% {2^(. - 1)} %>% sum
#     ASKP[i] <- data[8:5] %>% rawToBits %>% as.logical %>% which %>% {2^(. - 1)} %>% sum
#     BIDP[i] <- data[12:9] %>% rawToBits %>% as.logical %>% which %>% {2^(. - 1)} %>% sum
#     ASKV[i] <- data[16:13] %>% rawToBits %>% as.logical %>% which %>% {2^(. - 1)} %>% sum
#     BIDV[i] <- data[20:17] %>% rawToBits %>% as.logical %>% which %>% {2^(. - 1)} %>% sum
#   }
#   date_ <- metadata[i, "posix"]
#
#   # orginize data
#   data_by_hour <- data.frame(symbol = metadata[i, "ticker"],
#                              date = metadata[i, "posix"]+TIME/1000,
#                              askp = ASKP / 1000,
#                              bidp = BIDP / 1000,
#                              askv = ASKV / 1000,
#                              bidv = BIDV / 1000
#                              )
#
#   # save data
#
#
#   # remove new file
#   close(con)
#   file.remove(new_file_name)
# }
#
# # url <- "https://datafeed.dukascopy.com/datafeed/AAPLUSUSD/2022/07/29/15h_ticks.bi5"
#
#
#
# binF_ <- gsub("\\.lzma", "", binF)
# con <- file(binF_, "rb")
#
#
# for (i in 0:(file.size(binF_)/20-1)) {
#   data <- readBin(con = con, "raw", 20)
#   TIME <- data[4:1] %>% rawToBits %>% as.logical %>% which %>% {2^(. - 1)} %>% sum
#   ASKP <- data[8:5] %>% rawToBits %>% as.logical %>% which %>% {2^(. - 1)} %>% sum
#   BIDP <- data[12:9] %>% rawToBits %>% as.logical %>% which %>% {2^(. - 1)} %>% sum
#   ASKV <- data[16:13] %>% rawToBits %>% as.logical %>% which %>% {2^(. - 1)} %>% sum
#   BIDV <- data[20:17] %>% rawToBits %>% as.logical %>% which %>% {2^(. - 1)} %>% sum
#   print(paste(TIME, ASKP, BIDP, ASKV, BIDV))
# }
# #> [1] "246 33867231 33862497 946528651 943230116"
# #> [1] "296 33867291 33861749 947628162 943230116"
# #> [1] "421 33867003 33862299 897988541 943230116"
# #> [1] "472 33867781 33862219 947628162 943230116"
# #> [1] "2440 33867773 33862779 897988541 943230116"
# [...]
# #> [1] "3583902 33882501 33877289 947628162 943230116"
#
# ASKPRICE = 161223 / 1000
# BIDPRICE = 161197 / 1000
# ASKVOL = 1011129254 / 1000
# BIDVOL = 1011129254 / 1000
#
# as.POSIXct("2022-07-15 23:00:00")+3599976/1000
#
# 246 * 1000
