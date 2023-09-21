# library(data.table)
# library(httr)
# library(rvest)
# library(fbi)
# library(checkmate)
# library(fredr)
# 
# 
# 
# 
# # SETUP -------------------------------------------------------------------
# # globals
# NASPATH       = "C:/Users/Mislav/SynologyDrive/trading_data"
# 
# # check if we have all necessary env variables
# assert_choice("FRED-KEY", names(Sys.getenv()))
# 
# # set credentials
# fredr_set_key(Sys.getenv("FRED-KEY"))
# 
# 
# 
# # YIELD CURVE -------------------------------------------------------------
# # get raw yield data from Deparmetnt of Treasury
# url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView"
# urls = paste0(url, "?type=daily_treasury_yield_curve&field_tdr_date_value=all&page=", 1:27)
# yields_raw = list()
# for (i in seq_along(urls)) {
#   print(i)
#   req = GET(urls[i], user_agent("Mozilla/5.0"))
#   yields_raw[[i]] = content(req) %>%
#     html_element("table") %>%
#     html_table()
#   Sys.sleep(1L)
# }
# 
# # clean yields data
# yields = rbindlist(yields_raw)
# cols_keep = c("Date", colnames(yields)[11:ncol(yields)])
# yields = yields[, ..cols_keep]
# cols_num = colnames(yields)[2:ncol(yields)]
# yields[, (cols_num) := lapply(.SD, as.numeric), .SDcols = cols_num]
# yields[, Date := as.Date(Date, format = "%m/%d/%Y")]
# yields = yields[, .(date = Date,
#                     y30 = `30 Yr`,
#                     y20 = `20 Yr`,
#                     y10 = `10 Yr`,
#                     y2 = `2 Yr`,
#                     y1 = `1 Yr`,
#                     m6 = `6 Mo`)]
# yields[, ys_y10_m6 := y10 / m6]
# yields[, ys_y10_y1 := y10 / y1]
# yields[, ys_y10_y1 := y10 / y2]
# yields[, ys_y20_m6 := y20 / m6]
# yields[, ys_y20_y1 := y20 / y1]
# yields[, ys_y20_y1 := y20 / y2]
# 
# 
# 
# # FRED DATA ---------------------------------------------------------------
# # monthly and quaterly data
# # source: https://research.stlouisfed.org/econ/mccracken/fred-databases/
# url = "https://files.stlouisfed.org/files/htdocs/fred-md/monthly/current.csv"
# fredmd_data <- fredmd(url, date_start = NULL, date_end = NULL, transform = TRUE)
# fredmd_data <- rm_outliers.fredmd(fredmd_data)
# url = "https://files.stlouisfed.org/files/htdocs/fred-md/quarterly/current.csv"
# fredqd_data <- fredqd(url, date_start = NULL, date_end = NULL, transform = TRUE)
# 
# # fred help function
# get_fred <- function(id = "VIXCLS", name = "vix", calculate_returns = FALSE) {
#   x <- fredr_series_observations(
#     series_id = id,
#     observation_start = as.Date("2000-01-01"),
#     observation_end = Sys.Date()
#   )
#   x <- as.data.table(x)
#   x <- x[, .(date, value)]
#   x <- unique(x)
#   setnames(x, c("date", name))
#   
#   # calcualte returns
#   if (calculate_returns) {
#     x <- na.omit(x)
#     x[, paste0(name, "_ret_month") := get(name) / shift(get(name), 22) - 1]
#     x[, paste0(name, "_ret_year") := get(name) / shift(get(name), 252) - 1]
#     x[, paste0(name, "_ret_week") := get(name) / shift(get(name), 5) - 1]
#   }
#   return(x)
# }
# 
# # fred help function for vintage days
# get_fred_vintage <- function(id = "TB3MS", name = "tbl") {
#   start_dates <- seq.Date(as.Date("2000-01-01"), Sys.Date(), by = 365)
#   end_dates <- c(start_dates[-1], Sys.Date())
#   x_l = list()
#   good = FALSE
#   for (i in seq_along(start_dates)) {
#     x <- tryCatch({
#       fredr_series_observations(
#         series_id = id,
#         observation_start = start_dates[i],
#         observation_end = end_dates[i],
#         realtime_start = start_dates[i],
#         realtime_end = end_dates[i]
#       )
#     }, error = function(e) e)
#     if (("error" %in% class(x)) && grepl("The series does not exist in ALFRED", x$message)) {
#       break()
#     } else {
#       x_l[[i]] = x
#     }
#     good = TRUE
#   }
#   if (good) {
#     x_l <- mapply(map_fun, start_dates, end_dates, SIMPLIFY = FALSE)
#     x <- rbindlist(x_l)
#     x <- unique(x)
#     x <- x[, .(realtime_start, value)]
#     setnames(x, c("date", name))
#     return(x)
#   } else {
#     return(get_fred(id, id, FALSE))
#   }
# }
# 
# # get FRED ids
# fred_meta = fread(file.path(NASPATH, "macro_fred_meta.csv"))
# 
# # fitler fred meta
# fred_meta =  fred_meta[observation_end > Sys.Date() - 30]
# fred_meta =  fred_meta[observation_start < as.Date("2010-01-01")]
# 
# # get daily observations
# fred_daily_l = lapply(fred_meta[, id], get_fred_vintage)
# fred_daily = Reduce(function(x, y) merge(x, y, by = "date", all = TRUE), fred_daily_l)
# 
# # merge all fred data
# cols = colnames(fredmd_data)[2:ncol(fredmd_data)]
# setnames(fredmd_data, cols, paste0(cols, "_m"))
# cols = colnames(fredqd_data)[2:ncol(fredqd_data)]
# setnames(fredqd_data, cols, paste0(cols, "_q"))
# fred_data = Reduce(function(x, y) merge(x, y, by = "date", all = TRUE), 
#                    list(fred_daily, fredmd_data, fredqd_data))
# 
# 
# 
# # MERGE ALL DATA ----------------------------------------------------------
# # merge all macro data
# macro_indicators = Reduce(function(x, y) merge(x, y, by = "date", all = TRUE), 
#                           list(yields, fred_data))
# 
# # save to NAS
# fwrite(macro_indicators, file.path(NASPATH, "macro_predictors.csv"))
