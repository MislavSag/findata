# library(data.table)
# library(httr)
# library(rvest)
# library(fbi)
# library(checkmate)
# library(fredr)
# 
# 
# 
# # SETUP -------------------------------------------------------------------
# # globals
# NASPATH = "N:/home/Drive"
# 
# # check if we have all necessary env variables
# assert_choice("FRED-KEY", names(Sys.getenv()))
# 
# # set credentials
# fredr_set_key(Sys.getenv("FRED-KEY"))
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
# # FRED DATA ---------------------------------------------------------------
# # monthly and quaterly data
# # source: https://research.stlouisfed.org/econ/mccracken/fred-databases/
# url = "https://files.stlouisfed.org/files/htdocs/fred-md/monthly/current.csv"
# fredmd_data <- fredmd(url, date_start = NULL, date_end = NULL, transform = TRUE)
# fredmd_data <- rm_outliers.fredmd(fredmd_data)
# url = "https://files.stlouisfed.org/files/htdocs/fred-md/quarterly/current.csv"
# fredqd_data <- fredqd(url, date_start = NULL, date_end = NULL, transform = TRUE)
# 
# # get categories
# categories_id <- read_html("https://fred.stlouisfed.org/categories/") |>
#   html_elements("a") |>
#   html_attr("href")
# categories_id <- categories_id[grep("categories/\\d+", categories_id)]
# categories_id <- gsub("/categories/", "", categories_id)
# categories_id <- as.integer(categories_id)
# categories_id = unique(categories_id)
# 
# # get chinld categories
# child_categories <- lapply(categories_id, fredr_category_children)
# child_categories_ids <- rbindlist(child_categories, fill = TRUE)
# child_categories_ids <- child_categories_ids$id
# child_categories_ids = unique(child_categories_ids)
# 
# # scrap data for every category
# fred_meta_l = lapply(child_categories_ids, function(id) {
#   print(id)
#   # get first requetst for id
#   fred_meta_ <- fredr_category_series(
#     category_id = id,
#     limit = 1000L,
#     order_by = "last_updated",
#     offset = 0
#   )
#   Sys.sleep(0.8)
#   nrow_ = nrow(fred_meta_)
#   n = 1
#   while (nrow_ == 1000) {
#     print(n)
#     fred_meta_n = fredr_category_series(
#       category_id = id,
#       limit = 1000L,
#       order_by = "last_updated",
#       offset = 1000*n
#     )
#     n = n+1
#     fred_meta_ = rbindlist(list(fred_meta_, fred_meta_n), fill = TRUE)
#     nrow_ = nrow(fred_meta_n)
#     Sys.sleep(0.8)
#   }
#   if (length(fred_meta_) > 0) {
#     return(cbind(id = id, fred_meta_))  
#   } else {
#     return(NULL)
#   }
# })
# fred_meta = rbindlist(fred_meta_l, fill = TRUE)
# colnames(fred_meta)[1] = "id_category"
# dim(fred_meta)
# 
# # # save meta
# # fwrite(fred_meta, "F:/macro/fred_series_meta.csv")
# # fwrite(fred_meta, "N:/home/Drive/macro/fred_series_meta.csv")
# 
# # read meta
# fred_meta = fread("F:/macro/fred_series_meta.csv")
# 
# # clean meta
# str(fred_meta)
# date_cols = c("observation_start", "observation_end")
# fred_meta[, (date_cols) := lapply(.SD, as.Date), .SDcols = date_cols]
# 
# # filter data
# fred_meta_sample = fred_meta[observation_end > as.Date("2020-01-01")]
# dim(fred_meta_sample)
# fred_meta_sample = unique(fred_meta_sample)
# dim(fred_meta_sample)
# fred_meta_sample = unique(fred_meta_sample, by = c("id", "title"))
# 
# # fred help function
# cols = c("id", "frequency_short", "title")
# fred_meta_sample_unique = unique(fred_meta_sample, by = cols)
# str(fred_meta_sample)
# ids = fred_meta_sample_unique[, id] 
# save_path = "F:/macro/fred"
# vapply(ids, function(id_) {
#   # id_ = ids[[2]]
#   # id_ = "GDP"
#   # print(id_)
#   file_name_ = fs::path(save_path, id_, ext = "csv")
#   if (fs::file_exists(file_name_)) return(1L)
#   vin_dates = tryCatch(fredr_series_vintagedates(id_), error = function(e) NULL)
#   if (is.null(vin_dates) || length(vin_dates[[1]]) == 1) {
#     obs = fredr_series_observations(
#       series_id = id_,
#       observation_start = as.Date("1900-01-01"),
#       observation_end = Sys.Date()
#     )
#     obs$vintage = 0L
#   } else {
#     print(file_name_)
#     date_vec = vin_dates[[1]]
#     num_bins <- ceiling(length(date_vec) / 2000)
#     bins <- cut(seq_along(date_vec), 
#                 breaks = c(seq(1, length(date_vec), by = 1999), 
#                            length(date_vec)), include.lowest = TRUE, labels = FALSE)
#     split_dates <- split(date_vec, bins)
#     split_dates = lapply(split_dates, function(d) as.Date(d))
#     obs_l = lapply(split_dates, function(d) {
#       obs = fredr_series_observations(
#         series_id = id_,
#         observation_start = head(d, 1),
#         observation_end = tail(d, 1),
#         realtime_start=head(d, 1),
#         realtime_end=tail(d, 1),
#         limit = 2000
#       )
#     })
#     obs = rbindlist(obs_l)
#     obs$vintage = 1L
#   }
#   if (nrow(obs) > 90000) {
#     stop("Lots of vars")
#   }
#   fwrite(obs, fs::path(save_path, id_, ext = "csv"))
#   Sys.sleep(0.9)
#   return(1L)
# }, FUN.VALUE = integer(1L))
# 
# 
# 
# 
#   # # fred help function for vintage days
# # get_fred_vintage <- function(id = "TB3MS", name = "tbl") {
# #   start_dates <- seq.Date(as.Date("2000-01-01"), Sys.Date(), by = 365)
# #   end_dates <- c(start_dates[-1], Sys.Date())
# #   x_l = list()
# #   good = FALSE
# #   for (i in seq_along(start_dates)) {
# #     x <- tryCatch({
# #       fredr_series_observations(
# #         series_id = id,
# #         observation_start = start_dates[i],
# #         observation_end = end_dates[i],
# #         realtime_start = start_dates[i],
# #         realtime_end = end_dates[i]
# #       )
# #     }, error = function(e) e)
# #     if (("error" %in% class(x)) && grepl("The series does not exist in ALFRED", x$message)) {
# #       break()
# #     } else {
# #       x_l[[i]] = x
# #     }
# #     good = TRUE
# #   }
# #   if (good) {
# #     x_l <- mapply(map_fun, start_dates, end_dates, SIMPLIFY = FALSE)
# #     x <- rbindlist(x_l)
# #     x <- unique(x)
# #     x <- x[, .(realtime_start, value)]
# #     setnames(x, c("date", name))
# #     return(x)
# #   } else {
# #     return(get_fred(id, id, FALSE))
# #   }
# # }
