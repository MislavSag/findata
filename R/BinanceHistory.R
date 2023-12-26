#' @title BinanceHistory Class
#'
#' @description
#' Get history data from Binance.
#'
#' @export
BinanceHistory = R6::R6Class(
  "BinanceHistory",
  public = list(
    #' @field path_to_dump Local path to save data to.
    path_to_dump = NULL,
    
    #' @description
    #' Create a new BinanceHistory object.
    #'
    #' @param path_to_dump Local path to save data to.
    #'
    #' @return A new `BinanceHistory` object.
    initialize = function(path_to_dump) {
      # TODO: DEBUG
      # path_to_dump = "F:/binance"
      # self = list()
      # self$path_to_dump = path_to_dump
      
      # check path_to_dump
      assert_character(path_to_dump, len = 1L)
      
      # set init vars
      self$path_to_dump = path_to_dump
    },
    
    #' @description
    #' dump historic data from binance.
    #'
    #' @param asset binance asset. Can be cm, um, option or spot
    #' @param data_type binance data type.
    #' @param frequency time frequency.
    #'
    #' @return NULL.
    dump_binance = function(asset, data_type, frequency) {
      # TODO DEBUG
      # asset = "spot"
      # data_type = "klines"
      # frequency = c("1d", "1h")
      
      # data types by asset
      futures_types = c(
        "aggTrades",
        "bookTicker",
        "fundingRate",
        "indexPriceKlines",
        "klines",
        "markPriceKlines",
        "premiumIndexKlines",
        "trades"
      )
      option_types = c("BVOLIndex", "EOHSummary")
      spot_types = c("aggTrades", "klines", "trades")

      # frequencies
      freqs <- c("12h", "15m", "1d", "1h", "1m", "1mo", "1s", "1w", 
                 "2h", "30m", "3d", "3m", "4h", "5m", "6h", "8h")
      
      # checks
      assert_choice(asset, c("cm", "um", "option", "spot"))
      if (asset %in% c("cm", "um")) {
        assert_choice(data_type, choices = futures_types)
      } else if (asset == "option") {
        assert_choice(data_type, choices = option_types)
      } else if (asset == "spot") {
        assert_choice(data_type, choices = spot_types)
      }
      assert_vector(frequency)
      assert_subset(frequency, choices = freqs)
      
      # get symbols
      url_ = paste0(
        private$url_data,
        "data", "/",
        ifelse(asset %in% c("cm", "um"), paste0("futures/", asset), asset),
        "/", "monthly", "/", data_type, "/")
      
      # parse symbols
      urls = private$parse_xml(url_, 
                               "//CommonPrefixes", 
                               ".//Prefix", 
                               private$url_data)
      while ((length(urls) %% 1000 == 0)) {
        url_suffix_ = paste0("&marker=", gsub(".*prefix=", "", tail(urls, 1)))
        urls_ = private$parse_xml(url_,
                                  "//CommonPrefixes",
                                  ".//Prefix",
                                  private$url_data,
                                  url_suffix_)
        urls = c(urls, urls_)  
      }
      urls = unique(urls)
      
      # get data for every symbol
      for (t in c("monthly", "daily")) {
        # TODO DEBUG
        # t = "daily"
        print(t)
        
        # change url if monthly
        if (t == "daily") {
          urls_ = gsub("monthly", "daily", urls) 
        } else {
          urls_ = urls
        }
        
        # loop for all frequencies
        for (f in frequency) {
          # TODO DEBUG
          # f = "1d"
          
          # define url
          url_ = paste0(urls_, f, "/")
          
          # get ziped data
          for (u in url_) {
            # TODO DEBUG
            # u = url_[1]
            
            # get meta data for folder
            if (t == "daily") {
              date_floor = floor_date(Sys.Date(), unit = "month")
              pre = gsub("\\/", "-", gsub(paste0(".*", paste0(data_type, "/")), "", u))
              pre = paste0(pre, date_floor - 1, ".zip.CHECKSUM")
              u_ = paste0(u, "&marker=", gsub(".*prefix=", "", u), pre)
            } else {
              u_ = u
            }
            url_files = private$parse_xml(u_, "//Contents", ".//Key", private$url_host)
            if (is.null(url_files)) next()
            url_files = url_files[!grepl("CHECKSUM", url_files)]
            
            # create directory if it doesn't exists
            dir_name_ = path(self$path_to_dump, gsub(".*data/", "", u))
            if (!dir_exists(dir_name_)) dir_create(dir_name_, recurse = TRUE)
            
            # create file names
            file_names = path(dir_name_, path_file(url_files))

            # get already downloaded folders/files
            dir_data = dir_ls(dir_name_)
            if (length(dir_data) > 0) {
              # for daily, remove files not in daily anymore
              if (t == "daily") {
                files_remove = setdiff(dir_data, file_names)
                file_delete(files_remove)
              }
              # return only new files we need to download
              file_names = setdiff(file_names, dir_data)
              if (length(file_names) == 0) next() 
              url_files = url_files[path_file(url_files) %in% path_file(file_names)]
            }
            
            # final loop
            for (i in seq_along(file_names)) {
              download.file(url_files[i], file_names[i], quiet = TRUE)
            }
            # utils::unzip(file_name_, exdir = path_dir(file_name_))
            # fs::file_delete(file_name_)
          }
        }
      }
    }
    ),
  private = list(
    parse_xml = function(url, attr_1, attr_2, url_prefix, url_suffix = NULL) {
      # url = u_; attr_1 = "//Contents"; attr_2 = ".//Key"; url_prefix = private$url_host
      if (!is.null(url_suffix)) {
        url = paste0(url, url_suffix)
      }
      res = GET(url)
      p = content(res, "parsed", type = "application/xml", encoding = "UTF-8")
      xml_data <- xml_ns_strip(p)
      contents_nodes <- xml_find_all(xml_data, attr_1)
      if (length(contents_nodes) == 0) return(NULL)
      url_data_l <- lapply(contents_nodes, function(node) {
        prefix <- xml_text(xml_find_first(node, attr_2))
        return(data.frame(symbol = prefix))
      })
      urls = rbindlist(url_data_l)[[1]]
      urls = paste0(url_prefix, urls)
    },
    url_data = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=",
    url_host = "https://data.binance.vision/"
  )
)
