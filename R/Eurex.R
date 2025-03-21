#' @title Eurex Class
#'
#' @description
#' Get data data from Eurex.
#'
#' @export
Eurex = R6::R6Class(
  "Eurex",
  public = list(
    #' @field delay Delay between requests.
    delay = NULL,
    
    #' @description
    #' Create a new Eurex object.
    #'
    #' @param delay Delay between requests.
    #'
    #' @return A new `MacroData` object.
    initialize = function(delay = 0.8) {
      # DEBUG
      # path_to_dump = "F:/temp"
      # self = list()
      # self$path_to_dump = path_to_dump
      
      # checks
      # assert_character(path_to_dump, len = 1L)
      # assert_true(dir.exists(path_to_dump))
      assert_numeric(delay, len = 1L)
      
      # set init vars
      self$delay = delay
    },

    #' @description
    #' Get instruments from Eurex.
    #' 
    #' @return Get instruments.
    get_instruments = function() {
      res = GET(
        "https://www.eurex.com/ex-en!dynSearch/",
        add_headers(
          "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        )
      )
      res = content(res)
      return(rbindlist(lapply(res[[1]], as.data.table), fill = TRUE))
    },
    
    #' @description
    #' Get markets from Eurex.
    #' 
    #' @return Get markets.
    get_markets = function() {
      p = GET(
        "https://www.eurex.com/ex-en/markets",
        add_headers(
          "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        )
      )
      res = content(p)
      res = res |> 
        html_elements("a") |>
        html_attr("href") |>
        unique() %>% 
        .[grepl("/ex-en/markets", .)] |>
        tstrsplit(x = _, "/", keep = 1:5, fixed = TRUE) |>
        as.data.table() |>
        unique() |>
        na.omit() |>
        _[, url := paste0("https://www.eurex.com/", V2, "/", V3, "/", V4, "/", V5)]
      return(res)
    },
    
    # get_meta = function() {
    #   
    #   # markets = get_markets()
    #   markets = self$get_markets()
    #   
    #   # loop over market urls and get all hrefs
    #   markets_urls = list()
    #   for (i in 1:nrow(markets)) { # 1:nrow(markets)
    #     u = markets[i, url]
    #     print(u)
    #     html_ = RETRY("GET", u)
    #     html_ = content(html_)
    #     hrefs = html_ |>
    #       html_elements("a") |>
    #       html_attr("href") |>
    #       unique() %>% 
    #       .[grepl("ex-en/markets", .)] %>% 
    #       .[grepl("\\d+", .)] %>%
    #       paste0("https://www.eurex.com", .)
    #       
    #     markets_urls[[i]] = hrefs
    #     Sys.sleep(2L)
    #   }
    #   
    #   markets_urls = unique(unlist(markets_urls))
    #   
    #   desc = gsub(".*/", "", markets_urls)
    #   desc = gsub("-\\d+", "", desc)
    #   
    #   instruments = get_instruments()
    #   instruments[, desc := gsub(" ", "-", PRODUCT_NAME)]
    #   
    #   instruments[as.data.table(cbind(markets_urls, desc)), on = "desc"]
    #   
    #   return(markets_urls)
    # },
        
    #' @description Get daily data for future symbol.
    #'
    #' @param id Start date
    #' @param start_date Start date
    #' @param end_date End date
    #'
    #' @return Get investing com ea data.
    get_ticks = function(id, 
                         start_date = start_date,
                         end_date = end_date) {
      # debug
      # library(qlcal)
      # library(lubridate)
      # library(rusquant)
      # library(arrow)
      # library(checkmate)
      # qlcal::setCalendar("UnitedStates/NYSE")
      # id = 34642
      # start_date = "2025-01-01"
      # end_date = Sys.Date()
      
      # Create base url
      url = paste0("https://www.eurex.com/api/v1/overallstatistics/", id)
      
      # set download timeout argument using options
      options(timeout = 120)
      
      # Create days
      dates = seq.Date(as.Date(start_date), as.Date(end_date), 1)
      dates = format.Date(dates, "%Y%m%d")
      
      # main loop
      dt_ = lapply(dates, function(d) {
        # debug
        # d = dates[6] # 3123
        # d = "2020-02-28"
        print(d)
        
        # fair scraping
        Sys.sleep(self$delay)
        
        # get data
        p = RETRY(
          "GET",
          url,
          query = list("busdate" = d)
        )
        res = content(p)
        
        # Check if data is available
        if (!("dataRows" %in% names(res))) {
          return(NULL)
        }
        
        # clean data
        header = data.table(
          date_origin = d,
          underlying_close = res$header$underlyingClosingPrice,
          open_interest = res$header$openInterest,
          volume = res$header$volume
        )
        meta = as.data.table(res$meta)
        meta = cbind(meta, header)
        data = lapply(res$dataRows, as.data.table)
        data = rbindlist(data, fill = TRUE)
        data[, date_origin := d]
        
        return(list(meta = meta, data = data))
      })
      meta_dt = rbindlist(lapply(dt_, function(x) x$meta))
      data_dt = rbindlist(lapply(dt_, function(x) x$data))
      
      return(list(meta_dt = meta_dt, data_dt = data_dt))
    }
  )
)

# library(checkmate)
# 
# eurex = Eurex$new(delay = 0.1)
# dt = eurex$get_ticks(34642, "2025-01-01", "2025-01-10")
