#' @title Yields Class
#'
#' @description
#' Get data on yields curve for various countries.
#'
#' @export
Yields = R6::R6Class(
  "Yields",
  
  public = list(
    #' @field countries Countries to scrap data for. Default for all countres.
    countries = NULL,
    
    #' @description
    #' Create a new Yields object.
    #'
    #' @param countries character vector of countries to scrap data for.
    #'
    #' @return A new `Yields` object.
    initialize = function(countries = c("US", "China", "EU")) {
      self$countries = countries
    },
    
    #' @description
    #' Scrap yields.
    #'
    #' @param eu_yields_history_file yields histoty path.
    #'
    #' @return Data.table with yields data
    get_yields = function(eu_yields_history_file) {
      if ("US" %in% self$countries) {
        yields_us = self$get_yields_us()
      } else {
        yields_us = NULL
      }
      if ("China" %in% self$countries) {
        yields_china = self$get_yields_china()
      } else {
        yields_china = NULL
      }
      if ("EU" %in% self$countries) {
        yields_eu = self$get_yields_eu(eu_yields_history_file)
      } else {
        yields_eu = NULL
      }
      yields_all = rbindlist(list(yields_us, yields_china, yields_eu))
      return(yields_all)
    },
    
    #' @description
    #' Scrap yields for US.
    #'
    #' @return Data.table with yields data
    get_yields_us = function() {
      url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView"
      url_prefix = paste0(url, "?type=daily_treasury_yield_curve&field_tdr_date_value=all&page=")
      yields_raw = list()
      for (i in 1:100) {
        print(i)
        req = GET(paste0(url_prefix, i), user_agent("Mozilla/5.0"))
        tbl_ = content(req) %>%
          html_element("table")
        if (length(tbl_) == 0) break
        yields_raw[[i]] = tbl_ %>%
          html_table()
        Sys.sleep(1L)
      }
      yields = rbindlist(yields_raw)
      cols_keep = c("Date", colnames(yields)[11:ncol(yields)])
      yields = yields[, ..cols_keep]
      cols_num = colnames(yields)[2:ncol(yields)]
      yields[, (cols_num) := lapply(.SD, as.numeric), .SDcols = cols_num]
      yields[, Date := as.IDate(as.Date(Date, format = "%m/%d/%Y"))]
      yields = melt(yields, id.vars = "Date")
      yields[, variable := fcase(
        variable == "1 Mo", "Y1M_US",
        variable == "2 Mo", "Y2M_US",
        variable == "3 Mo", "Y3M_US",
        variable == "4 Mo", "Y4M_US",
        variable == "6 Mo", "Y6M_US",
        variable == "1 Yr", "Y12M_US",
        variable == "2 Yr", "Y24M_US",
        variable == "3 Yr", "Y36M_US",
        variable == "5 Yr", "Y60M_US",
        variable == "7 Yr", "Y84M_US",
        variable == "10 Yr", "Y120M_US",
        variable == "20 Yr", "Y240M_US",
        variable == "30 Yr", "Y360M_US"
      )]
      setnames(yields, c("date", "label", "yield"))
      return(yields)
    },
    
    #' @description
    #' Scrap yields for China.
    #'
    #' @return Data.table with yields data
    get_yields_china = function() {
      seq_dates = seq.Date(as.Date("2000-01-01"), Sys.Date(), by = "year")
      seq_dates = c(seq_dates, Sys.Date())
      responses = list()
      for (i in 1:(length(seq_dates)-1)) {
        print(i)
        date_1 = seq_dates[i]
        date_2 = seq_dates[i+1]
        url = modify_url(
          url       = "https://yield.chinabond.com.cn/cbweb-pbc-web/pbc/historyQuery",
          query = list(
            startDate = date_1,
            endDate   = date_2,
            gjqx      = 0,
            qxId      = "ycqx",
            locale    = "en_US"
          )
        )
        p = GET("https://yield.chinabond.com.cn/")
        c_ = cookies(p)
        headers <- c(
          "Accept" = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
          "Accept-Encoding" = "gzip, deflate, br",
          "Accept-Language" = "en-US,en;q=0.9",
          "Connection" = "keep-alive",
          "Cookie" = sprintf("BIGipServerPool_srvnew_cbwebft_9080=%s; JSESSIONID=%s", c_$value[2], c_$value[1]),
          "Dnt" = "1",
          "Host" = "yield.chinabond.com.cn",
          "Referer" = url,
          "Sec-Ch-Ua" = "\"Chromium\";v=\"118\", \"Google Chrome\";v=\"118\", \"Not=A?Brand\";v=\"99\"",
          "Sec-Ch-Ua-Mobile" = "?0",
          "Sec-Ch-Ua-Platform" = "\"Windows\"",
          "Sec-Fetch-Dest" = "document",
          "Sec-Fetch-Mode" = "navigate",
          "Sec-Fetch-Site" = "same-origin",
          "Sec-Fetch-User" = "?1",
          "Upgrade-Insecure-Requests" = "1",
          "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
        )
        responses[[i]] <- GET(url, add_headers(headers))
      }
      ch = lapply(responses, function(x) {
        content(x) %>%
          html_elements("table") %>%
          .[[2]] %>%
          html_table(header = TRUE)
      })
      ch = rbindlist(ch)
      ch = unique(ch)
      # ch[, unique(`Yield Curve Name`)]
      yields = ch[`Yield Curve Name` == "ChinaBond Government Bond Yield Curve"]
      yields[, `Yield Curve Name` := NULL]
      yields = melt(yields, id.vars = "Date")
      yields[, Date := as.IDate(as.Date(Date))]
      yields[, unique(variable)]
      setorder(yields, Date, variable)
      yields[, variable := fcase(
        variable == "3M", "Y3M_China",
        variable == "6M", "Y6M_China",
        variable == "1Y", "Y12M_China",
        variable == "3Y", "Y36M_China",
        variable == "5Y", "Y60M_China",
        variable == "7Y", "Y84M_China",
        variable == "10Y", "Y120M_China",
        variable == "30Y", "Y360M_China"
      )]
      setnames(yields, c("date", "label", "yield"))
      return(yields)
    },
    
    #' @description
    #' Scrap yields for EU from ECB.
    #'
    #' @param yields_history_file yields histoty path.
    #'
    #' @return Data.table with yields data for EU from ECB.
    get_yields_eu = function(yields_history_file) {
      # debug
      # https://www.ecb.europa.eu/stats/financial_markets_and_interest_rates/euro_area_yield_curves/html/index.en.html
      # yields_history_file = "F:/macro/yield_curves/eu_history.csv"
      
      # check
      checkFileExists(yields_history_file)
      
      # read history and current and merge
      yields_history = fread(yields_history_file)
      url = "https://data-api.ecb.europa.eu/service/data/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.?startPeriod=2023-01-01&format=csvdata"
      tmp_ = tempfile("eu_yields_current", fileext = ".csv")
      GET(url, write_disk(tmp_))
      yields_current = fread(tmp_)
      yields_raw = rbind(yields_history, yields_current)
      
      # clean yields
      yields = yields_raw[, lapply(.SD, function(v) if(uniqueN(v, na.rm = TRUE) > 1) v)]
      yields = yields[UNIT == "PCPA", .(date = as.Date(TIME_PERIOD), label = DATA_TYPE_FM, yield = OBS_VALUE)]
      yields = yields[grepl("SR", label)]
      yields[!grepl("Y", label), year_ := 0L]
      yields[is.na(year_), year_ := as.integer(gsub("SR_|Y.*", "", label))]
      yields[, month_ := as.integer(gsub("SR_|.*Y|M", "", label))]
      yields[, month_ := nafill(month_, fill = 0)]
      yields[, months_ := (year_ * 12) + month_]
      yields[, label := paste0("Y", months_, "M_EU")]
      yields[, unique(label)]
      yields[, `:=`(year_ = NULL, month_ = NULL, months_ = NULL)]
      yields[, date := as.IDate(date)]
      return(yields)
    }
  )
)
