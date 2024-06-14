#' @title Nasdaq data
#'
#' @description
#' Get data data from Nasdaq webpage.
#'
#' @export
Nasdaq = R6::R6Class(
  "Nasdaq",
  
  public = list(
    #' @description Get IPO calendar data.
    #'
    #' @param months Months to get data for in %Y-%m format. If NULL, get data 
    #'     for all months.
    #'
    #' @return List with priced, filed and withdrawn IPOs data.
    ipo_calendar = function(months = NULL) {
      
      # Define months
      if (is.null(months)) months = seq.Date(as.Date("1998-01-01"), Sys.Date(), by = "months")
      months = format(months, "%Y-%m")
      
      # Define urls
      urls = paste0("https://api.nasdaq.com/api/ipo/calendar?date=", months)
      
      # GET requests on urls
      ipo_html = lapply(urls, function(u) {
        # u = urls[1]
        Sys.sleep(1)
        p = GET(u, 
                add_headers(
                  "Connection" = "keep-alive",
                  "Referer" = "https://www.nasdaq.com/",
                  'User-Agent' = "Mozilla/5.0 (X11; Linux x86_64; rv:106.0) Gecko/20100101 Firefox/106.0"
                )
        )
        if (p$status_code == 200) {
          return(content(p))
        } else {
          sprintf("No data for url %s", u)
          return(NULL)
        }
      })
      
      # Clean data
      extract_ipos = function(tag = "priced") {
        res_ = lapply(ipo_html, function(x) {
          ipo_raw = x$data
          rbindlist(ipo_raw[["priced"]][["rows"]], fill = TRUE)
        })
        rbindlist(res_)
      }
      priced = extract_ipos("priced")
      filed = extract_ipos("filed")
      withdrawn = extract_ipos("withdrawn")
      
      return(list(priced = priced, filed = filed, withdrawn = withdrawn))
    }
  )
)
