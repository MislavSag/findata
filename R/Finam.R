#' @title Finam Class
#'
#' @description
#' Get data data from finam using Quanttools package.
#'
#' @export
Finam = R6::R6Class(
  "Finam",
  inherit = DataAbstract,
  
  public = list(
    #' @description
    #' Create a new Finam object.
    #'
    #' @param azure_storage_endpoint Azure storate endpont
    #' @param context_with_config AWS S3 Tiledb config
    #'
    #' @return A new `Finam` object.
    initialize = function(azure_storage_endpoint = NULL,
                          context_with_config = NULL) {
      # endpoint
      super$initialize(azure_storage_endpoint, context_with_config)
    },
    
    #' @description Get tick data from finam source using QuantTools.
    #'
    #' @param symbol Start date
    #' @param save_uri Tiledb uri to save.
    #' @param days Days to scrap data for
    #'
    #' @return Get investing com ea data.
    get_ticks = function(symbol,
                         save_uri,
                         days = getBusinessDays(as.Date("2010-01-01"), Sys.Date() - 1)) {
      # debug
      # library(qlcal)
      # library(lubridate)
      # library(rusquant)
      # library(arrow)
      # qlcal::setCalendar("UnitedStates/NYSE")
      # symbol <- "SPY"
      # days = getBusinessDays(as.Date("2020-01-01"), Sys.Date() - 1)
      # save_uri = "F:/equity/usa/tick/trades/finam"
      
#       [1] "2020-02-27"
#       [1] "Repeat 2"
#       [1] "Repeat 3"
#       Error in setnames(tick_data, c("date", "price", "volume")) : 
#         Can't assign 3 names to a 0 column data.table
# In addition: Warning messages:
# 1: In download.file(stock.URL, destfile = tmp, quiet = !verbose) :
#   URL 'http://export.finam.ru/table.csv?d=d&f=table&e=.csv&dtf=1&tmf=1&MSOR=0&sep=1&sep2=1&at=1&p=1&market=25&em=21053&df=27&mf=1&yf=2020&dt=27&mt=1&yt=2020&datf=6': Timeout of 60 seconds was reached
# 2: In read.table(file = file, header = header, sep = sep, quote = quote,  :
#   incomplete final line found by readTableHeader on 'C:\Users\Mislav\AppData\Local\Temp\Rtmp2vUE7U\file1a0415726815'
# 3: In download.file(stock.URL, destfile = tmp, quiet = !verbose) :
#   URL 'http://export.finam.ru/table.csv?d=d&f=table&e=.csv&dtf=1&tmf=1&MSOR=0&sep=1&sep2=1&at=1&p=1&market=25&em=21053&df=27&mf=1&yf=2020&dt=27&mt=1&yt=2020&datf=6': Timeout of 60 seconds was reached
# 4: In read.table(file = file, header = header, sep = sep, quote = quote,  :
#   incomplete final line found by readTableHeader on 'C:\Users\Mislav\AppData\Local\Temp\Rtmp2vUE7U\file1a0429a95710'
# 5: In download.file(stock.URL, destfile = tmp, quiet = !verbose) :
#   URL 'http://export.finam.ru/table.csv?d=d&f=table&e=.csv&dtf=1&tmf=1&MSOR=0&sep=1&sep2=1&at=1&p=1&market=25&em=21053&df=27&mf=1&yf=2020&dt=27&mt=1&yt=2020&datf=6': Timeout of 60 seconds was reached
#   
      # set download timeout argument using options
      options(timeout=120)
      
      # create directory for symbol
      dir_path = file.path(save_uri, tolower(symbol))
      if (!dir.exists(dir_path)) {
        print("Create directory.")
        dir.create(dir_path)
      }
      
      # main loop
      lapply(days, function(d) {
        # debug
        # d = days[1] # 3123
        # d = "2020-02-28"
        print(d)
        
        # create directory
        file_name = file.path(dir_path, paste0(strftime(d, "%Y%m%d"), ".parquet"))
        if (file.exists(file_name)) {
          print("File already exists.")
          return(NULL)
        }
        
        # fair scraping
        Sys.sleep(2L)
        
        # get data
        tries <- 1
        repeat {
          tick_data <- tryCatch(
            rusquant::getSymbols.Finam(
              toupper(symbol),
              from = d,
              to = d,
              period = "tick"
            ),
            error = function(e)
              NULL
          )
          
          # test for errors
          if (!is.null(tick_data) | tries > 2) {
            break()
          } else if (is.null(tick_data)) {
            Sys.sleep(10L)
            tick_data <- tryCatch(
              rusquant::getSymbols.Finam(
                toupper(symbol),
                from = d,
                to = d,
                period = "tick"
              ),
              error = function(e)
                NULL
            )
          }
          
          tries <- tries + 1
          print(paste0("Repeat ", tries))
          Sys.sleep(1L)
        }
        
        # check if there is data
        if (is.null(tick_data)) {
          print(paste0("There is no data for date ", d))
          return(NULL)
        }
        
        # clean data
        tick_data = as.data.table(tick_data)
        setnames(tick_data, c("date", "price", "volume"))
        tick_data[, date := force_tz(date, tzone = "America/New_York")]
        tick_data[, date := with_tz(date, tzone = "UTC")]
        
        # save data
        print(file_name)
        write_parquet(tick_data, file_name)
        
        return(NULL)
      })
      }
  )
)
