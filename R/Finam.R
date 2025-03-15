#' @title Finam Class
#'
#' @description
#' Get data data from finam using Quanttools package.
#'
#' @export
Finam = R6::R6Class(
  "Finam",
  public = list(
    #' @field path_to_dump Local path to save data to.
    path_to_dump = NULL,
    
    #' @field delay Delay between requests.
    delay = NULL,
    
    #' @field symbol_list List of symbols from finam.
    symbol_list = NULL,
    
    #' @description
    #' Create a new MacroData object.
    #'
    #' @param path_to_dump Local path to save data to.
    #' @param delay Delay between requests.
    #'
    #' @return A new `MacroData` object.
    initialize = function(path_to_dump, delay = 0.8) {
      # DEBUG
      # path_to_dump = "F:/temp"
      # self = list()
      # self$path_to_dump = path_to_dump
      
      # checks
      assert_character(path_to_dump, len = 1L)
      assert_true(dir.exists(path_to_dump))
      assert_numeric(delay, len = 1L)
      
      # Symbol list
      symbols = getSymbolList(src = "finam")
      while (nrow(symbols) <= 1) {
        symbols = getSymbolList(src = "finam")
        Sys.sleep(3L)
      }
      
      # set init vars
      self$path_to_dump = path_to_dump
      self$delay = delay
      self$symbol_list = symbols
    },
    
    #' @description Get tick data from finam source using QuantTools.
    #'
    #' @param symbol Start date
    #' @param save_uri Tiledb uri to save.
    #' @param days Days to scrap data for
    #'
    #' @return Get investing com ea data.
    get_ticks = function(symbol, days = getBusinessDays(as.Date("2010-01-01"), Sys.Date() - 1)) {
      # debug
      # library(qlcal)
      # library(lubridate)
      # library(rusquant)
      # library(arrow)
      # library(checkmate)
      # qlcal::setCalendar("UnitedStates/NYSE")
      # symbol <- "SPY"
      # days = getBusinessDays(as.Date("2007-01-01"), Sys.Date() - 1)
      
      # set download timeout argument using options
      options(timeout = 120)
      
      # create directory for symbol
      dir_path = file.path(self$path_to_dump, tolower(symbol))
      if (!dir.exists(dir_path)) {
        print("Create directory.")
        dir.create(dir_path)
      }
      
      # Assign symbols list Finam to this object
      # look at rusquant::getSymbols.Finam to see why
      symbol_list_FINAM = self$symbol_list
      
      # main loop
      lapply(days[1000:1001], function(d) {
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
        Sys.sleep(self$delay)
        
        # get data
        tries = 1
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
            Sys.sleep(self$delay)
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
          
          tries = tries + 1
          print(paste0("Repeat ", tries))
          Sys.sleep(self$delay)
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
