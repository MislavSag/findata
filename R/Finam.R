#' @title Finam Class
#'
#' @description
#' Get historical market data from Finam using rusquant.
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
    #' Create a new Finam object.
    #'
    #' @param path_to_dump Local path to save data to.
    #' @param delay Delay between requests.
    #'
    #' @return A new `Finam` object.
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
    
    #' @description Get tick data from finam source using rusquant.
    #'
    #' @param symbol Stock symbol.
    #' @param days Days to scrape.
    #' @param market Optional Finam market id for ambiguous symbols.
    #'
    #' @return Invisible `NULL`. Tick files are written to disk.
    get_ticks = function(symbol,
                         days = getBusinessDays(as.Date("2010-01-01"), Sys.Date() - 1),
                         market = NULL) {
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
        dir.create(dir_path, recursive = TRUE)
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
        Sys.sleep(self$delay)
        
        # get data
        tries = 1L
        tick_data = NULL
        repeat {
          tick_data <- tryCatch(
            rusquant::getSymbols.Finam(
              toupper(symbol),
              from = d,
              to = d,
              period = "tick",
              market = market
            ),
            error = function(e)
              NULL
          )
          
          # test for errors
          if (!is.null(tick_data) || tries >= 3L) {
            break()
          }
          
          tries = tries + 1L
          print(paste0("Repeat ", tries))
          Sys.sleep(self$delay)
        }
        
        # check if there is data
        if (is.null(tick_data) || NROW(tick_data) == 0L) {
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
        arrow::write_parquet(tick_data, file_name)
        
        return(NULL)
      })
      
      invisible(NULL)
    }
  )
)
