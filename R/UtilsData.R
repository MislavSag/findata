#' @title UtilsData Class
#'
#' @description
#' Various help data.
#'
#' @export
UtilsData = R6::R6Class(
  "UtilsData",
  inherit = DataAbstract,

  public = list(

    #' @description
    #' Create a new UtilsData object.
    #'
    #' @param azure_storage_endpoint Azure storate endpont
    #' @param context_with_config AWS S3 Tiledb config
    #'
    #' @return A new `UtilsData` object.
    initialize = function(azure_storage_endpoint = NULL,
                          context_with_config = NULL) {

      # endpoint
      super$initialize(azure_storage_endpoint, context_with_config)
    },

    #' @description Get tick data from finam source using QuantTools.
    #'
    #' @param symbol Equity symbol
    #'
    #' @return Get investing com ea data.
    get_ticker_data = function(symbol) {

      p <- RETRY("POST",
                 'https://www.quantumonline.com/search.cfm',
                 body = list(
                   tickersymbol = tolower(symbol),
                   sopt = 'symbol',
                   '1.0.1' = 'Search'
                 ),
                 times = 8L)
      changes <- content(p) %>%
        html_elements(xpath = "//*[contains(text(),'Previous Ticker')]") %>%
        html_text() %>%
        gsub('.*Symbol:', '', .) %>%
        trimws(.)
      date <- as.Date(str_extract(changes, '\\d+/\\d+/\\d+'), '%m/%d/%Y')
      tickers <- str_extract(changes, '\\w+')
      changes <- suppressWarnings(data.table(symbol = symbol, date = date, ticker_change = tickers))
      return(changes)
    },

    #' @description Get split data from the web and calculate split factors.
    #'
    #' @param ticker Stock ticker
    #'
    #' @return Stock splits
    get_split_factor = function(ticker) {
      splits <- content(GET("https://www.splithistory.com/", query = list(symbol = ticker)))
      splits <- html_nodes(splits, xpath = "//table[@width='208' and @style='font-family: Arial; font-size: 12px']")
      splits <- html_table(splits, header = TRUE)[[1]]
      splits$date <- as.Date(splits$Date, "%m/%d/%Y")
      ratio1 <- as.numeric(str_extract(splits$Ratio, "\\d+"))
      ratio2 <- as.numeric(str_extract(splits$Ratio, "\\d+$"))
      splits$ratio <- ratio2 / ratio1
      splits <- splits[splits$date > as.Date("2004-01-01"), ]
      splits <- splits[order(splits$date), ]
      splits <- splits[splits$ratio != 1, ]
      if (length(splits) == 0) {
        return(NA)
      } else {
        splits$split_factor <- rev(cumprod(rev(splits$ratio)))
      }
      return(splits[, c('date', 'ratio', 'split_factor')])
    },

    #' @description Adjusted instraday data for splits and dividends
    #'
    #' @param save_uri TileDB uri for saving
    #' @param minute_uri TileDB uri with unadjusted minute data.
    #'
    #' @return Adjusted data saved to AWS S3 bucket
    adjust_fm_tiledb = function(save_uri = "D:/equity-usa-minute-fmpcloud-adjusted",
                                minute_uri = "D:/equity-usa-minute-fmpcloud") {

      # debug
      # library(findata)
      # library(data.table)
      # library(httr)
      # library(RcppQuantuccia)
      # library(tiledb)
      # library(lubridate)
      # library(nanotime)
      # self = UtilsData$new()
      # save_uri = "D:/equity-usa-minute-fmpcloud-adjusted"
      # minute_uri = "D:/equity-usa-minute-fmpcloud"

      # factor files
      arr_ff <- tiledb_array("s3://equity-usa-factor-files", as.data.frame = TRUE)
      factor_files <- arr_ff[]
      tiledb_array_close(arr_ff)
      factor_files <- as.data.table(factor_files)
      factor_files <- setorder(factor_files, symbol, date)

      # get IPO data
      fmp_ipo <- FMP$new()
      ipo_dates <- vapply(unique(factor_files$symbol), function(x) {
        y <- fmp_ipo$get_ipo_date(x)
        if (is.null(y)) {
          return("2004-01-02")
        } else {
          return(y)
        }
      }, character(1))
      ipo_dates_dt <- as.data.table(ipo_dates, keep.rownames = TRUE)
      setnames(ipo_dates_dt, "rn", "symbol")
      ipo_dates_dt[, ipo_dates := as.Date(ipo_dates)]
      ipo_dates_dt[, symbol := toupper(gsub("\\.csv", "", symbol))]

      # delete uri
      tryCatch({tiledb_object_rm(save_uri, self$context_with_config)}, error = function(e) NA)
      save_uri_hour <- gsub("minute", "hour", save_uri)
      tryCatch({tiledb_object_rm(save_uri_hour, self$context_with_config)}, error = function(e) NA)

      # loop that import unadjusted data and adjust them
      loop_symbols <- unique(factor_files$symbol)
      lapply(loop_symbols, function(symbol) {

        # debug
        print(symbol)

        # import minute data
        arr <- tiledb_array(minute_uri,
                            as.data.frame = TRUE,
                            query_layout = "UNORDERED",
                            selected_ranges = list(symbol = cbind(symbol, symbol)))
        system.time(unadjusted_data <- arr[])
        tiledb_array_close(arr)

        # if there is no data next
        if (nrow(unadjusted_data) == 0) {
          return(NULL)
        }

        # change timezone
        unadjusted_data <- as.data.table(unadjusted_data)
        setorder(unadjusted_data, symbol, time)
        unadjusted_data[, time := as.POSIXct(time, "UTC",
                                             origin = as.POSIXct("1970-01-01 00:00:00", tz = "UTC"))]

        # unadjusted data from IPO
        unadj_from_ipo <- ipo_dates_dt[unadjusted_data, on = "symbol"]
        if (!(all(is.na(unadj_from_ipo$ipo_dates)))) {
          unadj_from_ipo <- unadj_from_ipo[as.Date(time) >= ipo_dates]
        }
        unadj_from_ipo_daily <- unadj_from_ipo[, .SD[.N], by = .(symbol, time = as.Date(time))]
        unadj_from_ipo_daily[, ipo_dates := NULL]

        # adjust
        df <- unadj_from_ipo[symbol %in% unique(factor_files$symbol)]
        df[, date := as.Date(time)]
        df <- merge(df, factor_files,
                    by.x = c("symbol", "date"), by.y = c("symbol", "date"),
                    all.x = TRUE, all.y = FALSE)
        df[, `:=`(split_factor = nafill(split_factor, "nocb"),
                  price_factor = nafill(price_factor, "nocb")), by = symbol]
        df[, `:=`(split_factor = ifelse(is.na(split_factor), 1, split_factor),
                  price_factor = ifelse(is.na(price_factor), 1, price_factor))]
        cols_change <- c("open", "high", "low", "close")
        df[, (cols_change) := lapply(.SD, function(x) {x * price_factor * split_factor}), .SDcols = cols_change]
        df <- df[, .(symbol, time, open, high, low, close, volume)]
        setorder(df, symbol, time)
        df <- unique(df)
        if (nrow(df) == 0) {
          return(NULL)
        }

        # Check if the array already exists.
        if (tiledb_object_type(save_uri) != "ARRAY") {
          system.time({
            fromDataFrame(
              as.data.frame(df),
              uri = save_uri,
              col_index = c("symbol", "time"),
              sparse = TRUE,
              allows_dups = FALSE,
              tile_domain=list(time=c(as.POSIXct("1970-01-01 00:00:00", tz = "UTC"),
                                      as.POSIXct("2099-12-31 23:59:59", tz = "UTC")))
            )
          })
        } else {
          # save to tiledb
          arr <- tiledb_array(save_uri, as.data.frame = TRUE)
          arr[] <- as.data.frame(df)
          tiledb_array_close(arr)
        }

        # get hour data
        df[, time := as.nanotime(time)]
        hour_data <- df[, .(open = head(open, 1),
                            high = max(high, na.rm = TRUE),
                            low = min(low, na.rm = TRUE),
                            close = tail(close, 1),
                            volume = sum(volume, na.rm = TRUE)),
                        by = .(symbol, time = nano_ceiling(time, as.nanoduration("01:00:00")))]
        hour_data[, time := as.POSIXct(time, tz = "UTC")]

        # save hour data
        if (tiledb_object_type(save_uri_hour) != "ARRAY") {
          system.time({
            fromDataFrame(
              as.data.frame(hour_data),
              uri = save_uri_hour,
              col_index = c("symbol", "time"),
              sparse = TRUE,
              allows_dups = FALSE,
              tile_domain=list(time=cbind(as.POSIXct("1970-01-01 00:00:00", tz = "UTC"),
                                          as.POSIXct("2099-12-31 23:59:59", tz = "UTC")))
            )
          })
        } else {
          # save to tiledb
          arr <- tiledb_array(save_uri_hour, as.data.frame = TRUE)
          arr[] <- as.data.frame(hour_data)
          tiledb_array_close(arr)
        }
        return(NULL)
      })
    },

    #' @description Get SP500 stocks for every date.
    #'
    #' @param qc_spy_path to spy constitues
    #'
    #' @return Data table of Sp500 tickers
    sp500_history = function(qc_spy_path) {

      # debug
      # qc_spy_path = "F:/lean_root/data/equity/usa/universes/etf/spy"
      
      # get infor on SP500 constitues from sp500 github repo
      url = "https://api.github.com/repos/fja05680/sp500/contents"
      sp500_repo = read_json(url)
      sp500_repo_urls = vapply(sp500_repo, `[[`, "download_url", FUN.VALUE = character(1L))
      index_file = grepl("Changes.*\\(", sp500_repo_urls)
      url = sp500_repo_urls[index_file]
      sp500_history = fread(url)
      symbols_long <- tstrsplit(sp500_history$tickers, split = ",")
      symbols_long <- as.data.table(do.call(cbind, symbols_long))
      sp500_history <- cbind(date = setDT(sp500_history)[, .(date)], symbols_long)
      setnames(sp500_history, "date.date", "date")
      sp500_history <- melt(sp500_history, id.vars = "date")
      sp500_history <- sp500_history[, .(date, value)]
      setnames(sp500_history, c("date", "symbol"))
      sp500_history <- na.omit(sp500_history)
      setorder(sp500_history, "date")

      # get info from wikipedia
      url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
      sp500_history_wiki = read_html(url) %>% 
        html_element("#changes") %>% 
        html_table()
      sp500_wiki = read_html(url) %>% 
        html_element("#constituents") %>% 
        html_table()
      sp500_history_wiki_symbols = c(sp500_history_wiki$Added,
                                     sp500_history_wiki$Removed,
                                     sp500_wiki$Symbol)
      sp500_history_wiki_symbols = sp500_history_wiki_symbols[sp500_history_wiki_symbols != ""]
      sp500_history_wiki_symbols = unique(sp500_history_wiki_symbols)
      
      # get info from QC
      qc_sp500_files = list.files(qc_spy_path, full.names = TRUE)
      sp500_history_qc = lapply(qc_sp500_files, fread)
      sp500_history_qc = rbindlist(sp500_history_qc)
      setnames(sp500_history_qc, c("ticker", "symbol", "date", "ratio", "value", "none"))
      sp500_history_qc[, date := as.Date(as.character(date), format = "%Y%m%d")]
      
      # all symbols
      symbols = unique(c(
        sp500_history$symbol,
        sp500_history_wiki_symbols,
        sp500_history_qc$ticker
      ))
      
      return(list(sp500_history = sp500_history, 
                  sp500_history_wiki = sp500_history_wiki,
                  sp500_history_qc = sp500_history_qc,
                  symbols = symbols))
    },

    #' @description Help function to import clean data from Azureblob.
    #'
    #' @param symbols Symbols
    #' @param frequency Frequency can be hour and minute.
    #'
    #' @return Data table with market prices
    get_market_data = function(symbols = c("aapl", "aal"),
                               frequency = "hour") {


      # DEBUG
      # library(pins)
      # library(data.table)
      # library(fasttime)
      # symbols = c("aapl", "aal", "fgsdf")
      # frequency = "hour"
      # self <- list()
      # self$azure_storage_endpoint = AzureStor::storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), Sys.getenv("BLOB-KEY"))
      # DEBUG

      # board
      sc <- storage_container(self$azure_storage_endpoint,
                              paste0("equity-usa-",
                                     frequency,
                                     "-trades-fmplcoud-adjusted"))
      board_market_data <- board_azure(
        container = sc,
        path = "",
        n_processes = 6L,
        versioned = NULL,
        cache = NULL
      )

      # check if symbols on blob azure
      symbols_exists <- tolower(symbols) %in% pin_list(board_market_data)
      print(paste0("This symbols are not on blob: ",
                   unlist(symbols[symbols_exists == FALSE])))
      symbols <- symbols[symbols_exists]

      # import minute market data
      market_data_l <- lapply(tolower(symbols), pin_read, board = board_market_data)
      names(market_data_l) <- tolower(symbols)
      market_data <- rbindlist(market_data_l, idcol = "symbol")
      market_data[, datetime := fastPOSIXct(as.character(datetime), tz = "GMT")]
      market_data[, datetime := as.POSIXct(as.numeric(datetime),
                                           origin=as.POSIXct("1970-01-01", tz="EST"),
                                           tz="EST")]
      market_data <- market_data[format.POSIXct(datetime, format = "%H:%M:%S") %between% c("09:30:00", "16:00:00")]
      market_data <- unique(market_data, by = c("symbol", "datetime"))
      market_data[, returns := close / shift(close, 1L) - 1, by = "symbol"]
      market_data <- na.omit(market_data)

      return(market_data)
    },

    #' @description adjust market data for splits and/or dividends
    #'
    #' @param market_data Market data of OHLCV type.
    #' @param factor_files Factor files must contain columns:
    #'    - symbol
    #'    - date
    #'    - price_factor
    #'    - split_factor
    #' @param type Type of adjustment.
    #' @param cols_adjust Columns to adjust.
    #'
    #' @return Data table with adjusted market prices.
    adjust_market_data = function(market_data, factor_files,
                                  type = c("all", "dividends", "splits"),
                                  cols_adjust = c("o", "h", "l", "c")) {

      # checks
      assert_names(colnames(market_data), must.include = c("symbol", "date"))
      assert_names(colnames(factor_files),
                   must.include = c("symbol", "date", "price_factor",
                                    "split_factor"))

      # merge market data and factorfiles
      market_data[, date_ := as.Date(date)]
      market_data <- factor_files[market_data, on = c("symbol" = "symbol", "date" = "date_"), roll = -Inf]
      market_data[symbol == "aapl"]

      # adjust for fividends and splits
      type <- match.arg(type)
      if (type == "all") {
        market_data[, (cols_adjust) := lapply(.SD, function(x) {x * price_factor * split_factor}),
                    .SDcols = cols_adjust]
      } else if (type == "dividends") {
        market_data[, (cols_adjust) := lapply(.SD, function(x) {x * price_factor}),
                    .SDcols = cols_adjust]
      } else if (type == "splits") {
        market_data[, (cols_adjust) := lapply(.SD, function(x) {x * split_factor}),
                    .SDcols = cols_adjust]
      }
      market_data <- market_data[, -2]
      setnames(market_data, "i.date", "date")

      return(market_data)
    },

    #' @description Help function to get map files.
    #'
    #' @param save_uri TileDB uri.
    #' @param symbol_uri TileDB uri for saved files
    #'
    #' @return Maped files.
    get_map_files_tiledb = function(save_uri = 's3://equity-usa-mapfiles',
                                    symbol_uri = "https://en.wikipedia.org/wiki/S%26P_100"){

      print("Downloading symbols...")
      # SP100
      sp100 <- read_html(symbol_uri) |>
        html_elements(x = _, "table") |>
        (`[[`)(3) |>
        html_table(x = _, fill = TRUE)
      sp100_symbols <- c("SPY", "TLT", "GLD", "USO", sp100$Symbol)

      fmp = FMP$new()
      # SP500 symbols
      symbols <- fmp$get_sp500_symbols()
      symbols <- na.omit(symbols)
      symbols <- symbols[!grepl("/", symbols)]

      # merge
      symbols <- unique(union(sp100_symbols, symbols))



      all_changes = fmp$get_symbol_changes()
      #all_changes = as.data.table(all_changes)

      all_delisted = fmp$get_delisted_companies()
      #all_delisted = as.data.table(all_delisted)

      all_mapfiles = data.table(date=character(), ticker=character(), exchange=character(),currentSymbol=character())

      print("Starting creation of map files...")
      for (sym in symbols){
        #FIND NAME CHANGES
        symb_rows = all_changes[all_changes$newSymbol == sym,][order(date)]

        comp_name = symb_rows$name[1]
        #ADD CHANGES TO EXCEL
        exc = symb_rows[,c('date','oldSymbol')][order(date),]

        #FIND FIRST SYMBOL FOR COMPANY
        pom = symb_rows$oldSymbol[1]


        ###### ADD DELISTED
        if (is.na(comp_name)){#NEMA PROMJENA
          delisted = all_delisted[all_delisted$symbol == sym][,c('delistedDate','symbol')]
        }else{#IMA PROMJENA
          delisted = all_delisted[all_delisted$symbol == sym & all_delisted$companyName == comp_name][,c('delistedDate','symbol')]
        }

        if (nrow(delisted) > 0){ #DELISTAN JE
          colnames(delisted) = colnames(exc)
          exc = rbind(exc,delisted)

        }else{ #NIJE DELISTAN
          fr = "2050-12-31"
          add = data.frame(fr, sym)
          colnames(add) = colnames(exc)
          exc = rbind(exc,add)
        }



        #####ADD LISTED
        fr = as.Date(fmp$get_ipo_date(sym))
        pom = if (is.na(pom)) sym else pom
        listed_before = data.frame(fr, pom)
        colnames(listed_before) = colnames(exc)
        exc = rbind(listed_before,exc)




        #REMOVE $ SIGNS
        exc$date = gsub("\\-","",as.character(exc$date))
        exc$oldSymbol = tolower(exc$oldSymbol)
        exc = cbind(exc,rep(c('Q'), nrow(exc)))

        #print("*******************END OF ITERATION***********************************")

        #write.table(exc,paste(c(folderPath,sym,'.csv'),collapse=""),sep=",", col.names=FALSE,row.names = FALSE)

        #SEND TO AWS TILEDB
        map_file = as.data.table(exc)
        colnames(map_file) = colnames(all_mapfiles)[1:3]
        map_file = cbind(map_file,replicate(nrow(map_file), map_file$ticker[nrow(map_file)]))
        colnames(map_file) = colnames(all_mapfiles)
        all_mapfiles = rbind(all_mapfiles,map_file)

      }
      print("Finished calculation, deleting old object...")

      del_obj <- tryCatch(tiledb_object_rm(save_uri), error = function(e) NA)
      if (is.na(del_obj)) {
        warning("Can't delete object")
      }
      print("Saving...")
      fromDataFrame(
        as.data.frame(all_mapfiles),
        col_index = c("currentSymbol"),
        uri = save_uri,
      )
      print("Map files created successfully!")
      return (as.data.frame(all_mapfiles))
    },
    
    #' @description Get map files from Quantconnect map_files folder
    #'
    #' @param symbols Symbols
    #' @param path_map_files Path to map_files directory
    #'
    #' @return Data table with mapped symbols.
    get_map_files_qc = function(symbols, path_map_files) {
      # debug
      # symbols = c("META")
      # path_map_files = "F:/lean_root/data/equity/usa/map_files"
      
      # get map dta fo every symbol
      maps_l = lapply(symbols, function(s) {
        map_file = file.path(path_map_files, paste0(tolower(s), ".csv"))
        if (!file.exists(map_file)) {
          print(paste0("No map data for ", s))
          return(NULL)
        } else {
          maps = fread(map_file)
          maps = as.data.table(cbind.data.frame(s, maps))
          if (length(maps) > 3) {
            setnames(maps, c("symbol_input", "date", "symbol", "exc"))  
          } else if (length(maps) == 3) {
            setnames(maps, c("symbol_input", "date", "symbol"))  
          }
          maps[, date := as.IDate(as.Date(as.character(date), format = "%Y%m%d"))]
          return(maps)
        }
      })
      
      # test for null
      if (any(lengths(maps_l) > 1)) {
        # merge an return
        maps = rbindlist(maps_l)
        maps[, symbol := toupper(symbol)]
        return(maps)
      } else {
        return(NULL)      
      }
    },
    
    #' @description Get factor files from Quantconnect factor_files folder
    #'
    #' @param symbols Symbols
    #' @param path_factor_files Path to factor_files directory
    #'
    #' @return Data table with factor files data for symbol.
    get_factor_files_qc = function(symbols, path_factor_files) {
      factors_l = lapply(symbols, function(s) {
        factor_file = file.path(path_factor_files, paste0(tolower(s), ".csv"))
        if (!file.exists(factor_file)) {
          print(paste0("No map data for ", s))
          return(NULL)
        } else {
          factors = fread(factor_file)
          factors = as.data.table(cbind.data.frame(symbol = s, factors))
          setnames(factors, c("symbol", "date", "div", "split", "prev_close"))
          factors[, date := as.IDate(as.Date(as.character(date), "%Y%m%d"))]
          return(factors)
        }
      })
      factors = rbindlist(factors_l)
      return(factors)  
    },
    
    #' @description Adjust FMP market data
    #'
    #' @param file_path File path to parquet file that contains market data.
    #' @param file_path_save Path to which save the data.
    #' @param path_map_files Path to map files directory
    #' @param path_factor_files Path to factor files directory
    #' @param trading_hours Should we keep only trading hours in output DT.
    #' @param parallel should we use parallle package. If yes set plan.
    #'
    #' @return Data table adjusted market data.
    adjust_fmp = function(file_path, 
                          file_path_save, 
                          path_map_files, 
                          path_factor_files,
                          trading_hours = TRUE,
                          parallel = FALSE) {
      
      # debug
      # library(checkmate)
      # library(arrow)
      # library(data.table)
      # library(lubridate)
      # file_path      = "F:/equity/usa/minute/AAPL"
      # file_path_save = "F:/equity/usa/minute-adjusted"
      # path_map_files  = "F:/lean_root/data/equity/usa/map_files"
      # path_factor_files = "F:/lean_root/data/equity/usa/factor_files"
      # trading_hours = TRUE
      # self = list()
      # # get map dta fo every symbol
      # self$get_map_files_qc = function(symbols, path_map_files) {
      #   # debug
      #   # symbols = c("AAPL", "AA")
      #   # path_map_files = "F:/lean_root/data/equity/usa/map_files"
      # 
      #   # get map dta fo every symbol
      #   maps_l = lapply(symbols, function(s) {
      #     map_file = file.path(path_map_files, paste0(tolower(s), ".csv"))
      #     if (!file.exists(map_file)) {
      #       print(paste0("No map data for ", s))
      #       return(NULL)
      #     } else {
      #       maps = fread(map_file)
      #       maps = as.data.table(cbind.data.frame(s, maps))
      #       if (length(maps) > 3) {
      #         setnames(maps, c("symbol_input", "date", "symbol", "exc"))
      #       } else if (length(maps) == 3) {
      #         setnames(maps, c("symbol_input", "date", "symbol"))
      #       }
      #       maps[, date := as.IDate(as.Date(as.character(date), format = "%Y%m%d"))]
      #       return(maps)
      #     }
      #   })
      # 
      #   # test for null
      #   if (any(lengths(maps_l) > 1)) {
      #     # merge an return
      #     maps = rbindlist(maps_l)
      #     maps[, symbol := toupper(symbol)]
      #     return(maps)
      #   } else {
      #     return(NULL)
      #   }
      # }
      # self$get_factor_files_qc = function(symbols, path_factor_files) {
      #   factors_l = lapply(symbols, function(s) {
      #     factor_file = file.path(path_factor_files, paste0(tolower(s), ".csv"))
      #     if (!file.exists(factor_file)) {
      #       print(paste0("No map data for ", s))
      #       return(NULL)
      #     } else {
      #       factors = fread(factor_file)
      #       factors = as.data.table(cbind.data.frame(symbol = s, factors))
      #       setnames(factors, c("symbol", "date", "div", "split", "prev_close"))
      #       factors[, date := as.IDate(as.Date(as.character(date), "%Y%m%d"))]
      #       return(factors)
      #     }
      #   })
      #   factors = rbindlist(factors_l)
      #   return(factors)
      # }

      # Error in if (nrow(maps) > 2) { : argument is of length zero
      
      # checks
      print("Checks")
      if (!file.exists(file_path)) {stop("File file_path doesnt exists")}
      assertDirectoryExists(file.path(file_path_save))
      assertDirectoryExists(file.path(path_map_files))
      assertDirectoryExists(file.path(path_factor_files))
      
      # get symbol
      symbol = gsub(".parquet", "", basename(file_path)) 
      print(paste0("Symbol ", symbol))
      
      # import market data
      if (file_test("-d", file_path)) {
        if (parallel) {
          dt = future_lapply(list.files(file_path, recursive = TRUE, full.names = TRUE), read_parquet)
        } else {
          dt = lapply(list.files(file_path, recursive = TRUE, full.names = TRUE), read_parquet)
        }
        dt = rbindlist(dt)
        dt = cbind(symbol = symbol, dt)
      } else {
        dt = read_parquet(file_path)
        dt = cbind(symbol = symbol, dt)
      }
      
      # choose columns and change columns names
      dt = dt[, .(symbol, date = t / 1000, open = o, high = h, low = l, close = c, volume = v)]
      
      # set time zone
      dt[, date := as.POSIXct(date, tz = "UTC")]
      dt[, date := with_tz(date, "America/New_York")]
      
      # keep only trading hours
      if (trading_hours) {
        dt[, hours := as.ITime(date)]
        dt = dt[hours %between% c(as.ITime("09:30:00"), as.ITime("16:00:00"))]
        dt[, hours := NULL]
      }
      
      # create help date column to merge dates later
      setnames(dt, "date", "time")
      dt[, date := as.IDate(time)]
      
      # get map files
      print("Get map data")
      maps = self$get_map_files_qc(symbol, path_map_files)
      
      # join map files
      if (length(maps) > 2 && nrow(maps) > 2) {
        # import additional market data collected from symbols
        symbols_new = setdiff(maps[, unique(symbol)], symbol)
        base_path = dirname(file_path)
        files_new = list.files(base_path, full.names = TRUE, pattern = paste0("^", symbols_new, ".par"))
        if (length(files_new) == 0) {
          maped_dt = copy(dt)
        } else {
          dt_new = lapply(files_new, read_parquet)
          names(dt_new) = gsub(".parquet", "", basename(files_new))
          dt_new = rbindlist(dt_new, idcol = "symbol") 
          dt_new[, date := with_tz(date, "America/New_York")]
          if (trading_hours) {
            dt_new[, hours := as.ITime(date)]
            dt_new = dt_new[hours %between% c(as.ITime("09:30:00"), as.ITime("16:00:00"))]
            dt_new[, hours := NULL]
          }
          setnames(dt_new, "date", "time")
          dt_new[, date := as.IDate(time)]
          dt = rbind(dt, dt_new)
          
          # merge mapped files and data 
          maps[, date_join := date]
          maped_dt = maps[dt, on = .(symbol, date), roll = -Inf]
          maped_dt = maped_dt[date <= date_join]
          maped_dt = maped_dt[!(symbol == symbol_input & time < tail(maps[, date_join], 2)[-2])]
          setnames(maped_dt, c("symbol_input", "symbol"), c("symbol", "symbol_map"))
          
          # sort
          setorder(maped_dt, time)
        }
      } else {
        maped_dt = copy(dt)
      }
      
      # debug
      # unique(maped_dt[, .(symbol, date_join)])
      # plot(as.xts.data.table(maped_dt[, .(time, close)]))
      # maped_dt[duplicated(maped_dt[, .(symbol, time)])]
      # maped_dt[time == as.POSIXct("2021-06-30 09:32:00", tz = "America/New_York")]  

      # get factor files
      print("Get factor data")
      factors = self$get_factor_files_qc(symbol, path_factor_files)
      
      # adjust if there are factor data
      if (length(factors) > 1) {
        # adjust ohlc
        dtadj = factors[maped_dt, on = c("symbol", "date"), roll = -Inf]
        setorder(dtadj, symbol, date)

        # adjust
        dtadj[, `:=`(
          open_adj = open * split * div,
          high_adj = high * split * div,
          low_adj = low * split * div,
          close_adj = close * split * div
        )]
        dtadj = dtadj[, .(date = time, open = open_adj, high = high_adj, 
                          low = low_adj, close = close_adj, volume, close_raw = close)]
        
        # time back to UTC 
        dtadj[, date := with_tz(date, "UTC")]
        
        # save adjusted to NAS or local uri
        print("Save to uri")
        file_name = file.path(file_path_save, paste0(symbol, ".parquet"))
        if (file.exists(file_name)) file.remove(file_name)
        write_parquet(dtadj, file_name)
      } else {
        # save without adjusting because there are no maps and factors data
        print("Save to uri without adjustment because there are no maps and factors data")
        file_name = file.path(file_path_save, paste0(symbol, ".parquet"))
        if (file.exists(file_name)) file.remove(file_name)
        write_parquet(dt, file_name)
      }
      return(NULL)
      
      # debug
      # x = "F:/equity/usa/minute-adjusted/META.parquet"
      # system.time({ohlcv = arrow::read_parquet(x)})
      # plot(ohlcv[seq(1, nrow(ohlcv), 50), close])
    },
    
    #' @description NAS sync
    #'
    #' @param local_dir File path to parquet file that contains market data.
    #' @param nas_dir Path to which save the data.
    #' @param direction downlaod or upload.
    #'
    #' @return Nothin to return
    sync = function(local_dir, nas_dir, direction = "upload") {
      
      # debug
      # local_dir = "F:/equity/usa/minute/AAPL"
      # nas_dir    = "N:/home/Drive/equity/usa/minute/AAPL"
      
      # warning message
      if (!dir.exists(nas_dir)) {
        warning("Folder ", nas_dir, " doesn't exists. It will be created automaticly.")
        dir.create(nas_dir)
      }
      
      # list files in local dir and NAS dir
      local_ = list.files(local_dir, full.names = TRUE)
      nas_   = list.files(nas_dir, full.names = TRUE)
      
      # check new files
      new_files = which(!(basename(local_) %in% basename(nas_)))
      new_files = local_[new_files]
      new_files_path = file.path(nas_dir, basename(new_files))
      
      # copy files from local to NAS
      file.copy(new_files, new_files_path)
    },
    
    #' @description Merged Fundamental data
    #'
    #' @param balance_sheet file to balance sheet csv's. 
    #' @param income_statement file to income statement csv's.
    #' @param cash_flow file to cash flow csv's.
    #' @param financial_growth file to financial growth csv's.
    #' @param financial_metrics file to financial metrics csv's.
    #' @param financial_ratios file to financial ratios csv's.
    #'
    #' @return Nothin to return
    fundamnetals_agg = function(balance_sheet,
                                income_statement,
                                cash_flow,
                                financial_growth,
                                financial_metrics,
                                financial_ratios) {
      
      # debug
      # library(arrow)
      # library(data.table)
      # balance_sheet     = "F:/equity/usa/fundamentals/balance-sheet-statement-bulk/quarter"
      # income_statement  = "F:/equity/usa/fundamentals/income-statement-bulk/quarter"
      # cash_flow         = "F:/equity/usa/fundamentals/cash-flow-statement-bulk/quarter"
      # financial_growth  = "F:/equity/usa/fundamentals/financial-growth-bulk/quarter"
      # financial_metrics = "F:/equity/usa/fundamentals/key-metrics-bulk/quarter"
      # financial_ratios  = "F:/equity/usa/fundamentals/ratios-bulk/quarter"
      
      # help function
      read_fs = function(path) {
        dt_files = list.files(path, full.names = TRUE)
        dt = lapply(dt_files, fread)
        dt = rbindlist(dt)
        if ("acceptedDateTime" %in% colnames(dt)) {
          dt[, `:=`(
            acceptedDateTime = as.POSIXct(acceptedDate, format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York"),
            acceptedDate = as.Date(acceptedDate, format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York")
          )]
        }
        return(dt)
      }
      
      # income statement data
      print("Import financial statement data")
      bs = read_fs(balance_sheet)
      pl = read_fs(income_statement)
      cf = read_fs(cash_flow)
      fin_growth = read_fs(financial_growth)
      fin_metrics = read_fs(financial_metrics)
      ratios = read_fs(financial_ratios)
      
      # merge all fundamental data
      columns_diff_pl <- c("symbol", "date", setdiff(colnames(pl), colnames(bs)))
      columns_diff_cf <- c("symbol", "date", setdiff(colnames(cf), colnames(bs)))
      columns_diff_cf <- c("symbol", "date", setdiff(columns_diff_cf, colnames(pl)))
      columns_diff_fg <- c("symbol", "date", setdiff(colnames(fin_growth), colnames(pl)))
      columns_diff_fm <- c("symbol", "date", setdiff(colnames(fin_metrics), colnames(pl)))
      columns_diff_fm <- c("symbol", "date", setdiff(columns_diff_fm, colnames(ratios)))
      columns_diff_fr <- c("symbol", "date", setdiff(colnames(ratios), colnames(pl)))
      fundamentals <-
        Reduce(
          function(x, y)
            merge(
              x,
              y,
              by = c("symbol", "date"),
              all.x = TRUE,
              all.y = FALSE
            ),
          list(bs,
               pl[, ..columns_diff_pl],
               cf[, ..columns_diff_cf],
               fin_growth[, ..columns_diff_fg],
               fin_metrics[, ..columns_diff_fm],
               ratios[, ..columns_diff_fr])
        )

      # remove unnecesary columns
      fundamentals[, c("link", "finalLink") := NULL]
      
      # conver characters to numerics
      char_cols = fundamentals[, colnames(.SD), .SDcols = is.character]
      char_cols = setdiff(char_cols, c("symbol", "reportedCurrency", "period"))
      fundamentals[, (char_cols) := lapply(.SD, as.numeric), .SDcols = char_cols]
      
      # order
      setorder(fundamentals, symbol, date)
      
      # create ttm variables for pl and cf
      ids = c("symbol", "date", "reportedCurrency", "cik", "fillingDate", 
              "acceptedDate", "calendarYear", "period")
      cols = setdiff(c(columns_diff_pl, columns_diff_cf), ids)
      cols_ttm = paste0(cols, "_ttm")
      fundamentals[, (cols_ttm) := lapply(.SD, frollsum, 4), .SDcols = cols, by = symbol]
      
      return(fundamentals)
    }
  )
)
