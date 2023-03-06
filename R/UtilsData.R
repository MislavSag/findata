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
    #' @return Data table of Sp500 tickers
    sp500_history = function() {

      # get SP500 stocks by date
      url <- "https://raw.githubusercontent.com/fja05680/sp500/master/S%26P%20500%20Historical%20Components%20%26%20Changes(03-14-2022).csv"
      sp500_symbols <- fread(url)
      symbols_long <- tstrsplit(sp500_symbols$tickers, split = ",")
      symbols_long <- as.data.table(do.call(cbind, symbols_long))
      sp500_symbols <- cbind(date = setDT(sp500_symbols)[, .(date)], symbols_long)
      setnames(sp500_symbols, "date.date", "date")
      sp500_symbols <- melt(sp500_symbols, id.vars = "date")
      sp500_symbols <- sp500_symbols[, .(date, value)]
      setnames(sp500_symbols, c("date", "symbol"))
      sp500_symbols <- na.omit(sp500_symbols)
      setorder(sp500_symbols, "date")

      return(sp500_symbols)
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
    }
  )
)
