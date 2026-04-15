function (Symbols, env = globalenv(), from = "2007-01-01", to = Sys.Date(), 
    adjust = FALSE, period = "day", market = NULL, verbose = FALSE, 
    auto.assign = FALSE, api.key = "", user_agent = NULL, ...) 
{
    Symbol <- Market <- Id <- NULL
    p <- 0
    if ("tick" == period) 
        p <- 1
    if ("1min" == period) 
        p <- 2
    if ("5min" == period) 
        p <- 3
    if ("10min" == period) 
        p <- 4
    if ("15min" == period) 
        p <- 5
    if ("30min" == period) 
        p <- 6
    if ("hour" == period) 
        p <- 7
    if ("day" == period) 
        p <- 8
    if ("week" == period) 
        p <- 9
    if ("month" == period) 
        p <- 10
    if (p == 0) {
        message(paste("Unkown period ", period))
    }
    if (api.key != "") {
        url = "https://trade-api.finam.ru"
        endpoint = "/api/v1/instruments/"
        params <- list(Ticker = Symbols)
        full_url <- paste0(url, endpoint, "?", paste(names(params), 
            params, sep = "=", collapse = "&"))
        headers = c(`X-Api-Key` = api.key)
        response <- GET(full_url, encode = "json", add_headers(headers))
        if (response$status_code == 200) {
            json_response <- content(response, "text", encoding = "UTF-8")
            data_result <- fromJSON(json_response)$data
            return(data_result)
        }
        if (response$status_code != 200) 
            if (verbose) 
                return(content(response, as = "parsed"))
    }
    if (api.key == "") {
        if (!exists("symbol_list_FINAM")) {
            symbol_list_FINAM = NULL
            if (is.null(user_agent)) 
                user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.37 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.37"
            symbol_list_FINAM = getSymbolList(src = "Finam", 
                auto.assign = FALSE, user_agent = user_agent)
            getSymbolList(src = "Finam", auto.assign = TRUE, 
                user_agent = user_agent)
        }
        fr <- NaN
        for (i in 1:length(Symbols)) {
            from.y <- as.numeric(format(as.Date(from), "%Y"))
            from.m <- as.numeric(format(as.Date(from), "%m")) - 
                1
            from.d <- as.numeric(format(as.Date(from), "%d"))
            to.y <- as.numeric(format(as.Date(to), "%Y"))
            to.m <- as.numeric(format(as.Date(to), "%m")) - 1
            to.d <- as.numeric(format(as.Date(to), "%d"))
            Symbols.name = Symbols[i]
            if (!is.null(market)) {
                finam.stock <- data.table(symbol_list_FINAM[Symbol == 
                  Symbols.name])
                Symbols.id <- finam.stock[Market == market][order(Id)]$Id[1]
            }
            if (is.null(market)) {
                finam.stock <- data.table(symbol_list_FINAM[Symbol == 
                  Symbols.name])
                setorder(finam.stock, "Market", "Id")
                Symbols.id = finam.stock$Id[1]
                market = finam.stock$Market[1]
            }
            finam.HOST <- "export.finam.ru"
            finam.URL <- "/table.csv?d=d&f=table&e=.csv&dtf=1&tmf=1&MSOR=0&sep=1&sep2=1&at=1&"
            stock.URL <- paste(finam.URL, "p=", p, "&market=", 
                market, "&em=", Symbols.id, "&df=", from.d, "&mf=", 
                from.m, "&yf=", from.y, "&dt=", to.d, "&mt=", 
                to.m, "&yt=", to.y, sep = "")
            tmp <- tempfile()
            if (p == 1) {
                stock.URL <- paste("http://", finam.HOST, stock.URL, 
                  "&datf=6", sep = "")
            }
            else {
                stock.URL <- paste("http://", finam.HOST, stock.URL, 
                  "&datf=1", sep = "")
            }
            download.file(stock.URL, destfile = tmp, quiet = !verbose)
            fr <- read.csv(tmp, as.is = TRUE, colClasses = "character")
            unlink(tmp)
            if (p == 1) {
                fr <- xts(apply(as.matrix(fr[, (5:6)]), 2, as.numeric), 
                  as.POSIXct(strptime(paste(fr[, 3], fr[, 4]), 
                    "%Y%m%d %H%M%S")), src = "finam", updated = Sys.time())
                colnames(fr) <- paste(toupper(gsub("\\^", "", 
                  Symbols.name)), c("Close", "Volume"), sep = ".")
            }
            else if (p > 7) {
                fr <- xts(apply(as.matrix(fr[, (5:9)]), 2, as.numeric), 
                  order.by = as.Date(strptime(fr[, 3], "%Y%m%d")), 
                  src = "finam", updated = Sys.time())
                colnames(fr) <- paste(toupper(gsub("\\^", "", 
                  Symbols.name)), c("Open", "High", "Low", "Close", 
                  "Volume"), sep = ".")
            }
            else {
                fr <- xts(apply(as.matrix(fr[, (5:9)]), 2, as.numeric), 
                  as.POSIXct(strptime(paste(fr[, 3], fr[, 4]), 
                    "%Y%m%d %H%M%S")), src = "finam", updated = Sys.time())
                colnames(fr) <- paste(toupper(gsub("\\^", "", 
                  Symbols.name)), c("Open", "High", "Low", "Close", 
                  "Volume"), sep = ".")
            }
            Symbols[[i]] <- toupper(gsub("\\^", "", Symbols[[i]]))
            if (auto.assign) 
                assign(Symbols[[i]], fr, env)
            if (i >= 3 && length(Symbols) > 3) {
                message("pausing 1 second between requests for more than 5 symbols")
                Sys.sleep(1)
            }
        }
        if (auto.assign) 
            return(Symbols)
        return(fr)
    }
}
