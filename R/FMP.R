#' @title FMP Class
#'
#' @description
#' Get data data from FMP Cloud.
#'
#' @export
FMP = R6::R6Class(
  "FMP",
  inherit = DataAbstract,

  public = list(

    #' @field api_key API KEY for FMP Cloud
    api_key = NULL,

    #' @description
    #' Create a new FMP object.
    #'
    #' @param api_key API KEY for FMP cloud data.
    #'
    #' @return A new `FMP` object.
    initialize = function(api_key = NULL) {

      # check and define variables
      if (is.null(api_key)) {
        self$api_key = assert_character(Sys.getenv("APIKEY-FMPCLOUD"))
      } else {
        self$api_key = api_key
      }
    },

    #' @description
    #' Create a new FMP object.
    #'
    #' @param days_history how long to the past to look
    #'
    #' @return Earning announcements data.
    get_earning_announcements = function(days_history = 10) {

      # define container
      cont <- storage_container(private$azure_storage_endpoint, "fmpcloud")
      blob_files <- list_blobs(cont)
      file_exists_on_blob <- private$ea_file_name %in% blob_files$name # file_exists_on_blob <- "EarningAnnouncements.csv" %in% blob_files$name
      if (file_exists_on_blob) {
        old_data <- storage_read_csv(cont, private$ea_file_name) # old_data <- storage_read_csv(cont, "EarningAnnouncements.csv")
        old_data$date <- as.Date(old_data$date)
        old_data <- as.data.table(old_data)
        start_date <- max(old_data$date, Sys.Date()) - days_history
      } else {
        start_date <- as.Date("2010-01-01")
      }
      end_date <- Sys.Date()

      # check if scrapng ncessary
      if (end_date < start_date) {
        print("No new data.")
        return(old_data)
      }

      # define listing dates
      dates_from <- seq.Date(start_date, end_date, by = 3)
      dates_to <- dates_from + 3

      # get data
      ea <- lapply(seq_along(dates_from), function(i) {
        private$fmpv_path("earning_calendar", from = dates_from[i], to = dates_to[i], apikey = self$api_key)
        # fmpv_path("earning_calendar", from = dates_from[i], to = dates_to[i], apikey = Sys.getenv("APIKEY-FMPCLOUD"))
      })
      new_data <- rbindlist(ea, fill = TRUE)

      # check if there are data available for timespan
      if (nrow(new_data) == 0) {
        print("No data for earning announcements.")
        return(NULL)
      }

      # clean data
      new_data$date <- as.Date(new_data$date)
      results <- unique(new_data)

      # add old data to new data
      if (file_exists_on_blob) {
        results <- rbind(old_data, results)
        results <- unique(results)
      }

      # save file to Azure blob if blob_file is not NA
      super$save_blob_files(results, file_name = private$ea_file_name)
      print(paste0("Data saved to blob file ", private$ea_file_name))

      return(results)
    },

    #' @description Get Earning Call Transcript from FMP cloud.
    #'
    #' @param symbols stock symbols
    #' @param years years to extract data from.
    #' @param blob_cont blob container
    #' @param save_files should transcripts be saved.
    #'
    #' @return Result of GET request
    get_transcripts = function(symbols,
                                years = 1992:2021,
                                blob_cont = "transcripts",
                                save_files = FALSE) {
      # overwrite_existing_files = FALSE) {

      # remove downloaded files
      # if (!overwrite_existing_files & save_files) {
      #   existing_files <- gsub("\\.rds", "", get_all_blob_files(blob_cont)[, 1])
      #   symbols <- setdiff(symbols, existing_files)
      # }

      # get data
      transcripts_list <- lapply(symbols, function(s) {
        inner <- lapply(years, function(y) {
          private$fmpv_path(paste0("batch_earning_call_transcript/", s), v = "v4", year = y, apikey = self$api_key)
          # fmpv_path(paste0("batch_earning_call_transcript/", s), v = "v4", year = y, apikey = Sys.getenv("APIKEY-FMPCLOUD"))
        })
        inner <- rbindlist(inner)
        if (nrow(inner) > 0) {
          inner$date <- as.POSIXct(inner$date, tz = "EST")
          # if (save_files) save_blob_files(inner, file_name = paste0(s, ".rds"), container = blob_cont)
        } else {
          print("There are no transcripts data.")
        }
        inner
      })
      transcripts <- rbindlist(transcripts_list)
      return(transcripts)
    },

    #' @description Update earnings announcements data from FMP cloud
    #'
    #' @param symbols Sybmols
    #' @param check_years Which years to check when updatingcheck_years
    #'
    #' @return Return transcripts from FMP cloud.
    update_transcripts = function(symbols, check_years = c(2021, 2022)) {

      # set up container
      cont <- storage_container(private$azure_storage_endpoint, "transcripts")
      # cont <- storage_container(storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY")), "transcripts")

      # import transcripts metadata for all symbols
      url <- "https://financialmodelingprep.com/api/v4/earning_call_transcript"
      print("Get all transcripts for symbols")
      all_transcripts_l <- lapply(symbols, function(x) {
        p <- GET(url, query = list(symbol = x, apikey = Sys.getenv("APIKEY-FMPCLOUD")))
        p <- rbindlist(content(p))
        if (nrow(p) > 0) return(cbind.data.frame(symbol = x, p)) else return(NULL)
      })
      all_transcripts <- rbindlist(all_transcripts_l)
      setnames(all_transcripts, c("symbol", "quarter", "year", "date"))
      all_transcripts <- all_transcripts[year %in% check_years]

      # main loop
      print("Get missing transcripts")
      for (s in unique(all_transcripts$symbol)) {
        print(s)
        file_name_ <- paste0(s, ".rds")
        # history <- tryCatch({AzureStor::download_blob(cont, file_name_)}, error = function(e) NULL)
        history <- tryCatch({storage_load_rds(cont, file_name_)}, error = function(e) NULL)
        sample_stranscripts <- all_transcripts[symbol == s]
        cols <- c("quarter", "year")
        if (is.null(history) & nrow(sample_stranscripts) > 0) {
          transcript_diff <- sample_stranscripts
        } else {
          transcript_diff <- fsetdiff(sample_stranscripts[, .SD, .SDcols = cols], history[, .SD, .SDcols = cols])
        }

        # get new data if necessary
        if (nrow(transcript_diff) == 0) {
          history_unique <- unique(history)
          print("No transcript data.")
          if (!(identical(history_unique, history))) {
            print(paste0("Data saved to blob file ", file_name_))
            super$save_blob_files(history_unique, file_name = file_name_, "transcripts")
          }
        } else {

          # get new data
          new <- self$get_transcripts(
            s,
            unique(transcript_diff$year),
            save_files = FALSE
          )

          # check if there are data available for timespan
          if (nrow(new) == 0) {
            print("No data for earning announcements.")
            return(NULL)
          }

          # clean data
          new_transcrupts_s <- rbind(new, history)
          setorder(new_transcrupts_s, "date")

          # save to blob file
          print(paste0("Data saved to blob file ", file_name_))
          super$save_blob_files(new_transcrupts_s, file_name = file_name_, "transcripts")
        }
      }
    },

    #' @description get daily data from FMP cloud
    #'
    #' @param symbols Symbols.
    #' @param date_start Start date.
    #' @param date_end End date.
    #'
    #' @return Return transcripts from FMP cloud.
    get_daily = function(symbols, date_start, date_end) {

      # define container
      cont <- storage_container(private$azure_storage_endpoint, "fmpcloud")

      # get daily data from FMP cloud
      result <- lapply(symbols, function(x) {
        url_fmp = paste0("https://financialmodelingprep.com/api/v3/historical-price-full/", x)
        q <- GET(url_fmp, query = list(from = date_start, to = date_end, apikey = Sys.getenv("APIKEY-FMPCLOUD")))
        result <- content(q)
        DT <- rbindlist(result$historical, fill = TRUE)
        DT[, symbol := result$symbol]
      })
      result <- rbindlist(result, fill = TRUE)

      # save file to Azure blob if blob_file is not NA
      super$save_blob_files(results, file_name = private$prices_file_name)
      print(paste0("Data saved to blob file ", private$ea_file_name))

      return(result)
    },

    get_daily_batch = function() {

      # date sequence()
      seq_date <- seq.Date(as.Date("2000-01-01"), Sys.Date() - 1, by = 1)

      # check blob
      cont <- storage_container(private$azure_storage_endpoint, "fmpcloud-daily")
      blob_files <- list_blobs(cont)
      if (nrow(blob_files) == 0) {
        print("scrap for the first time!")
      } else {
        seq_date <- as.Date(setdiff(seq_date, as.Date(gsub(".csv", "", blob_files$name))), origin = "1970-01-01")
      }

      # get daily with batch and save to azure blob
      url_base <- "https://financialmodelingprep.com/api/v4/batch-request-end-of-day-prices"
      lapply(seq_date, function(x) {
        p <- RETRY("GET", url_base, query = list(date = x, apikey = api_key), times = 4L) # ADD SELF HERE
        data_ <- content(p)
        storage_write_csv(data_, cont, paste0(x, ".csv"))
        return(NULL)
      })

      # merge all files and upload to blob
      cont_daily <- storage_container(private$azure_storage_endpoint, "fmpcloud")

      # save to azure by stocks
      tmp_file <- tempfile()
      new_files <- paste0(seq_date, ".csv")
      lapply(new_files, function(x) {
        # x <- blob_files$name[3]
        print(x)
        date_data <- suppressMessages(storage_read_csv(cont, paste0(x)))
        if (nrow(date_data) == 0) return(NULL)
        file_name <- paste0(tmp_file, ".csv")
        fwrite(date_data, file_name)
        if (!(blob_exists(cont_daily, "prices.csv"))) {
          suppressMessages(storage_upload(cont_daily, src=file_name, dest="prices.csv", type="AppendBlob"))
        } else {
          suppressMessages(storage_upload(cont_daily, src=file_name, dest="prices.csv", type="AppendBlob", append = TRUE))
        }
        return(NULL)
      })
    }
  ),
  private = list(
    ea_file_name = "EarningAnnouncements.csv",
    transcripts_file_name = "earnings-calendar.rds",
    prices_file_name = "prices.csv",

    fmpv_path = function(path = "earning_calendar", v = "v3", ...) {

      # query params
      query_params <- list(...)

      # define url
      url <- paste0("https://financialmodelingprep.com/api/", v, "/", path)

      # get data
      p <- RETRY("GET", url, query = query_params, times = 5)
      result <- suppressWarnings(rbindlist(httr::content(p), fill = TRUE))

      return(result)
    }
  )
)



