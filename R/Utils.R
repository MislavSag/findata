#' @title Utils Class
#'
#' @description
#' Helper functions
#'
#' @export
Utils = R6::R6Class(
  "Utils",
  inherit = DataAbstract,

  public = list(

    #' @description
    #' Create a new Utils object.
    #'
    #' @param azure_storage_endpoint Azure storate endpont
    #' @param context_with_config AWS S3 Tiledb config
    #'
    #' @return A new `Utils` object.
    initialize = function(azure_storage_endpoint = NULL,
                          context_with_config = NULL) {

      # endpoint
      super$initialize(azure_storage_endpoint, context_with_config)
    },

    #' @description Update map files
    #'
    #' @param save_uri Uri where tiledb saves files
    #' @param symbol_uri Uri where symbol list is
    #'
    #' @return Map files
    get_map_files_tiledb = function(save_uri = 's3://equity-usa-mapfiles', symbol_uri = "https://en.wikipedia.org/wiki/S%26P_100"){

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
