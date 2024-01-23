#' @title Macroeconomics Data
#'
#' @description
#' Peovide macroeconomics data from various sources:
#'     - FRED
#'
#' @export
MacroData = R6::R6Class(
  "MacroData",
  public = list(
    #' @field path_to_dump Local path to save data to.
    path_to_dump = NULL,
    
    #' @description
    #' Create a new MacroData object.
    #'
    #' @param path_to_dump Local path to save data to.
    #'
    #' @return A new `MacroData` object.
    initialize = function(path_to_dump) {
      # TODO: DEBUG
      # path_to_dump = "F:/macro_data"
      # self = list()
      # self$path_to_dump = path_to_dump
      
      # checks
      assert_character(path_to_dump, len = 1L)
      assert_choice("FRED-KEY", names(Sys.getenv()))
      assert_true(dir_exists(path_to_dump))
      
      # set credentials
      fredr_set_key(Sys.getenv("FRED-KEY"))
      
      # set init vars
      self$path_to_dump = path_to_dump
    },
    
    #' @description
    #' Get FRED metadata.
    #'
    #' @param asset binance asset. Can be cm, um, option or spot
    #'
    #' @return NULL.
    get_fred_metadata = function(asset) {
      # get categories
      categories_id <-
        read_html("https://fred.stlouisfed.org/categories/") |>
        html_elements("a") |>
        html_attr("href")
      categories_id <-
        categories_id[grep("categories/\\d+", categories_id)]
      categories_id = gsub("/categories/", "", categories_id)
      categories_id = as.integer(categories_id)
      categories_id = unique(categories_id)
      
      # get chinld categories
      child_categories = lapply(categories_id, fredr_category_children)
      child_categories_ids = rbindlist(child_categories, fill = TRUE)
      child_categories_ids = child_categories_ids$id
      child_categories_ids = unique(child_categories_ids)
      
      # scrap data for every category
      fred_meta_l = lapply(child_categories_ids, function(id) {
        print(id)
        # get first requetst for id
        fred_meta_ <- fredr_category_series(
          category_id = id,
          limit = 1000L,
          order_by = "last_updated",
          offset = 0
        )
        Sys.sleep(0.8)
        nrow_ = nrow(fred_meta_)
        n = 1
        while (nrow_ == 1000) {
          print(n)
          fred_meta_n = fredr_category_series(
            category_id = id,
            limit = 1000L,
            order_by = "last_updated",
            offset = 1000 * n
          )
          n = n + 1
          fred_meta_ = rbindlist(list(fred_meta_, fred_meta_n), fill = TRUE)
          nrow_ = nrow(fred_meta_n)
          Sys.sleep(0.8)
        }
        if (length(fred_meta_) > 0) {
          return(cbind(id = id, fred_meta_))
        } else {
          return(NULL)
        }
      })
      fred_meta = rbindlist(fred_meta_l, fill = TRUE)
      colnames(fred_meta)[1] = "id_category"

      # clean meta
      date_cols = c("observation_start", "observation_end")
      fred_meta[, (date_cols) := lapply(.SD, as.Date), .SDcols = date_cols]
            
      return(fred_meta)
    },
    
    #' @description
    #' Get data from alfred.
    #'
    #' @param id Fred series id.
    #' @param vintage_dates Vintage dates.
    #' @param bin_len Bin length.
    #'
    #' @return Data table with data.
    get_alfred = function(id, vintage_dates, bin_len=2000) {
      num_bins = ceiling(length(vintage_dates) / bin_len)
      bins = cut(
        seq_along(vintage_dates),
        breaks = c(seq(1, length(vintage_dates), by = bin_len-1),
                   length(vintage_dates)),
        include.lowest = TRUE,
        labels = FALSE
      )
      split_dates = split(vintage_dates, bins)
      split_dates = lapply(split_dates, function(d) as.Date(d))
      obs_l = lapply(split_dates, function(d) {
        obs = fredr_series_observations(
          series_id = id_,
          observation_start=head(d, 1),
          observation_end=tail(d, 1),
          realtime_start=head(d, 1),
          realtime_end=tail(d, 1)
        )
      })
      obs = rbindlist(obs_l)
      obs[, vintage := 1L]
      return(obs)
    },
    
    #' @description
    #' Bulk FRED database.
    #'
    #' @param ids Character vector, Fred series ids.
    #'
    #' @return NULL.
    bulk_fred = function(ids) {
      # DEBUG
      # filter data
      # fred_meta_sample = fred_meta[observation_end > as.Date("2018-01-01")]
      # fred_meta_sample = unique(fred_meta_sample, by = c("id", "title"))
      # ids = fred_meta_sample[, id]
      
      # create path to save data
      dir_ = path(self$path_to_dump, "fred")
      if (!dir_exists(dir_)) {
        dir_create(dir_, recurse = TRUE)
      }
      
      # get data from the FERD in a loop
      vapply(ids, function(id_) {
        # id_ = ids[[2]]
        # id_ = "NFCI"
        # print(id_)
        file_name_ = path(dir_, id_, ext = "csv")
        if (fs::file_exists(file_name_)) return(1L)
        vin_dates = tryCatch(fredr_series_vintagedates(id_), error = function(e) NULL)
        if (is.null(vin_dates) || length(vin_dates[[1]]) == 1) {
          obs = fredr_series_observations(
            series_id = id_,
            observation_start = as.Date("1900-01-01"),
            observation_end = Sys.Date()
          )
          obs$vintage = 0L
          if (nrow(obs) > 95000) {
            stop("Lots of vars")
          }
        } else {
          print(file_name_)
          date_vec = vin_dates[[1]]
          obs = self$get_alfred(id_, date_vec)
          if (nrow(obs) > 95000) {
            print("Lots of vars")
            obs = self$get_alfred(id_, date_vec, 500)
            if (nrow(obs) > 95000) {
              stop("Lots of vars")
            }
          }
        }
        fwrite(obs, file_name_)
        Sys.sleep(0.9)
        return(1L)
      }, FUN.VALUE = integer(1L))
    }
  )
)
