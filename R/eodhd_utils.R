.eodhd_fundamental_files = function(fundamentals_dir) {
  if (!dir.exists(fundamentals_dir)) {
    stop("Fundamentals directory does not exist: ", fundamentals_dir, call. = FALSE)
  }
  files = list.files(
    fundamentals_dir,
    pattern = "\\.json$",
    recursive = TRUE,
    full.names = TRUE,
    ignore.case = TRUE
  )
  if (length(files) == 0L) {
    stop("No JSON files found in fundamentals directory: ", fundamentals_dir,
         call. = FALSE)
  }
  files
}

.eodhd_read_json = function(file) {
  tryCatch(
    jsonlite::read_json(file, simplifyVector = FALSE),
    error = function(e) {
      warning("Cannot read fundamentals JSON: ", file, " (", conditionMessage(e), ")",
              call. = FALSE)
      NULL
    }
  )
}

.eodhd_file_symbol = function(file) {
  sub("\\.json$", "", basename(file), ignore.case = TRUE)
}

.eodhd_get = function(x, path, default = NA) {
  node = x
  for (name in path) {
    if (is.null(node) || is.null(node[[name]])) {
      return(default)
    }
    node = node[[name]]
  }
  if (is.null(node) || length(node) == 0L) {
    return(default)
  }
  if (length(node) > 1L && !is.list(node)) {
    return(node[[1L]])
  }
  node
}

.findata_first_existing_name = function(names_x, candidates) {
  candidates[candidates %in% names_x][1L]
}

.findata_numeric = function(x) {
  if (is.numeric(x)) {
    return(as.numeric(x))
  }
  if (inherits(x, "integer64")) {
    return(as.numeric(x))
  }
  if (is.logical(x)) {
    return(as.numeric(x))
  }
  x = trimws(as.character(x))
  x[x %in% c("", "NA", "N/A", "null", "NULL", "NaN", "Inf", "-Inf")] = NA_character_
  x = gsub(",", "", x, fixed = TRUE)
  suppressWarnings(as.numeric(x))
}

.findata_coalesce_numeric = function(dt, candidates, default = NA_real_) {
  existing = candidates[candidates %in% names(dt)]
  if (length(existing) == 0L) {
    return(rep(default, nrow(dt)))
  }
  values = lapply(existing, function(name) .findata_numeric(dt[[name]]))
  out = values[[1L]]
  if (length(values) > 1L) {
    for (i in 2:length(values)) {
      out = data.table::fcoalesce(out, values[[i]])
    }
  }
  out
}

.findata_coalesce_character = function(dt, candidates, default = NA_character_) {
  existing = candidates[candidates %in% names(dt)]
  if (length(existing) == 0L) {
    return(rep(default, nrow(dt)))
  }
  values = lapply(existing, function(name) as.character(dt[[name]]))
  out = values[[1L]]
  if (length(values) > 1L) {
    for (i in 2:length(values)) {
      out = data.table::fcoalesce(out, values[[i]])
    }
  }
  out
}

.findata_coalesce_date = function(dt, candidates) {
  existing = candidates[candidates %in% names(dt)]
  if (length(existing) == 0L) {
    return(as.Date(rep(NA_character_, nrow(dt))))
  }
  values = lapply(existing, function(name) .findata_as_date(dt[[name]]))
  out = values[[1L]]
  if (length(values) > 1L) {
    for (i in 2:length(values)) {
      out = data.table::fcoalesce(out, values[[i]])
    }
  }
  out
}

.findata_weighted_mean = function(x, w) {
  x = as.numeric(x)
  w = as.numeric(w)
  keep = is.finite(x) & is.finite(w) & w > 0
  if (!any(keep)) {
    return(NA_real_)
  }
  sum(x[keep] * w[keep]) / sum(w[keep])
}

.findata_mean = function(x) {
  x = as.numeric(x)
  x = x[is.finite(x)]
  if (length(x) == 0L) {
    return(NA_real_)
  }
  mean(x)
}

.findata_row_mean = function(dt, cols) {
  cols = cols[cols %in% names(dt)]
  if (length(cols) == 0L) {
    return(rep(NA_real_, nrow(dt)))
  }
  out = rowMeans(as.data.frame(dt[, ..cols]), na.rm = TRUE)
  out[is.nan(out)] = NA_real_
  out
}

.findata_score_signal = function(x) {
  out = as.integer(!is.na(x) & x)
  out[is.na(out)] = 0L
  out
}

