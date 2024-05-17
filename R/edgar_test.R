# library(data.table)
# library(lubridate)
# library(RCurl)
# library(fs)
# library(glue)
# library(httr)
# library(XML)
# library(stringr)
# 
# 
# # Setup
# out_dir = "F:/edgar"
# if(!dir.exists(path(out_dir, "forms"))) dir.create(path(out_dir, "forms"))
# if(!dir.exists(path(out_dir, "master"))) dir.create(path(out_dir, "master"))
# if(!dir.exists(path(out_dir, "clean_forms"))) dir.create(path(out_dir, "clean_forms"))
# if(!dir.exists(path(out_dir, "parsed_forms"))) dir.create(path(out_dir, "parsed_forms"))
# 
# # Get dates
# dates = seq(as.Date("2020-01-01"), as.Date("2024-05-01"), by = "quarters")
# 
# # Construct SEC master file
# # https://www.sec.gov/Archives/edgar/full-index/
# qtr_master_file = function(date) {
#   # Debug
#   # date = dates[1]
#   
#   # get master files 
#   master_link = paste0("https://www.sec.gov/Archives/edgar/full-index/", 
#                        year(date), "/QTR", quarter(date), "/master.idx")
#   print(sprintf("Downloading master file for quarter %d of year %s...", quarter(date), year(date)))
#   download.file(master_link, path(out_dir, "tmp.txt"),
#                 headers = c("User-Agent" = "Mislav Sagovac mislav.sagovac@contentio.biz")
#   )
#   master = path(out_dir, "tmp.txt") |>
#     readLines() |>
#     gsub("#", "", x = _) |>
#     paste0(collapse = "\n") |>
#     fread(sep = "|", skip = 11) |>
#     `colnames<-`(c("cik", "name", "type", "date", "link"))
#   master = master[grepl("SC 13(D|G)", type)]
#   master[, link := paste0("https://www.sec.gov/Archives/", link)]
#   master[, file := gsub(".*/", "", link)]
#   
#   # Close connections and remo temporary downloaded file 
#   closeAllConnections() # not sure why this
#   file.remove(path(out_dir, "tmp.txt"))
#   return(master)
# }
# 
# # download files into tmp dir
# # sometimes the SEC puts a limit on the number of downloads
# # put delay = T to account for that
# dwnld_files = function(master, delay = FALSE) {
#   # Debug
#   # master
#   # delay = TRUE
#   
#   # Create temporary directory
#   dir.create(file.path(out_dir, "temp_dir"))
#   master = master[!duplicated(file)]
#   
#   # Download files
#   for (j in 1:length(master[, file])) {
#     if (delay == TRUE) Sys.sleep(0.13)
#     file_name = path(out_dir, "temp_dir", master$file[j])
#     
#     # Download txt master file
#     res = RETRY(
#       "GET",
#       master$link[j], 
#       add_headers("User-Agent" = "Mislav Sagovac mislav.sagovac@contentio.biz"),
#       write_disk(file_name, overwrite = TRUE))
#     if (status_code(res) != 200) print(glue("Error for {j}"))
#   }
# }
# 
# # Loop for gathering 
# for (i in 1:length(dates)) {
#   # i = 3
#   print(Sys.time())
#   master = qtr_master_file(dates[i])
#   file_ = path(out_dir, "Master", glue("master_{year(dates[i])}{quarter(dates[i])}.csv"))
#   fwrite(master, file_)
#   print("Dowloading files, it takes up to 4 hours")
#   dwnld_files(master, delay = TRUE)
# }
# 
# # Save all text forms to one csv
# paths = path(out_dir, "temp_dir")
# files = dir_ls(paths)
# objects = lapply(files, readLines)
# objects = lapply(objects, function(x) paste(x, collapse = "\n"))
# data = data.table(
#   FILENAME    = files,
#   COMLSUBFILE = unlist(objects)
# )
# fwrite(x = data.table(FILENAME    = files, COMLSUBFILE = unlist(objects)),
#        file = path(out_dir, "compsubm.csv"))
# 
# 
# # Extracts text from txt/htmc of the first filings file in the sequence
# get_body = function(webpage) {
#   # Debug
#   # webpage = unlist(objects[1], use.names = FALSE)
#   
#   webpage = unlist(strsplit(webpage, "\n"))
#   file_name = gsub("<FILENAME>", "", grep("<FILENAME>.*$", webpage, perl = TRUE, value = TRUE))
#   start_line = grep("<DOCUMENT>.*$", webpage, perl = TRUE)
#   end_line = grep("</DOCUMENT>.*$", webpage, perl = TRUE)
#   
#   if (length(start_line) * length(end_line) == 0) {
#     return(NA)
#   }
#   
#   if (length(file_name) == 0) {
#     return(paste0(webpage[start_line:end_line], collapse = "\n"))
#   }
#   
#   file_ext = tolower(gsub(".*\\.(.*?)$", "\\1", file_name[1]))
#   
#   start_line = start_line[1]
#   end_line = end_line[1]
#   
#   if (file_ext %in% c("htm", "xls", "xlsx", "js", "css", "paper", "xsd")) {
#     temp = webpage[start_line:end_line]
#     pdf_start = grep("^<TEXT>", temp, perl = TRUE) + 1
#     pdf_end = grep("^</TEXT>", temp, perl = TRUE) - 1
#     res = try({
#       text = xpathApply(htmlParse(temp[pdf_start:pdf_end], encoding = "UTF-8"), "//body", xmlValue)[[1]]
#     }, silent = TRUE)
#     if (class(res) == "try-error") text <- temp[pdf_start:pdf_end]
#   }
#   
#   if (file_ext == "txt") {
#     text = webpage[start_line:end_line]
#     text = paste0(text, collapse = "\n")
#   }
#   return(text)
# }
# 
# body_text = lapply(unlist(objects[1:10]), get_body)
# 
# # Get subjects from the first filings file in the sequence
# get_sbj = function(webpage) {
#   # Debug
#   # webpage = unlist(objects[1], use.names = FALSE)
#   
#   webpage = unlist(strsplit(webpage, "\n"))
#   sbj_line = grep("SUBJECT COMPANY:", webpage) |> min()
#   fil_line = grep("FILED BY:", webpage) |> min()
#   start_line = grep("<DOCUMENT>.*$", webpage, perl = TRUE)
#   if (length(sbj_line) * length(fil_line) * length(start_line) == 0) {
#     return(NA)
#   }
#   start_line = start_line[1]
#   if (sbj_line < fil_line) {
#     sbj_info = webpage[sbj_line:fil_line]
#   }
#   
#   if (fil_line < sbj_line) {
#     sbj_info = webpage[sbj_line:start_line]
#   }
#   
#   
#   sbj_info = paste0(sbj_info, collapse = "\n")
#   return(sbj_info)
# }
# sbj_text = lapply(unlist(objects[1:10]), get_sbj)
# 
# # Get filers from the first filings file in the sequence
# get_fil = function(webpage) {
#   # Debug
#   # webpage = unlist(objects[1], use.names = FALSE)
#   
#   webpage = unlist(strsplit(webpage, "\n"))
#   sbj_line = grep("SUBJECT COMPANY:", webpage) |> min()
#   fil_line <- grep("FILED BY:", webpage) |> min()
#   start_line <- grep("<DOCUMENT>.*$", webpage, perl = TRUE)
#   if (length(sbj_line) * length(fil_line) * length(start_line) == 0) {
#     return(NA)
#   }
#   
#   start_line = start_line[1]
#   if (sbj_line < fil_line) {
#     sbj_info = webpage[sbj_line:fil_line]
#     fil_info = webpage[fil_line:start_line]
#   }
#   
#   if (fil_line < sbj_line) {
#     sbj_info = webpage[sbj_line:start_line]
#     fil_info = webpage[fil_line:sbj_line]
#   }
#   
#   sbj_info = paste0(sbj_info, collapse = "\n")
#   fil_info = paste0(fil_info, collapse = "\n")
#   return(fil_info)
# }
# 
# fil_text = lapply(unlist(objects[1:10]), get_fil)
# 
# # Create data frame with filings, subjects and body
# master_dt = rbindlist(lapply(dir_ls(path(out_dir, "master")), fread))
# length(files)
# nrow(master_dt)
# master_dt
# head(files)
# master$address[match(path_file(files), master$file)]
# 
# dt = data.table(
#   FILENAME = files,
#   SBJ      = unlist(lapply(unlist(objects), get_sbj)),
#   FIL      = unlist(lapply(unlist(objects), get_fil)),
#   FILING   = sapply(unlist(lapply(unlist(objects), get_body)), function(x) paste0(x, collapse = " "))
# )
# master_dt[, .(link, date, type, FILENAME = path(out_dir, "temp_dir", file))][dt, on = "FILENAME"]
# 
# fwrite(
#   x = master_dt[, .(link, date, type, FILENAME = path(out_dir, "temp_dir", file))][
#     data.table(
#       FILENAME = files,
#       SBJ      = unlist(lapply(unlist(objects), get_sbj)),
#       FIL      = unlist(lapply(unlist(objects), get_fil)),
#       FILING   = sapply(unlist(lapply(unlist(objects), get_body)), function(x) paste0(x, collapse = " "))
#     ), on = "FILENAME"
#   ],
#   file = path(out_dir, "clean_forms.csv")
# )
# 
# fwrite(x = master_dt[, .(link, date, type, FILENAME = path(out_dir, "temp_dir", file))][dt, on = "FILENAME"],
#        file = path(out_dir, "clean_forms.csv"))
# 
# 
# # PARSE SEC HEADERS -------------------------------------------------------
# # Parse headers
# dt = fread(path(out_dir, "clean_forms.csv"))
# dt[, let(
#   fil_CNAME = str_extract(FIL, "(?<=COMPANY CONFORMED NAME:\t\t\t).*(?=\n)"),
#   fil_CIK = str_extract(FIL, "(?<=INDEX KEY:\t\t\t).*(?=\n)"),
#   fil_SIC = str_extract(FIL, "(?<=STANDARD INDUSTRIAL CLASSIFICATION:\t).*(?=\n)"),
#   fil_IRS = str_extract(FIL, "(?<=IRS NUMBER:\t\t\t\t).*(?=\n)"),
#   fil_INC_STATE = str_extract(FIL, "(?<=STATE OF INCORPORATION:\t\t\t).*(?=\n)"),
#   fil_FYEAR_END = str_extract(FIL, "(?<=FISCAL YEAR END:\t\t\t).*(?=\n)"),
#   fil_business_address_street1 = str_extract(FIL, "(?<=STREET 1:\t\t).*(?=\n)"),
#   fil_business_address_street2 = str_extract(FIL, "(?<=STREET 2:\t\t).*(?=\n)"),
#   fil_business_address_city    = str_extract(FIL, "(?<=CITY:\t\t\t).*(?=\n)"),
#   fil_business_address_state   = str_extract(FIL, "(?<=STATE:\t\t\t).*(?=\n)"),
#   fil_business_address_zip     = str_extract(FIL, "(?<=ZIP:\t\t\t).*(?=\n)"),
#   fil_business_address_phone   = str_extract(FIL, "(?<=BUSINESS PHONE:\t\t).*(?=\n)")
# )]
# dt[, let(
#   sbj_CNAME = str_extract(SBJ, "(?<=COMPANY CONFORMED NAME:\t\t\t).*(?=\n)"),
#   sbj_CIK = str_extract(SBJ, "(?<=INDEX KEY:\t\t\t).*(?=\n)"),
#   sbj_SIC = str_extract(SBJ, "(?<=STANDARD INDUSTRIAL CLASSIFICATION:\t).*(?=\n)"),
#   sbj_IRS = str_extract(SBJ, "(?<=IRS NUMBER:\t\t\t\t).*(?=\n)"),
#   sbj_INC_STATE = str_extract(SBJ, "(?<=STATE OF INCORPORATION:\t\t\t).*(?=\n)"),
#   sbj_FYEAR_END = str_extract(SBJ, "(?<=FISCAL YEAR END:\t\t\t).*(?=\n)"),
#   sbj_business_address_street1 = str_extract(SBJ, "(?<=STREET 1:\t\t).*(?=\n)"),
#   sbj_business_address_street2 = str_extract(SBJ, "(?<=STREET 2:\t\t).*(?=\n)"),
#   sbj_business_address_city    = str_extract(SBJ, "(?<=CITY:\t\t\t).*(?=\n)"),
#   sbj_business_address_state   = str_extract(SBJ, "(?<=STATE:\t\t\t).*(?=\n)"),
#   sbj_business_address_zip     = str_extract(SBJ, "(?<=ZIP:\t\t\t).*(?=\n)"),
#   sbj_business_address_phone   = str_extract(SBJ, "(?<=BUSINESS PHONE:\t\t).*(?=\n)")
# )]
# fwrite(x = dt, file = path(out_dir, "parsed_forms.csv"))
# 
# 
# # EXTRACT CUSIP -----------------------------------------------------------
# ### regex to extract CUSIP from filing
# extract_CUSIP <- function(EFiling) {
#   ## get 6 lines before and 4 lines after line CUSIP
#   pat_1 <- "((\\n.*){6})CUSIP.*((\\n.*){4})"
#   get_block <- str_extract(EFiling, pat_1)
#   
#   # set pattern to extract CUSIP
#   pat_2 <- "(?=\\d.*\\d)[a-zA-Z0-9]{9}|\\d\\w{6} \\w\\w \\w|\\d\\w{5} \\w\\w \\w|
#   [a-zA-Z0-9]{7}\\r|\\d\\w{5} \\w\\w\\w|(?=#.*\\d)[a-zA-Z0-9]{9}|(?=\\w\\d.*)[a-zA-Z0-9]{9}|
#   \\d\\w{5}-\\w\\w-\\w|\\d\\w{5}-\\w\\w\\w|\\d\\w{6}|\\d\\w{5}-\\w{2}-\\w|\\d\\w{5}\\n.*\\n.*|
#   \\d\\w{2} \\d\\w{2} \\d\\w{2}|\\d\\w{2} \\w{3} \\d{2} \\d|\\d{6} \\d{2} \\d|
#   \\d\\w{4} \\w{1} \\w{2} \\w|\\w{6} \\d{2} \\d{1}|\\d{3} \\d{3} \\d{3}|\\d{6} \\d{2} \\d{1}|
#   \\w{3} \\w{3} \\d{2} \\d{1}|\\w{5} \\w{1} \\d{2} \\d{1}|\\d{6} \\d{1} \\d{2}|
#   \\d{3} \\d{3} \\d{1} \\d{2}|\\d\\w{2}\\n.*\\d\\w{2}|\\d{6} \\d{2}\\n.*|\\d{5} \\d{2} \\d{1}|
#   \\d{5} \\w{1} \\w{2} \\w{1}|\\d\\w{5}|\\d\\w{2}-\\w{3}-\\w{3}"
#   
#   
#   # Extract CUSIP from within the blocks extracted
#   CUSIP <- str_extract(get_block, pat_2)
#   return(CUSIP)
# }
# ### remove duplicates from CIK-CUSIP map
# CUSIP_table <- function(CUSIP) {
#   CUSIP_df <- data.table(CUSIP)
#   CUSIP_df[, quarter := paste0(year(dates[i]), quarter(dates[i]))]
#   CUSIP_df[, CUSIP := gsub("\\s", "", CUSIP)]
#   CUSIP_df[, CUSIP := gsub("-", "", CUSIP)]
#   CUSIP_df[, CUSIP := toupper(CUSIP)]
#   CUSIP_df[, CUSIP6 := substr(CUSIP, 1, 6)]
#   CUSIP_df[, CUSIP := substr(CUSIP, 1, 8)]
#   return(CUSIP_df)
# }
# 
# # Extract CUSIP
# dt[, let(
#   get_block = str_extract(FILING, "((\\n.*){6})CUSIP.*((\\n.*){4})")
# )]
# pattern_ = "(?=\\d.*\\d)[a-zA-Z0-9]{9}|\\d\\w{6} \\w\\w \\w|\\d\\w{5} \\w\\w \\w|
#   [a-zA-Z0-9]{7}\\r|\\d\\w{5} \\w\\w\\w|(?=#.*\\d)[a-zA-Z0-9]{9}|(?=\\w\\d.*)[a-zA-Z0-9]{9}|
#   \\d\\w{5}-\\w\\w-\\w|\\d\\w{5}-\\w\\w\\w|\\d\\w{6}|\\d\\w{5}-\\w{2}-\\w|\\d\\w{5}\\n.*\\n.*|
#   \\d\\w{2} \\d\\w{2} \\d\\w{2}|\\d\\w{2} \\w{3} \\d{2} \\d|\\d{6} \\d{2} \\d|
#   \\d\\w{4} \\w{1} \\w{2} \\w|\\w{6} \\d{2} \\d{1}|\\d{3} \\d{3} \\d{3}|\\d{6} \\d{2} \\d{1}|
#   \\w{3} \\w{3} \\d{2} \\d{1}|\\w{5} \\w{1} \\d{2} \\d{1}|\\d{6} \\d{1} \\d{2}|
#   \\d{3} \\d{3} \\d{1} \\d{2}|\\d\\w{2}\\n.*\\d\\w{2}|\\d{6} \\d{2}\\n.*|\\d{5} \\d{2} \\d{1}|
#   \\d{5} \\w{1} \\w{2} \\w{1}|\\d\\w{5}|\\d\\w{2}-\\w{3}-\\w{3}"
# dt[, let(
#   CUSIP = str_extract(get_block, pattern_)
# )]
# dt[, CUSIP := gsub("\\s", "", CUSIP)]
# dt[, CUSIP := gsub("-", "", CUSIP)]
# dt[, CUSIP := toupper(CUSIP)]
# dt[, CUSIP6 := substr(CUSIP, 1, 6)]
# dt[, CUSIP := substr(CUSIP, 1, 8)]
# 
# 
# # PARSING PRC POSITION ----------------------------------------------------
# get.lines <- function(x) {
#   if (is.na(x) | is.na(x)) {
#     return(x)
#   }
#   y <- textConnection(x)
#   body <- unlist(readLines(y))
#   # body <- tolower(body)
#   ### 98% of forms have length below 3000
#   ### and I am looking for mentions of prc only in the first part
#   body <- body[1:3000]
#   body <- body[which(str_detect(body, "[:graph:]"))]
#   
#   ### we with 15 lines after each word "percent" and collapse them into one line
#   ind <- grep("percent", body, ignore.case = T)
#   lines <- NULL
#   for (i in ind) lines <- c(lines, paste(body[(i):(i + 15)], collapse = " \n"))
#   close(y)
#   return(lines)
# }
# ### extract positions of all investors
# get.prc <- function(all_lines) {
#   ### clean this lines from extra spaces
#   all_lines <- unlist(all_lines)
#   get.first.lines <- function(x, n) x <- paste(unlist(strsplit(x, "\n"))[1:n], collapse = " ")
#   
#   locate.prc <- function(lines) {
#     lines <- tolower(lines)
#     regex_find_prc <- c(
#       "(\\d{1,4}((\\,|\\.)\\d{0,7}|)( |)\\%|\\d{0,3}(\\.\\d{1,7}|)( |)\\%)",
#       "-0-",
#       "\\d{0,3}\\.\\d{1,7}", "\\d{0,3}\\.\\d{1,7}", "0  %"
#     )
#     
#     for (regex_prc in regex_find_prc)
#     {
#       prc <- str_extract(lines, regex_prc)
#       prc <- prc[!is.na(prc)]
#       if (length(prc) > 0) break
#     }
#     return(prc)
#   }
#   
#   for (end in 1:5 * 3) {
#     lines <- lapply(all_lines, function(x) x <- get.first.lines(x, end))
#     lines <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", lines, perl = TRUE)
#     lines <- gsub("240.13", "", lines)
#     
#     lines <- gsub("-0-%", "0%", lines)
#     ### here I have two spaces to find this pattern last
#     lines <- gsub("none|n/a|less|-0-|lessthan5%", "0  %", lines)
#     prc <- locate.prc(lines)
#     if (length(prc) > 0) break
#   }
#   return(paste(prc, collapse = "|"))
# }
# ### extract the maximum position in the block
# ### in almost all cases maximum position is
# ### a aggregate position among subsidiaries
# get.max.prc <- function(x) {
#   x <- gsub("%", "", x)
#   x <- unlist(str_split(x, "\\|"))
#   # x[grep("none|n/a|-0-|less", x)] <- 0
#   x <- as.numeric(as.character(x))
#   ### this 9 comes from row (9) in form in some filings
#   
#   ind <- which(x %/% 100 == 9)
#   x[ind] <- x[ind] - 900
#   ind <- which(x %/% 100 == 11)
#   x[ind] <- x[ind] - 1100
#   ind <- which(x %/% 10 == 11)
#   x[ind] <- x[ind] - 110
#   x <- x[x <= 100]
#   return(max(x, na.rm = T))
# }
# 
# # 
# dt[1, FILING]
# 
# dt[1, {
#   y = textConnection(FILING)
#   body = readLines(y)
#   # body <- tolower(body)
#   ### 98% of forms have length below 3000
#   ### and I am looking for mentions of prc only in the first part
#   body = body[1:3000]
#   body = body[which(str_detect(body, "[:graph:]"))]
#   
#   ### we with 15 lines after each word "percent" and collapse them into one line
#   ind = grep("percent", body, ignore.case = T)
#   lines = NULL
#   for (i in ind) lines = c(lines, paste(body[(i):(i + 15)], collapse = " \n"))
#   close(y)
#   return(lines)
# }]
# 
# lines = lapply(res1$FILING, get.lines)
# 
