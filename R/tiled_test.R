# library(data.table)
# library(tiledb)
# library(microbenchmark)
# library(arrow)
# library(tiledbsc)
#
#
# # test data - slow
# df = fread("https://contentiobatch.blob.core.windows.net/test/hour_data.csv")
#
# # save data to csv
# csv_file = tempfile("equity-usa-hour-fmpcloud-adjusted", fileext = ".csv")
# fwrite(df, csv_file)
#
# # save data to parquet
# parquet_file <- tempfile()
# on.exit(unlink(parquet_file))
# write_parquet(df, parquet_file)
#
# # save data to tiledb (to gave aonly one fragment)
# tiledb_dir <- tempdir()
# # fromDataFrame(df, tiledb_dir)
# fromDataFrame(df, tiledb_dir, capacity = 500000L, allows_dups = FALSE)
#
#
# # parquet expression
# import_parquet = function() {
#   df = read_parquet(parquet_file)
#   return(df)
# }
#
# # csv expression
# import_csv = function() {
#   df = fread(csv_file, nThread = 4L, showProgress = FALSE)
#   return(df)
# }
#
# # tiledb expression
# import_tiledb = function() {
#   tiledb::limitTileDBCores(1L)
#   arr <- tiledb_array(tiledb_dir,
#                       as.data.frame = TRUE,
#                       query_layout = "UNORDERED")
#   df <- arr[]
#   tiledb_array_close(arr)
#
#   return(df)
# }
#
# # tiledb expression
# import_tiledbsc = function() {
#   tiledb::limitTileDBCores(4L)
#   arr <- tiledb_array(tiledb_dir,
#                       as.data.frame = TRUE,
#                       query_layout = "UNORDERED")
#   df <- arr[]
#   tiledb_array_close(arr)
#
#   return(df)
# }
# # tiledb SOMA expression
# # import_tiledb_soma = function() {
# #     do.call(rbind, SOMADataFrameOpen(tiledb_dir)$read(iterated=TRUE))
# # }
#
#
# # becnhmark
# microbenchmark(import_csv(), import_parquet(), import_tiledb(), times = 10L, unit = "s")
#
#
#
# # PARAMETER SENSITIVITY ---------------------------------------------------
# # parameter space
# tile_ordering = c("ROW_MAJOR", "COL_MAJOR")
# cell_ordering = c("ROW_MAJOR", "COL_MAJOR")
# capacities = c(10000, 20000, 50000, 100000, 500000, 1000000)
# expand.grid(tile_ordering, cell_ordering, capacities)
