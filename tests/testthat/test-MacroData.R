library(testthat)

# test_that("get_fred_metadata retries transient FRED server errors", {
#   obj <- MacroData$new(tempdir(), delay = 0)
  
#   series_attempts <- 0L
#   fake_server_error <- structure(
#     list(
#       message = "500 Internal Server Error",
#       response = list(status_code = 500L)
#     ),
#     class = c("error", "condition")
#   )
  
#   res <- with_mocked_bindings(
#     {
#       with_mocked_bindings(
#         {
#           obj$get_fred_metadata("ignored")
#         },
#         read_html = function(url) structure(list(url = url), class = "fake_html"),
#         html_elements = function(doc, css) c("/categories/10", "/categories/20"),
#         html_attr = function(nodes, attr) nodes,
#         fredr_category_children = function(id) {
#           data.table::data.table(id = id * 100L + 1:2)
#         },
#         fredr_category_series = function(category_id, limit, order_by, offset) {
#           series_attempts <<- series_attempts + 1L
#           if (series_attempts == 1L) {
#             stop(fake_server_error)
#           }
#           data.table::data.table(
#             id = paste0("SER", category_id, "_", offset + 1:2),
#             title = c("A", "B"),
#             observation_start = c("2020-01-01", "2020-01-02"),
#             observation_end = c("2024-01-01", "2024-01-02")
#           )
#         },
#         .package = "findata"
#       )
#     },
#     Sys.sleep = function(time) NULL,
#     .package = "base"
#   )
  
#   expect_s3_class(res, "data.table")
#   expect_equal(nrow(res), 8L)
#   expect_true("id_category" %in% names(res))
#   expect_s3_class(res$observation_start, "Date")
#   expect_gte(series_attempts, 2L)
# })

