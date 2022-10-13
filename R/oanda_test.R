# library(httr)
# library(data.table)
#
#
# token <- "cb12b995f63d6bb86f8a48e2612e9f95-24ab6ed65f4537e3a7a0db129c44d244"
# url <- "https://api-fxtrade.oanda.com"
# p <- GET(paste0(url, "/v3/accounts"),
#          add_headers("Authorization" = paste("Bearer", token)))
# accounts <- content(p)
# "Authorization: Bearer 12345678900987654321-abc34135acde13f13530"
#
# account_id <- accounts$accounts[[1]]$id
#
# instruments <- GET(paste0(url, "/v3/accounts/", account_id, "/instruments"),
#                    add_headers("Authorization" = paste("Bearer", token)))
# instruments <- content(instruments)
# instruments_dt <- lapply(instruments[[1]], `[`, 1:13)
# instruments_dt <- lapply(instruments_dt, as.data.table)
# instruments_dt <- rbindlist(instruments_dt, fill = TRUE)
# tags <- lapply(instruments[[1]], `[`, "tags")
# tags <- lapply(tags, unlist)
# tags <- lapply(tags, function(x) as.data.frame(as.list(x)))
# tags <- rbindlist(tags, fill = TRUE)
# instruments_dt <- cbind(instruments_dt, tags)
#
# # Indecies
# instruments_dt[tags.name == "INDEX"]
# str(instruments_dt)
#
# # Zorro file
# zorro <- instruments_dt[, .(Name = gsub("_.*", "", name),
#                             Price = 100,
#                             Spread = 0.5,
#                             RollLong = 0.0092,
#                             RollShort = -0.2495,
#                             PIP = 1,
#                             PIPCost = 0.0001,
#                             MarginCost = -1,
#                             Market = 0,
#                             LotAmount = 0,
#                             Commission = 0,
#                             symbol = name)]
# fwrite(zorro, "C:/Users/Mislav/Zorro/History/AssetsOandav2.csv")
