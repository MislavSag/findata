# library(httr)
# library(data.table)
# 
# url = "https://financialmodelingprep.com/api/v4/historical-price-tick/FB/2020-01-02?limit=500&apikey=15cd5d0adf4bc6805a724b4417bbaafc&ts=1577975139926900730"
# GET(url)
# 
# url = "https://financialmodelingprep.com/api/v4/historical-price-tick/SPY/2023-08-11?limit=500&apikey=15cd5d0adf4bc6805a724b4417bbaafc"
# p = GET(url)
# res = content(p)
# rbindlist(res$results)
# 
# 
# # rusquant
# library(rusquant)
# getSymbols('LKOH',src='Finam',period='day')
# 
# LKOH
# 
# getSymbols('AAPL', from = '2023-08-01', to = '2023-08-02', src='Finam',period='tick')
# AAPL
