# library(QuantTools)
# library(httr)
#
# sm <- QuantTools:::finam_download_symbol_map()
# sm[grep("AAPL", name, ignore.case = TRUE)]
# sm[grep("AAPL", symbol, ignore.case = TRUE)]
#
# symbol = "AAPL"
# from = "2022-01-05"
# to = "2022-01-07"
# period = "1min"
# trial_limit = 10
# trial_sleep = 0.5
# verbose = TRUE
#
#
# finam.globals = new.env()
#
#
# if( is.null( finam.globals$symbol_map ) ) finam.globals$symbol_map = QuantTools:::finam_download_symbol_map()
#
# args = as.list( environment() )
#
# from = min( as.Date( from ), Sys.Date() )
# to   = min( as.Date( to   ), Sys.Date() )
#
# is_split_ticks = period == 'tick' & from != to
# is_split_1min  = period == '1min' & to - from > as.difftime( 31, units = 'days' )
# is_split = is_split_ticks | is_split_1min
#
# if( is.numeric( symbol ) ) {
#
#   em = finam.globals$symbol_map[ , em[ em == ..symbol ] ][1]
#
# } else {
#
#   em = finam.globals$symbol_map[ , em[ symbol == ..symbol ] ][1]
#
# }
#
# if( is.na( em ) ) stop( symbol, ' symbol not found', call. = F )
#
# date_range = lapply( c( from, to ), function( date ) as.numeric( format( date, c( '%Y', '%m', '%d' ) ) ) )
#
#
# query = list(
#
#   market = 1,
#
#   # fsp       = 0, # fill periods without deals 0 - no, 1 - yes
#   em        = 6, # emitent code
#
#   token = "https://export.finam.ru/export9.out?market=1&em=6&token=03AIIukzg0-h8DzbKN-nhotiHzfgXhP68YHvkdHVEB_vccmcwtJIZVYj16vWzGJn9fzVIMajS87EYmh07sMMOZOaKkjOaEXtUOIHG-4rILKOdLn8rC3yBX_rt-1S6fxbvb_dz-yA7YyOWGUrmWtBWiEc-orhZl8pKEFdJhL9EQ2mvLlfobe7OVxtLQz1uwPEh3C9tuE9I5_bgapgN-OZInITY0jBU4u1-YIlgSKKNaYCRwKOGc53mTsT6gAaC0zZQqmL7UFIBqt6pzxO2QI9vDr2jI3Il3gNzQc88sMNKF1a4BwRmxUHWwqvVWBRYD9K6_rB8N9yknudjeDwea_3jQbEaXMqgrs66vzAj0B1tjxlLzAW8QeJxvame6bp-S46CSW0uSdBNrgucKrBDZNtoEEMz0YsJvzc9TbrCexWAL5SyaV0JHuMUnwz8v_zz46OPykEdfN-F4MlWjMHqyPu0-KW7EejQPnCtDdU9ucsxfC31X7Wo171Z2BRsfas2qwtDLiV2IzK7LuTmz&code=MSNG&apply=0&df=5&mf=0&yf=2022&from=05.01.2022&dt=7&mt=0&yt=2022&to=07.01.2022&p=1&f=MSNG_220105_220107&e=.txt&cn=AAPL&dtf=1&tmf=1&MSOR=1&mstimever=0&sep=1&sep2=1&datf=10",
#   code = "AAPL",
#   apply = "0",
#
#   df        = date_range[[1]][3], # date from
#   mf        = date_range[[1]][2] - 1,
#   yf        = date_range[[1]][1],
#
#   from = "05.01.2022",
#
#   dt        = date_range[[2]][3], # date to
#   mt        = date_range[[2]][2] - 1,
#   yt        = date_range[[2]][1],
#
#   to = "07.01.2022",
#
#   p         = switch( period, "tick" = 1, "1min" = 2, "5min" = 3, "10min" = 4, "15min" = 5, "30min" = 6, "hour" = 7, "day" = 8 ), # period
#
#   f = "MSNG_220105_2201007",
#   e = ".txt",   # output file format
#   cn = "AAPL", # symbol
#
#   dtf       = 1, # date format
#   tmf       = 1, # time format
#   MSOR      = 1, # candle time 0 - candle start, 1 - candle end
#   mstimever = 0,
#   sep       = 1, # column separator    1 - ",", 2 - ".", 3 - ";", 4 - "<tab>", 5 - " "
#   sep2      = 1, # thousands separator 1 - "" , 2 - ".", 3 - ",", 4 - " "    , 5 - "'"
#   datf      = 10 # switch( period, "tick" = 10, 5 ), # candle format
#   # at        = 0 # header	0 - no, 1 - yes
#
# )
#
# # create get query url
# url = httr::modify_url( url  = 'https://export.finam.ru', path = '/export9.out', query = query )
# if( verbose ) message( '\n', url )
#
# p <- GET(url, times = trial_limit, pause_cap = trial_sleep, pause_min = trial_sleep, quiet = !verbose)
# p
# content(p)
# # response = httr::RETRY( "GET", url, httr::user_agent( 'https://bitbucket.org/quanttools/quanttools' ), httr::config( http_version = 0 ), times = trial_limit, pause_cap = trial_sleep, pause_min = trial_sleep, quiet = !verbose )
#
#
# https://export.finam.ru/export9.out?
#   market=1&em=6&token=03AIIukzgnDhRTrsNP5vkzZysI8e-98maU56uANc4bfwOBc7mfSFY35UosE5TcOvCY-kdUvyAx-MrQ1dK0vetOsR0F3hXK0gdsTwsDaubIpM3dn-CF94egWr1Nk9pdLvU1V8K3UkE9H36POxF2NY-sBEVZBfbUnWqdSriTTN9y-oFoA2cBaBFp_l_toDovjMGW5Hbz5_EbbDyD6FPyQl1vHt8nJ4vfOeUQASgBmtI31HwBEvmhyaDKgQSIdO87644kESK8ndNCfes5bLFX4fvY43IZ2alb21eUHSyc3r0wtwJTX9cav6AZHb4alD5ab7nIU_cxjwP014SZlWIcbBtKPV93B93daz-aBEgEMZGC98S5Qr_Ox3scS5e55c3g2YRMrE8uCZLor3U7-_3lmGdMwtgcZ-U4XESRjxgGHg8nsZ8bjDB3u4xa2pWnvnhyVRTSBMp2D91_BBKN0DR2ngPDsglyeqMvoAuMLmu_3qy1A8oLjfrHBeguVRKs_Z388AGiwgc-UlYRdCGR
# &code=MSNG&apply=0&df=5&mf=8&yf=2014&from=05.09.2014&dt=8&mt=8&yt=2014&to=08.09.2014&p=2&f=MSNG_140905_140908&e=.txt&cn=MSNG
# # &dtf=1&tmf=1&MSOR=1&mstimever=0&sep=1&sep2=1&datf=3&at=1
#
# finam_get_emitent_data = function() {
#
#   source_url = 'https://www.finam.ru/cache/N72Hgd54/icharts/icharts.js'
#
#   #Sys.setlocale( 'LC_CTYPE', 'ru_RU.UTF-8' )
#
#   aEmitentChild = aEmitentUrls = aEmitentDecp = NULL
#
#   # read finam universe file, set encoding to UTF-8 and split lines
#   raw_lines = unlist( strsplit( gsub( '"', '', iconv( rawToChar( httr::content( httr::GET( source_url, httr::config( http_version = 0 ) ) ) ), 'CP1251', 'UTF-8' ) ), '\n|\r' ) )
#   # filter out empty lines
#   raw_lines = raw_lines[ raw_lines != '' ]
#   # get data inside square and curly brakets for each line
#   data_inside_brakets = unlist( regmatches( raw_lines, gregexpr( "(?<=\\[|\\{).+?(?=\\]|\\})", raw_lines, perl = T ) ) )
#   # get data names
#   var_names = unlist( regmatches( raw_lines, gregexpr( "(?<=var\\s).+?(?=\\s)", raw_lines, perl = T ) ) )
#   # combine all data into list
#   data = c(
#     # all data except aEmitentNames has no comma (,) inside element
#     strsplit( data_inside_brakets[ var_names != 'aEmitentNames' ], ',', fixed = T ),
#     # but aEmitentNames has the comma (,) inside element
#     {
#       x = strsplit( data_inside_brakets[ var_names == 'aEmitentNames' ], '' )[[1]]
#       # so we split by "','" and remove first and last quote mark '
#       strsplit( paste( x[ 2:( length( x ) - 1 ) ], collapse = '' ), split = "','", fixed = T )
#     }
#   )
#   # set data names
#   names( data ) = c( var_names[ var_names != 'aEmitentNames' ], 'aEmitentNames' )
#   # check if data is Emitent specific
#   is_emitent_data = sapply( names( data ), grepl, pattern = 'Emitent' )
#   # combine Emitent specific data into data.table
#   emitent_data = setDT( data[ is_emitent_data ] )[ aEmitentChild != 0 ]
#   # clean aEmitentUrls
#   emitent_data[, aEmitentUrls := matrix( unlist( strsplit( aEmitentUrls, ': ', fixed = T ) ), ncol = 2, byrow = T )[, 2 ] ]
#   # split aEmitentUrls into market and symbol parts
#   emitent_data[ , c( 'aEmitentUrlsMarket', 'aEmitentUrlsSymbol' ) := as.data.table( matrix( unlist( strsplit( aEmitentUrls, '/' ) ), ncol = 2, byrow = T ) ) ]
#   # clean aEmitentDecp
#   emitent_data[ , aEmitentDecp := matrix( unlist( strsplit( aEmitentDecp, ':' ) ), ncol = 2, byrow = T )[, 2 ] ]
#
#   return( emitent_data[] )
#
# }
# finam_get_emitent_data()
#
#
# url <- "https://export.finam.ru/export9.out?market=1&em=6&token=03AIIukzi12ofVPhgai_P-YtjJLKGGCIeYCyqcZZ271bhWsj458lVJ__bkaSoHoMkax9hduU3sjNBJ5Dvt4_bT54KegtG9QQR8wRUsn4srSJDiYYvvP3Q5-KC26V28qkXWwyAG9hoo6nPybogpYcEVS_l2zUy5plyHxiy8g89LTZ6hdZwwuYpqdVPg86nnHfqRrENLgq2FOM648xX_n3cWc8Lzlb66gXbycfYfvcjFKISlY_KaUXDXGJOWBi3C1lgZ1ayVCanWZA_UTHdUZ5ad-Nmbo8ImXRqcMcXQGeQrHIzl95NpnciO4OgYV5A8cY9O882Y4-ojdneMMqZ76J9oYaUm3WkiP6-tOztEQyb8_0szeqKx0cU6OTVO7ZOwMSifpLuyczDvXWlgRsxwmudlEQxQxZ1Pfr33DhbGv-_tuIv0dYWmxNT3YJ1mofVxm2aPpv0q_yPUU-xPD6iu8iUk-wQTPC6_Tu0V-VwYz6DXm1rhrCwWBcyPaMrvQbbZ71vQbJILxEXZBUEz&code=MSNG&apply=0&df=5&mf=0&yf=2022&from=05.01.2022&dt=9&mt=0&yt=2022&to=09.01.2022&p=1&f=MSNG_220105_220109&e=.txt&cn=AAPL&dtf=1&tmf=1&MSOR=1&mstimever=0&sep=1&sep2=1&datf=6"
# p <- GET(url, user_agent("Console"))
# content(p)
# rawToChar(content(p))
