# findata

`findata` skuplja i priprema podatke za trading i financial ML. Novi EODHD
sloj pretvara raw fundamentals JSON i daily adjusted price podatke u stabilne
panel tablice, racuna market cap, mjesecne fundamentalne indikatore i
long-short faktorske prinose bez look-ahead biasa.

## EODHD fundamentals pipeline

Raw input pretpostavke:

- fundamentals folder s EODHD JSON fileovima po simbolu, npr. `AAPL.US.json`
- prices folder s daily CSV/parquet fileovima koji imaju barem `date` i adjusted
  close kolonu, npr. `adjusted_close`, `adj_close` ili `adjClose`

Minimalni workflow:

```r
library(findata)

eod <- EodhdFundamentals$new(
  fundamentals_dir = "F:/fundamentals/fundamentals",
  prices_dir = "F:/eodhd-prices",
  output_dir = "data/eodhd"
)

eod$build_all(filter_common_us = TRUE)
```

To zapisuje:

```text
data/eodhd/security_master.parquet
data/eodhd/quarterly_fundamentals.parquet
data/eodhd/daily_market_cap.parquet
data/eodhd/monthly_returns.parquet
data/eodhd/monthly_characteristics.parquet
data/eodhd/market_cap_outliers.csv
```

Isti pipeline mozes pokrenuti korak po korak:

```r
sm <- eod$build_security_master(filter_common_us = TRUE)
qf <- eod$build_quarterly_fundamentals(security_master = sm)
dmc <- eod$build_daily_market_cap(quarterly_fundamentals = qf, security_master = sm)
mr <- eod$build_monthly_returns()

chars <- eod$build_monthly_characteristics(
  quarterly_fundamentals = qf,
  daily_market_cap = dmc,
  monthly_returns = mr,
  security_master = sm
)
```

`monthly_characteristics.parquet` je glavna tablica za faktore. Ima jedan red po
`symbol x month`, mjesecni return, `signal_date`, `fund_available_date`,
`market_cap`, sektor/industriju i fundamentalne indikatore.

Look-ahead pravilo:

```r
validate_no_lookahead(chars)
```

Za mjesecni return u mjesecu `t`, fundamentals podaci smiju biti dostupni samo
do kraja mjeseca `t - 1`.

## Faktor prinosi

Procitaj prethodno izgradjene indikatore:

```r
chars <- read_monthly_characteristics("data/eodhd")
```

Izracunaj cijeli factor zoo:

```r
factors <- build_factor_zoo(
  characteristics = chars,
  specs = factor_zoo_specs(),
  start_date = "2006-01-01",
  weighting = "value",
  min_stocks_side = 10
)

summarize_factor_returns(factors)
factor_correlation_matrix(factors, start_date = "2006-01-01")
```

Jedan long-short faktor:

```r
gp <- build_long_short_factor(
  characteristics = chars,
  characteristic = "gross_profitability",
  direction = "high_minus_low",
  breakpoint_universe = "NYSE",
  breakpoints = c(0.3, 0.7),
  weighting = "value",
  min_stocks_side = 10,
  start_date = "2006-01-01"
)
```

Sector-neutral faktor:

```r
cash <- build_sector_neutral_factor(
  characteristics = chars,
  characteristic = "cash_generation_score",
  direction = "high_minus_low",
  weighting = "value",
  min_stocks_side = 10,
  start_date = "2006-01-01"
)
```

Detaljniji primjer je u vignetteu `vignettes/eodhd-fundamentals.Rmd`.

