# TCC forecast: get data
# Wagner Scheid Mallmann
# 2025/02

# _____________________________________________________________________________
# _______________________________ HELPER FUNCTIONS ____________________________
# _____________________________________________________________________________

source("01_functions.R")
fredr_set_key(pull(read_csv("fred_key.txt", col_names = F)))
set.seed(123)

# _____________________________________________________________________________
# ________________________________ GET DATA IMF________________________________
# _____________________________________________________________________________

# ---------------------- PARÂMETROS E DOWNLOAD ----------------------
countries_imf <- c("BRA", "USA", "RUS", "CHN", "ZAF", "IND", "CAN", "JPN", "G163")
flow_er   <- "IMF.STA,ER"   ; patt_er   <- "XDC_USD.EOP_RT.M"     # câmbio (M)
flow_cpi  <- "IMF.STA,CPI"  ; patt_cpi  <- "CPI._T.IX.M"          # inflação (M)
flow_gdp  <- "IMF.STA,QNEA" ; patt_gdp  <- "B1GQ.V.NSA.XDC.Q"     # PIB (Q)

er_long  <- fetch_imf_series(flow_er,  patt_er,  countries_imf, startPeriod = "1996-01-01")
cpi_long <- fetch_imf_series(flow_cpi, patt_cpi, countries_imf, startPeriod = "1996-01-01")
gdp_long <- fetch_imf_series(flow_gdp, patt_gdp, countries_imf, startPeriod = "1996-01-01")

# ---------------------- CRIA OS DATA FRAMES SEPARADOS ----------------------
code_map <- c(BRA = "br", USA = "us", RUS = "ru", CHN = "cn", ZAF = "za", IND = "in", G163 = "eu", JPN = "jp", CAN = "ca")
split_indicator_dfs(er_long,  "exchange",  code_map)   # cria br_exchange, us_exchange, ...
split_indicator_dfs(cpi_long, "inflation", code_map)   # cria br_inflation, us_inflation, ...
split_indicator_dfs(gdp_long, "gdp_q",       code_map)   # cria br_gdp_q, us_gdp_q, ...

# _____________________________________________________________________________
# ________________________________ GET DATA FRED_______________________________
# _____________________________________________________________________________
fredr_set_key(pull(read_csv("fred_key.txt", col_names = F)))

# interest rate - interbank <24h
br_interest <- fredr(series_id = "IRSTCI01BRM156N", observation_start = as.Date("1996-01-01")) %>% select(date, value) 
ru_interest <- fredr(series_id = "IRSTCI01RUM156N", observation_start = as.Date("1996-01-01")) %>% select(date, value)
in_interest <- fredr(series_id = "IRSTCI01INM156N", observation_start = as.Date("1996-01-01")) %>% select(date, value)
cn_interest <- fredr(series_id = "IRSTCI01CNM156N", observation_start = as.Date("1996-01-01")) %>% select(date, value)
za_interest <- fredr(series_id = "IRSTCI01ZAM156N", observation_start = as.Date("1996-01-01")) %>% select(date, value)
us_interest <- fredr(series_id = "IRSTCI01USM156N", observation_start = as.Date("1996-01-01")) %>% select(date, value)
jp_interest <- fredr(series_id = "IRSTCI01JPM156N", observation_start = as.Date("1996-01-01")) %>% select(date, value)
ca_interest <- fredr(series_id = "IRSTCI01CAM156N", observation_start = as.Date("1996-01-01")) %>% select(date, value)
colnames(br_interest) <- c("date", "br_interest")
colnames(ru_interest) <- c("date", "ru_interest")
colnames(in_interest) <- c("date", "in_interest")
colnames(cn_interest) <- c("date", "cn_interest")
colnames(za_interest) <- c("date", "za_interest")
colnames(us_interest) <- c("date", "us_interest")
colnames(jp_interest) <- c("date", "jp_interest")
colnames(ca_interest) <- c("date", "ca_interest")

# money supply M1
br_m1 <- fredr(series_id = "MANMM101BRM189N", observation_start = as.Date("1996-01-01")) %>% select(date, value)
ru_m1 <- fredr(series_id = "MANMM101RUM189N", observation_start = as.Date("1996-01-01")) %>% select(date, value)
in_m1 <- fredr(series_id = "MANMM101INM189N", observation_start = as.Date("1996-01-01")) %>% select(date, value)
cn_m1 <- fredr(series_id = "MANMM101CNM189N", observation_start = as.Date("1996-01-01")) %>% select(date, value)
za_m1 <- fredr(series_id = "MANMM101ZAM189N", observation_start = as.Date("1996-01-01")) %>% select(date, value)
us_m1 <- fredr(series_id = "MANMM101USM189N", observation_start = as.Date("1996-01-01")) %>% select(date, value)
jp_m1 <- fredr(series_id = "MANMM101JPM189N", observation_start = as.Date("1996-01-01")) %>% select(date, value)
ca_m1 <- fredr(series_id = "MANMM101CAM189N", observation_start = as.Date("1996-01-01")) %>% select(date, value)
colnames(br_m1) <- c("date", "br_m1")
colnames(ru_m1) <- c("date", "ru_m1")
colnames(in_m1) <- c("date", "in_m1")
colnames(cn_m1) <- c("date", "cn_m1")
colnames(za_m1) <- c("date", "za_m1")
colnames(us_m1) <- c("date", "us_m1")
colnames(jp_m1) <- c("date", "jp_m1")
colnames(ca_m1) <- c("date", "ca_m1")

# Industrial production: GDP proxy, monthly, NSA
br_ip <- fredr(series_id = "BRAPRMNTO01IXOBM", observation_start = as.Date("1996-01-01")) %>% select(date, value)
ru_ip <- fredr(series_id = "RUSPRMNTO01IXOBM", observation_start = as.Date("1996-01-01")) %>% select(date, value)
in_ip <- fredr(series_id = "INDPRMNTO01IXOBM", observation_start = as.Date("1996-01-01")) %>% select(date, value)
cn_ip <- fredr(series_id = "CHNPRINTO01IXPYM", observation_start = as.Date("1996-01-01")) %>% select(date, value)
za_ip <- fredr(series_id = "ZAFPRMNTO01IXOBM", observation_start = as.Date("1996-01-01")) %>% select(date, value)
us_ip <- fredr(series_id = "USAPRMNTO01IXOBM", observation_start = as.Date("1996-01-01")) %>% select(date, value)
jp_ip <- fredr(series_id = "JPNPRMNTO01IXOBM", observation_start = as.Date("1996-01-01")) %>% select(date, value)
ca_ip <- fredr(series_id = "CANPRMNTO01IXOBM", observation_start = as.Date("1996-01-01")) %>% select(date, value)
colnames(br_ip) <- c("date", "br_ip")
colnames(ru_ip) <- c("date", "ru_ip")
colnames(in_ip) <- c("date", "in_ip")
colnames(cn_ip) <- c("date", "cn_ip")
colnames(za_ip) <- c("date", "za_ip")
colnames(us_ip) <- c("date", "us_ip")
colnames(jp_ip) <- c("date", "ru_ip")
colnames(ca_ip) <- c("date", "in_ip")


# _____________________________________________________________________________
# ________________________________ GET DATA EBC _______________________________
# _____________________________________________________________________________


eu_m1 <- get_data(key = 'BSI.M.U2.N.V.M10.X.1.U2.2300.Z01.E', list(startPeriod = "1996-01-01")) %>%
  transmute(date  = as.Date(as.yearmon(obstime)), eu_m1 = as.numeric(obsvalue))
eu_interest <- get_data(key = 'MIR.M.U2.B.L21.A.R.A.2240.EUR.N', list(startPeriod = "1996-01-01")) %>% 
  transmute(date  = as.Date(as.yearmon(obstime)), eu_interest = as.numeric(obsvalue))
eu_ip <- get_data(key = 'STS.M.I9.Y.PROD.2C0000.4.000', list(startPeriod = "1996-01-01")) %>% 
  transmute(date  = as.Date(as.yearmon(obstime)), eu_ip = as.numeric(obsvalue))


# _____________________________________________________________________________
# ____________________________ LINEAR INTERPOLATION ___________________________
# _____________________________________________________________________________

lista_interpolacao <-
  list(
    br_exchange = br_exchange, br_ip = br_ip, br_inflation = br_inflation, br_interest = br_interest, br_m1 = br_m1,
    ru_exchange = ru_exchange, ru_ip = ru_ip, ru_inflation = ru_inflation, ru_interest = ru_interest, ru_m1 = ru_m1,
    in_exchange = in_exchange, in_ip = in_ip, in_inflation = in_inflation, in_interest = in_interest, in_m1 = in_m1,
    cn_exchange = cn_exchange, cn_ip = cn_ip, cn_inflation = cn_inflation, cn_interest = cn_interest, cn_m1 = cn_m1,
    za_exchange = za_exchange, za_ip = za_ip, za_inflation = za_inflation, za_interest = za_interest, za_m1 = za_m1,
    us_exchange = us_exchange, us_ip = us_ip, us_inflation = us_inflation, us_interest = us_interest, us_m1 = us_m1, 
    eu_exchange = eu_exchange, eu_ip = eu_ip, eu_inflation = eu_inflation, eu_interest = eu_interest, eu_m1 = eu_m1,
    jp_exchange = jp_exchange, jp_ip = jp_ip, jp_inflation = jp_inflation, jp_interest = jp_interest, jp_m1 = jp_m1,
    ca_exchange = ca_exchange, ca_ip = ca_ip, ca_inflation = ca_inflation, ca_interest = ca_interest, ca_m1 = ca_m1
  )

lst_interp <- interp_linear_many_any(
  lista_interpolacao,
  maxgap = Inf,
  extrapolate = FALSE             # sem extrapolar bordas
)

# pegue o resultado de volta
cn_ip_inter <- lst_interp$cn_ip
br_exchange <- lst_interp$br_exchange; br_ip <- lst_interp$br_ip; br_inflation <- lst_interp$br_inflation; br_interest <- lst_interp$br_interest; br_m1 <- lst_interp$br_m1
ru_exchange <- lst_interp$ru_exchange; ru_ip <- lst_interp$ru_ip; ru_inflation <- lst_interp$ru_inflation; ru_interest <- lst_interp$ru_interest; ru_m1 <- lst_interp$ru_m1
in_exchange <- lst_interp$in_exchange; in_ip <- lst_interp$in_ip; in_inflation <- lst_interp$in_inflation; in_interest <- lst_interp$in_interest; in_m1 <- lst_interp$in_m1
cn_exchange <- lst_interp$cn_exchange; cn_ip <- lst_interp$cn_ip; cn_inflation <- lst_interp$cn_inflation; cn_interest <- lst_interp$cn_interest; cn_m1 <- lst_interp$cn_m1
za_exchange <- lst_interp$za_exchange; za_ip <- lst_interp$za_ip; za_inflation <- lst_interp$za_inflation; za_interest <- lst_interp$za_interest; za_m1 <- lst_interp$za_m1
us_exchange <- lst_interp$us_exchange; us_ip <- lst_interp$us_ip; us_inflation <- lst_interp$us_inflation; us_interest <- lst_interp$us_interest; us_m1 <- lst_interp$us_m1 
eu_exchange <- lst_interp$eu_exchange; eu_ip <- lst_interp$eu_ip; eu_inflation <- lst_interp$eu_inflation; eu_interest <- lst_interp$eu_interest; eu_m1 <- lst_interp$eu_m1
jp_exchange <- lst_interp$jp_exchange; jp_ip <- lst_interp$jp_ip; jp_inflation <- lst_interp$jp_inflation; jp_interest <- lst_interp$jp_interest; jp_m1 <- lst_interp$jp_m1
ca_exchange <- lst_interp$ca_exchange; ca_ip <- lst_interp$ca_ip; ca_inflation <- lst_interp$ca_inflation; ca_interest <- lst_interp$ca_interest; ca_m1 <- lst_interp$ca_m1

# _____________________________________________________________________________
# ___________________________  QUARTERLY TO MONTHLY ___________________________
# _____________________________________________________________________________

ccodes <- c("br","us","ru","cn","za","in","eu","jp","ca")
disagg_all <- lapply(ccodes, function(cc) {
  gdp_q <- get(paste0(cc, "_gdp_q"), inherits = TRUE)
  ip_m  <- get(paste0(cc, "_ip"),    inherits = TRUE)
  if (is.null(gdp_q) || is.null(ip_m)) return(NULL)
  
  out <- disagg_gdp_monthly(gdp_q, ip_m)
  assign(paste0(cc, "_gdp"), out, envir = .GlobalEnv)  
  out
})


# _____________________________________________________________________________
# ______________________  OUTPUT GAP VIA HAMILTON FILTER ______________________
# _____________________________________________________________________________


br_gap <- output_gap_m(data = br_gdp, colname = "br_gap")
ru_gap <- output_gap_m(data = ru_gdp, colname = "ru_gap")
in_gap <- output_gap_m(data = in_gdp, colname = "in_gap")
cn_gap <- output_gap_m(data = cn_gdp, colname = "cn_gap")
za_gap <- output_gap_m(data = za_gdp, colname = "za_gap")
us_gap <- output_gap_m(data = us_gdp, colname = "us_gap")
eu_gap <- output_gap_m(data = eu_gdp, colname = "eu_gap")
jp_gap <- output_gap_m(data = jp_gdp, colname = "jp_gap")
ca_gap <- output_gap_m(data = ca_gdp, colname = "ca_gap")


# _____________________________________________________________________________
# ____________________________  RENAME IMF COLUMNS ____________________________
# _____________________________________________________________________________


#Exchange
colnames(br_exchange) <- c("date", "br_exchange")
colnames(ru_exchange) <- c("date", "ru_exchange")
colnames(in_exchange) <- c("date", "in_exchange")
colnames(cn_exchange) <- c("date", "cn_exchange")
colnames(za_exchange) <- c("date", "za_exchange")
colnames(us_exchange) <- c("date", "us_exchange")
colnames(eu_exchange) <- c("date", "eu_exchange")
colnames(jp_exchange) <- c("date", "jp_exchange")
colnames(ca_exchange) <- c("date", "ca_exchange")

#Inflation
colnames(br_inflation) <- c("date", "br_inflation")
colnames(ru_inflation) <- c("date", "ru_inflation")
colnames(in_inflation) <- c("date", "in_inflation")
colnames(cn_inflation) <- c("date", "cn_inflation")
colnames(za_inflation) <- c("date", "za_inflation")
colnames(us_inflation) <- c("date", "us_inflation")
colnames(eu_inflation) <- c("date", "eu_inflation")
colnames(jp_inflation) <- c("date", "jp_inflation")
colnames(ca_inflation) <- c("date", "ca_inflation")

#GDP
colnames(br_gdp) <- c("date", "br_gdp")
colnames(ru_gdp) <- c("date", "ru_gdp")
colnames(in_gdp) <- c("date", "in_gdp")
colnames(cn_gdp) <- c("date", "cn_gdp")
colnames(za_gdp) <- c("date", "za_gdp")
colnames(us_gdp) <- c("date", "us_gdp")
colnames(eu_gdp) <- c("date", "eu_gdp")
colnames(jp_gdp) <- c("date", "jp_gdp")
colnames(ca_gdp) <- c("date", "ca_gdp")


# _____________________________________________________________________________
# ____________________________  LOG DAS VARIAVEIS _____________________________
# _____________________________________________________________________________

br_exchange <- mutate(br_exchange, br_et = log(br_exchange))
ru_exchange <- mutate(ru_exchange, ru_et = log(ru_exchange))
in_exchange <- mutate(in_exchange, in_et = log(in_exchange))
cn_exchange <- mutate(cn_exchange, cn_et = log(cn_exchange))
za_exchange <- mutate(za_exchange, za_et = log(za_exchange))
us_exchange <- mutate(us_exchange, us_et = log(us_exchange))
eu_exchange <- mutate(eu_exchange, eu_et = log(eu_exchange))
jp_exchange <- mutate(jp_exchange, jp_et = log(jp_exchange))
ca_exchange <- mutate(ca_exchange, ca_et = log(ca_exchange))

br_m1 <- mutate(br_m1, br_m1_log = log(br_m1))
ru_m1 <- mutate(ru_m1, ru_m1_log = log(ru_m1))
in_m1 <- mutate(in_m1, in_m1_log = log(in_m1))
cn_m1 <- mutate(cn_m1, cn_m1_log = log(cn_m1))
za_m1 <- mutate(za_m1, za_m1_log = log(za_m1))
us_m1 <- mutate(us_m1, us_m1_log = log(us_m1))
eu_m1 <- mutate(eu_m1, eu_m1_log = log(eu_m1))
jp_m1 <- mutate(jp_m1, jp_m1_log = log(jp_m1))
ca_m1 <- mutate(ca_m1, ca_m1_log = log(ca_m1))

br_gdp <- mutate(br_gdp, br_gdp_log = log(br_gdp))
ru_gdp <- mutate(ru_gdp, ru_gdp_log = log(ru_gdp))
in_gdp <- mutate(in_gdp, in_gdp_log = log(in_gdp))
cn_gdp <- mutate(cn_gdp, cn_gdp_log = log(cn_gdp))
za_gdp <- mutate(za_gdp, za_gdp_log = log(za_gdp))
us_gdp <- mutate(us_gdp, us_gdp_log = log(us_gdp))
eu_gdp <- mutate(eu_gdp, eu_gdp_log = log(eu_gdp))
jp_gdp <- mutate(jp_gdp, jp_gdp_log = log(jp_gdp))
ca_gdp <- mutate(ca_gdp, ca_gdp_log = log(ca_gdp))

br_inflation <- mutate(br_inflation, br_inflation_log = log(br_inflation))
ru_inflation <- mutate(ru_inflation, ru_inflation_log = log(ru_inflation))
in_inflation <- mutate(in_inflation, in_inflation_log = log(in_inflation))
cn_inflation <- mutate(cn_inflation, cn_inflation_log = log(cn_inflation))
za_inflation <- mutate(za_inflation, za_inflation_log = log(za_inflation))
us_inflation <- mutate(us_inflation, us_inflation_log = log(us_inflation))
eu_inflation <- mutate(eu_inflation, eu_inflation_log = log(eu_inflation))
jp_inflation <- mutate(jp_inflation, jp_inflation_log = log(jp_inflation))
ca_inflation <- mutate(ca_inflation, ca_inflation_log = log(ca_inflation))


# _____________________________________________________________________________
# ______________________________  EXCHANGE RATE _______________________________
# _____________________________________________________________________________

br_exchange <- br_exchange %>%
  mutate(
    br_et_dif_h1  = dplyr::lead(br_et, 1)  - br_et,
    br_et_dif_h12 = dplyr::lead(br_et, 12) - br_et,
    br_et_ecm_h1  = dplyr::lead(br_et, 1)  - br_et,
    br_et_ecm_h12 = dplyr::lead(br_et, 12) - br_et)

ru_exchange <- ru_exchange %>%
  mutate(
    ru_et_dif_h1  = dplyr::lead(ru_et, 1)  - ru_et,
    ru_et_dif_h12 = dplyr::lead(ru_et, 12) - ru_et,
    ru_et_ecm_h1  = dplyr::lead(ru_et, 1)  - ru_et,
    ru_et_ecm_h12 = dplyr::lead(ru_et, 12) - ru_et)

in_exchange <- in_exchange %>%
  mutate(
    in_et_dif_h1  = dplyr::lead(in_et, 1)  - in_et,
    in_et_dif_h12 = dplyr::lead(in_et, 12) - in_et,
    in_et_ecm_h1  = dplyr::lead(in_et, 1)  - in_et,
    in_et_ecm_h12 = dplyr::lead(in_et, 12) - in_et)

cn_exchange <- cn_exchange %>%
  mutate(
    cn_et_dif_h1  = dplyr::lead(cn_et, 1)  - cn_et,
    cn_et_dif_h12 = dplyr::lead(cn_et, 12) - cn_et,
    cn_et_ecm_h1  = dplyr::lead(cn_et, 1)  - cn_et,
    cn_et_ecm_h12 = dplyr::lead(cn_et, 12) - cn_et)

za_exchange <- za_exchange %>%
  mutate(
    za_et_dif_h1  = dplyr::lead(za_et, 1)  - za_et,
    za_et_dif_h12 = dplyr::lead(za_et, 12) - za_et,
    za_et_ecm_h1  = dplyr::lead(za_et, 1)  - za_et,
    za_et_ecm_h12 = dplyr::lead(za_et, 12) - za_et)

us_exchange <- us_exchange %>%
  mutate(
    us_et_dif_h1  = dplyr::lead(us_et, 1)  - us_et,
    us_et_dif_h12 = dplyr::lead(us_et, 12) - us_et,
    us_et_ecm_h1  = dplyr::lead(us_et, 1)  - us_et,
    us_et_ecm_h12 = dplyr::lead(us_et, 12) - us_et)

eu_exchange <- eu_exchange %>%
  mutate(
    eu_et_dif_h1  = dplyr::lead(eu_et, 1)  - eu_et,
    eu_et_dif_h12 = dplyr::lead(eu_et, 12) - eu_et,
    eu_et_ecm_h1  = dplyr::lead(eu_et, 1)  - eu_et,
    eu_et_ecm_h12 = dplyr::lead(eu_et, 12) - eu_et)

jp_exchange <- jp_exchange %>%
  mutate(
    jp_et_dif_h1  = dplyr::lead(jp_et, 1)  - jp_et,
    jp_et_dif_h12 = dplyr::lead(jp_et, 12) - jp_et,
    jp_et_ecm_h1  = dplyr::lead(jp_et, 1)  - jp_et,
    jp_et_ecm_h12 = dplyr::lead(jp_et, 12) - jp_et)

ca_exchange <- ca_exchange %>%
  mutate(
    ca_et_dif_h1  = dplyr::lead(ca_et, 1)  - ca_et,
    ca_et_dif_h12 = dplyr::lead(ca_et, 12) - ca_et,
    ca_et_ecm_h1  = dplyr::lead(ca_et, 1)  - ca_et,
    ca_et_ecm_h12 = dplyr::lead(ca_et, 12) - ca_et)
  
# _____________________________________________________________________________
# ______________________________ BUILD PANEL __________________________________
# _____________________________________________________________________________

# Brazilian data
br_panel_data <- reduce(
  list(
    br_exchange, br_gap, br_gdp, br_inflation, br_interest, br_m1,
    us_exchange, us_gap, us_gdp, us_inflation, us_interest, us_m1
  ), full_join, by = "date")

br_panel_data <- br_panel_data %>%
arrange(date) %>%
mutate(
  
  # ECM: níveis contemporâneos (t)
  br_m_hat            =  (br_m1_log - us_m1_log),
  br_y_hat            =  (br_gdp_log - us_gdp_log),
  br_r_hat            =  (br_inflation_log - us_inflation_log),
  br_i_hat            =  (br_interest - us_interest),
  br_ygap_hat         =  (br_gap - us_gap),

  # DIF: deltas em t-k: k = 1
  br_m_hat_dif_h1     =  (br_m1_log - us_m1_log) - dplyr::lag(br_m1_log - us_m1_log, 1),
  br_y_hat_dif_h1     =  (br_gdp_log - us_gdp_log) - dplyr::lag(br_gdp_log - us_gdp_log, 1),
  br_r_hat_dif_h1     =  (br_inflation_log - us_inflation_log) - dplyr::lag(br_inflation_log - us_inflation_log, 1),
  br_i_hat_dif_h1     =  (br_interest   - us_interest)   - dplyr::lag(br_interest   - us_interest,   1),
  br_ygap_hat_dif_h1  =  (br_gap - us_gap) - dplyr::lag(br_gap - us_gap, 1),
  
  # DIF: deltas em t-k: k = 12
  br_m_hat_dif_h12    =  (br_m1_log - us_m1_log) - dplyr::lag(br_m1_log - us_m1_log, 12),
  br_y_hat_dif_h12    =  (br_gdp_log - us_gdp_log) - dplyr::lag(br_gdp_log - us_gdp_log, 12),
  br_r_hat_dif_h12    =  (br_inflation_log - us_inflation_log) - dplyr::lag(br_inflation_log - us_inflation_log, 12),
  br_i_hat_dif_h12    =  (br_interest   - us_interest)   - dplyr::lag(br_interest   - us_interest,   12),
  br_ygap_hat_dif_h12 =  (br_gap - us_gap) - dplyr::lag(br_gap - us_gap, 12)
)

br_panel_data <- br_panel_data[rowSums(is.na(br_panel_data)) == 0,]
rownames(br_panel_data) <- seq(length = nrow(br_panel_data))

# Russian data
ru_panel_data <- reduce(
  list(
    ru_exchange, ru_gap, ru_gdp, ru_inflation, ru_interest, ru_m1,
    us_exchange, us_gap, us_gdp, us_inflation, us_interest, us_m1
  ), full_join, by = "date")

ru_panel_data <- ru_panel_data %>%
  arrange(date) %>%
  mutate(

  # ECM: níveis contemporâneos (t)
  ru_m_hat            =  (ru_m1_log - us_m1_log),
  ru_y_hat            =  (ru_gdp_log - us_gdp_log),
  ru_r_hat            =  (ru_inflation_log - us_inflation_log),
  ru_i_hat            =  (ru_interest - us_interest),
  ru_ygap_hat         =  (ru_gap - us_gap),
  
  # DIF: deltas em t-k: k = 1
  ru_m_hat_dif_h1     =  (ru_m1_log - us_m1_log) - dplyr::lag(ru_m1_log - us_m1_log, 1),
  ru_y_hat_dif_h1     =  (ru_gdp_log - us_gdp_log) - dplyr::lag(ru_gdp_log - us_gdp_log, 1),
  ru_r_hat_dif_h1     =  (ru_inflation_log - us_inflation_log) - dplyr::lag(ru_inflation_log - us_inflation_log, 1),
  ru_i_hat_dif_h1     =  (ru_interest   - us_interest)   - dplyr::lag(ru_interest   - us_interest,   1),
  ru_ygap_hat_dif_h1  =  (ru_gap - us_gap) - dplyr::lag(ru_gap - us_gap, 1),
  
  # DIF: deltas em t-k: k = 12
  ru_m_hat_dif_h12    =  (ru_m1_log - us_m1_log) - dplyr::lag(ru_m1_log - us_m1_log, 12),
  ru_y_hat_dif_h12    =  (ru_gdp_log - us_gdp_log) - dplyr::lag(ru_gdp_log - us_gdp_log, 12),
  ru_r_hat_dif_h12    =  (ru_inflation_log - us_inflation_log) - dplyr::lag(ru_inflation_log - us_inflation_log, 12),
  ru_i_hat_dif_h12    =  (ru_interest   - us_interest)   - dplyr::lag(ru_interest   - us_interest,   12),
  ru_ygap_hat_dif_h12 =  (ru_gap - us_gap) - dplyr::lag(ru_gap - us_gap, 12)
)

ru_panel_data <- ru_panel_data[rowSums(is.na(ru_panel_data)) == 0, ]
rownames(ru_panel_data) <- seq(length = nrow(ru_panel_data))

# Indian data
in_panel_data <- reduce(
  list(
    in_exchange, in_gap, in_gdp, in_inflation, in_interest, in_m1,
    us_exchange, us_gap, us_gdp, us_inflation, us_interest, us_m1
  ), full_join, by = "date")

in_panel_data <- in_panel_data %>%
  arrange(date) %>%
  mutate(
    
  # ECM: níveis contemporâneos (t)
  in_m_hat            =  (in_m1_log - us_m1_log),
  in_y_hat            =  (in_gdp_log - us_gdp_log),
  in_r_hat            =  (in_inflation_log - us_inflation_log),
  in_i_hat            =  (in_interest - us_interest),
  in_ygap_hat         =  (in_gap - us_gap),
  
  # DIF: deltas em t-k: k = 1
  in_m_hat_dif_h1     =  (in_m1_log - us_m1_log) - dplyr::lag(in_m1_log - us_m1_log, 1),
  in_y_hat_dif_h1     =  (in_gdp_log - us_gdp_log) - dplyr::lag(in_gdp_log - us_gdp_log, 1),
  in_r_hat_dif_h1     =  (in_inflation_log - us_inflation_log) - dplyr::lag(in_inflation_log - us_inflation_log, 1),
  in_i_hat_dif_h1     =  (in_interest   - us_interest)   - dplyr::lag(in_interest   - us_interest,   1),
  in_ygap_hat_dif_h1  =  (in_gap - us_gap) - dplyr::lag(in_gap - us_gap, 1),
  
  # DIF: deltas em t-k: k = 12
  in_m_hat_dif_h12    =  (in_m1_log - us_m1_log) - dplyr::lag(in_m1_log - us_m1_log, 12),
  in_y_hat_dif_h12    =  (in_gdp_log - us_gdp_log) - dplyr::lag(in_gdp_log - us_gdp_log, 12),
  in_r_hat_dif_h12    =  (in_inflation_log - us_inflation_log) - dplyr::lag(in_inflation_log - us_inflation_log, 12),
  in_i_hat_dif_h12    =  (in_interest   - us_interest)   - dplyr::lag(in_interest   - us_interest,   12),
  in_ygap_hat_dif_h12 =  (in_gap - us_gap) - dplyr::lag(in_gap - us_gap, 12)
)    

in_panel_data <- in_panel_data[rowSums(is.na(in_panel_data)) == 0, ]
rownames(in_panel_data) <- seq(length = nrow(in_panel_data))

# Chinese data
cn_panel_data <- reduce(
  list(
    cn_exchange, cn_gap, cn_gdp, cn_inflation, cn_interest, cn_m1,
    us_exchange, us_gap, us_gdp, us_inflation, us_interest, us_m1
  ), full_join, by = "date")

cn_panel_data <- cn_panel_data %>%
  arrange(date) %>%
  mutate(

  # ECM: níveis contemporâneos (t)
  cn_m_hat            =  (cn_m1_log - us_m1_log),
  cn_y_hat            =  (cn_gdp_log - us_gdp_log),
  cn_r_hat            =  (cn_inflation_log - us_inflation_log),
  cn_i_hat            =  (cn_interest - us_interest),
  cn_ygap_hat         =  (cn_gap - us_gap),
  
  # DIF: deltas em t-k: k = 1
  cn_m_hat_dif_h1     =  (cn_m1_log - us_m1_log) - dplyr::lag(cn_m1_log - us_m1_log, 1),
  cn_y_hat_dif_h1     =  (cn_gdp_log - us_gdp_log) - dplyr::lag(cn_gdp_log - us_gdp_log, 1),
  cn_r_hat_dif_h1     =  (cn_inflation_log - us_inflation_log) - dplyr::lag(cn_inflation_log - us_inflation_log, 1),
  cn_i_hat_dif_h1     =  (cn_interest   - us_interest)   - dplyr::lag(cn_interest   - us_interest,   1),
  cn_ygap_hat_dif_h1  =  (cn_gap - us_gap) - dplyr::lag(cn_gap - us_gap, 1),
  
  # DIF: deltas em t-k: k = 12
  cn_m_hat_dif_h12    =  (cn_m1_log - us_m1_log) - dplyr::lag(cn_m1_log - us_m1_log, 12),
  cn_y_hat_dif_h12    =  (cn_gdp_log - us_gdp_log) - dplyr::lag(cn_gdp_log - us_gdp_log, 12),
  cn_r_hat_dif_h12    =  (cn_inflation_log - us_inflation_log) - dplyr::lag(cn_inflation_log - us_inflation_log, 12),
  cn_i_hat_dif_h12    =  (cn_interest   - us_interest)   - dplyr::lag(cn_interest   - us_interest,   12),
  cn_ygap_hat_dif_h12 =  (cn_gap - us_gap) - dplyr::lag(cn_gap - us_gap, 12)
)

cn_panel_data <- cn_panel_data[rowSums(is.na(cn_panel_data)) == 0, ]
rownames(cn_panel_data) <- seq(length = nrow(cn_panel_data))

# South african data
za_panel_data <- reduce(
  list(
    za_exchange, za_gap, za_gdp, za_inflation, za_interest, za_m1,
    us_exchange, us_gap, us_gdp, us_inflation, us_interest, us_m1
  ), full_join, by = "date")

za_panel_data <- za_panel_data %>%
  arrange(date) %>%
  mutate(
  
  # ECM: níveis contemporâneos (t)
  za_m_hat            =  (za_m1_log - us_m1_log),
  za_y_hat            =  (za_gdp_log - us_gdp_log),
  za_r_hat            =  (za_inflation_log - us_inflation_log),
  za_i_hat            =  (za_interest - us_interest),
  za_ygap_hat         =  (za_gap - us_gap),
  
  # DIF: deltas em t-k: k = 1
  za_m_hat_dif_h1     =  (za_m1_log - us_m1_log) - dplyr::lag(za_m1_log - us_m1_log, 1),
  za_y_hat_dif_h1     =  (za_gdp_log - us_gdp_log) - dplyr::lag(za_gdp_log - us_gdp_log, 1),
  za_r_hat_dif_h1     =  (za_inflation_log - us_inflation_log) - dplyr::lag(za_inflation_log - us_inflation_log, 1),
  za_i_hat_dif_h1     =  (za_interest   - us_interest)   - dplyr::lag(za_interest   - us_interest,   1),
  za_ygap_hat_dif_h1  =  (za_gap - us_gap) - dplyr::lag(za_gap - us_gap, 1),
  
  # DIF: deltas em t-k: k = 12
  za_m_hat_dif_h12    =  (za_m1_log - us_m1_log) - dplyr::lag(za_m1_log - us_m1_log, 12),
  za_y_hat_dif_h12    =  (za_gdp_log - us_gdp_log) - dplyr::lag(za_gdp_log - us_gdp_log, 12),
  za_r_hat_dif_h12    =  (za_inflation_log - us_inflation_log) - dplyr::lag(za_inflation_log - us_inflation_log, 12),
  za_i_hat_dif_h12    =  (za_interest   - us_interest)   - dplyr::lag(za_interest   - us_interest,   12),
  za_ygap_hat_dif_h12 =  (za_gap - us_gap) - dplyr::lag(za_gap - us_gap, 12)
)

za_panel_data <- za_panel_data[rowSums(is.na(za_panel_data)) == 0, ]
rownames(za_panel_data) <- seq(length = nrow(za_panel_data))

#Europe data
eu_panel_data <- reduce(
  list(
    eu_exchange, eu_gap, eu_gdp, eu_inflation, eu_interest, eu_m1,
    us_exchange, us_gap, us_gdp, us_inflation, us_interest, us_m1
  ), full_join, by = "date")

eu_panel_data <- eu_panel_data %>%
  arrange(date) %>%
  mutate(
    
  # ECM: níveis contemporâneos (t)
  eu_m_hat            =  (eu_m1_log - us_m1_log),
  eu_y_hat            =  (eu_gdp_log - us_gdp_log),
  eu_r_hat            =  (eu_inflation_log - us_inflation_log),
  eu_i_hat            =  (eu_interest - us_interest),
  eu_ygap_hat         =  (eu_gap - us_gap),
  
  # DIF: deltas em t-k: k = 1
  eu_m_hat_dif_h1     =  (eu_m1_log - us_m1_log) - dplyr::lag(eu_m1_log - us_m1_log, 1),
  eu_y_hat_dif_h1     =  (eu_gdp_log - us_gdp_log) - dplyr::lag(eu_gdp_log - us_gdp_log, 1),
  eu_r_hat_dif_h1     =  (eu_inflation_log - us_inflation_log) - dplyr::lag(eu_inflation_log - us_inflation_log, 1),
  eu_i_hat_dif_h1     =  (eu_interest   - us_interest)   - dplyr::lag(eu_interest   - us_interest,   1),
  eu_ygap_hat_dif_h1  =  (eu_gap - us_gap) - dplyr::lag(eu_gap - us_gap, 1),
  
  # DIF: deltas em t-k: k = 12
  eu_m_hat_dif_h12    =  (eu_m1_log - us_m1_log) - dplyr::lag(eu_m1_log - us_m1_log, 12),
  eu_y_hat_dif_h12    =  (eu_gdp_log - us_gdp_log) - dplyr::lag(eu_gdp_log - us_gdp_log, 12),
  eu_r_hat_dif_h12    =  (eu_inflation_log - us_inflation_log) - dplyr::lag(eu_inflation_log - us_inflation_log, 12),
  eu_i_hat_dif_h12    =  (eu_interest   - us_interest)   - dplyr::lag(eu_interest   - us_interest,   12),
  eu_ygap_hat_dif_h12 =  (eu_gap - us_gap) - dplyr::lag(eu_gap - us_gap, 12)
)

eu_panel_data <- eu_panel_data[rowSums(is.na(eu_panel_data)) == 0, ]
rownames(eu_panel_data) <- seq(length = nrow(eu_panel_data))


# Japan data
jp_panel_data <- reduce(
  list(
    jp_exchange, jp_gap, jp_gdp, jp_inflation, jp_interest, jp_m1,
    us_exchange, us_gap, us_gdp, us_inflation, us_interest, us_m1
  ), full_join, by = "date")

jp_panel_data <- jp_panel_data %>%
  arrange(date) %>%
  mutate(
    
    # ECM: níveis contemporâneos (t)
    jp_m_hat            =  (jp_m1_log - us_m1_log),
    jp_y_hat            =  (jp_gdp_log - us_gdp_log),
    jp_r_hat            =  (jp_inflation_log - us_inflation_log),
    jp_i_hat            =  (jp_interest - us_interest),
    jp_ygap_hat         =  (jp_gap - us_gap),
    
    # DIF: deltas em t-k: k = 1
    jp_m_hat_dif_h1     =  (jp_m1_log - us_m1_log) - dplyr::lag(jp_m1_log - us_m1_log, 1),
    jp_y_hat_dif_h1     =  (jp_gdp_log - us_gdp_log) - dplyr::lag(jp_gdp_log - us_gdp_log, 1),
    jp_r_hat_dif_h1     =  (jp_inflation_log - us_inflation_log) - dplyr::lag(jp_inflation_log - us_inflation_log, 1),
    jp_i_hat_dif_h1     =  (jp_interest   - us_interest)   - dplyr::lag(jp_interest   - us_interest,   1),
    jp_ygap_hat_dif_h1  =  (jp_gap - us_gap) - dplyr::lag(jp_gap - us_gap, 1),
    
    # DIF: deltas em t-k: k = 12
    jp_m_hat_dif_h12    =  (jp_m1_log - us_m1_log) - dplyr::lag(jp_m1_log - us_m1_log, 12),
    jp_y_hat_dif_h12    =  (jp_gdp_log - us_gdp_log) - dplyr::lag(jp_gdp_log - us_gdp_log, 12),
    jp_r_hat_dif_h12    =  (jp_inflation_log - us_inflation_log) - dplyr::lag(jp_inflation_log - us_inflation_log, 12),
    jp_i_hat_dif_h12    =  (jp_interest   - us_interest)   - dplyr::lag(jp_interest   - us_interest,   12),
    jp_ygap_hat_dif_h12 =  (jp_gap - us_gap) - dplyr::lag(jp_gap - us_gap, 12)
  )

jp_panel_data <- jp_panel_data[rowSums(is.na(jp_panel_data)) == 0,]
rownames(jp_panel_data) <- seq(length = nrow(jp_panel_data))

# Canada data
ca_panel_data <- reduce(
  list(
    ca_exchange, ca_gap, ca_gdp, ca_inflation, ca_interest, ca_m1,
    us_exchange, us_gap, us_gdp, us_inflation, us_interest, us_m1
  ), full_join, by = "date")

ca_panel_data <- ca_panel_data %>%
  arrange(date) %>%
  mutate(
    
    # ECM: níveis contemporâneos (t)
    ca_m_hat            =  (ca_m1_log - us_m1_log),
    ca_y_hat            =  (ca_gdp_log - us_gdp_log),
    ca_r_hat            =  (ca_inflation_log - us_inflation_log),
    ca_i_hat            =  (ca_interest - us_interest),
    ca_ygap_hat         =  (ca_gap - us_gap),
    
    # DIF: deltas em t-k: k = 1
    ca_m_hat_dif_h1     =  (ca_m1_log - us_m1_log) - dplyr::lag(ca_m1_log - us_m1_log, 1),
    ca_y_hat_dif_h1     =  (ca_gdp_log - us_gdp_log) - dplyr::lag(ca_gdp_log - us_gdp_log, 1),
    ca_r_hat_dif_h1     =  (ca_inflation_log - us_inflation_log) - dplyr::lag(ca_inflation_log - us_inflation_log, 1),
    ca_i_hat_dif_h1     =  (ca_interest   - us_interest)   - dplyr::lag(ca_interest   - us_interest,   1),
    ca_ygap_hat_dif_h1  =  (ca_gap - us_gap) - dplyr::lag(ca_gap - us_gap, 1),
    
    # DIF: deltas em t-k: k = 12
    ca_m_hat_dif_h12    =  (ca_m1_log - us_m1_log) - dplyr::lag(ca_m1_log - us_m1_log, 12),
    ca_y_hat_dif_h12    =  (ca_gdp_log - us_gdp_log) - dplyr::lag(ca_gdp_log - us_gdp_log, 12),
    ca_r_hat_dif_h12    =  (ca_inflation_log - us_inflation_log) - dplyr::lag(ca_inflation_log - us_inflation_log, 12),
    ca_i_hat_dif_h12    =  (ca_interest   - us_interest)   - dplyr::lag(ca_interest   - us_interest,   12),
    ca_ygap_hat_dif_h12 =  (ca_gap - us_gap) - dplyr::lag(ca_gap - us_gap, 12)
  )

ca_panel_data <- ca_panel_data[rowSums(is.na(ca_panel_data)) == 0,]
rownames(ca_panel_data) <- seq(length = nrow(ca_panel_data))


# _____________________________________________________________________________
# _____________________________ TERMO COINTEGRAÇÃO ____________________________
# _____________________________________________________________________________

br_panel_data <- add_coint_term(br_panel_data, "br")
ru_panel_data <- add_coint_term(ru_panel_data, "ru")
in_panel_data <- add_coint_term(in_panel_data, "in")
cn_panel_data <- add_coint_term(cn_panel_data, "cn")
za_panel_data <- add_coint_term(za_panel_data, "za")
eu_panel_data <- add_coint_term(eu_panel_data, "eu")
jp_panel_data <- add_coint_term(jp_panel_data, "jp")
ca_panel_data <- add_coint_term(ca_panel_data, "ca")

# _____________________________________________________________________________
# ___________________________________ EXPORT __________________________________
# _____________________________________________________________________________

write_csv2(br_panel_data, "outputs/br_panel_data.csv")
write_csv2(ru_panel_data, "outputs/ru_panel_data.csv")
write_csv2(in_panel_data, "outputs/in_panel_data.csv")
write_csv2(cn_panel_data, "outputs/cn_panel_data.csv")
write_csv2(za_panel_data, "outputs/za_panel_data.csv")
write_csv2(eu_panel_data, "outputs/eu_panel_data.csv")
write_csv2(jp_panel_data, "outputs/jp_panel_data.csv")
write_csv2(ca_panel_data, "outputs/ca_panel_data.csv")
