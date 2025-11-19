# TCC forecast: helper functions
# Wagner Scheid Mallmann
# 2025/02


# _____________________________________________________________________________
# __________________________________ PACKAGES __________________________________
# _____________________________________________________________________________

{
  pacotes_necessarios <- list(
    "lubridate", "tidyverse", "httr", "tidyr", "stringr", "zoo", "writexl",
    "jsonlite", "tsibble", "fable", "feasts", "sidrar", "forecast",
    "ipeadatar", "neverhpfilter", "caret", "doParallel", "ggplot2",
    "fredr", "tempdisagg", "rsdmx", "dplyr", "purrr", "ecb", "rlang", "urca", "elasticnet", "glmnet"
    )
  
  for (i in pacotes_necessarios) {
    eval(bquote(
      if (!require(.(i))) {
        install.packages(.(i))
        library(.(i))
      }
    ))
  }
  
  rm(pacotes_necessarios, i)
}

# _______________________________________________________________________________
# __________________________________ FUNCTIONS __________________________________
# _______________________________________________________________________________

#FUNÇÃO 001 : Hiato do produto via Hamilton Filter
output_gap_m <- function(data, colname = "br_gap") {
  hf <- neverhpfilter::yth_filter(xts::as.xts(data[,1:2]), h = 24, p = 12)
  gap <- data$value / hf$value.trend
  gap <- as.data.frame(gap) %>%
    rownames_to_column() %>%
    `colnames<-`(c("date", colname))
  gap$date <- lubridate::ymd(gap$date)
  return(gap)
}


# FUNÇÃO 002 : Erro Quadrático Médio (RMSE) do Random Walk com base no painel
rmse_rw <- function(cty, start_window = 121) {
  
  data <- read_csv2(
    paste0("outputs/", cty, "_panel_data.csv"),
    col_types = cols(date = col_date(format = ""), .default = col_double())
  ) %>%
    arrange(date)
  
  # Usa as diferenças já calculadas no painel
  diff_h1  <- data[[paste0(cty, "_et_dif_h1")]]
  diff_h12 <- data[[paste0(cty, "_et_dif_h12")]]
  
  # RW alinhado ao período fora da amostra (a partir de start_window)
  df_rw <- data %>%
    select(date) %>%
    mutate(
      error_rw_h1  = diff_h1,
      error_rw_h12 = diff_h12
    ) %>%
    slice(start_window:n()) %>%   # desloca início do RW para 121º mês
    drop_na()
  
  # Calcula RMSE
  rmse_rw_h1  <- sqrt(mean(df_rw$error_rw_h1^2,  na.rm = TRUE))
  rmse_rw_h12 <- sqrt(mean(df_rw$error_rw_h12^2, na.rm = TRUE))
  
  return(list(
    rmse_rw      = c(rmse_rw_h1, rmse_rw_h12),
    data         = df_rw$date,
    error_rw_h1  = df_rw$error_rw_h1,
    error_rw_h12 = df_rw$error_rw_h12
  ))
}



#FUNÇÃO 003 : Coleta de dados do FMI 
fetch_imf_series <- function(flowref, pattern_suffix, countries, startPeriod = "2000") {
  get_one <- function(cc) {
    key <- if (identical(cc, "G163") && identical(flowref, "IMF.STA,CPI")) {
      paste(cc, "HICP._T.IX.M", sep = ".")} else {paste(cc, pattern_suffix, sep = ".")}
    
    obj <- readSDMX(
      providerId = "IMF_DATA",
      resource   = "data",
      flowRef    = flowref,
      key        = key,
      start      = startPeriod
    )
    as.data.frame(obj)
  }
  get_safe <- purrr::possibly(get_one, otherwise = NULL)
  raw_list <- countries %>% set_names() %>% map(get_safe)
  
  raw_list %>%
    compact() %>%
    bind_rows() %>%
    transmute(
      country = COUNTRY,
      date    = TIME_PERIOD,                           # "YYYY-MM" ou "YYYY-Qn"
      value   = suppressWarnings(as.numeric(OBS_VALUE))
    ) %>%
    arrange(country, date)
}


#FUNÇÃO 004 : Split tables - dados FMI 
split_indicator_dfs <- function(df_long, indicator_suffix, code_map) {
  dfs <- imap(code_map, function(cc2, cc3) {
    df <- df_long %>%
      filter(country == cc3) %>%
      select(date, value) %>%
      arrange(date)
    
    if (nrow(df) == 0) return(NULL)
    
    # se TODOS os períodos estiverem no formato mensal "YYYY-Mmm", converte p/ Date
    is_all_monthly <- all(grepl("^\\d{4}-M\\d{2}$", df$date))
    
    if (is_all_monthly) {
      df <- df %>%
        mutate(date = as.Date(as.yearmon(sub("-M", "-", date), format = "%Y-%m")))
    } else {
      # mantém como string (ex.: trimestral "YYYY-Qn")
      df$date <- as.character(df$date)
    }
    
    df  # retorna com apenas 'date' (Date ou chr) e 'value' (num)
  }) %>% compact()
  
  # nomes dos objetos no ambiente: br_exchange, us_inflation, cn_gdp, etc.
  names(dfs) <- paste0(unname(code_map[names(dfs)]), "_", indicator_suffix)
  
  list2env(dfs, envir = .GlobalEnv)
  invisible(dfs)
}

#FUNÇÃO 005 : Desagrega PIB trimestral (gdp_q) em mensal usando Prod. Industrial mensal (ip_m)
# helper: "YYYY-Qn" -> Date (primeiro dia do trimestre)
as_date_quarter <- function(x) as.Date(zoo::as.yearqtr(x, format = "%Y-Q%q"))
# helper: time(ts) mensal -> Date
date_from_ts_time_month <- function(tt) as.Date(zoo::as.yearmon(tt))

disagg_gdp_monthly <- function(gdp_q, ip_m,
                               method = "chow-lin-maxlog",
                               conversion = "sum") {
  stopifnot("date" %in% names(gdp_q), "date" %in% names(ip_m))
  
  # ordenar
  gdp_q <- gdp_q %>% arrange(date)
  ip_m  <- ip_m  %>% arrange(date)
  
  # identificar coluna de valor (qualquer nome que não seja 'date')
  gdp_val_col <- setdiff(names(gdp_q), "date")[1]
  ip_val_col  <- setdiff(names(ip_m),  "date")[1]
  if (is.na(gdp_val_col) || is.na(ip_val_col)) {
    stop("Cada tabela precisa ter 'date' e uma coluna de valores.")
  }
  
  # garantir tipos/parse
  # gdp trimestral: date string "YYYY-Qn" -> Date do 1º dia do trimestre + numeric value
  gdp_q <- gdp_q %>%
    mutate(
      date_q = as_date_quarter(date),
      value  = as.double(.data[[gdp_val_col]])
    )
  
  # ip mensal: date em Date (se vier string, tenta converter) + numeric
  if (!inherits(ip_m$date, "Date")) ip_m <- ip_m %>% mutate(date = as.Date(date))
  ip_m <- ip_m %>%
    mutate(
      date_m = date,
      value  = as.double(.data[[ip_val_col]])
    )
  
  if (nrow(gdp_q) == 0) stop("PIB trimestral vazio após parsing.")
  if (nrow(ip_m)  == 0) stop("IP mensal vazio após parsing.")
  
  # ---------------------------
  # AUTO-DETECÇÃO DE STARTS
  # ---------------------------
  # gdp: extrai ano e trimestre do menor período
  gdp_first_str <- gdp_q$date[1]  # presume "YYYY-Qn"
  # fallback robusto caso 'date' já não esteja nesse formato:
  if (!grepl("^\\d{4}-Q[1-4]$", gdp_first_str)) {
    stop("A coluna 'date' do PIB precisa estar no formato 'YYYY-Qn'. Ex.: '2020-Q1'.")
  }
  gdp_year  <- as.integer(substr(gdp_first_str, 1, 4))
  gdp_quart <- as.integer(sub(".*Q", "", gdp_first_str))
  start_gdp <- c(gdp_year, gdp_quart)
  
  # ip: menor data (Date) -> ano e mês
  ip_first_date <- min(ip_m$date_m, na.rm = TRUE)
  start_ip <- c(as.integer(format(ip_first_date, "%Y")),
                as.integer(format(ip_first_date, "%m")))
  
  # ---------------------------
  # ts objects
  # ---------------------------
  gdp_ts <- ts(gdp_q$value, start = start_gdp, frequency = 4)
  ip_ts  <- ts(ip_m$value,  start = start_ip,  frequency = 12) %>%
    forecast::na.interp()
  
  # ---------------------------
  # temporal disaggregation
  # ---------------------------
  gdp_m_fit <- tempdisagg::td(gdp_ts ~ ip_ts, to = "month",
                              method = method, conversion = conversion) %>%
    predict()
  
  # montar saída mensal com datas corretas
  gdp_monthly <- tibble(
    date  = date_from_ts_time_month(time(gdp_m_fit)),
    value = as.numeric(gdp_m_fit)
  ) %>%
    # cortar para o intervalo observado do PIB trimestral
    filter(date >= min(gdp_q$date_q), date <= max(gdp_q$date_q))
  
  # nome da coluna de saída: igual ao nome do PIB trimestral, removendo sufixo "_q" se existir
  out_colname <- sub("_q$", "", gdp_val_col)
  names(gdp_monthly)[names(gdp_monthly) == "value"] <- out_colname
  
  gdp_monthly
}

#FUNÇÃO 006 : Interpolação Linear para dados faltantes - Um Dataframe
interp_linear_df_any <- function(df, value_cols = NULL, maxgap = Inf, extrapolate = FALSE) {
  stopifnot("date" %in% names(df))
  df <- df %>% arrange(date)
  
  # detectar colunas de valor, se não informadas
  if (is.null(value_cols)) value_cols <- setdiff(names(df), "date")
  if (length(value_cols) == 0) return(df)
  
  df %>%
    mutate(date = if (!inherits(date, "Date")) as.Date(date) else date) %>%
    mutate(across(
      all_of(value_cols),
      ~ zoo::na.approx(
        # >>> CORREÇÃO: o primeiro argumento é o vetor a interpolar (object)
        object = suppressWarnings(as.numeric(.x)),
        x       = as.numeric(date),
        na.rm   = extrapolate,   # FALSE = sem extrapolar bordas
        maxgap  = maxgap
      ),
      .names = "{.col}"
    ))
}

#FUNÇÃO 007 : Interpolação Linear para dados faltantes - Vários Dataframes
interp_linear_many_any <- function(lst, maxgap = Inf, extrapolate = FALSE) {
  imap(lst, ~ interp_linear_df_any(.x, value_cols = setdiff(names(.x), "date"),
                                   maxgap = maxgap, extrapolate = extrapolate))
}

# FUNÇÃO 008 : Vetor de Cointegração - ECM
add_coint_term <- function(panel, cty_prefix, n_init = 120) {
  
  model_fundamentals <- list(
    taylor                = c("ygap_hat", "r_hat", "i_hat"),
    monetary_standard     = c("m_hat", "y_hat"),
    monetary_sticky       = c("m_hat", "y_hat", "r_hat"),
    monetary_sticky_uip   = c("m_hat", "y_hat", "r_hat", "i_hat"),
    monetary_flexible_uip = c("m_hat", "y_hat", "i_hat")
  )
  
  for (mdl in names(model_fundamentals)) {
    
    message(paste0(">> Estimando cointegração para ", cty_prefix, " - modelo: ", mdl))
    
    fundamentals <- model_fundamentals[[mdl]]
    
    # Base completa (toda a série)
    df_full <- panel %>%
      select(date,
             !!sym(paste0(cty_prefix, "_et")),
             all_of(paste0(cty_prefix, "_", fundamentals))) %>%
      drop_na() %>%
      arrange(date)
    
    colnames(df_full) <- c("date", "y", fundamentals)
    
    # Base restrita (apenas primeiros 120 meses)
    df_init <- df_full %>% slice(1:n_init)
    
    # Estima Johansen
    jtest <- urca::ca.jo(df_init[, c("y", fundamentals)],
                         ecdet = "const", type = "trace", K = 2)
    
    # Extrai o vetor cointegrante principal
    beta_full <- jtest@V[, 1]
    
    # Remove a constante (última linha)
    beta_no_const <- beta_full[-length(beta_full)]
    
    # Separa o coeficiente de y e os dos fundamentos
    beta_y <- beta_no_const[1]
    beta_x <- beta_no_const[-1]
    beta_norm <- -beta_x / beta_y
    const <- beta_full[length(beta_full)] / beta_y
    
    # Termo cointegrante: y - (Xβ + c)
    x_mat <- as.matrix(df_full[, fundamentals])
    coint_term <- df_full$y - as.numeric(x_mat %*% beta_norm) - const
    
    # Padroniza escala (zero média, desvio padrão 1)
    coint_term <- as.numeric(scale(coint_term, center = TRUE, scale = TRUE))
    
    # Adiciona ao painel (substitui se já existir)
    new_colname <- paste0(cty_prefix, "_coint_", mdl)
    if (new_colname %in% names(panel)) panel[[new_colname]] <- NULL
    
    panel <- left_join(
      panel,
      data.frame(date = df_full$date, coint_term = coint_term),
      by = "date"
    )
    colnames(panel)[ncol(panel)] <- new_colname
  }
  
  return(panel)
}