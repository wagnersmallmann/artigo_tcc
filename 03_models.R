# TCC forecast: train models
# Wagner Scheid Mallmann
# 2025/02

t0 <- Sys.time()
# _________________________________________________________________________________
# _____________________________ HELPER FUNCTIONS __________________________________
# _________________________________________________________________________________

source("01_functions.R")

# Set seeds for reproducibility
set.seed(123)

# set number o cores for parallel processing
registerDoParallel(cores = 8)

# set the number of alternativa to metaparameters
n_tuneLength <- 15

# _________________________________________________________________________________
# ________________________________ COUNTRY LOOP ___________________________________
# _________________________________________________________________________________

country_list <- c("br", "ru", "in", "cn", "za", "eu", "jp", "ca")
#country_list <- c("br")

output_models <- list()

for (cty in country_list) {
print(cty)

# get data
data <- read_csv2(paste0("outputs/", cty, "_panel_data.csv"), col_types = cols(
date = col_date(format = ""), .default = col_double())) %>% arrange(date)

# _________________________________________________________________________________
# ________________________________ WINDOW LOOP ____________________________________
# _________________________________________________________________________________

wdw_list <- c("rolling", "expanding")
for (wdw in wdw_list) {
print(paste0(cty, "_", wdw))

# set estimation window
windown <- (wdw == "rolling")

# Adjustments for cross validation
cv_control <- trainControl(
method = "timeslice",
initialWindow = 120,
horizon = 1,
fixedWindow = windown,
allowParallel = TRUE,
savePredictions = TRUE,
#seeds = seeds,
returnResamp = "all"
)

# _________________________________________________________________________________
# ____________________ HORIZON LOOP AND MODELS (FORMULAS) _________________________
# _________________________________________________________________________________

model_type_list <- c("dif", "ecm")
horizon_list    <- c("h1", "h12")

for (mdl_type in model_type_list) {
  
  for (hzn in horizon_list) {
    
    message(cty, "_", wdw, "_", mdl_type, "_", hzn)
    
    is_ecm <- (mdl_type == "ecm")
    et_ecm <- paste0(cty, "_et")
    
    target <- function(cty, mdl, h) paste0(cty, "_et_", mdl, "_", h)
    
    if (is_ecm) {
      feat   <- function(cty, base, mdl, h) paste0(cty, "_", base)
    }
    else {
      feat   <- function(cty, base, mdl, h) paste0(cty, "_", base, "_", mdl, "_", h)  
    }
    
    # famílias base de preditores (sem sufixo)
    vars <- list(
      taylor                = c("ygap_hat", "r_hat", "i_hat"),
      monetary_standard     = c("m_hat", "y_hat"),
      monetary_sticky       = c("m_hat", "y_hat", "r_hat"),
      monetary_sticky_uip   = c("m_hat", "y_hat", "r_hat", "i_hat"),
      monetary_flexible_uip = c("m_hat", "y_hat", "i_hat")
    )
    
    # constrói preditores COM sufixo (dif/ecm + h1/h12)
    
    preds <- list(
      taylor                  = feat(cty, vars$taylor,                mdl_type, hzn),
      monetary_standard       = feat(cty, vars$monetary_standard,     mdl_type, hzn),
      monetary_sticky         = feat(cty, vars$monetary_sticky,       mdl_type, hzn),
      monetary_sticky_uip     = feat(cty, vars$monetary_sticky_uip,   mdl_type, hzn),
      monetary_flexible_uip   = feat(cty, vars$monetary_flexible_uip, mdl_type, hzn)
    )
    
    if (is_ecm) {
      preds <- lapply(preds, function(v, extra) c(v, extra), extra = et_ecm)
    }
    
    # fórmulas finais (response ~ preditores)
    formula <- list(
      form_taylor                 = reformulate(preds$taylor,                response = target(cty, mdl_type, hzn)),
      form_monetary_standard      = reformulate(preds$monetary_standard,     response = target(cty, mdl_type, hzn)),
      form_monetary_sticky        = reformulate(preds$monetary_sticky,       response = target(cty, mdl_type, hzn)),
      form_monetary_sticky_uip    = reformulate(preds$monetary_sticky_uip,   response = target(cty, mdl_type, hzn)),
      form_monetary_flexible_uip  = reformulate(preds$monetary_flexible_uip, response = target(cty, mdl_type, hzn))
    )

# _________________________________________________________________________________
# ___________________________ FORMULA LOOP AND METHODS ____________________________
# _________________________________________________________________________________

for (form in names(formula)) {
  message(cty, "_", wdw, "_", mdl_type, "_", hzn, "_", form)
  
  train_formula <- formula[[form]]
  
  # ECM: LM usa somente o termo de cointegração; demais modelos ficam como estão
  if (is_ecm) {
    coint_var <- paste0(cty, "_coint_", sub("^form_", "", form))
    # reescreve a fórmula do LM para TER APENAS o termo cointegrante
    train_formula_lm <- reformulate(coint_var, response = target(cty, mdl_type, hzn))
    cat("Fórmula ECM (LM, Colombo): ", deparse(train_formula_lm), "\n",
        "Fórmula ECM (não-lineares): ", deparse(train_formula), "\n")
  } else {
    # DIF: LM igual à base
    train_formula_lm <- train_formula
    cat("Fórmula DIF: ", deparse(train_formula), "\n")
  }
  
  # --- Linear model (ECM inclui termo de cointegração) -> stats::lm()
  print(paste0(cty, "_", wdw, "_", mdl_type, "_", hzn, "_", form, "_lm"))
  model_lm <- train(
    if (is_ecm) train_formula_lm else train_formula,
    data = data,
    method = "lm",
    trControl = cv_control,
    tuneLength = n_tuneLength,
    metric = "RMSE"
  )
  
  # --- LASSO regression (alpha = 1) -> glmnet::glmnet()
  print(paste0(cty, "_", wdw, "_", mdl_type, "_", hzn, "_", form, "_lasso"))
  model_lasso <- train(
    train_formula,
    data = data,
    method = "glmnet",
    preProcess = c("center", "scale"),
    tuneGrid = expand.grid(alpha = 1, lambda = 10^seq(-4, 1, length = 50)),
    trControl = cv_control,
    metric = "RMSE"
  )
  
  # --- Ridge regression (alpha = 0) -> glmnet::glmnet()
  print(paste0(cty, "_", wdw, "_", mdl_type, "_", hzn, "_", form, "_ridge"))
  model_ridge <- train(
    train_formula,
    data = data,
    method = "glmnet",
    preProcess = c("center", "scale"),
    tuneGrid = expand.grid(alpha = 0, lambda = 10^seq(-4, 1, length = 50)),
    trControl = cv_control,
    metric = "RMSE"
  )
  
  # --- Support Vector Machine (Radial) -> kernlab::ksvm()
  print(paste0(cty, "_", wdw, "_", mdl_type, "_", hzn, "_", form, "_svm_r"))
  model_svm_r <- train(
    train_formula,
    data = data,
    method = "svmRadial",
    preProcess = c("center", "scale"),
    trControl = cv_control,
    tuneLength = n_tuneLength,
    metric = "RMSE"
  )
  
  # --- Random Forest -> randomForest::randomForest()
  print(paste0(cty, "_", wdw, "_", mdl_type, "_", hzn, "_", form, "_forest"))
  model_rf <- train(
    train_formula,
    data = data,
    method = "rf",
    ntree = 1000,
    tuneLength = n_tuneLength,
    trControl = cv_control,
    metric = "RMSE"
  )
  
  # --- Generalized Additive Model (GAM) Regularized Splines -> mgcv::gam()
  print(paste0(cty, "_", wdw, "_", mdl_type, "_", hzn, "_", form, "_gam"))
  model_gam <- train(
    train_formula,
    data = data,
    method = "gamSpline",
    preProcess = c("center", "scale"),
    tuneLength = n_tuneLength,
    trControl = cv_control,
    metric = "RMSE"
  )
  
  # Salva todos os modelos no output list
  key <- paste0(cty, "_", wdw, "_", mdl_type, "_", hzn, "_", form)
  output_models[[key]] <- list(
    "lm" = model_lm,
    "lasso" = model_lasso,
    "ridge" = model_ridge,
    "svm_r" = model_svm_r,
    "forest" = model_rf,
    "gam" = model_gam
  )
}}}}}

# export trained models
saveRDS(output_models, "outputs/output_models.rds")
doParallel::stopImplicitCluster()

tf <- Sys.time()
tf - t0
