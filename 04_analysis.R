# TCC forecast: analysis (apenas BR, novo esquema dif/ecm)
# Wagner Scheid Mallmann
# 2025/02

# _________________________________________________________________________________
# _____________________________ HELPER FUNCTIONS __________________________________
# _________________________________________________________________________________

source("01_functions.R")

RMSE_manual <- function(pred, obs) sqrt(mean((pred - obs)^2))

# ___________________________________________________________________________________
# _________________________________ GET MODELS ____________________________________
# ___________________________________________________________________________________

models <- readRDS("outputs/output_models.rds")

# ___________________________________________________________________________________
# ___________________________________ RANDOM WALK __________________________________
# ___________________________________________________________________________________

# Random walk RMSE (ajuste se quiser outros países)

country_list <- c("br", "ru", "in", "cn", "za", "eu", "jp", "ca")
#country_list <- c("br")
rw_results <- list()

for (cty in country_list) {
  rw_results[[cty]] <- rmse_rw(cty)
}

# ___________________________________________________________________________________
# _____________________________________ GET RMSE ___________________________________
# ___________________________________________________________________________________

rmse_results <- list()
dm_results <- list()

model_list       <- c("taylor", "monetary_standard", "monetary_sticky", "monetary_sticky_uip", "monetary_flexible_uip")
window_list      <- c("rolling", "expanding")
model_type_list  <- c("dif", "ecm")
horizon_list     <- c("h1", "h12")

for (cty in country_list) {
  for (wdw in window_list) {
    for (mdl_type in model_type_list) {
      for (hzn in horizon_list) {
        for (mdl in model_list) {
          
          # Horizonte numérico
          base_h <- hzn
          h_num  <- ifelse(base_h == "h12", 12, 1)
          
          # Dados e erros do random walk (já alinhados com janela fora da amostra)
          rw_data <- rw_results[[cty]]$data
          e1_dm   <- rw_results[[cty]][[paste0("error_rw_", base_h)]]
          
          # Monta chave do modelo salvo
          key <- paste0(cty, "_", wdw, "_", mdl_type, "_", hzn, "_form_", mdl)
          model_group <- models[[key]]
          if (is.null(model_group)) next
          
          for (method_name in names(model_group)) {
            model_obj <- model_group[[method_name]]
            if (is.null(model_obj)) next
            
            preds <- model_obj$pred
            if (is.null(preds) || nrow(preds) == 0) next
            
            # Seleciona melhor combinação de hiperparâmetros, se existir
            if ("bestTune" %in% names(model_obj)) {
              best <- model_obj$bestTune
              for (col in names(best)) {
                preds <- preds[preds[[col]] == best[[col]], ]
              }
            }
            
            # Calcula erro e RMSE
            error_model <- preds$pred - preds$obs
            rmse_val <- RMSE_manual(preds$pred, preds$obs)
            
            # RW errors (sem ajuste de comprimento)
            e1 <- rw_results[[cty]][[paste0("error_rw_", base_h)]]
            e2 <- preds$pred - preds$obs
            
            # Teste de Diebold–Mariano
            dm_pval <- forecast::dm.test(
              e1 = e1,
              e2 = e2,
              h = h_num,
              power = 2,
              alternative = "greater",
              varestimator = "bartlett"
            )[["p.value"]]
            
            # Salva resultados
            rmse_results[[length(rmse_results) + 1]] <- data.frame(
              country = cty,
              model_type = mdl_type,
              horizon = hzn,
              window = wdw,
              model = mdl,
              method = method_name,
              rmse = rmse_val,
              relative_rmse = ifelse(
                hzn == "h12",
                rmse_val / rw_results[[cty]]$rmse_rw[2],
                rmse_val / rw_results[[cty]]$rmse_rw[1]
              )
            )
            
            dm_results[[length(dm_results) + 1]] <- data.frame(
              country = cty,
              model_type = mdl_type,
              horizon = hzn,
              window = wdw,
              model = mdl,
              method = method_name,
              dm_test_p_value = dm_pval
            )
          }
        }
      }
    }
  }
}


# ___________________________________________________________________________________
# _________________________________ COMBINE RESULTS _________________________________
# ___________________________________________________________________________________

results_all <- left_join(
  bind_rows(rmse_results),
  bind_rows(dm_results),
  by = c("country", "model_type", "horizon", "window", "model", "method")
)

# ___________________________________________________________________________________
# _____________________________________ EXPORT _____________________________________
# ___________________________________________________________________________________

# Cria lista de abas por país
sheets <- lapply(split(results_all, results_all$country), as.data.frame)
names(sheets) <- paste0("Resultados_", toupper(names(sheets)))

# Adiciona aba consolidada com todos os resultados
sheets <- c(list("Resultados_Todos" = results_all), sheets)

# Exporta para Excel
writexl::write_xlsx(sheets, path = "outputs/Resultados_Todos.xlsx")

# Exibe amostra
head(results_all)