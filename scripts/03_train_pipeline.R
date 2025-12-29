library(sparklyr)
library(dplyr)
library(dbplyr)

# Optional: keep your environment consistent
options(sparklyr.collect.method = "spark")

sc <- spark_connect(master = "local", version = "4.0")

# Ensure features table exists (rebuild if needed)
if (!"okc_features" %in% src_tbls(sc)) {
  source("scripts/02_features.R")
}
okc <- tbl(sc, "okc_features")

# Load features table
okc <- tbl(sc, "okc_features")

# --- Split data ---
splits <- sdf_random_split(okc, training = 0.8, validation = 0.2, seed = 42)
train <- splits$training
valid <- splits$validation

# Quick sanity check: class balance in training
train %>%
  group_by(not_working) %>%
  tally() %>%
  collect() %>%
  print()

# --- Build pipeline ---
pipeline <- ml_pipeline(sc) %>%
  ft_string_indexer(input_col = "sex",    output_col = "sex_idx") %>%
  ft_string_indexer(input_col = "drinks", output_col = "drinks_idx") %>%
  ft_string_indexer(input_col = "drugs",  output_col = "drugs_idx") %>%
  ft_one_hot_encoder(
    input_cols  = c("sex_idx", "drinks_idx", "drugs_idx"),
    output_cols = c("sex_ohe", "drinks_ohe", "drugs_ohe")
  ) %>%
  ft_vector_assembler(
    input_cols = c("age", "essay_length", "sex_ohe", "drinks_ohe", "drugs_ohe"),
    output_col = "features"
  ) %>%
  # Keep with_mean = FALSE for sparse one-hot vectors (safer on laptop)
  ft_standard_scaler(
    input_col = "features",
    output_col = "features_scaled",
    with_mean = FALSE,
    with_std = TRUE
  ) %>%
  ml_logistic_regression(
    features_col = "features_scaled",
    label_col = "not_working",
    threshold = 0.2 
  )

# --- Fit ---
model <- ml_fit(pipeline, train)

# --- Predict on validation set ---
pred_valid <- ml_predict(model, valid)

# --- Evaluate with BinaryClassificationEvaluator ---
evaluator <- ml_binary_classification_evaluator(
  sc,
  label_col = "not_working",
  raw_prediction_col = "rawPrediction",   # Spark default column name
  metric_name = "areaUnderROC"
)

auc <- ml_evaluate(evaluator, pred_valid)
cat("\nValidation AUC:", auc, "\n\n")


# --- Save model ---
model_path <- file.path("models", "okc_lr_pipeline")
if (dir.exists(model_path)) unlink(model_path, recursive = TRUE, force = TRUE)
ml_save(model, model_path)

cat("Saved model to:", model_path, "\n")

dir.create("report", showWarnings = FALSE)
writeLines(sprintf("Validation AUC: %.6f", auc), "report/metrics.txt")

