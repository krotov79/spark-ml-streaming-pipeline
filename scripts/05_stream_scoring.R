library(sparklyr)
library(dplyr)

options(sparklyr.collect.method = "spark")

sc <- spark_connect(master = "local", version = "4.0")

model_path <- file.path("models", "okc_lr_pipeline")
stopifnot(dir.exists(model_path))

model <- ml_load(sc, model_path)

in_dir  <- "stream_in"
out_dir <- "stream_out"

dir.create(in_dir, showWarnings = FALSE)
dir.create(out_dir, showWarnings = FALSE)

events <- stream_read_json(
  sc,
  name = "events_stream",
  path = in_dir
)

scored <- ml_transform(model, events) %>%
  select(
    age, sex, drinks, drugs, essay_length,
    prediction
    # NOTE: probability/rawPrediction are VectorUDT and can be awkward to consume outside Spark
  )

q <- stream_write_parquet(
  scored,
  path = out_dir,
  mode = "append",
  checkpoint = file.path(out_dir, "_checkpoint")
)

cat("Streaming scoring is running.\n")
cat("Input folder :", normalizePath(in_dir), "\n")
cat("Output folder:", normalizePath(out_dir), "\n\n")
cat("To stop: stream_stop(q)\n")

invisible(readline("Press ENTER to stop streaming...\n"))
stream_stop(q)

spark_disconnect(sc)
