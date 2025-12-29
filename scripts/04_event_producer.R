library(readr)
library(dplyr)
library(jsonlite)

set.seed(42)

csv_path <- file.path("data", "profiles.csv")
stopifnot(file.exists(csv_path))

df <- read_csv(csv_path, show_col_types = FALSE) %>%
  transmute(
    age = as.integer(age),
    sex = if_else(is.na(sex), "missing", sex),
    drinks = if_else(is.na(drinks), "missing", drinks),
    drugs = if_else(is.na(drugs), "missing", drugs),
    essay_text = paste(
      coalesce(essay0, ""), coalesce(essay1, ""), coalesce(essay2, ""),
      coalesce(essay3, ""), coalesce(essay4, ""), coalesce(essay5, ""),
      coalesce(essay6, ""), coalesce(essay7, ""), coalesce(essay8, ""),
      coalesce(essay9, ""),
      sep = " "
    ),
    essay_length = nchar(essay_text)
  ) %>%
  select(age, sex, drinks, drugs, essay_length) %>%
  filter(!is.na(age))

out_dir <- "stream_in"
dir.create(out_dir, showWarnings = FALSE)

n_events <- 300
sleep_s <- 0.5

cat("Starting event producer...\n")
cat("Writing to folder:", normalizePath(out_dir), "\n\n")

for (i in seq_len(n_events)) {
  row <- df[sample.int(nrow(df), 1), ]
  
  event <- list(
    age = row$age,
    sex = row$sex,
    drinks = row$drinks,
    drugs = row$drugs,
    essay_length = row$essay_length
  )
  
  fn <- sprintf("event_%s_%04d.json", format(Sys.time(), "%Y%m%d_%H%M%S"), i)
  path <- file.path(out_dir, fn)
  writeLines(toJSON(event, auto_unbox = TRUE), con = path)
  
  if (i %% 25 == 0) cat("Wrote", i, "events...\n")
  Sys.sleep(sleep_s)
}

cat("\nDone. Wrote", n_events, "events.\n")

