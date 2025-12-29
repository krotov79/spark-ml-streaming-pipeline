library(sparklyr)
library(dplyr)
library(dbplyr)

# Connection to Spark
sc <- spark_connect(master = "local", version = "4.0")

okc <- spark_read_csv(
  sc,
  name = "okc_raw",
  path = "data/profiles.csv",
  escape = "\"",
  memory = FALSE,
  options = list(multiline = TRUE)
) %>%
  mutate(
    # Spark-safe numeric conversions:
    # try_cast turns malformed strings (like "NA") into NULL instead of erroring.
    height = !!sql("try_cast(height as double)"),
    income = !!sql("nullif(try_cast(income as double), -1)"),
    
    # Replace missing categoricals with an explicit level
    sex    = if_else(is.na(sex),    "missing", sex),
    drinks = if_else(is.na(drinks), "missing", drinks),
    drugs  = if_else(is.na(drugs),  "missing", drugs),
    job    = if_else(is.na(job),    "missing", job),
    
    # Label
    not_working = !!dbplyr::sql(
      "CAST(CASE WHEN job IN ('student','unemployed','retired') THEN 1 ELSE 0 END AS INT)"
    )
  )

# Quick check
print(glimpse(okc))

okc %>%
  group_by(not_working) %>%
  tally() %>%
  collect() %>%
  print()
