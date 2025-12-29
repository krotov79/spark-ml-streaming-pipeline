library(sparklyr)
library(dplyr)
library(dbplyr)

options(sparklyr.collect.method = "spark")

sc <- spark_connect(master = "local", version = "4.0")

okc_raw <- tbl(sc, "okc_raw")

okc <- okc_raw %>%
  mutate(
    height = !!sql("try_cast(height as double)"),
    income = !!sql("nullif(try_cast(income as double), -1)"),
    
    sex    = if_else(is.na(sex),    "missing", sex),
    drinks = if_else(is.na(drinks), "missing", drinks),
    drugs  = if_else(is.na(drugs),  "missing", drugs),
    job    = if_else(is.na(job),    "missing", job),
    
    not_working = !!sql(
      "CAST(CASE WHEN job IN ('student','unemployed','retired') THEN 1 ELSE 0 END AS INT)"
    ),
    
    essay_length = !!sql(
      "length(concat_ws(' ',
        coalesce(essay0,''), coalesce(essay1,''), coalesce(essay2,''),
        coalesce(essay3,''), coalesce(essay4,''), coalesce(essay5,''),
        coalesce(essay6,''), coalesce(essay7,''), coalesce(essay8,''),
        coalesce(essay9,'')
      ))"
    )
  ) %>%
  select(age, sex, drinks, drugs, essay_length, not_working)

okc %>% sdf_register("okc_features")

# Check without collect() to avoid Arrow warnings
okc %>% summarise(
  n = n(),
  null_essay_length = !!sql("SUM(CASE WHEN essay_length IS NULL THEN 1 ELSE 0 END)")
) %>% print()




