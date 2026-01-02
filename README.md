# Spark ML Streaming Pipeline

End-to-end Apache Spark ML pipeline with batch training and structured streaming scoring.

## Overview

This project demonstrates:
- Feature engineering and ML pipeline training in Spark
- Persisted ML pipeline models
- Real-time scoring using Spark Structured Streaming
- Local file-based streaming for demo purposes
- HTML monitoring report generated with Quarto

## Project Structure

scripts/
  01_load_and_clean.R
  02_features.R
  03_train_pipeline.R
  04_event_producer.R
  05_stream_scoring.R

models/
  okc_lr_pipeline/

stream_in/
stream_out/

report/
  stream_scoring.qmd
  stream_scoring.html
  
## How to Run Locally

### Prerequisites
- Java 8+ (for Spark)
- Apache Spark (local mode)
- RStudio or R console(>= 4.2)
- R packages: sparklyr, dplyr, arrow, ggplot2, quarto

### Steps

1. Start a local Spark session and train the model:

```{r}
   source("scripts/03_train_pipeline.R")
```

2. Start the streaming scorer:

```{r}
   source("scripts/05_stream_scoring.R")
```

3. Generate sample streaming events:

```{r}
   source("scripts/04_event_producer.R")
```

4. Open the monitoring report:
report/stream_scoring.html

## Pipeline Flow
```md
Batch data
   ↓
Feature engineering
   ↓
Spark ML Pipeline (Logistic Regression)
   ↓
Saved model
   ↓
Structured Streaming
   ↓
Live scoring (JSON → Parquet)
   ↓
Quarto HTML report
```


## Notes

- Streaming source is file-based (JSON drop-in) for local testing
- This project is designed as a portfolio/demo pipeline, not production infrastructure
