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
  
## How to Run

1. Run batch training:
   - `scripts/01_load_and_clean.R`
   - `scripts/02_features.R`
   - `scripts/03_train_pipeline.R`

2. Start streaming:
   - Run `scripts/05_stream_scoring.R`
   - Generate events with `scripts/04_event_producer.R`

3. Open the report:
   - `report/stream_scoring.html`

## Notes

- Streaming source is file-based (JSON drop-in) for local testing
- This project is designed as a portfolio/demo pipeline, not production infrastructure
