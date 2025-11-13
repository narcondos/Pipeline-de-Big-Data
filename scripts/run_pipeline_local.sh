#!/usr/bin/env bash
# Ejecuta el pipeline localmente con spark-submit (recomendado)
OUT_SUMMARY=output/sentiment_summary
OUT_TOP_NEG=output/top_negative_reasons
INPUT=data/raw_tweets.json

mkdir -p $(dirname $OUT_SUMMARY)
mkdir -p $(dirname $OUT_TOP_NEG)

spark-submit \
  --master local[*] \
  src/pyspark_pipeline.py \
  --input "$INPUT" \
  --out-summary "$OUT_SUMMARY" \
  --out-top-neg "$OUT_TOP_NEG"
