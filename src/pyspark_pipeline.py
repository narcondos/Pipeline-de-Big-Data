"""
Pipeline PySpark: lectura, limpieza, UDF de sentimiento (simulado), agregaciones y escritura de resultados.
Ejecutar con spark-submit o python (si pyspark está instalado).
"""
import argparse
import random
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, avg, count, regexp_replace, lower, to_timestamp, month, year, coalesce, lit
from pyspark.sql.types import DoubleType
from pyspark.storagelevel import StorageLevel

def analyze_sentiment(text, raw_sentiment):
    if raw_sentiment == 'positive':
        return float(random.uniform(0.5, 1.0))
    elif raw_sentiment == 'negative':
        return float(random.uniform(-1.0, -0.5))
    else:
        return float(random.uniform(-0.4, 0.4))

def build_spark_session(app_name="SentimentAnalysisPipeline"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def run_pipeline(input_path, output_summary_dir, top_neg_dir, persist=True):
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    sentiment_udf = udf(analyze_sentiment, DoubleType())

    try:
        df = spark.read.json(input_path)
    except Exception as e:
        print(f"ERROR: fallo al leer {input_path}: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    df_processed = (
        df.withColumn("tweet_date", to_timestamp(col("tweet_date"), "yyyy-MM-dd HH:mm:ss"))
          .withColumn("text_clean", lower(col("text")))
          .withColumn("text_clean", regexp_replace(col("text_clean"), r'http\\S+|@\\S+|#\\S+', ''))
          .withColumn("user_location", coalesce(col("user_location"), lit("Desconocido")))
          .withColumn("sentiment_score", sentiment_udf(col("text_clean"), col("raw_sentiment")))
          .withColumn("analysis_month", month(col("tweet_date")))
          .withColumn("analysis_year", year(col("tweet_date")))
          .select("tweet_id", "airline", "text", "sentiment_score", "analysis_month", "analysis_year", "user_location", "negativereason")
    )

    if persist:
        df_processed.persist(StorageLevel.MEMORY_ONLY)
        print("DataFrame persistido en memoria para optimización.")

    df_sentiment_summary = (
        df_processed.groupBy("airline", "analysis_year", "analysis_month")
                    .agg(avg("sentiment_score").alias("avg_sentiment"),
                         count("tweet_id").alias("total_tweets"))
                    .orderBy(col("avg_sentiment").asc())
    )

    df_sentiment_summary.write.mode("overwrite").parquet(output_summary_dir)
    print(f"Resumen de sentimiento escrito en {output_summary_dir}")

    worst_airline_row = df_sentiment_summary.limit(1).collect()
    if worst_airline_row:
        worst_airline = worst_airline_row[0]["airline"]
        df_negative_reasons = (
            df_processed.filter((col("airline") == worst_airline) & col("negativereason").isNotNull())
                        .groupBy("negativereason")
                        .agg(count("tweet_id").alias("count"))
                        .orderBy(col("count").desc())
                        .limit(5)
        )
        df_negative_reasons.write.mode("overwrite").json(top_neg_dir)
        print(f"Top razones negativas para {worst_airline} guardadas en {top_neg_dir}")
    else:
        print("No se pudo determinar la aerolínea con peor sentimiento (dataset vacío o formato distinto).")

    if persist:
        df_processed.unpersist()
    spark.stop()

def main():
    parser = argparse.ArgumentParser(description="PySpark processing pipeline")
    parser.add_argument("--input", type=str, default="data/raw_tweets.json", help="Ruta JSON Lines de entrada")
    parser.add_argument("--out-summary", type=str, default="output/sentiment_summary", help="Directorio de salida para resumen")
    parser.add_argument("--out-top-neg", type=str, default="output/top_negative_reasons", help="Directorio para razones negativas")
    parser.add_argument("--no-persist", action="store_true", help="Desactivar persistencia en memoria")
    args = parser.parse_args()

    run_pipeline(args.input, args.out_summary, args.out_top_neg, persist=not args.no_persist)

if __name__ == "__main__":
    main()