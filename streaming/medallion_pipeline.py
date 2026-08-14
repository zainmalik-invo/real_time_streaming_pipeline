"""
PySpark Structured Streaming medallion pipeline:

  Kafka (transactions)
       ↓
    Bronze  — raw events (Parquet)
       ↓
    Silver  — cleaned, validated, deduplicated (Parquet + Delta)
       ↓
    Gold    — business metrics by time window (Parquet + Delta)

Also writes invalid records to data/lake/quarantine.

Usage:
    export JAVA_HOME="$(brew --prefix openjdk)/libexec/openjdk.jdk/Contents/Home"
    source .venv/bin/activate
    python -m streaming.medallion_pipeline
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from config.settings import settings


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LAKE = PROJECT_ROOT / "data" / "lake"
DEFAULT_CHECKPOINTS = PROJECT_ROOT / "checkpoints"

EVENT_SCHEMA = StructType(
    [
        StructField("transaction_id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("event_id", StringType(), True),
    ]
)


def build_spark(app_name: str = "TransactionMedallionStreaming") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.showConsoleProgress", "true")
    )
    # Delta + Kafka source packages resolved via Ivy on first run.
    return configure_spark_with_delta_pip(
        builder,
        extra_packages=[
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
            "org.apache.kafka:kafka-clients:3.9.0",
        ],
    ).getOrCreate()


def parse_kafka_batch(raw_df: DataFrame) -> DataFrame:
    """Parse Kafka value JSON into columns + keep Kafka metadata."""
    parsed = (
        raw_df.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ingest_ts"),
            F.col("value").cast("string").alias("raw_value"),
        )
        .withColumn("parsed", F.from_json(F.col("raw_value"), EVENT_SCHEMA))
        .withColumn("bronze_ingest_ts", F.current_timestamp())
    )
    return parsed.select(
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_ingest_ts",
        "bronze_ingest_ts",
        "raw_value",
        "parsed.*",
    )


def validate_and_split(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split into clean (silver-bound) and quarantine dataframes."""
    with_event_ts = bronze_df.withColumn(
        "event_ts",
        F.to_timestamp(F.col("timestamp")),
    )

    is_valid = (
        F.col("transaction_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("amount").isNotNull()
        & (F.col("amount") > 0)
        & F.col("event_ts").isNotNull()
    )

    valid = (
        with_event_ts.filter(is_valid)
        .withColumn("currency", F.coalesce(F.col("currency"), F.lit("USD")))
        .withColumn(
            "event_id",
            F.coalesce(
                F.col("event_id"),
                F.concat_ws(
                    "-",
                    F.col("transaction_id").cast("string"),
                    F.col("customer_id").cast("string"),
                    F.col("timestamp"),
                ),
            ),
        )
        .withColumn("amount", F.col("amount").cast(DoubleType()))
        .withColumn("transaction_id", F.col("transaction_id").cast(LongType()))
        .withColumn("customer_id", F.col("customer_id").cast(LongType()))
    )

    invalid = with_event_ts.filter(~is_valid).withColumn(
        "failure_reason",
        F.when(F.col("transaction_id").isNull(), "null_transaction_id")
        .when(F.col("customer_id").isNull(), "null_customer_id")
        .when(F.col("amount").isNull(), "null_amount")
        .when(F.col("amount") <= 0, "non_positive_amount")
        .when(F.col("event_ts").isNull(), "bad_timestamp")
        .otherwise("unknown"),
    )
    return valid, invalid


def process_microbatch(lake: Path, checkpoints: Path, trigger_seconds: int):
    """
    ForeachBatch handler factory: bronze → silver → gold in one micro-batch.
    Using foreachBatch lets us:
      - write bronze/silver/gold atomically per batch
      - quarantine invalids
      - dedupe within the batch + against recent silver via Delta merge-like drop
    """

    bronze_path = lake / "bronze" / "transactions"
    silver_path = lake / "silver" / "transactions"
    gold_path = lake / "gold" / "metrics"
    quarantine_path = lake / "quarantine" / "transactions"
    silver_delta = lake / "delta" / "silver_transactions"
    gold_delta = lake / "delta" / "gold_metrics"

    def foreach_batch(raw_batch: DataFrame, batch_id: int) -> None:
        if raw_batch.rdd.isEmpty():
            print(f"[batch {batch_id}] empty")
            return

        bronze = parse_kafka_batch(raw_batch)
        # Persist bronze (append parquet + day partition)
        bronze_out = bronze.withColumn(
            "event_date",
            F.coalesce(
                F.to_date(F.to_timestamp(F.col("timestamp"))),
                F.to_date(F.col("bronze_ingest_ts")),
            ),
        )
        (
            bronze_out.write.mode("append")
            .partitionBy("event_date")
            .parquet(str(bronze_path))
        )

        valid, invalid = validate_and_split(bronze_out)

        if not invalid.rdd.isEmpty():
            (
                invalid.withColumn("batch_id", F.lit(batch_id))
                .write.mode("append")
                .parquet(str(quarantine_path))
            )
            print(f"[batch {batch_id}] quarantined invalid rows")

        if valid.rdd.isEmpty():
            print(f"[batch {batch_id}] no valid rows")
            return

        # Late-arriving data: drop events older than 10 minutes vs newest event in batch.
        # (In a continuous streaming aggregation you'd use withWatermark + window.)
        max_ts = valid.agg(F.max("event_ts").alias("max_ts")).collect()[0]["max_ts"]
        silver = (
            valid.filter(
                F.col("event_ts") >= F.lit(max_ts) - F.expr("INTERVAL 10 MINUTES")
            )
            .dropDuplicates(["event_id"])
            .dropDuplicates(["transaction_id"])
            .withColumn("event_date", F.to_date(F.col("event_ts")))
            .withColumn("silver_processed_ts", F.current_timestamp())
            .withColumn("batch_id", F.lit(batch_id))
            .select(
                "event_id",
                "transaction_id",
                "customer_id",
                "amount",
                "currency",
                "merchant",
                "event_ts",
                "event_date",
                "kafka_partition",
                "kafka_offset",
                "bronze_ingest_ts",
                "silver_processed_ts",
                "batch_id",
            )
        )

        silver.write.mode("append").partitionBy("event_date").parquet(str(silver_path))
        silver.write.mode("append").format("delta").partitionBy("event_date").save(
            str(silver_delta)
        )

        # Gold: 1-minute tumbling window metrics per customer (per micro-batch)
        gold = (
            silver.groupBy(
                F.window(F.col("event_ts"), "1 minute").alias("time_window"),
                F.col("customer_id"),
            )
            .agg(
                F.count("*").alias("transaction_count"),
                F.sum("amount").alias("total_revenue"),
                F.avg("amount").alias("avg_transaction_value"),
            )
            .select(
                F.col("time_window.start").alias("window_start"),
                F.col("time_window.end").alias("window_end"),
                "customer_id",
                "transaction_count",
                "total_revenue",
                "avg_transaction_value",
                F.lit(batch_id).alias("batch_id"),
                F.current_timestamp().alias("gold_processed_ts"),
            )
            .withColumn("window_date", F.to_date(F.col("window_start")))
        )

        gold.write.mode("append").partitionBy("window_date").parquet(str(gold_path))
        gold.write.mode("append").format("delta").partitionBy("window_date").save(
            str(gold_delta)
        )

        # Also maintain a simple global snapshot table (overwrite latest)
        snapshot = silver.agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_revenue"),
            F.avg("amount").alias("avg_transaction_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
        ).withColumn("as_of", F.current_timestamp())
        (
            snapshot.write.mode("overwrite")
            .parquet(str(lake / "gold" / "snapshot"))
        )

        counts = silver.count()
        print(f"[batch {batch_id}] silver_rows={counts} gold_windows_written")

    return foreach_batch


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Medallion streaming pipeline")
    parser.add_argument("--bootstrap", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--topic", default=settings.kafka_topic)
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE)
    parser.add_argument("--checkpoints", type=Path, default=DEFAULT_CHECKPOINTS)
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument(
        "--starting-offsets",
        default="latest",
        choices=["latest", "earliest"],
        help="Kafka startingOffsets for a fresh checkpoint",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete lake + checkpoints before starting (destructive)",
    )
    return parser.parse_args()


def main() -> None:
    # Line-buffer stdout so micro-batch logs appear immediately in pipes/terminals.
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    args = parse_args()

    if args.reset:
        for path in (args.lake, args.checkpoints):
            if path.exists():
                shutil.rmtree(path)
                print(f"Removed {path}")

    args.lake.mkdir(parents=True, exist_ok=True)
    args.checkpoints.mkdir(parents=True, exist_ok=True)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Kafka → Bronze → Silver → Gold streaming job")
    print(f"  bootstrap      = {args.bootstrap}")
    print(f"  topic          = {args.topic}")
    print(f"  lake           = {args.lake}")
    print(f"  checkpoints    = {args.checkpoints}")
    print(f"  trigger        = {args.trigger_seconds}s")
    print(f"  startingOffsets= {args.starting_offsets}")
    print("Press Ctrl+C to stop.\n")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        kafka_df.writeStream.foreachBatch(
            process_microbatch(args.lake, args.checkpoints, args.trigger_seconds)
        )
        .option("checkpointLocation", str(args.checkpoints / "medallion"))
        .queryName("medallion_pipeline")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    # Ensure packages / jars available when launched as a module
    os.environ.setdefault(
        "PYSPARK_PYTHON",
        os.environ.get("VIRTUAL_ENV", "") + "/bin/python"
        if os.environ.get("VIRTUAL_ENV")
        else os.environ.get("PYSPARK_PYTHON", "python3"),
    )
    main()
