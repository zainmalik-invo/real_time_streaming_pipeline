"""
Query Gold / Silver lake layers with DuckDB.

Usage:
    python -m sql.query_gold
    python -m sql.query_gold --lake data/lake
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LAKE = PROJECT_ROOT / "data" / "lake"


def run_queries(lake: Path) -> None:
    gold_metrics = lake / "gold" / "metrics"
    gold_snapshot = lake / "gold" / "snapshot"
    silver = lake / "silver" / "transactions"
    quarantine = lake / "quarantine" / "transactions"

    con = duckdb.connect()

    print("=" * 72)
    print("STREAMING LAKEHOUSE QUERIES (DuckDB)")
    print("=" * 72)

    if gold_snapshot.exists():
        print("\n1) Latest gold snapshot")
        con.execute(
            f"""
            SELECT *
            FROM read_parquet('{(gold_snapshot / "**" / "*.parquet").as_posix()}', hive_partitioning=true)
            ORDER BY as_of DESC
            LIMIT 5
            """
        )
        print(con.fetchdf().to_string(index=False))
    else:
        print("\n1) Gold snapshot not found yet (pipeline still warming up)")

    if gold_metrics.exists():
        print("\n2) Total revenue (all gold windows, may double-count across batches)")
        print("   Prefer snapshot / re-aggregated silver for exact totals.")
        con.execute(
            f"""
            SELECT
                SUM(total_revenue) AS revenue_sum_of_windows,
                SUM(transaction_count) AS txn_sum_of_windows
            FROM read_parquet('{(gold_metrics / "**" / "*.parquet").as_posix()}', hive_partitioning=true)
            """
        )
        print(con.fetchdf().to_string(index=False))

        print("\n3) Top customers by revenue (from gold window table)")
        con.execute(
            f"""
            SELECT
                customer_id,
                SUM(total_revenue) AS revenue,
                SUM(transaction_count) AS transactions,
                AVG(avg_transaction_value) AS avg_txn_value
            FROM read_parquet('{(gold_metrics / "**" / "*.parquet").as_posix()}', hive_partitioning=true)
            GROUP BY customer_id
            ORDER BY revenue DESC
            LIMIT 10
            """
        )
        print(con.fetchdf().to_string(index=False))

        print("\n4) Transactions per 1-minute window")
        con.execute(
            f"""
            SELECT
                window_start,
                window_end,
                SUM(transaction_count) AS transactions,
                SUM(total_revenue) AS revenue,
                AVG(avg_transaction_value) AS avg_transaction_value
            FROM read_parquet('{(gold_metrics / "**" / "*.parquet").as_posix()}', hive_partitioning=true)
            GROUP BY window_start, window_end
            ORDER BY window_start DESC
            LIMIT 20
            """
        )
        print(con.fetchdf().to_string(index=False))
    else:
        print("\n2–4) Gold metrics not found yet")

    if silver.exists():
        print("\n5) Exact totals from Silver (source of truth)")
        con.execute(
            f"""
            SELECT
                COUNT(*) AS total_transactions,
                ROUND(SUM(amount), 2) AS total_revenue,
                ROUND(AVG(amount), 2) AS avg_transaction_value,
                COUNT(DISTINCT customer_id) AS unique_customers
            FROM read_parquet('{(silver / "**" / "*.parquet").as_posix()}', hive_partitioning=true)
            """
        )
        print(con.fetchdf().to_string(index=False))

        print("\n6) Which customer generated the most revenue? (Silver)")
        con.execute(
            f"""
            SELECT
                customer_id,
                COUNT(*) AS transactions,
                ROUND(SUM(amount), 2) AS revenue,
                ROUND(AVG(amount), 2) AS avg_transaction_value
            FROM read_parquet('{(silver / "**" / "*.parquet").as_posix()}', hive_partitioning=true)
            GROUP BY customer_id
            ORDER BY revenue DESC
            LIMIT 5
            """
        )
        print(con.fetchdf().to_string(index=False))

        print("\n7) Transactions per minute (from Silver event_ts)")
        con.execute(
            f"""
            SELECT
                date_trunc('minute', event_ts) AS minute,
                COUNT(*) AS transactions,
                ROUND(SUM(amount), 2) AS revenue,
                ROUND(AVG(amount), 2) AS avg_transaction_value
            FROM read_parquet('{(silver / "**" / "*.parquet").as_posix()}', hive_partitioning=true)
            GROUP BY 1
            ORDER BY 1 DESC
            LIMIT 20
            """
        )
        print(con.fetchdf().to_string(index=False))
    else:
        print("\n5–7) Silver layer not found yet")

    if quarantine.exists():
        print("\n8) Quarantined / failed messages")
        con.execute(
            f"""
            SELECT failure_reason, COUNT(*) AS n
            FROM read_parquet('{(quarantine / "**" / "*.parquet").as_posix()}', hive_partitioning=true)
            GROUP BY failure_reason
            ORDER BY n DESC
            """
        )
        print(con.fetchdf().to_string(index=False))

    con.close()
    print("\nDone.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query lakehouse layers with DuckDB")
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.lake.exists():
        raise SystemExit(
            f"Lake path not found: {args.lake}\n"
            "Start the streaming pipeline and producer first."
        )
    run_queries(args.lake)


if __name__ == "__main__":
    main()
