#!/usr/bin/env python3

import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
import matplotlib.pyplot as plt

# Add project root to path so we can import from utils package
project_root = str(Path(__file__).parent.parent.parent.parent)
sys.path.insert(0, project_root)
from utils.snowflake_connection import SnowflakeHook


def fetch_data() -> pd.DataFrame:
    # Compute store-level deliveries across all items using the materialized table
    query = """
WITH src AS (
  SELECT store_id,
         deliveries_30d, deliveries_60d, deliveries_90d,
         deliveries_120d, deliveries_150d, deliveries_180d
  FROM proddb.fionafan.store_item_deliveries_90d_pre_20250908
), stores_by_window AS (
  SELECT
    store_id,
    SUM(deliveries_30d)   AS deliveries_30d,
    SUM(deliveries_60d)   AS deliveries_60d,
    SUM(deliveries_90d)   AS deliveries_90d,
    SUM(deliveries_120d)  AS deliveries_120d,
    SUM(deliveries_150d)  AS deliveries_150d,
    SUM(deliveries_180d)  AS deliveries_180d
  FROM src
  GROUP BY all
), unpvt AS (
  SELECT window_col, deliveries
  FROM stores_by_window
  UNPIVOT(deliveries FOR window_col IN (
    deliveries_30d, deliveries_60d, deliveries_90d,
    deliveries_120d, deliveries_150d, deliveries_180d
  ))
), norm AS (
  SELECT LOWER(REPLACE(window_col, 'DELIVERIES_', '')) AS window_label, deliveries
  FROM unpvt
), bucketed AS (
  SELECT
    window_label,
    CASE
      WHEN deliveries < 50 THEN '<50'
      WHEN deliveries BETWEEN 50 AND 99 THEN '50-99'
      WHEN deliveries BETWEEN 100 AND 199 THEN '100-199'
      WHEN deliveries BETWEEN 200 AND 499 THEN '200-499'
      WHEN deliveries BETWEEN 500 AND 999 THEN '500-999'
      ELSE '1000+'
    END AS bucket,
    CASE
      WHEN deliveries < 50 THEN 1
      WHEN deliveries BETWEEN 50 AND 99 THEN 2
      WHEN deliveries BETWEEN 100 AND 199 THEN 3
      WHEN deliveries BETWEEN 200 AND 499 THEN 4
      WHEN deliveries BETWEEN 500 AND 999 THEN 5
      ELSE 6
    END AS bucket_order
  FROM norm
), counts AS (
  SELECT window_label, bucket, bucket_order, COUNT(1) AS store_count
  FROM bucketed
  GROUP BY window_label, bucket, bucket_order
)
SELECT
  window_label, bucket, bucket_order, store_count,
  ROUND(store_count * 100.0 / SUM(store_count) OVER (PARTITION BY window_label), 2) AS pct,
  ROUND(SUM(store_count) OVER (PARTITION BY window_label ORDER BY bucket_order
       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 100.0 /
       SUM(store_count) OVER (PARTITION BY window_label), 2) AS cum_pct
FROM counts
ORDER BY window_label, bucket_order
"""
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(query, method="pandas")
    return df


def plot_distributions(df: pd.DataFrame) -> Path:
    labels = ["<50", "50-99", "100-199", "200-499", "500-999", "1000+"]

    fig, axes = plt.subplots(2, 3, figsize=(20, 12))
    axes = axes.flatten()
    windows = [
        ("deliveries_30d", "30-Day Deliveries per Store (All Items)"),
        ("deliveries_60d", "60-Day Deliveries per Store (All Items)"),
        ("deliveries_90d", "90-Day Deliveries per Store (All Items)"),
        ("deliveries_120d", "120-Day Deliveries per Store (All Items)"),
        ("deliveries_150d", "150-Day Deliveries per Store (All Items)"),
        ("deliveries_180d", "180-Day Deliveries per Store (All Items)"),
    ]

    for ax, (col, title) in zip(axes, windows):
        window_key = col.replace("deliveries_", "")
        sub = df[df["window_label"] == window_key].copy()
        sub = sub.sort_values("bucket_order")
        perc = sub.set_index("bucket")["pct"].reindex(labels).fillna(0).astype(float)
        cum_perc = (
            sub.set_index("bucket")["cum_pct"].reindex(labels).ffill().fillna(0).astype(float)
        )

        x = range(len(labels))
        bars = ax.bar(x, perc.values, color="#1f77b4", alpha=0.85)
        ax.set_xticks(list(x))
        ax.set_xticklabels(labels, rotation=45, ha="right")
        ax.set_title(title, fontsize=12, fontweight="bold")
        ax.set_xlabel("Deliveries (bucketed)")
        ax.set_ylabel("Stores (%)")
        ax.grid(axis="y", alpha=0.3)

        for rect, p in zip(bars, perc.values):
            if p >= 1:
                ax.text(
                    rect.get_x() + rect.get_width() / 2,
                    rect.get_height() + max(float(perc.max()) * 0.01, 0.2),
                    f"{p:.1f}%",
                    ha="center",
                    va="bottom",
                    fontsize=9,
                    color="#333",
                )

        ax2 = ax.twinx()
        ax2.plot(x, cum_perc.values, color="#ff7f0e", marker="o", linewidth=2)
        ax2.set_ylim(0, 100)
        ax2.set_ylabel("Cumulative (%)", color="#ff7f0e")
        ax2.tick_params(axis='y', labelcolor="#ff7f0e")

    plt.tight_layout()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    plots_dir = Path(__file__).parent.parent / "plots"
    plots_dir.mkdir(exist_ok=True)
    outfile = plots_dir / f"all_items_store_delivery_distributions_6windows_{timestamp}.png"
    plt.savefig(outfile, dpi=300, bbox_inches="tight")
    return outfile


def main():
    df = fetch_data()
    print(f"Loaded aggregated distribution rows: {len(df):,}")
    outfile = plot_distributions(df)
    print(f"Saved plots to: {outfile}")


if __name__ == "__main__":
    main()



