#!/usr/bin/env python3

import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
import matplotlib.pyplot as plt

# Add utils directory to path
utils_path = str(Path(__file__).parent.parent.parent.parent / "utils")
sys.path.insert(0, utils_path)
from snowflake_connection import SnowflakeHook


def fetch_data() -> pd.DataFrame:
    # Aggregate into bins in Snowflake to avoid transferring ~2M rows
    query = """
WITH src AS (
  SELECT deliveries_30d, deliveries_60d, deliveries_90d,
         deliveries_120d, deliveries_150d, deliveries_180d
  FROM proddb.fionafan.m_card_view_most_liked_deliveries
), unpvt AS (
  SELECT window_col, deliveries FROM src
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
  SELECT window_label, bucket, bucket_order, COUNT(1) AS item_count
  FROM bucketed
  GROUP BY window_label, bucket, bucket_order
)
SELECT
  window_label, bucket, bucket_order, item_count,
  ROUND(item_count * 100.0 / SUM(item_count) OVER (PARTITION BY window_label), 2) AS pct,
  ROUND(SUM(item_count) OVER (PARTITION BY window_label ORDER BY bucket_order
       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 100.0 /
       SUM(item_count) OVER (PARTITION BY window_label), 2) AS cum_pct
FROM counts
ORDER BY window_label, bucket_order
"""
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(query, method="pandas")
    return df


def plot_distributions(df: pd.DataFrame) -> Path:
    # Ensure integer types and handle nulls
    for col in ["deliveries_30d", "deliveries_60d", "deliveries_90d"]:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)

    # Expected ordered labels
    labels = ["<50", "50-99", "100-199", "200-499", "500-999", "1000+"]

    fig, axes = plt.subplots(2, 3, figsize=(20, 12))
    axes = axes.flatten()
    windows = [
        ("deliveries_30d", "30-Day Deliveries per Item"),
        ("deliveries_60d", "60-Day Deliveries per Item"),
        ("deliveries_90d", "90-Day Deliveries per Item"),
        ("deliveries_120d", "120-Day Deliveries per Item"),
        ("deliveries_150d", "150-Day Deliveries per Item"),
        ("deliveries_180d", "180-Day Deliveries per Item"),
    ]

    # Use aggregated results from Snowflake
    for ax, (col, title) in zip(axes, windows):
        window_key = col.replace("deliveries_", "")  # e.g., 30d, 60d
        sub = df[df["window_label"] == window_key].copy()
        # Ensure order by bucket_order
        sub = sub.sort_values("bucket_order")
        perc = sub.set_index("bucket")["pct"].reindex(labels).fillna(0).astype(float)
        cum_perc = (
            sub.set_index("bucket")["cum_pct"].reindex(labels).ffill().fillna(0).astype(float)
        )

        x = range(len(labels))

        # Bar plot of percentages
        bars = ax.bar(x, perc.values, color="#1f77b4", alpha=0.85)
        ax.set_xticks(list(x))
        ax.set_xticklabels(labels, rotation=45, ha="right")
        ax.set_title(title, fontsize=12, fontweight="bold")
        ax.set_xlabel("Deliveries (bucketed)")
        ax.set_ylabel("Unique Items (%)")
        ax.grid(axis="y", alpha=0.3)

        # Annotate bar percentages (show for >= 1%)
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

        # Cumulative percentage line on secondary axis
        ax2 = ax.twinx()
        ax2.plot(x, cum_perc.values, color="#ff7f0e", marker="o", linewidth=2)
        ax2.set_ylim(0, 100)
        ax2.set_ylabel("Cumulative (%)", color="#ff7f0e")
        ax2.tick_params(axis='y', labelcolor="#ff7f0e")

        # No raw-value stats here since we aggregated on the server

    plt.tight_layout()

    # Save plot
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    plots_dir = Path(__file__).parent.parent / "plots"
    plots_dir.mkdir(exist_ok=True)
    outfile = plots_dir / f"most_liked_item_delivery_distributions_6windows_{timestamp}.png"
    plt.savefig(outfile, dpi=300, bbox_inches="tight")
    return outfile


def main():
    df = fetch_data()
    print(f"Loaded {len(df):,} store-item rows from Snowflake")
    outfile = plot_distributions(df)
    print(f"Saved plots to: {outfile}")


if __name__ == "__main__":
    main()


