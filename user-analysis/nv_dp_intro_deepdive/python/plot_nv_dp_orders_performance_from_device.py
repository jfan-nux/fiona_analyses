#!/usr/bin/env python3
"""
Plots ratios from proddb.fionafan.nv_dp_orders_performance_from_device.

Filters to order_type = 'overall' and cohort_flag = 1 (cohort rows),
then plots two subplots:
- Left: ratio_order_rate_cohort_over_non by month_bucket (color = cohort_name)
- Right: ratio_retention_cohort_over_non by month_bucket (color = cohort_name)

Output: nv_dp_intro_deepdive/plots/nv_dp_orders_perf_device_ratios.png
"""

import sys
from pathlib import Path

# Add the root utils directory to path
sys.path.append(str(Path(__file__).parent.parent.parent.parent / "utils"))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from snowflake_connection import SnowflakeHook


OUTPUT_PNG = (
    "/Users/fiona.fan/Documents/fiona_analyses/user-analysis/"
    "nv_dp_intro_deepdive/plots/nv_dp_orders_perf_device_ratios.png"
)


def fetch_data() -> pd.DataFrame:
    hook = SnowflakeHook()
    query = (
        "select cohort_name, cohort_flag, month_bucket, order_type, "
        "devices, consumers_retained, distinct_deliveries, order_rate, retention, "
        "ratio_order_rate_cohort_over_non, ratio_retention_cohort_over_non "
        "from proddb.fionafan.nv_dp_orders_performance_from_device "
        "order by cohort_name, month_bucket, order_type, cohort_flag desc"
    )
    print("🔄 Fetching NV/DP orders performance (device) from Snowflake...")
    df = hook.fetch_pandas_all(query)
    print(f"✅ Loaded {len(df)} rows")
    df.columns = [c.lower() for c in df.columns]
    return df


def _order_months(values):
    try:
        keyed = []
        for v in values:
            s = str(v)
            num = int(s.replace("month", "")) if s.startswith("month") else None
            keyed.append((num if num is not None else 9999, s))
        keyed.sort()
        return [s for _, s in keyed]
    except Exception:
        return sorted(values)


def create_subplots(df: pd.DataFrame):
    # Keep cohort rows only
    df = df[df["cohort_flag"] == 1].copy()
    order_types = ["non_nv_order", "nv_order", "overall"]

    # Determine global month order and cohorts across all order_types
    months = _order_months(sorted(df["month_bucket"].unique()))
    cohorts = sorted(df["cohort_name"].unique())

    fig, axes = plt.subplots(3, 2, figsize=(16, 18), sharex=True)
    fig.suptitle("NV/DP Orders Performance (Device): Ratios vs Non-Cohort by Order Type", fontsize=16, fontweight="bold")

    for i, ot in enumerate(order_types):
        f = df[df["order_type"] == ot].copy()

        # Left column: Order Rate ratio
        ax1 = axes[i, 0]
        for cohort in cohorts:
            sdf = f[f["cohort_name"] == cohort].set_index("month_bucket").reindex(months).reset_index()
            ax1.plot(
                sdf["month_bucket"],
                sdf["ratio_order_rate_cohort_over_non"],
                marker="o",
                linewidth=2,
                label=cohort,
            )
        ax1.axhline(1.0, color="gray", linewidth=1, linestyle="--", alpha=0.7)
        ax1.set_title(f"{ot}: Order Rate Ratio", fontweight="bold")
        ax1.set_xlabel("Month bucket")
        ax1.set_ylabel("Ratio to non-cohort")
        ax1.grid(axis="y", alpha=0.3)

        # Right column: Retention ratio
        ax2 = axes[i, 1]
        for cohort in cohorts:
            sdf = f[f["cohort_name"] == cohort].set_index("month_bucket").reindex(months).reset_index()
            ax2.plot(
                sdf["month_bucket"],
                sdf["ratio_retention_cohort_over_non"],
                marker="o",
                linewidth=2,
                label=cohort,
            )
        ax2.axhline(1.0, color="gray", linewidth=1, linestyle="--", alpha=0.7)
        ax2.set_title(f"{ot}: Retention Ratio", fontweight="bold")
        ax2.set_xlabel("Month bucket")
        ax2.set_ylabel("Ratio to non-cohort")
        ax2.grid(axis="y", alpha=0.3)

    # Combined legend (from bottom-left axes to capture labels)
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, title="Cohort", bbox_to_anchor=(1.01, 0.98), loc="upper left")
    plt.tight_layout()
    return fig


def main():
    df = fetch_data()
    fig = create_subplots(df)
    Path(OUTPUT_PNG).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUTPUT_PNG, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"📊 Saved combined plot to: {OUTPUT_PNG}")


if __name__ == "__main__":
    main()


