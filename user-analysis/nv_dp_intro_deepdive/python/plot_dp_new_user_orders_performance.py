#!/usr/bin/env python3
"""
DP new user cohort performance: plots order rate and MAU rate by month bucket and cohort.

Single figure with two subplots:
- Left: Order Rate by month_bucket (color = cohort)
- Right: MAU Rate by month_bucket (color = cohort)

Output: nv_dp_intro_deepdive/plots/dp_new_user_rates_by_month_cohort.png
"""

import sys
from pathlib import Path

# Add the root utils directory to path
sys.path.append(str(Path(__file__).parent.parent.parent.parent / "utils"))

import matplotlib
matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt
import pandas as pd

from snowflake_connection import SnowflakeHook


OUTPUT_PNG = (
    "/Users/fiona.fan/Documents/fiona_analyses/user-analysis/"
    "nv_dp_intro_deepdive/plots/dp_new_user_rates_by_month_cohort.png"
)


def fetch_dp_new_user_performance() -> pd.DataFrame:
    """Read the performance table directly from Snowflake (inline SQL)."""
    hook = SnowflakeHook()
    query = (
        "select cohort, month_bucket, devices, order_rate, mau_rate, orders "
        "from proddb.fionafan.dp_new_user_orders_performance "
        "order by 1,2"
    )
    print("🔄 Fetching DP new user performance from Snowflake...")
    df = hook.fetch_pandas_all(query)
    print(f"✅ Loaded {len(df)} rows")
    df.columns = [c.lower() for c in df.columns]
    return df


def _order_months(values):
    """Return month buckets in natural order like month1..month12 if possible."""
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


def compute_ratio_vs_baseline(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute ratios vs baselines per month:
    - For cohorts in {dp_first_order, dp_second_order}: ratio vs no_dp_order
    - For cohorts starting with dp_signup_: ratio vs no_dp_signup
    Returns a DataFrame with columns: cohort, month_bucket, order_rate_ratio, mau_rate_ratio
    """
    df = df.copy()
    # Ensure lower-case columns
    df.columns = [c.lower() for c in df.columns]

    # Baselines
    base_order = df[df["cohort"] == "no_dp_order"][
        ["month_bucket", "order_rate", "mau_rate"]
    ].rename(columns={"order_rate": "order_rate_base", "mau_rate": "mau_rate_base"})

    base_signup = df[df["cohort"] == "no_dp_signup"][
        ["month_bucket", "order_rate", "mau_rate"]
    ].rename(columns={"order_rate": "order_rate_base", "mau_rate": "mau_rate_base"})

    if base_order.empty and base_signup.empty:
        raise ValueError("Baseline cohorts 'no_dp_order' and 'no_dp_signup' not found in data")

    # Order cohorts
    order_targets = df[df["cohort"].isin(["dp_first_order", "dp_second_order"])].copy()
    order_ratio = pd.DataFrame(columns=["cohort", "month_bucket", "order_rate_ratio", "mau_rate_ratio"])  # fallback
    if not order_targets.empty and not base_order.empty:
        merged_order = order_targets.merge(base_order, on="month_bucket", how="inner")
        merged_order["order_rate_ratio"] = merged_order["order_rate"] / merged_order["order_rate_base"]
        merged_order["mau_rate_ratio"] = merged_order["mau_rate"] / merged_order["mau_rate_base"]
        order_ratio = merged_order[["cohort", "month_bucket", "order_rate_ratio", "mau_rate_ratio"]]

    # Signup cohorts
    signup_targets = df[df["cohort"].str.startswith("dp_signup") & (df["cohort"] != "no_dp_signup")].copy()
    signup_ratio = pd.DataFrame(columns=["cohort", "month_bucket", "order_rate_ratio", "mau_rate_ratio"])  # fallback
    if not signup_targets.empty and not base_signup.empty:
        merged_signup = signup_targets.merge(base_signup, on="month_bucket", how="inner")
        merged_signup["order_rate_ratio"] = merged_signup["order_rate"] / merged_signup["order_rate_base"]
        merged_signup["mau_rate_ratio"] = merged_signup["mau_rate"] / merged_signup["mau_rate_base"]
        signup_ratio = merged_signup[["cohort", "month_bucket", "order_rate_ratio", "mau_rate_ratio"]]

    ratio_df = pd.concat([order_ratio, signup_ratio], ignore_index=True)
    return ratio_df


def create_subplots(df: pd.DataFrame):
    # Compute ratios vs baselines
    ratio_df = compute_ratio_vs_baseline(df)

    months = _order_months(sorted(ratio_df["month_bucket"].unique()))
    cohorts = sorted(ratio_df["cohort"].unique())

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7), sharex=True)
    fig.suptitle("DP New User: Month-over-Month Ratios vs Baseline", fontsize=16, fontweight="bold")

    # Left: Order Rate Ratio
    for cohort in cohorts:
        sdf = (
            ratio_df[ratio_df["cohort"] == cohort]
            .set_index("month_bucket")
            .reindex(months)
            .reset_index()
        )
        ax1.plot(
            sdf["month_bucket"],
            sdf["order_rate_ratio"],
            marker="o",
            linewidth=2,
            label=cohort,
        )
    ax1.axhline(1.0, color="gray", linewidth=1, linestyle="--", alpha=0.7)
    ax1.set_title("Order Rate Ratio", fontweight="bold")
    ax1.set_xlabel("Month bucket")
    ax1.set_ylabel("Ratio to baseline")
    ax1.grid(axis="y", alpha=0.3)

    # Right: MAU Rate Ratio
    for cohort in cohorts:
        sdf = (
            ratio_df[ratio_df["cohort"] == cohort]
            .set_index("month_bucket")
            .reindex(months)
            .reset_index()
        )
        ax2.plot(
            sdf["month_bucket"],
            sdf["mau_rate_ratio"],
            marker="o",
            linewidth=2,
            label=cohort,
        )
    ax2.axhline(1.0, color="gray", linewidth=1, linestyle="--", alpha=0.7)
    ax2.set_title("MAU Rate Ratio", fontweight="bold")
    ax2.set_xlabel("Month bucket")
    ax2.set_ylabel("Ratio to baseline")
    ax2.grid(axis="y", alpha=0.3)

    # Combined legend
    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(handles, labels, title="Cohort", bbox_to_anchor=(1.01, 0.95), loc="upper left")

    plt.tight_layout()
    return fig


def main():
    df = fetch_dp_new_user_performance()
    fig = create_subplots(df)
    Path(OUTPUT_PNG).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUTPUT_PNG, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"📊 Saved combined plot to: {OUTPUT_PNG}")


if __name__ == "__main__":
    main()


