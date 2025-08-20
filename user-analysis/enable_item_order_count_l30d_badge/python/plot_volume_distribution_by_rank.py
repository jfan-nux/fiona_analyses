#!/usr/bin/env python3
"""
Plot distribution of `pct_order_volume_exp` for top ranks (1-5) across
experiment arms vs control, and flag statistically significant
differences.

For each rank 1-5:
• Show box plots (or violin plots) of the percentage contribution to
  orders for that rank for Control, Icon Treatment and No-Icon Treatment.
• Compute two-sided Mann-Whitney U test between each treatment arm and
  Control; if p-value < 0.05, add an asterisk above the corresponding
  box to indicate significance.

Outputs a single PNG `volume_dist_by_rank.png` in the same directory.

Requirements:
  pip install pandas matplotlib seaborn jinja2 scipy
"""
import os
from typing import List
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import mannwhitneyu
from jinja2 import Template
from dotenv import load_dotenv
from utils.snowflake_connection import SnowflakeHook

# ---------------------------------------------------------------------------
# Load environment variables (SnowflakeHook also loads config/.env)
# ---------------------------------------------------------------------------
repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
load_dotenv(os.path.join(repo_root, "config", ".env"))

# ---------------------------------------------------------------------------
# SQL template – fetch rows for ranks 1-5
# ---------------------------------------------------------------------------
SQL = Template(
    """
    SELECT badge_rank, badge_rank_coalesced, experiment_group, pct_order_volume_exp
    FROM proddb.fionafan.store_item_mixshift_by_arm_d20250725_31_pct_modified
    """
)

RANKS: List[str] = ['1', '2', '3', '4', '5', 'not_ranked']
ARMS_ORDER: List[str] = ["control", "icon treatment", "no icon treatment"]


def fetch_data() -> pd.DataFrame:
    """Query Snowflake and return dataframe with columns in lowercase."""
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(SQL.render(), method="pandas")
    df.columns = df.columns.str.lower()
    return df


def run_stats_and_plot(df: pd.DataFrame, out_path: str):
    """Generate boxplots per rank and annotate significance stars."""
    # Convert pct to actual percentage values for plotting
    df["pct"] = df["pct_order_volume_exp"].astype(float) * 100.0

    sns.set_style("whitegrid")
    fig, axes = plt.subplots(1, len(RANKS), figsize=(18, 4), sharey=True)

    palette = {
        "control": "#1f77b4",          # blue
        "icon treatment": "#ff7f0e",    # orange
        "no icon treatment": "#2ca02c", # green
    }

    for idx, rank in enumerate(RANKS):
        ax = axes[idx]
        sub = df[df["badge_rank_coalesced"] == rank]

        # Plot distributions overlayed (histogram with KDE line)
        for arm in ARMS_ORDER:
            vals = sub[sub["experiment_group"] == arm]["pct"].values
            if len(vals) == 0:
                continue

            sns.histplot(
                vals,
                bins=15,
                stat="density",
                kde=False,
                element="step",
                fill=False,
                color=palette[arm],
                linewidth=1.2,
                ax=ax,
                label=arm.capitalize() if idx == 0 else None,
            )
            # KDE line
            sns.kdeplot(
                vals,
                color=palette[arm],
                linewidth=1.5,
                ax=ax,
                legend=False,
            )

        ax.set_title(f"Rank {rank}")
        if idx == 0:
            ax.set_ylabel("Density")
        else:
            ax.set_ylabel("")
        ax.set_xlabel("% of Orders")

        # Significance tests vs control (Mann-Whitney U)
        ctrl_vals = sub[sub["experiment_group"] == "control"]["pct"].values
        ylim_top = ax.get_ylim()[1]
        xlim_left, xlim_right = ax.get_xlim()
        x_range = xlim_right - xlim_left

        for idx_arm, arm in enumerate(["icon treatment", "no icon treatment"]):
            treat_vals = sub[sub["experiment_group"] == arm]["pct"].values
            if len(treat_vals) == 0 or len(ctrl_vals) == 0:
                continue
            try:
                _, pval = mannwhitneyu(treat_vals, ctrl_vals, alternative="two-sided")
            except ValueError:
                continue

            # if pval < 0.05:
            #     # place stars at right edge with slight horizontal offsets
            #     x_star = xlim_right - (0.05 + 0.05 * idx_arm) * x_range
            #     y_star = ylim_top * 0.9
            #     ax.text(x_star, y_star, "*", ha="center", va="bottom", fontsize=16, color=palette[arm])

        # Annotate mean & median % delta for each treatment vs control
        ctrl_mean = ctrl_vals.mean() if len(ctrl_vals) else np.nan
        ctrl_median = np.median(ctrl_vals) if len(ctrl_vals) else np.nan

        # prepare spacing parameters for annotation block on upper right
        block_spacing = ylim_top * 0.3  # more spacing to avoid overlap
        base_y = ylim_top * 0.95
        for idx_arm, arm in enumerate(["icon treatment", "no icon treatment"]):
            treat_vals = sub[sub["experiment_group"] == arm]["pct"].values
            if len(treat_vals) == 0 or np.isnan(ctrl_mean):
                continue

            mean_delta = (treat_vals.mean() - ctrl_mean) / ctrl_mean * 100
            median_delta = (np.median(treat_vals) - ctrl_median) / ctrl_median * 100

            # significance test
            try:
                _, pval = mannwhitneyu(treat_vals, ctrl_vals, alternative="two-sided")
            except ValueError:
                pval = 1.0

            star = "*" if pval < 0.05 else ""

            txt = (
                f"μΔ={mean_delta:+.1f}%{star}\n"
                f"median Δ={median_delta:+.1f}%{star}"
            )

            # position annotation on upper right
            text_x = xlim_right - 0.15 * x_range
            y_pos = base_y - idx_arm * block_spacing

            ax.text(
                text_x,
                y_pos,
                txt,
                color=palette[arm],
                fontsize=9,
                ha="left",
                va="center",
                bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.25, edgecolor="none"),
            )

    # Add legend once
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper right")

    fig.suptitle("Distribution of % Order Volume by Rank (1-5)")
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def main():
    df = fetch_data()
    out_file = os.path.join(os.path.dirname(__file__), "volume_dist_by_rank.png")
    run_stats_and_plot(df, out_file)


if __name__ == "__main__":
    main()
