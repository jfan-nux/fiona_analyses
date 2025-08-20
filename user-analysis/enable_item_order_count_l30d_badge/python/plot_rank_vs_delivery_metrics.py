#!/usr/bin/env python3
"""
Plot relationship between cumulative item rank and delivery KPIs for Social Cue experiment.
Generates PNGs under the same directory.
Requires environment variables for Snowflake authentication:
  SF_USER, SF_PASSWORD, SF_ACCOUNT, SF_WAREHOUSE, (optional) SF_ROLE
Install deps: pip install pandas matplotlib jinja2 snowflake-connector-python
"""
import os
from typing import List, Union
import pandas as pd
import matplotlib.pyplot as plt
from jinja2 import Template
from dotenv import load_dotenv

# Local utilities
from utils.snowflake_connection import SnowflakeHook

# Load Snowflake credentials from config/.env if present. SnowflakeHook will also load this file,
# but we keep it for backwards-compatibility with any other local environment usage.
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config", ".env"))

SQL_TEMPLATE = Template(
    """
WITH filtered AS (
    SELECT *
    FROM proddb.fionafan.store_item_mixshift_by_arm_d20250725_31_pct_modified
    {% if rank_filter %}WHERE badge_rank_coalesced IN ({{ rank_filter }}){% endif %}
),
item_kpi AS (
    SELECT f.store_id, f.item_id, f.experiment_group,
           k.pct_ontime_7d AS pct_ontime,
           k.pct_late_7d   AS pct_late,
           k.pct_late20_7d AS pct_late20
    FROM filtered f
    JOIN proddb.fionafan.store_item_delivery_status_by_arm_d20250725_31 k
      ON k.store_id = f.store_id
     AND k.item_id  = f.item_id
     AND k.experiment_group = f.experiment_group
)
SELECT * FROM item_kpi
"""
)


# --- Helper functions using SnowflakeHook -----------------------------------

def fetch_slice(hook: SnowflakeHook, rank_spec: Union[int, str]) -> pd.DataFrame:
    """Run the SQL for a given badge rank specification and return a DataFrame.

    rank_spec can be:
    - An integer (1-5): includes ranks 1 through rank_spec
    - 'all': includes all items (no filtering)
    
    SnowflakeHook converts columns to lowercase by default; for consistency with the
    original plotting logic we convert them back to uppercase immediately.
    """
    if rank_spec == 'all':
        rank_filter = None
    else:
        # Generate string like "'1','2','3'" for rank_spec=3
        rank_values = [f"'{i}'" for i in range(1, rank_spec + 1)]
        rank_filter = ','.join(rank_values)
    
    sql = SQL_TEMPLATE.render(rank_filter=rank_filter)
    df = hook.query_snowflake(sql, method="pandas")
    df.columns = df.columns.str.upper()
    return df


def _calc_sig_z(series_t: pd.Series, series_c: pd.Series) -> float:
    """Return z-score for difference in means between two independent samples."""
    import numpy as np

    mean_t, mean_c = series_t.mean(), series_c.mean()
    var_t, var_c = series_t.var(ddof=1), series_c.var(ddof=1)
    n_t, n_c = len(series_t), len(series_c)

    se = np.sqrt(var_t / n_t + var_c / n_c)
    if se == 0:
        return 0.0
    return (mean_t - mean_c) / se


def collect_data(rank_list: List[Union[int, str]]) -> pd.DataFrame:
    """Collect aggregated deltas & significance for each badge rank specification."""
    frames: List[dict] = []

    with SnowflakeHook() as hook:
        for r in rank_list:
            df = fetch_slice(hook, r)

            # Separate control rows once for each metric computation
            control_df = df[df["EXPERIMENT_GROUP"] == "control"]

            for arm in df["EXPERIMENT_GROUP"].unique():
                if arm == "control":
                    continue
                arm_df = df[df["EXPERIMENT_GROUP"] == arm]

                for metric in ["pct_ontime", "pct_late", "pct_late20"]:
                    # Prepare series as percent (0-1) rather than 0-100 if necessary
                    series_t = arm_df[metric.upper()].astype(float)
                    series_c = control_df[metric.upper()].astype(float)

                    mean_t, mean_c = series_t.mean(), series_c.mean()
                    pct_delta = (mean_t / mean_c - 1) * 100 if mean_c else None  # % delta
                    abs_delta = (mean_t - mean_c) * 100  # absolute percentage-point delta

                    z = _calc_sig_z(series_t, series_c)
                    sig = abs(z) > 1.96

                    frames.append({
                        "badge_rank_spec": r,
                        "arm": arm,
                        "metric": metric,
                        "pct_delta": pct_delta,
                        "abs_delta": abs_delta,
                        "significant": sig,
                    })
    return pd.DataFrame(frames)

def plot_metric(df: pd.DataFrame, metric: str, out_path: str):
    plt.style.use("ggplot")
    fig, ax1 = plt.subplots(figsize=(10,4.5))
    ax2 = ax1.twinx()
    colors = {"icon treatment": "C0", "no icon treatment": "C1"}
    
    # Create a custom ordering for x-axis (numeric first, then 'all')
    rank_order = [1, 2, 3, 4, 5, 'all']
    
    for arm in ["icon treatment", "no icon treatment"]:
        sub = df[(df["metric"]==metric) & (df["arm"]==arm)].copy()
        if sub.empty:
            continue
            
        # Sort by the custom ordering
        sub['sort_key'] = sub['badge_rank_spec'].apply(lambda x: rank_order.index(x) if x in rank_order else 999)
        sub = sub.sort_values('sort_key')
        
        x_positions = range(len(sub))
        x_labels = [str(x) for x in sub['badge_rank_spec']]
        
        ax1.plot(x_positions, sub["pct_delta"], marker="o", color=colors[arm], label=f"{arm} %Δ")
        ax2.bar(x_positions, sub["abs_delta"], color=colors[arm], alpha=0.3, width=0.4, label=f"{arm} absΔ")
        
        for x,y,s in zip(x_positions, sub["pct_delta"], sub["significant"]):
            if s:
                ax1.text(x, y, "*", ha="center", va="bottom", color=colors[arm])
    
    ax1.set_xlabel("Badge Rank Specification (1=rank1 only, 5=ranks1-5, all=all items)")
    ax1.set_ylabel("% delta vs control")
    ax2.set_ylabel("Abs Δ (pct-pts)")
    ax1.set_title(f"{metric} – Treatment vs Control by Badge Rank Inclusion")
    ax1.set_xticks(range(len(rank_order)))
    ax1.set_xticklabels([str(x) for x in rank_order])
    ax1.legend(loc="upper left")
    ax2.legend(loc="upper right")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)

def main():
    rank_list = [1, 2, 3, 4, 5, 'all']
    df = collect_data(rank_list)
    
    # Create plots directory if it doesn't exist
    plots_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'plots')
    os.makedirs(plots_dir, exist_ok=True)
    
    for metric in ["pct_ontime", "pct_late", "pct_late20"]:
        out_file = os.path.join(plots_dir, f"{metric}_badge_rank_vs_delta.png")
        plot_metric(df, metric, out_file)
        print(f"Saved {out_file}")

if __name__ == "__main__":
    main()
