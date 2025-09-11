import os
import argparse
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

try:
    import seaborn as sns
    import matplotlib.pyplot as plt
except ImportError as e:
    print("Missing plotting libs. Please install: pip install seaborn matplotlib")
    raise

from utils.logger import get_logger
from utils.snowflake_connection import SnowflakeHook


logger = get_logger(__name__)


def fetch_source_df() -> pd.DataFrame:
    query = """
        SELECT 
            notification_source,
            avg_pushes_per_customer,
            has_geographic_targeting,
            has_cx,
            has_new_verticals_grocery,
            has_cart_abandonment,
            has_mx,
            has_reminders_notifications,
            has_multichannel,
            has_offers_promos,
            has_technical_automation,
            has_communication_channels,
            has_new_customer_experience,
            has_engagement_recommendations,
            has_dashpass,
            has_occasions_seasonal,
            has_other,
            has_churned_reengagement,
            has_customer_segments,
            has_partnerships,
            open_rate,
            avg_ordered,
            uninstall_rate,
            unsubscribed_rate
        FROM proddb.fionafan.notif_campaign_performance
        WHERE LOWER(notification_source) = 'braze'
    """
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(query, method="pandas")
    return df


def prepare_features_with_targets(
    df: pd.DataFrame,
    quantile: float = 0.5,
) -> pd.DataFrame:
    df = df.copy()

    # Binary flags to 0/1 ints
    binary_cols = [
        "has_geographic_targeting",
        "has_cx",
        "has_new_verticals_grocery",
        "has_cart_abandonment",
        "has_mx",
        "has_reminders_notifications",
        "has_multichannel",
        "has_offers_promos",
        "has_technical_automation",
        "has_communication_channels",
        "has_new_customer_experience",
        "has_engagement_recommendations",
        "has_dashpass",
        "has_occasions_seasonal",
        "has_other",
        "has_churned_reengagement",
        "has_customer_segments",
        "has_partnerships",
    ]
    for col in binary_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.lower()
                .replace({"true": 1, "false": 0, "1": 1, "0": 0, "yes": 1, "no": 0})
            )
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Continuous covariate: keep as numeric
    if "avg_pushes_per_customer" in df.columns:
        df["avg_pushes_per_customer"] = pd.to_numeric(df["avg_pushes_per_customer"], errors="coerce")

    # Build four binary targets via quantile threshold
    outcomes = ["open_rate", "avg_ordered", "uninstall_rate", "unsubscribed_rate"]
    for out in outcomes:
        if out in df.columns:
            y = pd.to_numeric(df[out], errors="coerce")
            thr = y.quantile(quantile)
            df[f"target_{out}"] = (y > thr).astype(int)

    return df


def compute_full_correlation(df_feat: pd.DataFrame) -> pd.DataFrame:
    """Compute full Pearson correlation among covariates + four targets.
    - Binary-binary reduces to phi
    - Binary-continuous is point-biserial
    - Continuous-continuous is Pearson
    """
    covariates = [
        "avg_pushes_per_customer",
        "has_geographic_targeting",
        "has_cx",
        "has_new_verticals_grocery",
        "has_cart_abandonment",
        "has_mx",
        "has_reminders_notifications",
        "has_multichannel",
        "has_offers_promos",
        "has_technical_automation",
        "has_communication_channels",
        "has_new_customer_experience",
        "has_engagement_recommendations",
        "has_dashpass",
        "has_occasions_seasonal",
        "has_other",
        "has_churned_reengagement",
        "has_customer_segments",
        "has_partnerships",
    ]
    targets = [
        "target_open_rate",
        "target_avg_ordered",
        "target_uninstall_rate",
        "target_unsubscribed_rate",
    ]
    cols = [c for c in covariates + targets if c in df_feat.columns]
    if not cols:
        return pd.DataFrame()

    df_num = df_feat[cols].apply(pd.to_numeric, errors="coerce")
    corr = df_num.corr(method="pearson")
    return corr


def plot_heatmap(corr: pd.DataFrame, out_path: Path, title: str):
    plt.figure(figsize=(max(10, corr.shape[1] * 0.6), max(8, corr.shape[0] * 0.6)))
    sns.heatmap(
        corr,
        annot=False,
        cmap="coolwarm",
        vmin=-1,
        vmax=1,
        center=0,
        square=False,
        cbar_kws={"shrink": 0.8},
    )
    plt.title(title)
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=200)
    plt.close()


def main():
    parser = argparse.ArgumentParser(description="Covariate vs Targets heatmap (Pearson/phi/point-biserial)")
    parser.add_argument("--quantile", type=float, default=0.5, help="Quantile threshold for binarizing targets (default: 0.5)")
    args = parser.parse_args()

    logger.info("Fetching source data…")
    df_src = fetch_source_df()

    logger.info("Preparing features and four targets…")
    df_feat = prepare_features_with_targets(df_src, quantile=args.quantile)

    logger.info("Computing full correlation among covariates + targets…")
    corr = compute_full_correlation(df_feat)

    csv_path = Path("user-analysis/notification_audit/outputs/covariates_vs_targets_corr.csv")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    corr.to_csv(csv_path)
    logger.info(f"Saved correlation CSV to {csv_path}")

    plot_path = Path("user-analysis/notification_audit/plots/covariates_vs_targets_heatmap.png")
    plot_heatmap(corr, plot_path, title=f"Covariates + Targets Correlation (q={args.quantile})")
    logger.info(f"Saved heatmap to {plot_path}")


if __name__ == "__main__":
    # Run from repo root
    repo_root = Path(__file__).resolve().parents[3]
    os.chdir(repo_root)
    main()


