import argparse
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

try:
    import seaborn as sns
    import matplotlib.pyplot as plt
except ImportError as e:
    raise

from utils.logger import get_logger
from utils.snowflake_connection import SnowflakeHook


logger = get_logger(__name__)


def select_covariates(df: pd.DataFrame) -> List[str]:
    excluded_prefixes = ["target_"]
    excluded_columns = {"device_id", "consumer_id", "join_time", "join_day"}
    allowed_prefixes = ("had_", "has_", "dp_has_", "minutes_to_")

    covars: List[str] = []
    for col in df.columns:
        if col in excluded_columns:
            continue
        if any(col.startswith(p) for p in excluded_prefixes):
            continue
        if "order" in col:
            continue
        if not col.startswith(allowed_prefixes):
            continue
        if pd.api.types.is_numeric_dtype(df[col]) or pd.api.types.is_bool_dtype(df[col]):
            covars.append(col)
    return covars


def select_targets(df: pd.DataFrame) -> List[str]:
    preferred = [
        "target_ordered_24h",
        "target_first_order_new_cx",
        "target_ordered_month1",
        "target_ordered_month2",
        "target_ordered_month3",
        "target_ordered_month4",
    ]
    return [t for t in preferred if t in df.columns]


def fetch_df(hook: SnowflakeHook, table_fqn: str) -> pd.DataFrame:
    q = f"select * from {table_fqn}"
    df = hook.query_snowflake(q, method="pandas")
    return df


def compute_corr(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    # Coerce to numeric, impute missing with 0 as requested
    X = df[cols].apply(pd.to_numeric, errors="coerce")
    X = X.fillna(0)
    corr = X.corr(method="pearson")
    return corr


def melt_corr(corr: pd.DataFrame) -> pd.DataFrame:
    long_df = (
        corr.reset_index()
            .melt(id_vars="index", var_name="var_col", value_name="correlation")
            .rename(columns={"index": "var_row"})
    )
    return long_df


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
    parser = argparse.ArgumentParser(description="EDA heatmap for covariates and targets")
    parser.add_argument("--input_fqn", default="proddb.fionafan.nv_dp_features_targets")
    parser.add_argument("--corr_table", default="nv_dp_eda_covariate_target_corr")
    parser.add_argument("--database", default="proddb")
    parser.add_argument("--schema", default="fionafan")
    args = parser.parse_args()

    with SnowflakeHook(database=args.database, schema=args.schema) as hook:
        df = fetch_df(hook, args.input_fqn)
        covars = select_covariates(df)
        targets = select_targets(df)
        cols = covars + targets
        logger.info(f"EDA columns ({len(cols)}): {cols}")

        corr = compute_corr(df, cols)

        # Save CSV locally
        csv_path = Path("user-analysis/nv_dp_intro_deepdive/outputs/covariates_targets_corr.csv")
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        corr.to_csv(csv_path)
        logger.info(f"Saved correlation CSV to {csv_path}")

        # Plot heatmap
        plot_path = Path("user-analysis/nv_dp_intro_deepdive/plots/covariates_targets_heatmap.png")
        plot_heatmap(corr, plot_path, title="Covariates + Targets Correlation")
        logger.info(f"Saved heatmap to {plot_path}")

        # Write long-form correlation to Snowflake
        long_corr = melt_corr(corr)
        hook.create_and_populate_table(long_corr, table_name=args.corr_table, method="pandas")
        logger.info(f"Wrote correlation table to {args.database}.{args.schema}.{args.corr_table}")


if __name__ == "__main__":
    main()


