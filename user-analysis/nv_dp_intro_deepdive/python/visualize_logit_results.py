import argparse
import os
import datetime as dt
from typing import List

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from utils.logger import get_logger
from utils.snowflake_connection import SnowflakeHook


logger = get_logger(__name__)


def _ensure_dirs(base_dir: str) -> tuple:
    plots_dir = os.path.join(base_dir, "plots")
    outputs_dir = os.path.join(base_dir, "outputs")
    os.makedirs(plots_dir, exist_ok=True)
    os.makedirs(outputs_dir, exist_ok=True)
    return plots_dir, outputs_dir


def _clean_results(df: pd.DataFrame) -> pd.DataFrame:
    # Standardize column names
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]

    required = {"target", "covariate", "coef", "p_value"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in results: {sorted(missing)}")

    # Drop intercept from plots but keep in CSV
    mask_const = df["covariate"].str.lower().eq("const")
    if mask_const.any():
        logger.info("Excluding 'const' row from visualizations")
    df_plot = df.loc[~mask_const].copy()

    # Ensure AME exists
    if "ame" not in df_plot.columns:
        df_plot["ame"] = np.nan

    return df, df_plot


def _pivot(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    pivot = df.pivot(index="covariate", columns="target", values=value_col)
    # Order covariates by overall magnitude of value_col (descending)
    order = pivot.abs().sum(axis=1).sort_values(ascending=False).index
    pivot = pivot.loc[order]
    return pivot


def _build_annotation(values: pd.DataFrame, pvals: pd.DataFrame, alpha: float, fmt: str = ".2f") -> pd.DataFrame:
    annotated = values.copy()
    ann = annotated.applymap(lambda v: "" if pd.isna(v) else f"{v:{fmt}}")
    # Add significance stars
    stars = pvals.applymap(lambda p: "" if pd.isna(p) else ("***" if p < 0.001 else ("**" if p < 0.01 else ("*" if p < alpha else ""))))
    return ann.where(stars.eq(""), ann + stars)


def plot_heatmap(values: pd.DataFrame,
                 pvals: pd.DataFrame,
                 title: str,
                 out_path: str,
                 center: float = 0.0,
                 cmap: str = "RdBu_r",
                 alpha: float = 0.05,
                 fmt: str = ".2f") -> None:
    plt.figure(figsize=(max(8, values.shape[1] * 1.2), max(6, values.shape[0] * 0.35)))
    sns.set(style="whitegrid", font_scale=0.9)

    # Annotation with stars for significance
    ann = _build_annotation(values, pvals, alpha=alpha, fmt=fmt)

    # Mask for NaNs
    mask = values.isna()

    ax = sns.heatmap(
        values,
        mask=mask,
        cmap=cmap,
        center=center,
        annot=ann,
        fmt="",
        linewidths=0.5,
        linecolor="lightgray",
        cbar_kws={"label": title.split(" – ")[-1]},
    )
    ax.set_title(title)
    ax.set_xlabel("Target")
    ax.set_ylabel("Covariate")
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()


def plot_top_covariates_per_target(df_plot: pd.DataFrame,
                                   metric: str,
                                   alpha: float,
                                   top_k: int,
                                   out_path: str) -> None:
    # Keep significant rows
    df_sig = df_plot.loc[df_plot["p_value"].lt(alpha)].copy()
    if df_sig.empty:
        logger.warning("No significant rows to plot for top covariates per target.")
        return
    df_sig["abs_metric"] = df_sig[metric].abs()

    targets: List[str] = sorted(df_sig["target"].unique().tolist())
    n = len(targets)
    ncols = min(3, n)
    nrows = int(np.ceil(n / ncols))

    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(6 * ncols, 3.5 * nrows), squeeze=False)
    sns.set(style="whitegrid", font_scale=0.95)

    for i, target in enumerate(targets):
        r, c = divmod(i, ncols)
        ax = axes[r][c]
        sub = (
            df_sig.loc[df_sig["target"].eq(target)]
            .nlargest(top_k, columns="abs_metric")
            .sort_values("abs_metric", ascending=True)
        )
        if sub.empty:
            ax.axis("off")
            continue
        ax.barh(sub["covariate"], sub[metric], color=["#2c7fb8" if v >= 0 else "#de2d26" for v in sub[metric]])
        ax.axvline(0, color="black", linewidth=0.8)
        ax.set_title(f"{target} – top {top_k} | {metric}")
        ax.set_xlabel(metric)
        ax.set_ylabel("")

    # Hide unused axes
    for j in range(i + 1, nrows * ncols):
        r, c = divmod(j, ncols)
        axes[r][c].axis("off")

    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()


def main():
    parser = argparse.ArgumentParser(description="Visualize logistic regression results: coef/AME heatmaps, top covariates per target, and CSV export.")
    parser.add_argument("--table", default="proddb.fionafan.nv_dp_logit_results_0_5", help="Fully qualified Snowflake table to read")
    parser.add_argument("--database", default="proddb", help="Snowflake database")
    parser.add_argument("--schema", default="fionafan", help="Snowflake schema")
    parser.add_argument("--alpha", type=float, default=0.05, help="Significance level for p-values")
    parser.add_argument("--top_k", type=int, default=10, help="Top K covariates per target for bar charts")
    parser.add_argument("--output_base", default="/Users/fiona.fan/Documents/fiona_analyses/user-analysis/nv_dp_intro_deepdive", help="Base analysis directory; plots/ and outputs/ will be created here")
    args = parser.parse_args()

    plots_dir, outputs_dir = _ensure_dirs(args.output_base)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    with SnowflakeHook(database=args.database, schema=args.schema) as hook:
        logger.info(f"Loading results from {args.table}")
        df = hook.query_snowflake(f"select * from {args.table}", method="pandas")

    df_all, df_plot = _clean_results(df)

    # Save raw summary CSV for reference
    csv_path = os.path.join(outputs_dir, f"logit_results_summary_{ts}.csv")
    df_all.to_csv(csv_path, index=False)
    logger.info(f"Wrote summary CSV: {csv_path}")

    # Build pivots for coef and AME; align p-value matrix
    coef_pivot = _pivot(df_plot, "coef")
    ame_pivot = _pivot(df_plot, "ame")
    pval_mat = df_plot.pivot(index="covariate", columns="target", values="p_value")
    pval_mat = pval_mat.reindex(index=coef_pivot.index, columns=coef_pivot.columns)

    # Heatmaps
    coef_path = os.path.join(plots_dir, f"coef_heatmap_{ts}.png")
    plot_heatmap(coef_pivot, pval_mat, title="Coefficients – centered at 0", out_path=coef_path, center=0.0, cmap="RdBu_r", alpha=args.alpha, fmt=".3f")
    logger.info(f"Saved coef heatmap: {coef_path}")

    ame_path = os.path.join(plots_dir, f"ame_heatmap_{ts}.png")
    plot_heatmap(ame_pivot, pval_mat, title="Average marginal effects – centered at 0", out_path=ame_path, center=0.0, cmap="RdBu_r", alpha=args.alpha, fmt=".4f")
    logger.info(f"Saved AME heatmap: {ame_path}")

    # Within-target comparison: top covariates per target (by |coef| and by |ame|)
    top_coef_path = os.path.join(plots_dir, f"top_covariates_per_target_coef_{ts}.png")
    plot_top_covariates_per_target(df_plot, metric="coef", alpha=args.alpha, top_k=args.top_k, out_path=top_coef_path)
    logger.info(f"Saved top covariates per target (coef): {top_coef_path}")

    top_ame_path = os.path.join(plots_dir, f"top_covariates_per_target_ame_{ts}.png")
    plot_top_covariates_per_target(df_plot, metric="ame", alpha=args.alpha, top_k=args.top_k, out_path=top_ame_path)
    logger.info(f"Saved top covariates per target (ame): {top_ame_path}")

    logger.info("Done.")


if __name__ == "__main__":
    main()


