import argparse
from typing import List, Tuple

import numpy as np
import pandas as pd
import statsmodels.api as sm
from concurrent.futures import ProcessPoolExecutor, as_completed

from utils.logger import get_logger
from utils.snowflake_connection import SnowflakeHook


logger = get_logger(__name__)


def fetch_features_targets(hook: SnowflakeHook, table_fqn: str) -> pd.DataFrame:
    query = f"select * from {table_fqn}"
    df = hook.query_snowflake(query, method="pandas")
    return df


def select_covariates(df: pd.DataFrame) -> List[str]:
    """
    Select covariates while avoiding target leakage and obvious collinearity with targets.

    Rules:
    - Exclude identifier/time fields and all target_* columns
    - Exclude any columns that contain 'order' (e.g., nv_order_*, dp_order_*, first_order_*),
      which leak information about order outcomes
    - Prefer impression/notification features: had_*, has_*, dp_has_*, minutes_to_*
    - Keep only numeric/boolean columns
    """
    excluded_prefixes = ["target_"]
    excluded_columns = {"device_id", "consumer_id", "join_time", "join_day"}

    allowed_prefixes = ("had_", "has_", "dp_has_", "minutes_to_")

    candidates: List[str] = []
    for col in df.columns:
        if col in excluded_columns:
            continue
        if any(col.startswith(p) for p in excluded_prefixes):
            continue
        # Exclude any order-related covariates to prevent leakage/separation
        if "order" in col:
            continue
        # Keep impression/notification/time covariates only
        if not col.startswith(allowed_prefixes):
            continue
        if pd.api.types.is_numeric_dtype(df[col]) or pd.api.types.is_bool_dtype(df[col]):
            candidates.append(col)
    return candidates


def select_targets(df: pd.DataFrame) -> List[str]:
    # Targets created in SQL
    preferred = [
        "target_ordered_24h",
        "target_first_order_new_cx",
        "target_ordered_month1",
        "target_ordered_month2",
        "target_ordered_month3",
        "target_ordered_month4",
    ]
    return [t for t in preferred if t in df.columns]


def fit_one_target(df: pd.DataFrame, covariates: List[str], target: str) -> pd.DataFrame:
    covars_for_target = filter_covariates_for_target(df, covariates, target)
    cols = list(set(covars_for_target + [target]))
    df_sub = df[cols].copy()
    X, _ = impute_and_prepare_X(df_sub, covars_for_target)
    y = df_sub[target].astype(float).fillna(0.0)
    try:
        res = fit_logit_with_margins(y, X)
        res["target"] = target
        return res
    except Exception as e:
        logger.error(f"Failed model for {target}: {e}")
        return pd.DataFrame({
            "covariate": ["<model_failed>"],
            "coef": [np.nan],
            "std_err": [np.nan],
            "p_value": [np.nan],
            "conf_low": [np.nan],
            "conf_high": [np.nan],
            "ame": [np.nan],
            "ame_se": [np.nan],
            "ame_z": [np.nan],
            "n_obs": [int(y.notna().sum())],
            "converged": [False],
            "pseudo_r2": [np.nan],
            "target": [target],
        })

def filter_covariates_for_target(df: pd.DataFrame, covariates: List[str], target: str) -> List[str]:
    disallow_tokens: List[str] = []
    if target == "target_ordered_24h":
        disallow_tokens = ["7d", "30d", "60d"]
    elif target == "target_ordered_month1":
        disallow_tokens = ["60d"]
    else:
        disallow_tokens = []

    filtered = []
    for c in covariates:
        if any(tok in c for tok in disallow_tokens):
            continue
        # Explicitly drop dp_has_notif_any_day for 24h target as requested
        if target == "target_ordered_24h" and c == "dp_has_notif_any_day":
            continue
        filtered.append(c)

    # For month3/4 targets, explicitly include first/second NV/DP and within-window NV/DP flags if available
    df_cols = set(df.columns)
    if target == "target_ordered_month3":
        allow = [
            "nv_first_order", "nv_second_order",
            "dp_first_order", "dp_second_order",
            "nv_order_month3", "dp_order_month3",
        ]
        filtered += [c for c in allow if c in df_cols]
    if target == "target_ordered_month4":
        allow = [
            "nv_first_order", "nv_second_order",
            "dp_first_order", "dp_second_order",
            "nv_order_month4", "dp_order_month4",
        ]
        filtered += [c for c in allow if c in df_cols]
    if target == "target_ordered_month2":
        allow = [
            "nv_first_order", "nv_second_order",
            "dp_first_order", "dp_second_order",
            "nv_order_month2", "dp_order_month2",
        ]
        filtered += [c for c in allow if c in df_cols]
    return filtered


def impute_and_prepare_X(df: pd.DataFrame, covariates: List[str]) -> Tuple[pd.DataFrame, pd.Series]:
    X = df[covariates].copy()

    # Convert booleans to int
    for c in X.columns:
        if pd.api.types.is_bool_dtype(X[c]):
            X[c] = X[c].astype(int)

    # Impute missing with 0 (as requested)
    X = X.fillna(0)

    # Drop zero-variance columns
    variances = X.var(numeric_only=True)
    zero_var_cols = variances[variances == 0].index.tolist()
    if zero_var_cols:
        logger.info(f"Dropping zero-variance columns: {zero_var_cols}")
        X = X.drop(columns=zero_var_cols)

    # Drop exact-duplicate columns to mitigate perfect multicollinearity
    X = X.loc[:, ~X.T.duplicated()]

    # Standardize to reduce numerical overflow (z-score), skip if std == 0
    means = X.mean(numeric_only=True)
    stds = X.std(numeric_only=True).replace(0, 1.0)
    X = (X - means) / stds

    # Add intercept after scaling
    X = sm.add_constant(X, has_constant="add")
    medians = pd.Series(dtype=float)  # placeholder to keep return signature
    return X, medians


def fit_logit_with_margins(y: pd.Series, X: pd.DataFrame):
    # Drop rows with NA in y
    mask = ~y.isna()
    y_fit = y.loc[mask]
    X_fit = X.loc[mask]

    # If target has no variance, skip
    if y_fit.nunique() < 2:
        raise ValueError("Target has no variance after NA drop.")

    model = sm.Logit(y_fit, X_fit)
    result = model.fit(disp=0, method="lbfgs", maxiter=200, cov_type="HC3")

    # Coefficients and inference
    params = result.params
    bse = result.bse
    pvalues = result.pvalues
    conf_int = result.conf_int(alpha=0.05)
    conf_int.columns = ["conf_low", "conf_high"]

    # Average marginal effects (overall)
    try:
        margeff = result.get_margeff(at="overall")
        me_frame = margeff.summary_frame()
        # Standardize column names
        me_frame = me_frame.rename(columns={
            "dy/dx": "ame",
            "Std. Err.": "ame_se",
            "z": "ame_z",
        })
    except Exception as e:
        logger.warning(f"Failed to compute marginal effects: {e}")
        me_frame = pd.DataFrame()

    # Assemble results per covariate (include const too)
    out = (
        pd.concat([
            params.rename("coef").to_frame(),
            bse.rename("std_err").to_frame(),
            pvalues.rename("p_value").to_frame(),
            conf_int,
        ], axis=1)
        .reset_index()
        .rename(columns={"index": "covariate"})
    )

    # Merge AME when available (margins excludes const)
    if not me_frame.empty:
        me_frame = me_frame.reset_index().rename(columns={"index": "covariate"})
        # Ensure expected columns exist even if statsmodels omits some in this version
        for col in ["ame", "ame_se", "ame_z"]:
            if col not in me_frame.columns:
                me_frame[col] = np.nan
        out = out.merge(
            me_frame[[
                "covariate",
                "ame",
                "ame_se",
                "ame_z",
            ]],
            on="covariate",
            how="left",
        )
    else:
        out["ame"] = np.nan
        out["ame_se"] = np.nan
        out["ame_z"] = np.nan

    # Add N and other meta
    out["n_obs"] = int(len(y_fit))
    out["converged"] = bool(result.mle_retvals.get("converged", True))
    # McFadden pseudo R-squared
    try:
        out["pseudo_r2"] = float(result.prsquared)
    except Exception:
        out["pseudo_r2"] = np.nan
    return out


def run(target_table: str,
        output_table: str,
        database: str = "proddb",
        schema: str = "fionafan",
        sample_rate: float = 1.0,
        num_workers: int = 1) -> None:
    with SnowflakeHook(database=database, schema=schema) as hook:
        df = fetch_features_targets(hook, f"{database}.{schema}.{target_table}")

        # Optional sampling
        if 0 < sample_rate < 1 and len(df) > 0:
            df = df.sample(frac=sample_rate, random_state=42).reset_index(drop=True)
            logger.info(f"Sampled {sample_rate:.4f} of rows for modeling: n={len(df)}")
        else:
            sample_rate = 1.0
            logger.info(f"Using full dataset for modeling: n={len(df)}")

        targets = select_targets(df)
        covariates = select_covariates(df)
        logger.info(f"Targets: {targets}")
        logger.info(f"Covariates ({len(covariates)}): {covariates}")

        # Missingness audit
        miss_X = df[covariates].isna().sum().sum()
        miss_any = int(miss_X)
        logger.info(f"Total missing values in raw covariates: {miss_any}")

        all_results: List[pd.DataFrame] = []

        if num_workers and num_workers > 1:
            logger.info(f"Parallel fitting with {num_workers} workers…")
            with ProcessPoolExecutor(max_workers=num_workers) as ex:
                futures = {ex.submit(fit_one_target, df, covariates, t): t for t in targets}
                for fut in as_completed(futures):
                    all_results.append(fut.result())
        else:
            for target in targets:
                logger.info(f"Fitting logistic regression for target: {target}")
                all_results.append(fit_one_target(df, covariates, target))

        results_df = pd.concat(all_results, ignore_index=True)

        # Reorder columns
        col_order = [
            "target", "covariate", "coef", "std_err", "p_value", "conf_low", "conf_high",
            "ame", "ame_se", "ame_z",
            "n_obs", "converged", "pseudo_r2"
        ]
        results_df = results_df[col_order]

        # Write results to Snowflake with sample suffix
        suffix = "all" if sample_rate >= 1.0 else str(sample_rate).replace('.', '_')
        output_table_final = f"{output_table}_{suffix}"
        hook.create_and_populate_table(results_df, table_name=output_table_final, method="pandas")
        logger.info(f"Wrote {len(results_df)} rows to {database}.{schema}.{output_table_final}")


def parse_args():
    parser = argparse.ArgumentParser(description="Run logistic regressions for NV/DP targets.")
    parser.add_argument("--input_table", default="nv_dp_features_targets",
                        help="Fully qualified input table name without database/schema (defaults to nv_dp_features_targets)")
    parser.add_argument("--output_table", default="nv_dp_logit_results",
                        help="Output table name (created in same database/schema)")
    parser.add_argument("--database", default="proddb", help="Snowflake database")
    parser.add_argument("--schema", default="fionafan", help="Snowflake schema")
    parser.add_argument("--sample_rate", type=float, default=1.0, help="Sampling fraction in (0,1]; 1.0 means all")
    parser.add_argument("--num_workers", type=int, default=1, help="Parallel workers for distributed fitting")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        target_table=args.input_table,
        output_table=args.output_table,
        database=args.database,
        schema=args.schema,
        sample_rate=args.sample_rate,
        num_workers=args.num_workers,
    )


