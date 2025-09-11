import os
import sys
from pathlib import Path
from typing import List, Dict

import numpy as np
import pandas as pd

try:
    import statsmodels.api as sm
    from statsmodels.genmod.families import Binomial
    from statsmodels.stats.outliers_influence import variance_inflation_factor
except ImportError as e:
    print("Missing dependency: statsmodels. Please install it in your venv: pip install statsmodels")
    raise

from utils.logger import get_logger
from utils.snowflake_connection import SnowflakeHook


logger = get_logger(__name__)


def fetch_campaign_performance() -> pd.DataFrame:
    """Fetch campaign performance data from Snowflake."""
    query = """
        SELECT 
            notification_source,
            AVG_PUSHES_PER_CUSTOMER,
            HAS_GEOGRAPHIC_TARGETING,
            HAS_CX,
            HAS_NEW_VERTICALS_GROCERY,
            HAS_CART_ABANDONMENT,
            HAS_MX,
            HAS_REMINDERS_NOTIFICATIONS,
            HAS_MULTICHANNEL,
            HAS_OFFERS_PROMOS,
            HAS_TECHNICAL_AUTOMATION,
            HAS_COMMUNICATION_CHANNELS,
            HAS_NEW_CUSTOMER_EXPERIENCE,
            HAS_ENGAGEMENT_RECOMMENDATIONS,
            HAS_DASHPASS,
            HAS_OCCASIONS_SEASONAL,
            HAS_OTHER,
            HAS_CHURNED_REENGAGEMENT,
            HAS_CUSTOMER_SEGMENTS,
            HAS_PARTNERSHIPS,
            OPEN_RATE,
            AVG_ORDERED,
            UNINSTALL_RATE,
            UNSUBSCRIBED_RATE
        FROM proddb.fionafan.notif_campaign_performance
        WHERE LOWER(notification_source) = 'braze'
    """

    with SnowflakeHook() as hook:
        df = hook.query_snowflake(query, method="pandas")
    return df


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    """Engineer features and clean data for regression."""
    # All columns are lower-cased by SnowflakeHook
    df = df.copy()

    # Ensure expected columns exist; if not, create as NaN
    expected_cols = [
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
        # outcomes
        "open_rate",
        "avg_ordered",
        "uninstall_rate",
        "unsubscribed_rate",
    ]

    for col in expected_cols:
        if col not in df.columns:
            logger.warning(f"Column '{col}' missing in source; filling with NaN")
            df[col] = np.nan

    # Binary flags: coerce to 0/1 and fill NaN with 0
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

    # Numeric predictor: ensure float
    if "avg_pushes_per_customer" in df.columns:
        df["avg_pushes_per_customer"] = pd.to_numeric(
            df["avg_pushes_per_customer"], errors="coerce"
        )

    # Drop rows missing outcomes
    outcomes = ["open_rate", "avg_ordered", "uninstall_rate", "unsubscribed_rate"]
    df = df.dropna(subset=[c for c in outcomes if c in df.columns])

    # Coerce outcomes to numeric
    for out in outcomes:
        if out in df.columns:
            df[out] = pd.to_numeric(df[out], errors="coerce")

    # Final predictor set
    predictors = [
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

    existing_predictors = [c for c in predictors if c in df.columns]
    missing_predictors = [c for c in predictors if c not in df.columns]
    if missing_predictors:
        logger.warning(f"Missing predictors (excluded): {missing_predictors}")

    df = df.dropna(subset=existing_predictors, how="all")

    return df


def compute_vif(X: pd.DataFrame) -> pd.DataFrame:
    """Compute variance inflation factors for X (without constant)."""
    if X.empty:
        return pd.DataFrame(columns=["feature", "vif"])
    X_no_const = X.copy()
    X_no_const = X_no_const.loc[:, X_no_const.columns.notna()]
    X_no_const = X_no_const.select_dtypes(include=[np.number]).fillna(0)
    vif_data = []
    for i in range(X_no_const.shape[1]):
        try:
            vif_val = variance_inflation_factor(X_no_const.values, i)
        except Exception:
            vif_val = np.nan
        vif_data.append({"feature": X_no_const.columns[i], "vif": vif_val})
    return pd.DataFrame(vif_data)


def run_regressions(df: pd.DataFrame) -> pd.DataFrame:
    """Run logistic regression (GLM Binomial with logit link) for each outcome and return tidy results."""
    predictors = [
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
    predictors = [c for c in predictors if c in df.columns]

    outcomes = ["open_rate", "avg_ordered", "uninstall_rate", "unsubscribed_rate"]
    outcomes = [o for o in outcomes if o in df.columns]

    results_rows: List[Dict] = []

    # Prepare X
    X = df[predictors].copy()
    X = X.apply(pd.to_numeric, errors="coerce").fillna(0)
    X_const = sm.add_constant(X, has_constant="add")

    for outcome in outcomes:
        y = pd.to_numeric(df[outcome], errors="coerce")
        # Clip to (0,1) to avoid perfect 0/1 which can cause complete separation
        eps = 1e-6
        y = y.clip(lower=eps, upper=1 - eps)
        mask = y.notna() & X_const.notna().all(axis=1)
        y_clean = y.loc[mask]
        Xc = X_const.loc[mask]

        if y_clean.shape[0] < 10:
            logger.warning(f"Too few rows for outcome {outcome}; skipping")
            continue

        # Full model
        glm_full = sm.GLM(y_clean, Xc, family=Binomial())
        res_full = glm_full.fit()

        # Null model (intercept only) for McFadden pseudo R^2
        glm_null = sm.GLM(y_clean, np.ones((y_clean.shape[0], 1)), family=Binomial())
        res_null = glm_null.fit()
        try:
            pseudo_r2 = 1.0 - (res_full.llf / res_null.llf)
        except Exception:
            pseudo_r2 = np.nan

        # Average marginal effects (AME) for logit: mean_i [ p_i * (1 - p_i) * beta_j ]
        mu = res_full.predict(Xc)
        ame_factor = float(np.mean(mu * (1.0 - mu))) if mu.size > 0 else np.nan

        # Extract coefficients for predictors (exclude constant)
        params = res_full.params.drop(labels=["const"], errors="ignore")
        pvals = res_full.pvalues.drop(labels=["const"], errors="ignore")
        ci = res_full.conf_int().rename(columns={0: "ci_low", 1: "ci_high"})
        ci = ci.drop(labels=["const"], errors="ignore")

        for col in params.index:
            results_rows.append(
                {
                    "outcome": outcome,
                    "predictor": col,
                    "coefficient": params[col],
                    "p_value": pvals.get(col, np.nan),
                    "lower_bound": ci.loc[col, "ci_low"] if col in ci.index else np.nan,
                    "upper_bound": ci.loc[col, "ci_high"] if col in ci.index else np.nan,
                    "ame": params[col] * ame_factor if pd.notna(ame_factor) else np.nan,
                    "pseudo_r_squared": float(pseudo_r2) if pd.notna(pseudo_r2) else np.nan,
                    "n_obs": int(y_clean.shape[0]),
                }
            )

    results_df = pd.DataFrame(results_rows)

    return results_df


def main():
    outdir = Path("user-analysis/notification_audit/outputs")
    outdir.mkdir(parents=True, exist_ok=True)

    logger.info("Fetching notif_campaign_performance from Snowflake…")
    df_raw = fetch_campaign_performance()
    logger.info(f"Fetched shape: {df_raw.shape}")

    logger.info("Preparing features…")
    df = prepare_features(df_raw)
    logger.info(f"Prepared shape: {df.shape}")

    logger.info("Running regressions…")
    results = run_regressions(df)

    results_path = outdir / "campaign_regression_results.csv"
    results.to_csv(results_path, index=False)
    logger.info(f"Saved results to {results_path}")

    # Write selected columns to Snowflake table in proddb.fionafan
    cols_to_write = [
        "outcome",
        "predictor",
        "coefficient",
        "p_value",
        "lower_bound",
        "upper_bound",
        "ame",
        "pseudo_r_squared",
    ]
    df_out = results.loc[:, [c for c in cols_to_write if c in results.columns]].copy()
    # Ensure string columns are strings
    for c in ["outcome", "predictor"]:
        if c in df_out.columns:
            df_out[c] = df_out[c].astype(str)

    target_table = "notif_campaign_performance_factors"
    logger.info(
        f"Writing {df_out.shape[0]} rows to proddb.fionafan.{target_table} (create or replace)"
    )
    with SnowflakeHook(database="proddb", schema="fionafan") as hook:
        hook.create_and_populate_table(
            df=df_out,
            table_name=target_table,
            schema="fionafan",
            database="proddb",
            method="pandas",
        )
    logger.info(
        f"Successfully wrote results to proddb.fionafan.{target_table}"
    )

    # Also print top lines for quick view
    with pd.option_context("display.max_rows", 50, "display.max_columns", 20):
        print(results.head(50))


if __name__ == "__main__":
    # Ensure we run from repo root
    repo_root = Path(__file__).resolve().parents[3]
    os.chdir(repo_root)
    main()


