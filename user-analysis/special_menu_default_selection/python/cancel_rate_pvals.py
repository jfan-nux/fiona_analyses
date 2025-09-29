import os
from pathlib import Path
from typing import Dict, List

import numpy as np
import math
import pandas as pd

from utils.snowflake_connection import SnowflakeHook


def two_proportion_z_test(x1: int, n1: int, x2: int, n2: int) -> Dict[str, float]:
    """
    Compute two-proportion z-test (pooled) between group1 and group2.

    Returns dict with: p1, p2, diff, se, z, p_value
    """
    p1 = x1 / n1 if n1 else 0.0
    p2 = x2 / n2 if n2 else 0.0
    # Pooled proportion
    pooled_num = x1 + x2
    pooled_den = n1 + n2
    p = pooled_num / pooled_den if pooled_den else 0.0
    se = np.sqrt(p * (1 - p) * (1 / n1 + 1 / n2)) if n1 and n2 and 0 < p < 1 else np.nan
    z = (p1 - p2) / se if se and not np.isnan(se) and se > 0 else np.nan
    # Two-sided p-value from normal approximation
    # p = 2 * (1 - Phi(|z|)) = erfc(|z|/sqrt(2))
    if np.isnan(z):
        pval = np.nan
    else:
        pval = float(math.erfc(abs(z) / math.sqrt(2)))
    return {
        "p1": p1,
        "p2": p2,
        "diff": p1 - p2,
        "se": se,
        "z": z,
        "p_value": pval,
    }


def fetch_aggregates(sql_text: str) -> pd.DataFrame:
    with SnowflakeHook() as sf:
        # Use pandas connector with a single-statement query
        df = sf.query_snowflake(sql_text, method="pandas")
    return df


def identify_groups(df: pd.DataFrame, group_col: str = "experiment_group") -> Dict[str, str]:
    groups = sorted(df[group_col].dropna().unique().tolist())
    if len(groups) < 2:
        raise ValueError("Expected at least two experiment groups (e.g., treatment and control)")
    # Heuristic: prioritize common labels; fallback to first two sorted
    mapping = {}
    lower = [g.lower() for g in groups]
    if "treatment" in lower and "control" in lower:
        mapping["treatment"] = groups[lower.index("treatment")]
        mapping["control"] = groups[lower.index("control")]
    else:
        mapping["control"], mapping["treatment"] = groups[0], groups[1]
    return mapping


def compute_rates_pvals(df: pd.DataFrame,
                        denom_col: str = "cancel_denominator",
                        numer_cols: List[str] = None,
                        group_col: str = "experiment_group") -> pd.DataFrame:
    if numer_cols is None:
        numer_cols = [
            "cancelled_core_numerator",
            "mx_induced_cancel_numerator",
            "cx_induced_cancel_numerator",
            "logistics_induced_cancel_numerator",
            "pos_error_induced_cancel_numerator",
            "ottl_cancel_numerator",
            "store_closed_cancel_numerator",
            "pos_proactive_cancel_numerator",
            "item_unavailable_cancel_numerator",
            "wrong_dx_handoff_cancel_numerator",
        ]

    # Identify control/treatment labels
    cg = identify_groups(df, group_col=group_col)

    # Aggregate just to be safe (in case multiple rows per group)
    agg = df.groupby(group_col, as_index=False).sum(numeric_only=True)

    try:
        g_control = agg.loc[agg[group_col] == cg["control"]].iloc[0]
        g_treat = agg.loc[agg[group_col] == cg["treatment"]].iloc[0]
    except IndexError:
        raise ValueError("Unable to locate both control and treatment groups in the results.")

    n1 = int(g_treat[denom_col])
    n2 = int(g_control[denom_col])

    rows = []
    for col in numer_cols:
        x1 = int(g_treat[col]) if col in g_treat else 0
        x2 = int(g_control[col]) if col in g_control else 0
        stats = two_proportion_z_test(x1, n1, x2, n2)
        rows.append({
            "metric": col,
            "treatment_n": n1,
            "control_n": n2,
            "treatment_x": x1,
            "control_x": x2,
            "treatment_rate": stats["p1"],
            "control_rate": stats["p2"],
            "rate_diff": stats["diff"],
            "se": stats["se"],
            "z": stats["z"],
            "p_value": stats["p_value"],
        })

    result = pd.DataFrame(rows)
    # Sort by p-value ascending
    result = result.sort_values("p_value", na_position="last").reset_index(drop=True)
    return result


def main():
    # Inline single-statement SQL (no CTEs, no semicolons)
    sql_text = (
        "SELECT\n"
        "  experiment_group,\n"
        "  SUM(core_quality_cancel_common_denominator)        AS cancel_denominator,\n"
        "  SUM(core_quality_cancel_common_denominator_count)  AS cancel_denominator_count,\n"
        "  SUM(core_quality_cancelled_core_numerator)         AS cancelled_core_numerator,\n"
        "  SUM(core_quality_mx_induced_cancel_numerator)      AS mx_induced_cancel_numerator,\n"
        "  SUM(core_quality_cx_induced_cancel_numerator)      AS cx_induced_cancel_numerator,\n"
        "  SUM(core_quality_logistics_induced_cancel_numerator) AS logistics_induced_cancel_numerator,\n"
        "  SUM(core_quality_pos_error_induced_cancel_numerator) AS pos_error_induced_cancel_numerator,\n"
        "  SUM(core_quality_ottl_cancel_numerator)            AS ottl_cancel_numerator,\n"
        "  SUM(core_quality_store_closed_cancel_numerator)    AS store_closed_cancel_numerator,\n"
        "  SUM(core_quality_pos_proactive_cancel_numerator)   AS pos_proactive_cancel_numerator,\n"
        "  SUM(core_quality_item_unavailable_cancel_numerator) AS item_unavailable_cancel_numerator,\n"
        "  SUM(core_quality_wrong_dx_handoff_cancel_numerator) AS wrong_dx_handoff_cancel_numerator\n"
        "FROM (\n"
        "  SELECT\n"
        "    exposure.experiment_group,\n"
        "    SUM(is_cancelled_core)                                   AS core_quality_cancelled_core_numerator,\n"
        "    SUM(is_mx_induced_cancel_category)                       AS core_quality_mx_induced_cancel_numerator,\n"
        "    SUM(is_cx_induced_cancel_category)                       AS core_quality_cx_induced_cancel_numerator,\n"
        "    SUM(is_logistics_induced_cancel_category)                AS core_quality_logistics_induced_cancel_numerator,\n"
        "    SUM(is_pos_error_induced_cancel_category)                AS core_quality_pos_error_induced_cancel_numerator,\n"
        "    SUM(is_ottl_cancel)                                      AS core_quality_ottl_cancel_numerator,\n"
        "    SUM(is_store_closed_cancel)                              AS core_quality_store_closed_cancel_numerator,\n"
        "    SUM(is_pos_proactive_cancel)                             AS core_quality_pos_proactive_cancel_numerator,\n"
        "    SUM(is_item_unavailable_cancel)                          AS core_quality_item_unavailable_cancel_numerator,\n"
        "    SUM(is_wrong_dx_handoff_cancel)                          AS core_quality_wrong_dx_handoff_cancel_numerator,\n"
        "    SUM(is_cancelled_denom)                                  AS core_quality_cancel_common_denominator,\n"
        "    COUNT(is_cancelled_denom)                                AS core_quality_cancel_common_denominator_count\n"
        "  FROM METRICS_REPO.PUBLIC.special_menu_default_selection___All_Users_IOS_exposures AS exposure\n"
        "  LEFT JOIN metrics_repo.public.core_quality_events AS core_quality_events\n"
        "    ON TO_CHAR(exposure.bucket_key) = TO_CHAR(core_quality_events.device_id)\n"
        "   AND exposure.first_exposure_time <= DATEADD(SECOND, 10, core_quality_events.event_ts)\n"
        "  WHERE CAST(core_quality_events.event_ts AS DATETIME)\n"
        "        BETWEEN CAST('2025-09-16T00:00:00' AS DATETIME) AND CAST('2025-10-30T00:00:00' AS DATETIME)\n"
        "  GROUP BY exposure.experiment_group\n"
        ") t\n"
        "GROUP BY experiment_group\n"
        "ORDER BY experiment_group"
    )

    # Execute query and compute stats
    df = fetch_aggregates(sql_text)
    result = compute_rates_pvals(df)

    # Save to outputs
    out_dir = Path(__file__).resolve().parents[1] / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "cancel_rate_pvals.csv"
    result.to_csv(out_path, index=False)

    # Print a small preview
    print(f"Wrote: {out_path}")
    print(result.head(20).to_string(index=False))


if __name__ == "__main__":
    main()


