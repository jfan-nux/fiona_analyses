import os
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

from utils.snowflake_connection import SnowflakeHook


def fetch_hour_counts() -> pd.DataFrame:
    # Single-statement SQL: get distinct delivery_ids from the core query,
    # join to dimension_deliveries to compute local hour
    sql_text = (
        "SELECT\n"
        "  DATE_PART('hour', CONVERT_TIMEZONE('UTC', dd.timezone, dd.actual_order_place_time)) AS local_hour,\n"
        "  COUNT(*) AS deliveries\n"
        "FROM (\n"
        "  SELECT DISTINCT core_quality_events.delivery_id\n"
        "  FROM METRICS_REPO.PUBLIC.special_menu_default_selection___All_Users_IOS_exposures AS exposure\n"
        "  LEFT JOIN metrics_repo.public.core_quality_events AS core_quality_events\n"
        "    ON TO_CHAR(exposure.bucket_key) = TO_CHAR(core_quality_events.device_id)\n"
        "    AND exposure.segment = 'All Users IOS'\n"
        "   AND exposure.first_exposure_time <= DATEADD(SECOND, 10, core_quality_events.event_ts)\n"
        "  WHERE\n"
        "    CAST(core_quality_events.event_ts AS DATETIME) BETWEEN CAST('2025-09-16T00:00:00' AS DATETIME) AND CAST('2025-10-30T00:00:00' AS DATETIME)\n"
        "    AND core_quality_events.is_pos_error_induced_cancel_category = 1\n"
        "    AND exposure.experiment_group = 'treatment'\n"
        ") ids\n"
        "JOIN proddb.public.dimension_deliveries dd\n"
        "  ON dd.delivery_id = ids.delivery_id\n"
        "WHERE dd.actual_order_place_time IS NOT NULL\n"
        "GROUP BY 1\n"
        "ORDER BY 1"
    )

    with SnowflakeHook() as sf:
        df = sf.query_snowflake(sql_text, method="pandas")
    # Ensure integer hour
    df["local_hour"] = df["local_hour"].astype(int)
    return df


def plot_distribution(df: pd.DataFrame, out_path: Path):
    # Ensure all 24 hours present
    full = pd.DataFrame({"local_hour": list(range(24))})
    df = full.merge(df, on="local_hour", how="left").fillna({"deliveries": 0})
    df["deliveries"] = df["deliveries"].astype(int)

    plt.figure(figsize=(10, 5))
    plt.bar(df["local_hour"], df["deliveries"], color="#4C78A8")
    plt.xticks(range(0, 24))
    plt.xlabel("Local Hour of Day")
    plt.ylabel("Deliveries")
    plt.title("Order Placed – Local Hour Distribution (Treatment, POS Error Cancels)")
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=150)
    plt.close()


def fetch_minute_counts() -> pd.DataFrame:
    # Same delivery_ids set; aggregate by minute within the hour
    sql_text = (
        "SELECT\n"
        "  DATE_PART('minute', CONVERT_TIMEZONE('UTC', dd.timezone, dd.actual_order_place_time)) AS local_minute,\n"
        "  COUNT(*) AS deliveries\n"
        "FROM (\n"
        "  SELECT DISTINCT core_quality_events.delivery_id\n"
        "  FROM METRICS_REPO.PUBLIC.special_menu_default_selection___All_Users_IOS_exposures AS exposure\n"
        "  LEFT JOIN metrics_repo.public.core_quality_events AS core_quality_events\n"
        "    ON TO_CHAR(exposure.bucket_key) = TO_CHAR(core_quality_events.device_id)\n"
        "    AND exposure.segment = 'All Users IOS'\n"
        "   AND exposure.first_exposure_time <= DATEADD(SECOND, 10, core_quality_events.event_ts)\n"
        "  WHERE\n"
        "    CAST(core_quality_events.event_ts AS DATETIME) BETWEEN CAST('2025-09-16T00:00:00' AS DATETIME) AND CAST('2025-10-30T00:00:00' AS DATETIME)\n"
        "    AND core_quality_events.is_pos_error_induced_cancel_category = 1\n"
        "    AND exposure.experiment_group = 'treatment'\n"
        ") ids\n"
        "JOIN proddb.public.dimension_deliveries dd\n"
        "  ON dd.delivery_id = ids.delivery_id\n"
        "WHERE dd.actual_order_place_time IS NOT NULL\n"
        "GROUP BY 1\n"
        "ORDER BY 1"
    )

    with SnowflakeHook() as sf:
        df = sf.query_snowflake(sql_text, method="pandas")
    df["local_minute"] = df["local_minute"].astype(int)
    return df


def plot_minute_distribution(df: pd.DataFrame, out_path: Path):
    full = pd.DataFrame({"local_minute": list(range(60))})
    df = full.merge(df, on="local_minute", how="left").fillna({"deliveries": 0})
    df["deliveries"] = df["deliveries"].astype(int)

    plt.figure(figsize=(12, 5))
    plt.bar(df["local_minute"], df["deliveries"], color="#72B7B2")
    plt.xticks(range(0, 60, 5))
    plt.xlabel("Local Minute within Hour")
    plt.ylabel("Deliveries")
    plt.title("Order Placed – Local Minute Distribution (Treatment, POS Error Cancels)")
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=150)
    plt.close()


def main():
    df = fetch_hour_counts()
    out_dir = Path(__file__).resolve().parents[1] / "plots"
    out_path = out_dir / "order_hour_distribution_treatment_pos_error_cancel.png"
    plot_distribution(df, out_path)
    print(f"Saved plot: {out_path}")
    print(df.to_string(index=False))

    # Minute within hour
    df_m = fetch_minute_counts()
    out_path_m = out_dir / "order_minute_distribution_treatment_pos_error_cancel.png"
    plot_minute_distribution(df_m, out_path_m)
    print(f"Saved plot: {out_path_m}")
    print(df_m.to_string(index=False))


if __name__ == "__main__":
    main()


