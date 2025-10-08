import os
from pathlib import Path

import pandas as pd
from pyspark.sql.functions import date_format

from utils.snowflake_connection import SnowflakeHook


def fetch_by_store() -> pd.DataFrame:
    # Single-statement SQL computing numerator/denominator per store
    sql_text = (
        "SELECT\n"
        "  dd.store_id,\n"
        "  dd.store_name,\n"
        "  SUM(core_quality_events.is_pos_error_induced_cancel_category) AS pos_error_cancel_num,\n"
        "  SUM(core_quality_events.is_cancelled_denom)                  AS cancel_denom\n"
        "FROM METRICS_REPO.PUBLIC.special_menu_default_selection___All_Users_IOS_exposures AS exposure\n"
        "LEFT JOIN metrics_repo.public.core_quality_events AS core_quality_events\n"
        "  ON TO_CHAR(exposure.bucket_key) = TO_CHAR(core_quality_events.device_id)\n"
        " /* segment filter removed due to column absence */\n"
        " AND exposure.first_exposure_time <= DATEADD(SECOND, 10, core_quality_events.event_ts)\n"
        "JOIN proddb.public.dimension_deliveries dd\n"
        "  ON dd.delivery_id = core_quality_events.delivery_id\n"
        "  AND exposure.segment = 'All Users IOS'\n"
        "WHERE\n"
        "  CAST(core_quality_events.event_ts AS DATETIME)\n"
        "    BETWEEN CAST('2025-09-16T00:00:00' AS DATETIME) AND CAST('2025-10-30T00:00:00' AS DATETIME)\n"
        "  AND exposure.experiment_group = 'treatment'\n"
        "GROUP BY dd.store_id, dd.store_name\n"
        "HAVING SUM(core_quality_events.is_cancelled_denom) > 0\n"
        "ORDER BY pos_error_cancel_num / NULLIF(cancel_denom, 0) DESC, pos_error_cancel_num DESC"
    )

    with SnowflakeHook() as sf:
        df = sf.query_snowflake(sql_text, method="pandas")
    # Compute rate
    df["cancel_rate"] = df["pos_error_cancel_num"] / df["cancel_denom"]
    # Sort for presentation
    df = df.sort_values(["cancel_rate", "pos_error_cancel_num"], ascending=[False, False])
    # df = df[df['pos_error_cancel_num']>0]
    return df


def main():
    df = fetch_by_store()
    out_dir = Path(__file__).resolve().parents[1] / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "top_merchants_pos_error_cancel.csv"
    df.to_csv(out_path, index=False)
    print(f"Wrote: {out_path}")
    # Preview top 30
    print(df.head(30).to_string(index=False))


if __name__ == "__main__":
    main()


