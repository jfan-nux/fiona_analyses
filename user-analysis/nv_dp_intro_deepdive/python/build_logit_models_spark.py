import argparse
from typing import List, Tuple

import numpy as np
import pandas as pd

from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression

from utils.logger import get_logger
from utils.snowflake_connection import SnowflakeHook


logger = get_logger(__name__)


def fetch_features_targets(hook: SnowflakeHook, table_fqn: str) -> SparkDataFrame:
    query = f"select * from {table_fqn}"
    df = hook.query_snowflake(query, method="spark")
    return df


def select_covariates(sdf: SparkDataFrame) -> List[str]:
    excluded_prefixes = ["target_"]
    excluded_columns = {"device_id", "consumer_id", "join_time", "join_day"}
    allowed_prefixes = ("had_", "has_", "dp_has_", "minutes_to_")

    numeric_like_types = (
        T.IntegerType,
        T.LongType,
        T.ShortType,
        T.ByteType,
        T.FloatType,
        T.DoubleType,
        T.DecimalType,
    )

    candidates: List[str] = []
    for field in sdf.schema.fields:
        col = field.name
        if col in excluded_columns:
            continue
        if any(col.startswith(p) for p in excluded_prefixes):
            continue
        if "order" in col:
            continue
        if not col.startswith(allowed_prefixes):
            continue
        if isinstance(field.dataType, (T.BooleanType, *numeric_like_types)):
            candidates.append(col)
    return candidates


def select_targets(sdf: SparkDataFrame) -> List[str]:
    preferred = [
        "target_ordered_24h",
        "target_first_order_new_cx",
        "target_ordered_month1",
        "target_ordered_month2",
        "target_ordered_month3",
        "target_ordered_month4",
    ]
    cols = set(sdf.columns)
    return [t for t in preferred if t in cols]


def filter_covariates_for_target(covariates: List[str], target: str) -> List[str]:
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
        filtered.append(c)
    if target == "target_ordered_month3":
        allow = [
            "nv_first_order",
            "nv_second_order",
            "dp_first_order",
            "dp_second_order",
            "nv_order_month3",
            "dp_order_month3",
        ]
        filtered += [c for c in allow if c in covariates]
    if target == "target_ordered_month4":
        allow = [
            "nv_first_order",
            "nv_second_order",
            "dp_first_order",
            "dp_second_order",
            "nv_order_month4",
            "dp_order_month4",
        ]
        filtered += [c for c in allow if c in covariates]
    if target == "target_ordered_month2":
        allow = [
            "nv_first_order",
            "nv_second_order",
            "dp_first_order",
            "dp_second_order",
            "nv_order_month2",
            "dp_order_month2",
        ]
        filtered += [c for c in allow if c in covariates]
    return filtered


def prepare_model_dataframe(
    spark: SparkSession, sdf_all: SparkDataFrame, covariates: List[str], target: str
) -> Tuple[SparkDataFrame, List[str]]:
    cols = list(set(covariates + [target]))
    sdf = sdf_all.select(*cols)

    # Determine zero-variance columns via variance aggregation
    if covariates:
        agg_exprs = [F.variance(F.col(c)).alias(c) for c in covariates]
        var_row = sdf.agg(*agg_exprs).collect()[0]
    else:
        var_row = {}
    usable_covars: List[str] = []
    for c in covariates:
        v = var_row[c] if isinstance(var_row, dict) else getattr(var_row, c)
        try:
            if v is not None and float(v) > 0.0:
                usable_covars.append(c)
        except Exception:
            continue

    if not usable_covars:
        raise ValueError("No usable covariates after preprocessing (all zero-variance or invalid).")

    # Cast covariates to double and impute nulls with 0.0
    for c in usable_covars:
        field = next((f for f in sdf.schema.fields if f.name == c), None)
        if isinstance(field.dataType, T.BooleanType):
            col_double = F.when(F.col(c), F.lit(1.0)).otherwise(F.lit(0.0))
        else:
            col_double = F.col(c).cast(T.DoubleType())
        sdf = sdf.withColumn(c, F.coalesce(col_double, F.lit(0.0)))

    # Prepare label column (double, nulls to 0.0)
    label_double = F.coalesce(F.col(target).cast(T.DoubleType()), F.lit(0.0))
    sdf = sdf.withColumn("label", label_double)

    return sdf.select(*(usable_covars + ["label"])), usable_covars


def fit_one_target_spark(
    spark: SparkSession,
    sdf_all: SparkDataFrame,
    covariates: List[str],
    target: str,
    max_iter: int,
    reg_param: float,
    elastic_net: float,
) -> pd.DataFrame:
    covars_for_target = filter_covariates_for_target(covariates, target)
    sdf, usable_covars = prepare_model_dataframe(spark, sdf_all, covars_for_target, target)

    # Drop rows where label is null (should already be numeric)
    sdf = sdf.filter(F.col("label").isNotNull())
    n_obs = sdf.count()
    if n_obs == 0:
        return pd.DataFrame(
            {
                "covariate": ["<no_data>"],
                "coef": [np.nan],
                "std_err": [np.nan],
                "p_value": [np.nan],
                "conf_low": [np.nan],
                "conf_high": [np.nan],
                "ame": [np.nan],
                "ame_se": [np.nan],
                "ame_z": [np.nan],
                "n_obs": [0],
                "converged": [False],
                "pseudo_r2": [np.nan],
                "target": [target],
            }
        )

    assembler = VectorAssembler(inputCols=usable_covars, outputCol="features", handleInvalid="keep")
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=max_iter,
        regParam=reg_param,
        elasticNetParam=elastic_net,
        standardization=True,
        fitIntercept=True,
    )
    pipeline = Pipeline(stages=[assembler, lr])

    try:
        model = pipeline.fit(sdf)
        lr_model = model.stages[-1]
        coeffs = lr_model.coefficients.toArray().tolist()
        intercept = float(lr_model.intercept)

        # Convergence heuristic: if iterations reached maxIter, we assume non-convergence
        try:
            total_iter = int(lr_model.summary.totalIterations)
        except Exception:
            total_iter = max_iter
        converged = total_iter < max_iter

        # Build output rows (const + covariates)
        rows = []
        rows.append(
            {
                "covariate": "const",
                "coef": intercept,
                "std_err": np.nan,
                "p_value": np.nan,
                "conf_low": np.nan,
                "conf_high": np.nan,
                "ame": np.nan,
                "ame_se": np.nan,
                "ame_z": np.nan,
                "n_obs": n_obs,
                "converged": converged,
                "pseudo_r2": np.nan,
                "target": target,
            }
        )
        for name, beta in zip(usable_covars, coeffs):
            rows.append(
                {
                    "covariate": name,
                    "coef": float(beta),
                    "std_err": np.nan,
                    "p_value": np.nan,
                    "conf_low": np.nan,
                    "conf_high": np.nan,
                    "ame": np.nan,
                    "ame_se": np.nan,
                    "ame_z": np.nan,
                    "n_obs": n_obs,
                    "converged": converged,
                    "pseudo_r2": np.nan,
                    "target": target,
                }
            )

        return pd.DataFrame(rows)
    except Exception as e:
        logger.error(f"Failed model for {target}: {e}")
        return pd.DataFrame(
            {
                "covariate": ["<model_failed>"],
                "coef": [np.nan],
                "std_err": [np.nan],
                "p_value": [np.nan],
                "conf_low": [np.nan],
                "conf_high": [np.nan],
                "ame": [np.nan],
                "ame_se": [np.nan],
                "ame_z": [np.nan],
                "n_obs": [n_obs],
                "converged": [False],
                "pseudo_r2": [np.nan],
                "target": [target],
            }
        )


def run(
    target_table: str,
    output_table: str,
    database: str = "proddb",
    schema: str = "fionafan",
    sample_rate: float = 1.0,
    max_iter: int = 200,
    reg_param: float = 1e-4,
    elastic_net: float = 0.0,
) -> None:
    with SnowflakeHook(database=database, schema=schema, use_persistent_spark=True) as hook:
        spark = hook.spark or (
            SparkSession.builder.appName("nv_dp_logit_models_spark")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        sdf_all = fetch_features_targets(hook, f"{database}.{schema}.{target_table}")

        if 0 < sample_rate < 1:
            sdf_all = sdf_all.sample(withReplacement=False, fraction=sample_rate, seed=42)
            n_rows = sdf_all.count()
            logger.info(f"Sampled {sample_rate:.4f} of rows for modeling: n={n_rows}")
        else:
            sample_rate = 1.0
            n_rows = sdf_all.count()
            logger.info(f"Using full dataset for modeling: n={n_rows}")

        targets = select_targets(sdf_all)
        covariates = select_covariates(sdf_all)
        logger.info(f"Targets: {targets}")
        logger.info(f"Covariates ({len(covariates)}): {covariates}")

        # Missingness audit (Spark)
        if covariates:
            miss_exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in covariates]
            miss_row = sdf_all.agg(*miss_exprs).collect()[0]
            miss_X = int(sum((getattr(miss_row, c) or 0) for c in covariates))
        else:
            miss_X = 0
        logger.info(f"Total missing values in raw covariates: {int(miss_X)}")

        all_results: List[pd.DataFrame] = []
        for target in targets:
            logger.info(f"Fitting Spark logistic regression for target: {target}")
            res = fit_one_target_spark(
                spark=spark,
                sdf_all=sdf_all,
                covariates=covariates,
                target=target,
                max_iter=max_iter,
                reg_param=reg_param,
                elastic_net=elastic_net,
            )
            all_results.append(res)

        if not all_results:
            logger.warning("No results were produced. Skipping write.")
            return

        results_df = pd.concat(all_results, ignore_index=True)

        # Reorder/ensure columns
        col_order = [
            "target",
            "covariate",
            "coef",
            "std_err",
            "p_value",
            "conf_low",
            "conf_high",
            "ame",
            "ame_se",
            "ame_z",
            "n_obs",
            "converged",
            "pseudo_r2",
        ]
        for c in col_order:
            if c not in results_df.columns:
                results_df[c] = np.nan
        results_df = results_df[col_order]

        suffix = "all" if sample_rate >= 1.0 else str(sample_rate).replace(".", "_")
        output_table_final = f"{output_table}_{suffix}_spark"
        hook.create_and_populate_table(results_df, table_name=output_table_final, method="pandas")
        logger.info(f"Wrote {len(results_df)} rows to {database}.{schema}.{output_table_final}")

    spark.stop()


def parse_args():
    parser = argparse.ArgumentParser(description="Run PySpark logistic regressions for NV/DP targets.")
    parser.add_argument(
        "--input_table",
        default="nv_dp_features_targets",
        help="Input table name without database/schema (defaults to nv_dp_features_targets)",
    )
    parser.add_argument(
        "--output_table",
        default="nv_dp_logit_results",
        help="Base output table name (created in same database/schema); _spark suffix added",
    )
    parser.add_argument("--database", default="proddb", help="Snowflake database")
    parser.add_argument("--schema", default="fionafan", help="Snowflake schema")
    parser.add_argument(
        "--sample_rate",
        type=float,
        default=1.0,
        help="Sampling fraction in (0,1]; 1.0 means all",
    )
    parser.add_argument("--max_iter", type=int, default=200, help="Maximum iterations for solver")
    parser.add_argument(
        "--reg_param",
        type=float,
        default=1e-4,
        help="L2 regularization strength (use >0 to mitigate collinearity)",
    )
    parser.add_argument(
        "--elastic_net",
        type=float,
        default=0.0,
        help="Elastic net mixing (0=L2, 1=L1)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        target_table=args.input_table,
        output_table=args.output_table,
        database=args.database,
        schema=args.schema,
        sample_rate=args.sample_rate,
        max_iter=args.max_iter,
        reg_param=args.reg_param,
        elastic_net=args.elastic_net,
    )


