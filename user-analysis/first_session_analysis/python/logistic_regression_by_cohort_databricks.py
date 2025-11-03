# Databricks notebook source
# MAGIC %md
# MAGIC # Session Conversion Prediction by Cohort - Logistic Regression Models (PySpark)
# MAGIC
# MAGIC Builds logistic regression models to predict session conversion using PySpark.
# MAGIC Models are built separately for each cohort type (new, active, post_onboarding).
# MAGIC
# MAGIC **Key Steps**:
# MAGIC 1. Load session features from Snowflake (sampled data)
# MAGIC 2. Train Pre-Funnel Models (browsing behavior only) - 3 separate models by cohort
# MAGIC 3. Train Post-Funnel Models (with funnel progression) - 3 separate models by cohort
# MAGIC 4. Compare coefficients across cohorts for pre-funnel
# MAGIC 5. Compare coefficients across cohorts for post-funnel
# MAGIC 6. Save all model coefficients to Snowflake

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Required Packages

# COMMAND ----------

# Restart Python to load new packages
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from typing import List, Tuple

# Set plotting style
plt.style.use('default')
sns.set_palette("husl")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snowflake Connection Setup

# COMMAND ----------

# Snowflake connection setup
your_scope = "fionafan-scope"
user = dbutils.secrets.get(scope=your_scope, key="snowflake-user")
password = dbutils.secrets.get(scope=your_scope, key="snowflake-password")
default_schema = "fionafan"

OPTIONS = dict(
    sfurl="doordash.snowflakecomputing.com/",
    sfuser=user,
    sfpassword=password,
    sfaccount="DOORDASH",
    sfdatabase="PRODDB",
    sfrole=default_schema,
    sfwarehouse="TEAM_DATA_ANALYTICS_ETL",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Features

# COMMAND ----------

# All features (will be split into pre-funnel and post-funnel)
ALL_FEATURES = [
    'ONBOARDING_DAY_OF_WEEK',
    'NUM_UNIQUE_STORES',
    'IMPRESSION_UNIQUE_STORES',
    'ACTION_HAD_FOREGROUND',
    'ACTION_HAD_ADDRESS',
    'ACTION_HAD_AUTH',
    'ATTRIBUTION_CNT_TRADITIONAL_CAROUSEL',
    'ATTRIBUTION_CNT_CORE_SEARCH',
    'ATTRIBUTION_CNT_PILL_FILTER',
    'ATTRIBUTION_CNT_CUISINE_FILTER',
    'ATTRIBUTION_CNT_DOUBLEDASH',
    'ATTRIBUTION_CNT_HOME_FEED',
    'ATTRIBUTION_CNT_BANNER',
    'ATTRIBUTION_CNT_OFFERS',
    'ATTRIBUTION_CNT_TAB_EXPLORE',
    'ATTRIBUTION_CNT_TAB_ME',
    'ATTRIBUTION_ENTROPY',
    'ERROR_HAD_ANY',
    'FUNNEL_HAD_ADD',
    'FUNNEL_REACHED_STORE_BOOL',
    'FUNNEL_REACHED_CART_BOOL',
    'FUNNEL_SECONDS_TO_FIRST_ADD',
    'FUNNEL_SECONDS_TO_FIRST_ORDER',
    'FUNNEL_SECONDS_TO_STORE_PAGE',
    'FUNNEL_SECONDS_TO_CART',
    'STORE_NV_IMPRESSION_OCCURRED',
]

# Pre-funnel: exclude all funnel_* features
PRE_FUNNEL_FEATURES = [f for f in ALL_FEATURES if not f.upper().startswith('FUNNEL_')]

# Post-funnel: use all features
POST_FUNNEL_FEATURES = ALL_FEATURES.copy()

print(f"📊 Feature Sets Defined:")
print(f"  - Pre-Funnel Features: {len(PRE_FUNNEL_FEATURES)}")
print(f"  - Post-Funnel Features: {len(POST_FUNNEL_FEATURES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# Load session features from Snowflake
# Sample 2000 users and include ALL their sessions for faster testing
query = """
WITH sampled_users AS (
    SELECT DISTINCT user_id
    FROM proddb.fionafan.all_user_sessions_with_events_features_gen
    WHERE funnel_converted_bool IS NOT NULL
        AND user_id IS NOT NULL
        AND session_type != 'first_session'
    ORDER BY RANDOM()
    LIMIT 2000
)
SELECT s.*
FROM proddb.fionafan.all_user_sessions_with_events_features_gen s
INNER JOIN sampled_users u ON s.user_id = u.user_id
WHERE s.funnel_converted_bool IS NOT NULL
    AND s.session_type != 'first_session'
"""

df_spark = (
    spark.read.format("snowflake")
    .options(**OPTIONS)
    .option("query", query)
    .load()
)

session_count = df_spark.count()
conversion_rate = df_spark.agg(F.avg("funnel_converted_bool")).collect()[0][0]

print(f"Loaded {session_count:,} sessions")
print(f"Conversion rate: {conversion_rate:.2%}")

# Cache for reuse
df_spark.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cohort Distribution

# COMMAND ----------

print("📊 Cohort Distribution:\n")
print(f"{'Cohort Type':<20} {'Total Sessions':<15} {'Converted':<12} {'Conv. Rate':<12}")
print("-" * 60)

cohort_summary = df_spark.groupBy("cohort_type").agg(
    F.count("*").alias("total_sessions"),
    F.sum("funnel_converted_bool").alias("converted"),
    F.avg("funnel_converted_bool").alias("conv_rate")
).orderBy("cohort_type").collect()

for row in cohort_summary:
    cohort = row['cohort_type']
    total = row['total_sessions']
    converted = row['converted']
    conv_rate = row['conv_rate']
    print(f"{cohort:<20} {total:<15,} {converted:<12,} {conv_rate:<12.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Modeling Functions

# COMMAND ----------

def prepare_model_dataframe(
    spark: SparkSession, 
    sdf_all: SparkDataFrame,
    cohort_type: str,
    covariates: List[str], 
    target: str
) -> Tuple[SparkDataFrame, List[str]]:
    """Prepare dataframe for modeling with feature engineering and validation"""
    
    # Filter for cohort
    sdf = sdf_all.filter(F.col("COHORT_TYPE") == cohort_type)
    
    # Check available features (case-insensitive)
    sdf_columns_upper = [c.upper() for c in sdf.columns]
    column_name_map = {c.upper(): c for c in sdf.columns}
    
    available_features = []
    missing_features = []
    
    for feat in covariates:
        feat_upper = feat.upper()
        if feat_upper in sdf_columns_upper:
            available_features.append(column_name_map[feat_upper])
        else:
            missing_features.append(feat)
    
    if missing_features:
        print(f"  ⚠️  Missing {len(missing_features)} features: {missing_features[:5]}")
    
    # Select columns
    target_col = column_name_map.get(target.upper(), target)
    cols = list(set(available_features + [target_col]))
    sdf = sdf.select(*cols)
    
    # Determine zero-variance columns via variance aggregation
    if available_features:
        agg_exprs = [F.variance(F.col(c)).alias(c) for c in available_features]
        var_row = sdf.agg(*agg_exprs).collect()[0]
    else:
        var_row = {}
    
    usable_covars = []
    zero_var_features = []
    
    for c in available_features:
        v = var_row[c] if isinstance(var_row, dict) else getattr(var_row, c)
        try:
            if v is not None and float(v) > 0.0:
                usable_covars.append(c)
            else:
                zero_var_features.append(c)
        except Exception:
            continue
    
    if zero_var_features:
        print(f"  ⚠️  Excluding {len(zero_var_features)} zero-variance features")
    
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
    label_double = F.coalesce(F.col(target_col).cast(T.DoubleType()), F.lit(0.0))
    sdf = sdf.withColumn("label", label_double)
    
    print(f"  ✅ Prepared {len(usable_covars)} features for {cohort_type}")
    
    return sdf.select(*(usable_covars + ["label"])), usable_covars

# COMMAND ----------

def fit_logistic_model_spark(
    spark: SparkSession,
    sdf_all: SparkDataFrame,
    cohort_type: str,
    covariates: List[str],
    target: str,
    max_iter: int,
    reg_param: float,
    model_name: str,
) -> pd.DataFrame:
    """Fit a logistic regression model using PySpark for a specific cohort"""
    print(f"\n{'='*80}")
    print(f"Training {model_name} - {cohort_type.upper()} Cohort")
    print(f"{'='*80}")
    
    sdf, usable_covars = prepare_model_dataframe(spark, sdf_all, cohort_type, covariates, target)
    
    # Drop rows where label is null
    sdf = sdf.filter(F.col("label").isNotNull())
    n_obs = sdf.count()
    
    if n_obs == 0:
        print(f"  ⚠️  No observations for {cohort_type}")
        return pd.DataFrame({
            "model_name": [model_name],
            "cohort_type": [cohort_type],
            "covariate": ["<no_data>"],
            "coefficient": [np.nan],
            "odds_ratio_1sd": [np.nan],
            "pct_impact_1sd": [np.nan],
            "n_obs": [0],
            "converged": [False],
            "target": [target],
        })
    
    print(f"  📊 Dataset: {n_obs:,} observations")
    print(f"  📊 Features: {len(usable_covars)}")
    
    # Split data for train/test
    train_df, test_df = sdf.randomSplit([0.8, 0.2], seed=42)
    train_count = train_df.count()
    test_count = test_df.count()
    
    print(f"  📊 Train: {train_count:,} | Test: {test_count:,}")
    
    # Assemble features and standardize
    assembler = VectorAssembler(inputCols=usable_covars, outputCol="features_raw", handleInvalid="keep")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=max_iter,
        regParam=reg_param,
        elasticNetParam=0.0,
        standardization=False,
        fitIntercept=True,
    )
    pipeline = Pipeline(stages=[assembler, scaler, lr])
    
    try:
        # Fit model
        print(f"  🔄 Training model...")
        model = pipeline.fit(train_df)
        lr_model = model.stages[-1]
        coeffs = lr_model.coefficients.toArray().tolist()
        intercept = float(lr_model.intercept)
        
        # Evaluate on test set
        predictions = model.transform(test_df)
        
        # Calculate metrics
        evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
        test_auc = evaluator.evaluate(predictions)
        
        # Convergence check
        try:
            total_iter = int(lr_model.summary.totalIterations)
        except Exception:
            total_iter = max_iter
        converged = total_iter < max_iter
        
        print(f"  ✅ Test AUC-ROC: {test_auc:.4f}")
        print(f"  ✅ Converged: {converged} (iterations: {total_iter}/{max_iter})")
        
        # Build output rows
        rows = []
        rows.append({
            "model_name": model_name,
            "cohort_type": cohort_type,
            "covariate": "const",
            "coefficient": intercept,
            "odds_ratio_1sd": np.exp(intercept),
            "pct_impact_1sd": (np.exp(intercept) - 1) * 100,
            "n_obs": n_obs,
            "test_count": test_count,
            "test_auc": test_auc,
            "converged": converged,
            "iterations": total_iter,
            "target": target,
        })
        
        for name, beta in zip(usable_covars, coeffs):
            odds_ratio = np.exp(beta)
            pct_impact = (odds_ratio - 1) * 100
            
            rows.append({
                "model_name": model_name,
                "cohort_type": cohort_type,
                "covariate": name,
                "coefficient": float(beta),
                "odds_ratio_1sd": odds_ratio,
                "pct_impact_1sd": pct_impact,
                "n_obs": n_obs,
                "test_count": test_count,
                "test_auc": test_auc,
                "converged": converged,
                "iterations": total_iter,
                "target": target,
            })
        
        return pd.DataFrame(rows)
        
    except Exception as e:
        print(f"  ❌ Failed to fit {model_name} - {cohort_type}: {e}")
        return pd.DataFrame({
            "model_name": [model_name],
            "cohort_type": [cohort_type],
            "covariate": ["<model_failed>"],
            "coefficient": [np.nan],
            "odds_ratio_1sd": [np.nan],
            "pct_impact_1sd": [np.nan],
            "n_obs": [n_obs],
            "test_count": [0],
            "test_auc": [np.nan],
            "converged": [False],
            "iterations": [0],
            "target": [target],
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Pre-Funnel Models (by Cohort)

# COMMAND ----------

target = "FUNNEL_CONVERTED_BOOL"
max_iter = 1000
reg_param = 1.0

# Train Pre-Funnel Model for NEW cohort
res_pre_new = fit_logistic_model_spark(
    spark=spark,
    sdf_all=df_spark,
    cohort_type="new",
    covariates=PRE_FUNNEL_FEATURES,
    target=target,
    max_iter=max_iter,
    reg_param=reg_param,
    model_name="Pre-Funnel Model",
)

# COMMAND ----------

# Train Pre-Funnel Model for ACTIVE cohort
res_pre_active = fit_logistic_model_spark(
    spark=spark,
    sdf_all=df_spark,
    cohort_type="active",
    covariates=PRE_FUNNEL_FEATURES,
    target=target,
    max_iter=max_iter,
    reg_param=reg_param,
    model_name="Pre-Funnel Model",
)

# COMMAND ----------

# Train Pre-Funnel Model for POST_ONBOARDING cohort
res_pre_post = fit_logistic_model_spark(
    spark=spark,
    sdf_all=df_spark,
    cohort_type="post_onboarding",
    covariates=PRE_FUNNEL_FEATURES,
    target=target,
    max_iter=max_iter,
    reg_param=reg_param,
    model_name="Pre-Funnel Model",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Post-Funnel Models (by Cohort)

# COMMAND ----------

# Train Post-Funnel Model for NEW cohort
res_post_new = fit_logistic_model_spark(
    spark=spark,
    sdf_all=df_spark,
    cohort_type="new",
    covariates=POST_FUNNEL_FEATURES,
    target=target,
    max_iter=max_iter,
    reg_param=reg_param,
    model_name="Post-Funnel Model",
)

# COMMAND ----------

# Train Post-Funnel Model for ACTIVE cohort
res_post_active = fit_logistic_model_spark(
    spark=spark,
    sdf_all=df_spark,
    cohort_type="active",
    covariates=POST_FUNNEL_FEATURES,
    target=target,
    max_iter=max_iter,
    reg_param=reg_param,
    model_name="Post-Funnel Model",
)

# COMMAND ----------

# Train Post-Funnel Model for POST_ONBOARDING cohort
res_post_post = fit_logistic_model_spark(
    spark=spark,
    sdf_all=df_spark,
    cohort_type="post_onboarding",
    covariates=POST_FUNNEL_FEATURES,
    target=target,
    max_iter=max_iter,
    reg_param=reg_param,
    model_name="Post-Funnel Model",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine Results

# COMMAND ----------

# Combine all results
results_df = pd.concat([
    res_pre_new, res_pre_active, res_pre_post,
    res_post_new, res_post_active, res_post_post
], ignore_index=True)

# Reorder columns
col_order = [
    "model_name",
    "cohort_type",
    "target",
    "covariate",
    "coefficient",
    "odds_ratio_1sd",
    "pct_impact_1sd",
    "n_obs",
    "test_count",
    "test_auc",
    "converged",
    "iterations",
]
for c in col_order:
    if c not in results_df.columns:
        results_df[c] = np.nan
results_df = results_df[col_order]

# Add timestamp
results_df['created_at'] = datetime.now()

print(f"\n📊 Total results: {len(results_df)} rows")
print(f"   - Pre-Funnel Models: {len(res_pre_new) + len(res_pre_active) + len(res_pre_post)} coefficients")
print(f"   - Post-Funnel Models: {len(res_post_new) + len(res_post_active) + len(res_post_post)} coefficients")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Performance Comparison

# COMMAND ----------

# Compare model performance
print(f"\n{'='*80}")
print("MODEL PERFORMANCE COMPARISON")
print(f"{'='*80}")
print(f"\n{'Model':<25} {'Cohort':<15} {'Test AUC':<12} {'N Obs':<12} {'N Features':<12}")
print("-" * 80)

for model_name in ["Pre-Funnel Model", "Post-Funnel Model"]:
    for cohort in ['new', 'active', 'post_onboarding']:
        metrics_df = results_df[
            (results_df['model_name'] == model_name) &
            (results_df['cohort_type'] == cohort) &
            (results_df['covariate'] == 'const')
        ]
        if len(metrics_df) > 0:
            metrics = metrics_df.iloc[0]
            n_features = len(results_df[
                (results_df['model_name'] == model_name) &
                (results_df['cohort_type'] == cohort) &
                (results_df['covariate'] != 'const')
            ])
            print(f"{model_name:<25} {cohort:<15} {metrics['test_auc']:<12.4f} "
                  f"{metrics['n_obs']:<12,.0f} {n_features:<12}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-Funnel Model: Top Features by Cohort

# COMMAND ----------

print(f"\n{'='*100}")
print("PRE-FUNNEL MODEL - TOP 15 FEATURES BY COHORT")
print(f"{'='*100}")

for cohort in ['new', 'active', 'post_onboarding']:
    print(f"\n{cohort.upper()} COHORT:")
    print("-" * 100)
    
    cohort_results = results_df[
        (results_df['model_name'] == 'Pre-Funnel Model') &
        (results_df['cohort_type'] == cohort) &
        (results_df['covariate'] != 'const')
    ].copy()
    
    cohort_results['abs_pct_impact'] = cohort_results['pct_impact_1sd'].abs()
    cohort_results = cohort_results.sort_values('abs_pct_impact', ascending=False)
    
    print(f"{'Rank':<6} {'Feature':<50} {'Coef':<10} {'Odds Ratio':<12} {'% Impact':<12}")
    print("-" * 100)
    
    for rank, (idx, row) in enumerate(cohort_results.head(15).iterrows(), 1):
        print(f"{rank:<6} {row['covariate']:<50} {row['coefficient']:>9.4f} "
              f"{row['odds_ratio_1sd']:>11.4f} {row['pct_impact_1sd']:>11.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Funnel Model: Top Features by Cohort

# COMMAND ----------

print(f"\n{'='*100}")
print("POST-FUNNEL MODEL - TOP 15 FEATURES BY COHORT")
print(f"{'='*100}")

for cohort in ['new', 'active', 'post_onboarding']:
    print(f"\n{cohort.upper()} COHORT:")
    print("-" * 100)
    
    cohort_results = results_df[
        (results_df['model_name'] == 'Post-Funnel Model') &
        (results_df['cohort_type'] == cohort) &
        (results_df['covariate'] != 'const')
    ].copy()
    
    cohort_results['abs_pct_impact'] = cohort_results['pct_impact_1sd'].abs()
    cohort_results = cohort_results.sort_values('abs_pct_impact', ascending=False)
    
    print(f"{'Rank':<6} {'Feature':<50} {'Coef':<10} {'Odds Ratio':<12} {'% Impact':<12}")
    print("-" * 100)
    
    for rank, (idx, row) in enumerate(cohort_results.head(15).iterrows(), 1):
        print(f"{rank:<6} {row['covariate']:<50} {row['coefficient']:>9.4f} "
              f"{row['odds_ratio_1sd']:>11.4f} {row['pct_impact_1sd']:>11.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize Pre-Funnel Model Comparison

# COMMAND ----------

# Filter for Pre-Funnel models
plot_df_pre = results_df[
    (results_df['model_name'] == 'Pre-Funnel Model') &
    (results_df['covariate'] != 'const') &
    (results_df['covariate'] != '<no_data>') &
    (results_df['covariate'] != '<model_failed>')
].copy()

if len(plot_df_pre) > 0:
    # Get top features by average absolute coefficient
    feature_importance = plot_df_pre.groupby('covariate')['coefficient'].apply(
        lambda x: x.abs().mean()
    ).sort_values(ascending=False)
    
    top_features = feature_importance.head(20).index.tolist()
    
    # Plot: Coefficient comparison for top features
    fig, ax = plt.subplots(figsize=(14, 10))
    
    plot_data = plot_df_pre[plot_df_pre['covariate'].isin(top_features)].copy()
    
    # Pivot for easier plotting
    pivot_df = plot_data.pivot(index='covariate', columns='cohort_type', values='coefficient')
    pivot_df = pivot_df.reindex(top_features)
    
    # Create grouped bar chart
    x = np.arange(len(top_features))
    width = 0.25
    cohorts = pivot_df.columns.tolist()
    colors = {'new': '#ff7f0e', 'active': '#1f77b4', 'post_onboarding': '#2ca02c'}
    
    for i, cohort in enumerate(cohorts):
        offset = (i - len(cohorts)/2 + 0.5) * width
        values = pivot_df[cohort].values
        ax.barh(x + offset, values, width, 
                label=cohort.replace('_', ' ').title(),
                color=colors.get(cohort, None),
                alpha=0.8)
    
    ax.set_yticks(x)
    ax.set_yticklabels(top_features, fontsize=9)
    ax.set_xlabel('Coefficient', fontsize=11, fontweight='bold')
    ax.set_title('PRE-FUNNEL MODEL: Top 20 Features - Coefficient Comparison Across Cohorts', 
                 fontsize=13, fontweight='bold')
    ax.legend(loc='best', fontsize=10)
    ax.axvline(x=0, color='black', linestyle='-', linewidth=0.8)
    ax.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    display(fig)
else:
    print("No Pre-Funnel features to plot")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize Post-Funnel Model Comparison

# COMMAND ----------

# Filter for Post-Funnel models
plot_df_post = results_df[
    (results_df['model_name'] == 'Post-Funnel Model') &
    (results_df['covariate'] != 'const') &
    (results_df['covariate'] != '<no_data>') &
    (results_df['covariate'] != '<model_failed>')
].copy()

if len(plot_df_post) > 0:
    # Get top features by average absolute coefficient
    feature_importance = plot_df_post.groupby('covariate')['coefficient'].apply(
        lambda x: x.abs().mean()
    ).sort_values(ascending=False)
    
    top_features = feature_importance.head(20).index.tolist()
    
    # Plot: Coefficient comparison for top features
    fig, ax = plt.subplots(figsize=(14, 10))
    
    plot_data = plot_df_post[plot_df_post['covariate'].isin(top_features)].copy()
    
    # Pivot for easier plotting
    pivot_df = plot_data.pivot(index='covariate', columns='cohort_type', values='coefficient')
    pivot_df = pivot_df.reindex(top_features)
    
    # Create grouped bar chart
    x = np.arange(len(top_features))
    width = 0.25
    cohorts = pivot_df.columns.tolist()
    colors = {'new': '#ff7f0e', 'active': '#1f77b4', 'post_onboarding': '#2ca02c'}
    
    for i, cohort in enumerate(cohorts):
        offset = (i - len(cohorts)/2 + 0.5) * width
        values = pivot_df[cohort].values
        ax.barh(x + offset, values, width, 
                label=cohort.replace('_', ' ').title(),
                color=colors.get(cohort, None),
                alpha=0.8)
    
    ax.set_yticks(x)
    ax.set_yticklabels(top_features, fontsize=9)
    ax.set_xlabel('Coefficient', fontsize=11, fontweight='bold')
    ax.set_title('POST-FUNNEL MODEL: Top 20 Features - Coefficient Comparison Across Cohorts', 
                 fontsize=13, fontweight='bold')
    ax.legend(loc='best', fontsize=10)
    ax.axvline(x=0, color='black', linestyle='-', linewidth=0.8)
    ax.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    display(fig)
else:
    print("No Post-Funnel features to plot")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Individual Cohort Plots - Pre-Funnel

# COMMAND ----------

if len(plot_df_pre) > 0:
    fig, axes = plt.subplots(1, 3, figsize=(20, 10))
    
    for idx, cohort in enumerate(['new', 'active', 'post_onboarding']):
        ax = axes[idx]
        
        cohort_data = plot_df_pre[plot_df_pre['cohort_type'] == cohort].copy()
        cohort_data['abs_coef'] = cohort_data['coefficient'].abs()
        cohort_data = cohort_data.sort_values('abs_coef', ascending=False).head(15)
        
        # Create color based on sign
        colors_bar = ['#d62728' if x < 0 else '#2ca02c' for x in cohort_data['coefficient']]
        
        # Horizontal bar plot
        y_pos = np.arange(len(cohort_data))
        ax.barh(y_pos, cohort_data['coefficient'], color=colors_bar, alpha=0.7)
        
        # Customize
        ax.set_yticks(y_pos)
        ax.set_yticklabels(cohort_data['covariate'], fontsize=8)
        ax.invert_yaxis()
        ax.set_xlabel('Coefficient', fontsize=10, fontweight='bold')
        ax.set_title(f'Pre-Funnel: {cohort.replace("_", " ").title()}\nTop 15 Features', 
                    fontsize=11, fontweight='bold')
        ax.axvline(x=0, color='black', linestyle='-', linewidth=0.8)
        ax.grid(axis='x', alpha=0.3)
        
        # Add value labels
        for i, value in enumerate(cohort_data['coefficient']):
            x_pos = value + (0.02 if value > 0 else -0.02)
            ha = 'left' if value > 0 else 'right'
            ax.text(x_pos, i, f'{value:+.3f}', va='center', ha=ha, fontsize=7)
    
    plt.tight_layout()
    display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Individual Cohort Plots - Post-Funnel

# COMMAND ----------

if len(plot_df_post) > 0:
    fig, axes = plt.subplots(1, 3, figsize=(20, 10))
    
    for idx, cohort in enumerate(['new', 'active', 'post_onboarding']):
        ax = axes[idx]
        
        cohort_data = plot_df_post[plot_df_post['cohort_type'] == cohort].copy()
        cohort_data['abs_coef'] = cohort_data['coefficient'].abs()
        cohort_data = cohort_data.sort_values('abs_coef', ascending=False).head(15)
        
        # Create color based on sign
        colors_bar = ['#d62728' if x < 0 else '#2ca02c' for x in cohort_data['coefficient']]
        
        # Horizontal bar plot
        y_pos = np.arange(len(cohort_data))
        ax.barh(y_pos, cohort_data['coefficient'], color=colors_bar, alpha=0.7)
        
        # Customize
        ax.set_yticks(y_pos)
        ax.set_yticklabels(cohort_data['covariate'], fontsize=8)
        ax.invert_yaxis()
        ax.set_xlabel('Coefficient', fontsize=10, fontweight='bold')
        ax.set_title(f'Post-Funnel: {cohort.replace("_", " ").title()}\nTop 15 Features', 
                    fontsize=11, fontweight='bold')
        ax.axvline(x=0, color='black', linestyle='-', linewidth=0.8)
        ax.grid(axis='x', alpha=0.3)
        
        # Add value labels
        for i, value in enumerate(cohort_data['coefficient']):
            x_pos = value + (0.02 if value > 0 else -0.02)
            ha = 'left' if value > 0 else 'right'
            ax.text(x_pos, i, f'{value:+.3f}', va='center', ha=ha, fontsize=7)
    
    plt.tight_layout()
    display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results to Snowflake

# COMMAND ----------

# Convert to Spark DataFrame
results_spark = spark.createDataFrame(results_df)

# Save to Snowflake
output_table = "session_conversion_by_cohort_model_results"

results_spark.write.format("snowflake") \
    .options(**OPTIONS) \
    .option("dbtable", f"proddb.fionafan.{output_table}") \
    .mode("overwrite") \
    .save()

print(f"\n✅ Saved {len(results_df)} rows to proddb.fionafan.{output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis Complete

# COMMAND ----------

print("\n" + "="*80)
print("ANALYSIS COMPLETE!")
print("="*80)
print(f"\n📊 Results Summary:")
print(f"   - Total models trained: 6 (3 cohorts × 2 model types)")
print(f"   - Pre-Funnel Models: new, active, post_onboarding")
print(f"   - Post-Funnel Models: new, active, post_onboarding")
print(f"\n💾 Results saved to:")
print(f"   - Table: proddb.fionafan.{output_table}")
print(f"\n🎯 Key Insights:")
print(f"   1. Compare Pre-Funnel coefficients to see browsing behavior differences")
print(f"   2. Compare Post-Funnel coefficients to see full journey differences")
print(f"   3. Look for features that matter more for specific cohorts")
print(f"   4. Use insights to personalize experiences by cohort type")

# COMMAND ----------



