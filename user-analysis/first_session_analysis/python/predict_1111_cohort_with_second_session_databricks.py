# Databricks notebook source
# MAGIC %md
# MAGIC # Predict 1111 Cohort Membership from First Session Features + Second Session Return
# MAGIC
# MAGIC Builds logistic regression models to predict if a user will be in the 1111 cohort
# MAGIC (users active 1, 1, 1, 1 weeks after onboarding) based on their first session behavior
# MAGIC **PLUS whether they had a second session**.
# MAGIC
# MAGIC **Models are built separately for each onboarding cohort:**
# MAGIC - New users
# MAGIC - Active users  
# MAGIC - Post-onboarding users
# MAGIC
# MAGIC **Key Steps:**
# MAGIC 1. Load first session features from Snowflake
# MAGIC 2. Add second session feature (second_session_id IS NOT NULL)
# MAGIC 3. Join with 1111 cohort membership (target)
# MAGIC 4. Build separate logistic regression models for each cohort type
# MAGIC 5. Compare coefficients across cohorts
# MAGIC 6. Save results to Snowflake

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
# MAGIC ## Define Features (Including Second Session Return)

# COMMAND ----------

FEATURES = [
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
    'FUNNEL_REACHED_CHECKOUT_BOOL',
    'FUNNEL_CONVERTED_BOOL',
    'FUNNEL_SECONDS_TO_FIRST_ADD',
    'FUNNEL_SECONDS_TO_FIRST_ORDER',
    'FUNNEL_SECONDS_TO_STORE_PAGE',
    'FUNNEL_SECONDS_TO_CART',
    'FUNNEL_SECONDS_TO_CHECKOUT',
    'FUNNEL_SECONDS_TO_SUCCESS',
    'STORE_NV_IMPRESSION_OCCURRED',
    'HAD_SECOND_SESSION',  # NEW FEATURE
]

print(f"📊 Total features defined: {len(FEATURES)}")
print(f"   - Includes HAD_SECOND_SESSION feature (second_session_id IS NOT NULL)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data with 1111 Cohort Target and Second Session

# COMMAND ----------

# Build feature list for query (excluding the second session feature which comes from enriched table)
first_session_features = [f for f in FEATURES if f != 'HAD_SECOND_SESSION']
feature_cols = ", ".join([f"fs.{feat}" for feat in first_session_features])

query = f"""
WITH cohort_1111 AS (
    SELECT DISTINCT consumer_id
    FROM proddb.fionafan.users_activity_1111
)
SELECT 
    fs.user_id,
    fs.dd_device_id,
    fs.cohort_type,
    {feature_cols},
    CASE WHEN e.second_session_id IS NOT NULL THEN 1 ELSE 0 END as had_second_session,
    CASE WHEN c.consumer_id IS NOT NULL THEN 1 ELSE 0 END as is_1111_cohort
FROM proddb.fionafan.all_user_sessions_with_events_features_gen fs
LEFT JOIN proddb.fionafan.all_user_sessions_enriched e 
    ON fs.user_id = e.consumer_id
LEFT JOIN cohort_1111 c 
    ON fs.user_id = c.consumer_id
WHERE fs.session_type = 'first_session'
    AND fs.cohort_type IS NOT NULL
    AND fs.user_id IS NOT NULL
"""

print("📊 Loading first session data with second session feature...")

df_spark = (
    spark.read.format("snowflake")
    .options(**OPTIONS)
    .option("query", query)
    .load()
)

total_count = df_spark.count()
print(f"\n✅ Loaded {total_count:,} first sessions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cohort Distribution Summary

# COMMAND ----------

# Show distribution by cohort type
print("📊 Cohort Distribution:\n")
print(f"{'Cohort Type':<20} {'Total Sessions':<15} {'In 1111':<12} {'% in 1111':<12} {'Had 2nd Session':<15}")
print("-" * 80)

cohort_summary = df_spark.groupBy("cohort_type").agg(
    F.count("*").alias("total_sessions"),
    F.sum("is_1111_cohort").alias("in_1111"),
    F.sum("had_second_session").alias("had_second_session")
).orderBy("cohort_type").collect()

for row in cohort_summary:
    cohort = row['cohort_type']
    total = row['total_sessions']
    in_1111 = row['in_1111']
    had_2nd = row['had_second_session']
    pct = 100 * in_1111 / total if total > 0 else 0
    print(f"{cohort:<20} {total:<15,} {in_1111:<12,} {pct:<12.2f}% {had_2nd:<15,}")

# Cache for reuse
df_spark.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Modeling Functions

# COMMAND ----------

def prepare_model_dataframe(
    spark: SparkSession, 
    sdf_all: SparkDataFrame, 
    cohort_type: str,
    covariates: List[str], 
    target: str = "IS_1111_COHORT"
) -> Tuple[SparkDataFrame, List[str]]:
    """
    Prepare dataframe for modeling with feature engineering and validation
    """
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
    cols = available_features + [target_col]
    sdf = sdf.select(*cols)
    
    # Determine zero-variance columns
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
) -> pd.DataFrame:
    """
    Fit a logistic regression model using PySpark for a specific cohort
    """
    print(f"\n{'='*80}")
    print(f"Training Model for {cohort_type.upper()} Cohort")
    print(f"{'='*80}")
    
    sdf, usable_covars = prepare_model_dataframe(
        spark, sdf_all, cohort_type, covariates, target
    )
    
    # Drop rows where label is null
    sdf = sdf.filter(F.col("label").isNotNull())
    n_obs = sdf.count()
    
    if n_obs == 0:
        print(f"  ⚠️  No observations for {cohort_type}")
        return pd.DataFrame({
            "cohort_type": [cohort_type],
            "feature": ["<no_data>"],
            "coefficient": [np.nan],
            "odds_ratio": [np.nan],
            "pct_impact": [np.nan],
            "n_obs": [0],
            "converged": [False],
        })
    
    # Calculate base rate
    n_positive = sdf.filter(F.col("label") == 1).count()
    base_rate = n_positive / n_obs if n_obs > 0 else 0
    
    print(f"  📊 Dataset: {n_obs:,} observations")
    print(f"  📊 Positive rate: {base_rate:.2%} ({n_positive:,} / {n_obs:,})")
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
        standardization=False,  # Already standardized
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
        
        # Intercept
        rows.append({
            "cohort_type": cohort_type,
            "feature": "intercept",
            "coefficient": intercept,
            "odds_ratio": np.exp(intercept),
            "pct_impact": (np.exp(intercept) - 1) * 100,
            "n_obs": n_obs,
            "n_positive": n_positive,
            "base_rate": base_rate * 100,
            "test_count": test_count,
            "test_auc": test_auc,
            "converged": converged,
            "iterations": total_iter,
            "n_features": len(usable_covars),
        })
        
        # Features
        for name, beta in zip(usable_covars, coeffs):
            odds_ratio = np.exp(beta)
            pct_impact = (odds_ratio - 1) * 100
            
            rows.append({
                "cohort_type": cohort_type,
                "feature": name,
                "coefficient": float(beta),
                "odds_ratio": odds_ratio,
                "pct_impact": pct_impact,
                "n_obs": n_obs,
                "n_positive": n_positive,
                "base_rate": base_rate * 100,
                "test_count": test_count,
                "test_auc": test_auc,
                "converged": converged,
                "iterations": total_iter,
                "n_features": len(usable_covars),
            })
        
        return pd.DataFrame(rows)
        
    except Exception as e:
        print(f"  ❌ Failed to fit {cohort_type} model: {e}")
        return pd.DataFrame({
            "cohort_type": [cohort_type],
            "feature": ["<model_failed>"],
            "coefficient": [np.nan],
            "odds_ratio": [np.nan],
            "pct_impact": [np.nan],
            "n_obs": [n_obs],
            "n_positive": [n_positive],
            "base_rate": [base_rate * 100],
            "test_count": [0],
            "test_auc": [np.nan],
            "converged": [False],
            "iterations": [0],
            "n_features": [0],
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Models for Each Cohort

# COMMAND ----------

target = "IS_1111_COHORT"
max_iter = 1000
reg_param = 1.0

# Train model for NEW cohort
res_new = fit_logistic_model_spark(
    spark=spark,
    sdf_all=df_spark,
    cohort_type="new",
    covariates=FEATURES,
    target=target,
    max_iter=max_iter,
    reg_param=reg_param,
)

# COMMAND ----------

# Train model for ACTIVE cohort
res_active = fit_logistic_model_spark(
    spark=spark,
    sdf_all=df_spark,
    cohort_type="active",
    covariates=FEATURES,
    target=target,
    max_iter=max_iter,
    reg_param=reg_param,
)

# COMMAND ----------

# Train model for POST_ONBOARDING cohort
res_post_onboarding = fit_logistic_model_spark(
    spark=spark,
    sdf_all=df_spark,
    cohort_type="post_onboarding",
    covariates=FEATURES,
    target=target,
    max_iter=max_iter,
    reg_param=reg_param,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine Results

# COMMAND ----------

# Combine all results
results_df = pd.concat([res_new, res_active, res_post_onboarding], ignore_index=True)

# Add timestamp
results_df['created_at'] = datetime.now()

print(f"\n📊 Total results: {len(results_df)} rows")
print(f"   - New cohort: {len(res_new)} coefficients")
print(f"   - Active cohort: {len(res_active)} coefficients")
print(f"   - Post-onboarding cohort: {len(res_post_onboarding)} coefficients")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Performance Comparison

# COMMAND ----------

print(f"\n{'='*80}")
print("MODEL PERFORMANCE COMPARISON")
print(f"{'='*80}")
print(f"\n{'Cohort':<20} {'N Obs':<12} {'Positive %':<12} {'Test AUC':<12} {'N Features':<12}")
print("-" * 80)

for cohort in ['new', 'active', 'post_onboarding']:
    cohort_metrics = results_df[
        (results_df['cohort_type'] == cohort) & 
        (results_df['feature'] == 'intercept')
    ]
    if len(cohort_metrics) > 0:
        metrics = cohort_metrics.iloc[0]
        print(f"{cohort:<20} {metrics['n_obs']:<12,.0f} "
              f"{metrics['base_rate']:<12.2f} "
              f"{metrics['test_auc']:<12.4f} "
              f"{metrics['n_features']:<12.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Features by Cohort (including HAD_SECOND_SESSION)

# COMMAND ----------

print(f"\n{'='*80}")
print("TOP 10 FEATURES BY COHORT (by absolute coefficient)")
print(f"{'='*80}")

for cohort in ['new', 'active', 'post_onboarding']:
    print(f"\n{cohort.upper()} COHORT:")
    print("-" * 80)
    cohort_data = results_df[
        (results_df['cohort_type'] == cohort) &
        (results_df['feature'] != 'intercept') &
        (results_df['feature'] != '<no_data>') &
        (results_df['feature'] != '<model_failed>')
    ].copy()
    
    cohort_data['abs_coef'] = cohort_data['coefficient'].abs()
    cohort_data = cohort_data.sort_values('abs_coef', ascending=False).head(10)
    
    print(f"{'Rank':<6} {'Feature':<45} {'Coef':<10} {'% Impact':<12}")
    print("-" * 80)
    for rank, (idx, row) in enumerate(cohort_data.iterrows(), 1):
        feat_display = row['feature']
        if 'HAD_SECOND_SESSION' in feat_display.upper():
            feat_display = feat_display + ' ⭐'  # Mark the new feature
        print(f"{rank:<6} {feat_display:<45} {row['coefficient']:>9.4f} {row['pct_impact']:>11.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check HAD_SECOND_SESSION Feature Coefficient

# COMMAND ----------

print(f"\n{'='*80}")
print("HAD_SECOND_SESSION FEATURE ANALYSIS")
print(f"{'='*80}")
print(f"\n{'Cohort':<20} {'Coefficient':<15} {'% Impact':<15} {'Rank':<10}")
print("-" * 65)

for cohort in ['new', 'active', 'post_onboarding']:
    cohort_data = results_df[
        (results_df['cohort_type'] == cohort) &
        (results_df['feature'] != 'intercept')
    ].copy()
    
    cohort_data['abs_coef'] = cohort_data['coefficient'].abs()
    cohort_data = cohort_data.sort_values('abs_coef', ascending=False).reset_index(drop=True)
    
    second_session_row = cohort_data[cohort_data['feature'].str.upper() == 'HAD_SECOND_SESSION']
    
    if len(second_session_row) > 0:
        row = second_session_row.iloc[0]
        rank = cohort_data[cohort_data['feature'].str.upper() == 'HAD_SECOND_SESSION'].index[0] + 1
        print(f"{cohort:<20} {row['coefficient']:<15.4f} {row['pct_impact']:<15.1f}% {rank:<10}")
    else:
        print(f"{cohort:<20} {'Not found':<15} {'N/A':<15} {'N/A':<10}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize Coefficient Comparison

# COMMAND ----------

# Filter out intercept and failed models
plot_df = results_df[
    (results_df['feature'] != 'intercept') & 
    (results_df['feature'] != '<no_data>') &
    (results_df['feature'] != '<model_failed>')
].copy()

if len(plot_df) > 0:
    # Get top features by average absolute coefficient
    feature_importance = plot_df.groupby('feature')['coefficient'].apply(
        lambda x: x.abs().mean()
    ).sort_values(ascending=False)
    
    top_features = feature_importance.head(20).index.tolist()
    
    # Plot: Coefficient comparison for top features
    fig, ax = plt.subplots(figsize=(14, 10))
    
    plot_data = plot_df[plot_df['feature'].isin(top_features)].copy()
    
    # Pivot for easier plotting
    pivot_df = plot_data.pivot(index='feature', columns='cohort_type', values='coefficient')
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
    # Highlight HAD_SECOND_SESSION if in top features
    ytick_labels = [f + ' ⭐' if 'HAD_SECOND_SESSION' in f.upper() else f for f in top_features]
    ax.set_yticklabels(ytick_labels, fontsize=9)
    ax.set_xlabel('Coefficient', fontsize=11, fontweight='bold')
    ax.set_title('Top 20 Features: Coefficient Comparison Across Cohorts\n(includes HAD_SECOND_SESSION)', 
                 fontsize=13, fontweight='bold')
    ax.legend(loc='best', fontsize=10)
    ax.axvline(x=0, color='black', linestyle='-', linewidth=0.8)
    ax.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    display(fig)
else:
    print("No features to plot")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Features by Cohort (Separate Plots)

# COMMAND ----------

if len(plot_df) > 0:
    fig, axes = plt.subplots(1, 3, figsize=(20, 10))
    
    for idx, cohort in enumerate(['new', 'active', 'post_onboarding']):
        ax = axes[idx]
        
        cohort_data = plot_df[plot_df['cohort_type'] == cohort].copy()
        cohort_data['abs_coef'] = cohort_data['coefficient'].abs()
        cohort_data = cohort_data.sort_values('abs_coef', ascending=False).head(15)
        
        # Create color based on sign
        colors_bar = ['#d62728' if x < 0 else '#2ca02c' for x in cohort_data['coefficient']]
        
        # Horizontal bar plot
        y_pos = np.arange(len(cohort_data))
        ax.barh(y_pos, cohort_data['coefficient'], color=colors_bar, alpha=0.7)
        
        # Customize
        ax.set_yticks(y_pos)
        # Highlight HAD_SECOND_SESSION
        ytick_labels = [f + ' ⭐' if 'HAD_SECOND_SESSION' in f.upper() else f for f in cohort_data['feature']]
        ax.set_yticklabels(ytick_labels, fontsize=8)
        ax.invert_yaxis()
        ax.set_xlabel('Coefficient', fontsize=10, fontweight='bold')
        ax.set_title(f'{cohort.replace("_", " ").title()} Cohort\nTop 15 Features', 
                    fontsize=11, fontweight='bold')
        ax.axvline(x=0, color='black', linestyle='-', linewidth=0.8)
        ax.grid(axis='x', alpha=0.3)
        
        # Add value labels
        for i, value in enumerate(cohort_data['coefficient']):
            x_pos = value + (0.01 if value > 0 else -0.01)
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

# Save to Snowflake with different table name
output_table = "first_session_1111_with_second_session_prediction_results"

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
print(f"\n📊 Summary:")
print(f"   - Models trained: 3 (new, active, post_onboarding)")
print(f"   - Total coefficients: {len(results_df)}")
print(f"   - Features used: {len(FEATURES)} (includes HAD_SECOND_SESSION)")
print(f"\n💾 Results saved to:")
print(f"   - Table: proddb.fionafan.{output_table}")
print(f"\n🎯 Key Insight:")
print(f"   - HAD_SECOND_SESSION shows whether user came back for any session")
print(f"   - Compare its coefficient to understand importance of second session")
print(f"   - This feature captures immediate engagement beyond first session")

# COMMAND ----------



