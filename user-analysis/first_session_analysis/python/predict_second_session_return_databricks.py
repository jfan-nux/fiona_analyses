# Databricks notebook source
# MAGIC %md
# MAGIC # Predict Second Session Return - Logistic Regression Model (PySpark)
# MAGIC
# MAGIC Builds logistic regression models to predict whether a user will return for a second session
# MAGIC based on their first session behavior.
# MAGIC
# MAGIC **Models are built separately for each onboarding cohort:**
# MAGIC - New users
# MAGIC - Active users  
# MAGIC - Post-onboarding users
# MAGIC
# MAGIC **Key Steps**:
# MAGIC 1. Load first session features from Snowflake (50% sample)
# MAGIC 2. Join with second session indicator from enriched table
# MAGIC 3. Train logistic regression models for each cohort
# MAGIC 4. Train XGBoost models for each cohort
# MAGIC 5. Compare feature importance across cohorts
# MAGIC 6. Save results to Snowflake
# MAGIC
# MAGIC **Target:** second_session_id IS NOT NULL (from all_user_sessions_enriched)

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

# Feature list (funnel-inclusive)
FEATURES = [
    # Temporal features
    'days_since_onboarding',
    'days_since_onboarding_mod7',
    'session_day_of_week',
    'onboarding_day_of_week',
    
    # Session duration
    'session_duration_seconds',
    
    # Impression features
    'impression_unique_stores',
    'impression_unique_features',
    'impression_to_attribution_rate',
    
    # Action features
    'action_num_backgrounds',
    'action_num_foregrounds',
    'action_num_tab_switches',
    'action_num_address_actions',
    'action_num_auth_actions',
    'action_had_background',
    'action_had_foreground',
    'action_had_tab_switch',
    'action_had_address',
    'action_had_auth',
    
    # Attribution features
    'attribution_event_count',
    'attribution_num_card_clicks',
    'attribution_unique_stores_clicked',
    'attribution_to_add_rate',
    'attribution_cnt_traditional_carousel',
    'attribution_cnt_core_search',
    'attribution_cnt_pill_filter',
    'attribution_cnt_cuisine_filter',
    'attribution_cnt_home_feed',
    'attribution_cnt_banner',
    'attribution_cnt_offers',
    'attribution_cnt_tab_explore',
    'attribution_cnt_tab_me',
    
    # Error features
    'error_event_count',
    
    # Funnel features
    'funnel_num_adds',
    'funnel_num_store_page',
    'funnel_num_cart',
    'funnel_num_checkout',
    'funnel_add_per_attribution_click',
    'funnel_had_add',
    'funnel_reached_store_bool',
    'funnel_reached_cart_bool',
    'funnel_reached_checkout_bool',
    'funnel_converted_bool',
    
    # Timing features
    'timing_mean_inter_event_seconds',
    'timing_median_inter_event_seconds',
    'timing_std_inter_event_seconds',
    'timing_min_inter_event_seconds',
    'timing_max_inter_event_seconds',
    'funnel_seconds_to_first_add',
    'funnel_seconds_to_store_page',
    'funnel_seconds_to_cart',
    'funnel_seconds_to_checkout',
    'attribution_seconds_to_first_card_click',
    'attribution_first_click_from_search',
    'action_seconds_to_first_tab_switch',
    'action_seconds_to_first_foreground',
    'timing_events_first_30s',
    'timing_events_first_2min',
    
    # Sequence features
    'sequence_impression_to_attribution',
    'sequence_attribution_to_funnel_store',
    'sequence_funnel_store_to_action_add',
    
    # Store features
    'store_nv_impression_occurred',
    'store_nv_impression_position',
]

print(f"📊 Total features defined: {len(FEATURES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data (50% Sample)

# COMMAND ----------

# Build feature list for query
feature_cols = ", ".join([f"s.{feat}" for feat in FEATURES])

# Load first session features with 50% sampling and join with second session indicator
query = f"""
SELECT 
    s.user_id,
    s.dd_device_id,
    s.cohort_type,
    {feature_cols},
    CASE WHEN e.second_session_id IS NOT NULL THEN 1 ELSE 0 END as had_second_session
FROM proddb.fionafan.all_user_sessions_with_events_features_gen s
LEFT JOIN proddb.fionafan.all_user_sessions_enriched e 
    ON s.user_id = e.consumer_id
WHERE s.session_type = 'first_session'
    AND s.cohort_type IS NOT NULL
    AND s.user_id IS NOT NULL
    AND RANDOM() < 0.5
"""

print("📊 Loading first session data (50% sample)...")

df_spark = (
    spark.read.format("snowflake")
    .options(**OPTIONS)
    .option("query", query)
    .load()
)

df_spark.cache()

session_count = df_spark.count()
second_session_rate = df_spark.agg(F.avg("had_second_session")).collect()[0][0]

print(f"\n✅ Loaded {session_count:,} first sessions")
print(f"   Second session return rate: {second_session_rate:.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cohort Distribution

# COMMAND ----------

print("📊 Cohort Distribution:\n")
print(f"{'Cohort Type':<20} {'Total Sessions':<15} {'Had 2nd Session':<15} {'Return Rate':<12}")
print("-" * 65)

cohort_summary = df_spark.groupBy("cohort_type").agg(
    F.count("*").alias("total_sessions"),
    F.sum("had_second_session").alias("had_second"),
    F.avg("had_second_session").alias("return_rate")
).orderBy("cohort_type").collect()

for row in cohort_summary:
    cohort = row['cohort_type']
    total = row['total_sessions']
    had_second = row['had_second']
    return_rate = row['return_rate']
    print(f"{cohort:<20} {total:<15,} {had_second:<15,} {return_rate:<12.2%}")

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
    sdf = sdf_all.filter(F.col("cohort_type") == cohort_type)
    
    # Check available features (case-insensitive)
    sdf_columns_lower = [c.lower() for c in sdf.columns]
    column_name_map = {c.lower(): c for c in sdf.columns}
    
    available_features = []
    missing_features = []
    
    for feat in covariates:
        feat_lower = feat.lower()
        if feat_lower in sdf_columns_lower:
            available_features.append(column_name_map[feat_lower])
        else:
            missing_features.append(feat)
    
    if missing_features:
        print(f"  ⚠️  Missing {len(missing_features)} features")
    
    # Select columns
    target_col = column_name_map.get(target.lower(), target)
    cols = list(set(available_features + [target_col]))
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
        raise ValueError("No usable covariates after preprocessing.")
    
    # Cast covariates to double and impute nulls with 0.0
    for c in usable_covars:
        field = next((f for f in sdf.schema.fields if f.name == c), None)
        if isinstance(field.dataType, T.BooleanType):
            col_double = F.when(F.col(c), F.lit(1.0)).otherwise(F.lit(0.0))
        else:
            col_double = F.col(c).cast(T.DoubleType())
        sdf = sdf.withColumn(c, F.coalesce(col_double, F.lit(0.0)))
    
    # Prepare label column
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
    """Fit a logistic regression model using PySpark for a specific cohort"""
    print(f"\n{'='*80}")
    print(f"Training Model for {cohort_type.upper()} Cohort")
    print(f"{'='*80}")
    
    sdf, usable_covars = prepare_model_dataframe(spark, sdf_all, cohort_type, covariates, target)
    
    # Drop rows where label is null
    sdf = sdf.filter(F.col("label").isNotNull())
    n_obs = sdf.count()
    
    if n_obs == 0:
        print(f"  ⚠️  No observations for {cohort_type}")
        return pd.DataFrame({
            "cohort_type": [cohort_type],
            "covariate": ["<no_data>"],
            "coefficient": [np.nan],
            "odds_ratio_1sd": [np.nan],
            "pct_impact_1sd": [np.nan],
            "n_obs": [0],
            "converged": [False],
        })
    
    # Calculate base rate
    n_positive = sdf.filter(F.col("label") == 1).count()
    base_rate = n_positive / n_obs if n_obs > 0 else 0
    
    print(f"  📊 Dataset: {n_obs:,} observations")
    print(f"  📊 Return rate: {base_rate:.2%} ({n_positive:,} / {n_obs:,})")
    print(f"  📊 Features: {len(usable_covars)}")
    
    # Split data for train/test
    train_df, test_df = sdf.randomSplit([0.8, 0.2], seed=42)
    train_count = train_df.count()
    test_count = test_df.count()
    
    print(f"  📊 Train: {train_count:,} | Test: {test_count:,}")
    
    # Assemble features and standardize
    from pyspark.ml.feature import VectorAssembler as VA
    from pyspark.ml.feature import StandardScaler as SS
    from pyspark.ml.classification import LogisticRegression as LR
    
    assembler = VA()
    assembler.setInputCols(usable_covars)
    assembler.setOutputCol("features_raw")
    assembler.setHandleInvalid("keep")
    
    scaler = SS()
    scaler.setInputCol("features_raw")
    scaler.setOutputCol("features")
    scaler.setWithMean(True)
    scaler.setWithStd(True)
    
    lr = LR()
    lr.setFeaturesCol("features")
    lr.setLabelCol("label")
    lr.setMaxIter(max_iter)
    lr.setRegParam(reg_param)
    lr.setElasticNetParam(0.0)
    lr.setStandardization(False)
    lr.setFitIntercept(True)
    
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
        print(f"  ❌ Failed to fit {cohort_type} model: {e}")
        return pd.DataFrame({
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
# MAGIC ## Train Logistic Regression Models

# COMMAND ----------

target = "had_second_session"
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
# MAGIC ## Combine Logistic Regression Results

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
print(f"\n{'Cohort':<20} {'N Obs':<12} {'Return Rate %':<15} {'Test AUC':<12}")
print("-" * 65)

for cohort in ['new', 'active', 'post_onboarding']:
    cohort_metrics = results_df[
        (results_df['cohort_type'] == cohort) & 
        (results_df['covariate'] == 'const')
    ]
    if len(cohort_metrics) > 0:
        metrics = cohort_metrics.iloc[0]
        n_features = len(results_df[
            (results_df['cohort_type'] == cohort) &
            (results_df['covariate'] != 'const')
        ])
        print(f"{cohort:<20} {metrics['n_obs']:<12,.0f} "
              f"{metrics['n_obs'] * (metrics['test_auc'] if 'test_auc' in metrics else 0) / metrics['n_obs']:<15.2f} "
              f"{metrics['test_auc']:<12.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Features by Cohort

# COMMAND ----------

print(f"\n{'='*100}")
print("TOP 15 FEATURES BY COHORT (by absolute coefficient)")
print(f"{'='*100}")

for cohort in ['new', 'active', 'post_onboarding']:
    print(f"\n{cohort.upper()} COHORT:")
    print("-" * 100)
    
    cohort_results = results_df[
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
# MAGIC ## Visualize Coefficient Comparison Across Cohorts

# COMMAND ----------

# Filter out intercept and failed models
plot_df = results_df[
    (results_df['covariate'] != 'const') & 
    (results_df['covariate'] != '<no_data>') &
    (results_df['covariate'] != '<model_failed>')
].copy()

if len(plot_df) > 0:
    # Get top features by average absolute coefficient
    feature_importance = plot_df.groupby('covariate')['coefficient'].apply(
        lambda x: x.abs().mean()
    ).sort_values(ascending=False)
    
    top_features = feature_importance.head(20).index.tolist()
    
    # Plot: Coefficient comparison for top features
    fig, ax = plt.subplots(figsize=(14, 10))
    
    plot_data = plot_df[plot_df['covariate'].isin(top_features)].copy()
    
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
    ax.set_title('Top 20 Features: Coefficient Comparison Across Cohorts\nPredicting Second Session Return', 
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
        ax.set_yticklabels(cohort_data['covariate'], fontsize=8)
        ax.invert_yaxis()
        ax.set_xlabel('Coefficient', fontsize=10, fontweight='bold')
        ax.set_title(f'{cohort.replace("_", " ").title()} Cohort\nTop 15 Features', 
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
# MAGIC ## Save Logistic Regression Results to Snowflake

# COMMAND ----------

# Convert to Spark DataFrame
results_spark = spark.createDataFrame(results_df)

# Save to Snowflake
output_table = "first_session_second_session_return_logit_results"

results_spark.write.format("snowflake") \
    .options(**OPTIONS) \
    .option("dbtable", f"proddb.fionafan.{output_table}") \
    .mode("overwrite") \
    .save()

print(f"\n✅ Saved {len(results_df)} rows to proddb.fionafan.{output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## XGBoost Models with Feature Importance

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install XGBoost

# COMMAND ----------

%pip install xgboost

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Re-import after restart
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score

# Set plotting style
plt.style.use('default')
sns.set_palette("husl")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reload Data for XGBoost

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

FEATURES = [
    'days_since_onboarding',
    'days_since_onboarding_mod7',
    'session_day_of_week',
    'onboarding_day_of_week',
    'session_duration_seconds',
    'impression_unique_stores',
    'impression_unique_features',
    'impression_to_attribution_rate',
    'action_num_backgrounds',
    'action_num_foregrounds',
    'action_num_tab_switches',
    'action_num_address_actions',
    'action_num_auth_actions',
    'action_had_background',
    'action_had_foreground',
    'action_had_tab_switch',
    'action_had_address',
    'action_had_auth',
    'attribution_event_count',
    'attribution_num_card_clicks',
    'attribution_unique_stores_clicked',
    'attribution_to_add_rate',
    'attribution_cnt_traditional_carousel',
    'attribution_cnt_core_search',
    'attribution_cnt_pill_filter',
    'attribution_cnt_cuisine_filter',
    'attribution_cnt_home_feed',
    'attribution_cnt_banner',
    'attribution_cnt_offers',
    'attribution_cnt_tab_explore',
    'attribution_cnt_tab_me',
    'error_event_count',
    'funnel_num_adds',
    'funnel_num_store_page',
    'funnel_num_cart',
    'funnel_num_checkout',
    'funnel_add_per_attribution_click',
    'funnel_had_add',
    'funnel_reached_store_bool',
    'funnel_reached_cart_bool',
    'funnel_reached_checkout_bool',
    'funnel_converted_bool',
    'timing_mean_inter_event_seconds',
    'timing_median_inter_event_seconds',
    'timing_std_inter_event_seconds',
    'timing_min_inter_event_seconds',
    'timing_max_inter_event_seconds',
    'funnel_seconds_to_first_add',
    'funnel_seconds_to_store_page',
    'funnel_seconds_to_cart',
    'funnel_seconds_to_checkout',
    'attribution_seconds_to_first_card_click',
    'attribution_first_click_from_search',
    'action_seconds_to_first_tab_switch',
    'action_seconds_to_first_foreground',
    'timing_events_first_30s',
    'timing_events_first_2min',
    'sequence_impression_to_attribution',
    'sequence_attribution_to_funnel_store',
    'sequence_funnel_store_to_action_add',
    'store_nv_impression_occurred',
    'store_nv_impression_position',
]

# Build feature list for query
feature_cols = ", ".join([f"s.{feat}" for feat in FEATURES])

query = f"""
SELECT 
    s.user_id,
    s.cohort_type,
    {feature_cols},
    CASE WHEN e.second_session_id IS NOT NULL THEN 1 ELSE 0 END as had_second_session
FROM proddb.fionafan.all_user_sessions_with_events_features_gen s
LEFT JOIN proddb.fionafan.all_user_sessions_enriched e 
    ON s.user_id = e.consumer_id
WHERE s.session_type = 'first_session'
    AND s.cohort_type IS NOT NULL
    AND s.user_id IS NOT NULL
    AND RANDOM() < 0.5
"""

print("📊 Loading first session data for XGBoost models...")

df_spark = (
    spark.read.format("snowflake")
    .options(**OPTIONS)
    .option("query", query)
    .load()
)

# Convert to pandas for XGBoost
df_pd = df_spark.toPandas()

print(f"✅ Loaded {len(df_pd):,} first sessions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define XGBoost Training Function

# COMMAND ----------

def train_xgboost_model(
    df: pd.DataFrame,
    cohort_type: str,
    features: list,
    target: str = 'had_second_session',
    n_estimators: int = 100,
    max_depth: int = 6,
    learning_rate: float = 0.1,
) -> dict:
    """
    Train XGBoost model for a specific cohort and return feature importance
    """
    print(f"\n{'='*80}")
    print(f"Training XGBoost Model for {cohort_type.upper()} Cohort")
    print(f"{'='*80}")
    
    # Filter for cohort
    df_cohort = df[df['cohort_type'] == cohort_type].copy()
    
    # Check available features (case-insensitive)
    df_columns_lower = [c.lower() for c in df_cohort.columns]
    column_name_map = {c.lower(): c for c in df_cohort.columns}
    
    available_features = []
    for feat in features:
        feat_lower = feat.lower()
        if feat_lower in df_columns_lower:
            available_features.append(column_name_map[feat_lower])
    
    # Get target column
    target_col = column_name_map.get(target.lower(), target)
    
    # Prepare features and target
    X = df_cohort[available_features].fillna(0).values
    y = df_cohort[target_col].fillna(0).astype(int).values
    
    n_obs = len(X)
    n_positive = y.sum()
    base_rate = n_positive / n_obs if n_obs > 0 else 0
    
    print(f"  📊 Dataset: {n_obs:,} observations")
    print(f"  📊 Return rate: {base_rate:.2%} ({n_positive:,} / {n_obs:,})")
    print(f"  📊 Features: {len(available_features)}")
    
    if n_obs == 0 or n_positive == 0 or n_positive == n_obs:
        print(f"  ⚠️  Insufficient data for {cohort_type}")
        return {
            'cohort_type': cohort_type,
            'feature_importance': pd.DataFrame(),
            'n_obs': 0,
            'test_auc': np.nan,
        }
    
    # Split train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    print(f"  📊 Train: {len(X_train):,} | Test: {len(X_test):,}")
    
    # Train XGBoost
    print(f"  🔄 Training XGBoost model...")
    
    model = xgb.XGBClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=learning_rate,
        objective='binary:logistic',
        random_state=42,
        use_label_encoder=False,
        eval_metric='logloss'
    )
    
    model.fit(X_train, y_train)
    
    # Evaluate
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    test_auc = roc_auc_score(y_test, y_pred_proba)
    
    print(f"  ✅ Test AUC-ROC: {test_auc:.4f}")
    
    # Get feature importance
    importance_dict = model.get_booster().get_score(importance_type='gain')
    
    # Map feature names back
    feature_importance = []
    for i, feat_name in enumerate(available_features):
        xgb_feat_name = f'f{i}'
        importance = importance_dict.get(xgb_feat_name, 0)
        feature_importance.append({
            'cohort_type': cohort_type,
            'feature': feat_name,
            'importance': importance,
            'n_obs': n_obs,
            'n_positive': n_positive,
            'base_rate': base_rate * 100,
            'test_auc': test_auc,
        })
    
    importance_df = pd.DataFrame(feature_importance)
    
    # Normalize importance to sum to 100
    total_importance = importance_df['importance'].sum()
    if total_importance > 0:
        importance_df['importance_pct'] = 100 * importance_df['importance'] / total_importance
    else:
        importance_df['importance_pct'] = 0
    
    importance_df = importance_df.sort_values('importance', ascending=False)
    
    print(f"  ✅ Top 5 Features:")
    for idx, row in importance_df.head(5).iterrows():
        print(f"     {row['feature']:<45} {row['importance_pct']:>6.2f}%")
    
    return {
        'cohort_type': cohort_type,
        'feature_importance': importance_df,
        'n_obs': n_obs,
        'n_positive': n_positive,
        'base_rate': base_rate * 100,
        'test_auc': test_auc,
        'model': model,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train XGBoost Models for Each Cohort

# COMMAND ----------

# Train for NEW cohort
xgb_results_new = train_xgboost_model(
    df=df_pd,
    cohort_type='new',
    features=FEATURES,
    n_estimators=100,
    max_depth=6,
    learning_rate=0.1,
)

# COMMAND ----------

# Train for ACTIVE cohort
xgb_results_active = train_xgboost_model(
    df=df_pd,
    cohort_type='active',
    features=FEATURES,
    n_estimators=100,
    max_depth=6,
    learning_rate=0.1,
)

# COMMAND ----------

# Train for POST_ONBOARDING cohort
xgb_results_post = train_xgboost_model(
    df=df_pd,
    cohort_type='post_onboarding',
    features=FEATURES,
    n_estimators=100,
    max_depth=6,
    learning_rate=0.1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combine XGBoost Results

# COMMAND ----------

# Combine feature importance from all cohorts
xgb_importance_df = pd.concat([
    xgb_results_new['feature_importance'],
    xgb_results_active['feature_importance'],
    xgb_results_post['feature_importance']
], ignore_index=True)

# Add timestamp
xgb_importance_df['created_at'] = datetime.now()

print(f"\n📊 XGBoost Feature Importance Summary:")
print(f"   - Total rows: {len(xgb_importance_df)}")
print(f"   - New cohort: {len(xgb_results_new['feature_importance'])} features")
print(f"   - Active cohort: {len(xgb_results_active['feature_importance'])} features")
print(f"   - Post-onboarding cohort: {len(xgb_results_post['feature_importance'])} features")

# COMMAND ----------

# MAGIC %md
# MAGIC ### XGBoost Model Performance Comparison

# COMMAND ----------

print(f"\n{'='*80}")
print("XGBOOST MODEL PERFORMANCE COMPARISON")
print(f"{'='*80}")
print(f"\n{'Cohort':<20} {'N Obs':<12} {'Return Rate %':<15} {'Test AUC':<12}")
print("-" * 65)

for result in [xgb_results_new, xgb_results_active, xgb_results_post]:
    cohort = result['cohort_type']
    n_obs = result['n_obs']
    base_rate = result['base_rate']
    test_auc = result['test_auc']
    print(f"{cohort:<20} {n_obs:<12,} {base_rate:<15.2f} {test_auc:<12.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top Features by Cohort (XGBoost)

# COMMAND ----------

print(f"\n{'='*80}")
print("XGBOOST - TOP 15 FEATURES BY COHORT (by importance %)")
print(f"{'='*80}")

for cohort in ['new', 'active', 'post_onboarding']:
    print(f"\n{cohort.upper()} COHORT:")
    print("-" * 80)
    
    cohort_importance = xgb_importance_df[
        xgb_importance_df['cohort_type'] == cohort
    ].sort_values('importance_pct', ascending=False).head(15)
    
    print(f"{'Rank':<6} {'Feature':<45} {'Importance':<12} {'% of Total':<12}")
    print("-" * 80)
    
    for rank, (idx, row) in enumerate(cohort_importance.iterrows(), 1):
        print(f"{rank:<6} {row['feature']:<45} {row['importance']:>11.2f} {row['importance_pct']:>11.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Feature Importance Comparison

# COMMAND ----------

# Get top features by average importance across cohorts
feature_avg_importance = xgb_importance_df.groupby('feature')['importance_pct'].mean().sort_values(ascending=False)
top_features = feature_avg_importance.head(20).index.tolist()

# Plot: Feature importance comparison
fig, ax = plt.subplots(figsize=(14, 10))

plot_data = xgb_importance_df[xgb_importance_df['feature'].isin(top_features)].copy()

# Pivot for easier plotting
pivot_df = plot_data.pivot(index='feature', columns='cohort_type', values='importance_pct')
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
ax.set_xlabel('Feature Importance (%)', fontsize=11, fontweight='bold')
ax.set_title('XGBoost: Top 20 Features - Importance Comparison Across Cohorts\nPredicting Second Session Return', 
             fontsize=13, fontweight='bold')
ax.legend(loc='best', fontsize=10)
ax.grid(axis='x', alpha=0.3)

plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Individual Cohort Plots - XGBoost Feature Importance

# COMMAND ----------

fig, axes = plt.subplots(1, 3, figsize=(20, 10))

for idx, cohort in enumerate(['new', 'active', 'post_onboarding']):
    ax = axes[idx]
    
    cohort_data = xgb_importance_df[
        xgb_importance_df['cohort_type'] == cohort
    ].sort_values('importance_pct', ascending=False).head(15)
    
    # Horizontal bar plot
    y_pos = np.arange(len(cohort_data))
    ax.barh(y_pos, cohort_data['importance_pct'], 
            color=colors.get(cohort, None), alpha=0.7)
    
    # Customize
    ax.set_yticks(y_pos)
    ax.set_yticklabels(cohort_data['feature'], fontsize=8)
    ax.invert_yaxis()
    ax.set_xlabel('Importance (%)', fontsize=10, fontweight='bold')
    ax.set_title(f'XGBoost: {cohort.replace("_", " ").title()}\nTop 15 Features', 
                fontsize=11, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    
    # Add value labels
    for i, value in enumerate(cohort_data['importance_pct']):
        ax.text(value + 0.5, i, f'{value:.1f}%', va='center', ha='left', fontsize=7)

plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare Logistic Regression vs XGBoost Performance

# COMMAND ----------

# Reload logistic regression results from Snowflake
print("📊 Loading logistic regression results for comparison...")

lr_results_query = """
SELECT cohort_type, test_auc
FROM proddb.fionafan.first_session_second_session_return_logit_results
WHERE covariate = 'const'
"""

try:
    lr_results_spark = (
        spark.read.format("snowflake")
        .options(**OPTIONS)
        .option("query", lr_results_query)
        .load()
    )
    
    lr_results_pd = lr_results_spark.toPandas()
    
    print(f"\n{'='*80}")
    print("MODEL COMPARISON: Logistic Regression vs XGBoost")
    print(f"{'='*80}")
    print(f"\n{'Cohort':<20} {'Logistic AUC':<15} {'XGBoost AUC':<15} {'Difference':<12}")
    print("-" * 70)
    
    # Get logistic regression AUCs
    lr_aucs = {}
    for _, row in lr_results_pd.iterrows():
        # Handle case-insensitive column names
        cohort_col = 'COHORT_TYPE' if 'COHORT_TYPE' in lr_results_pd.columns else 'cohort_type'
        auc_col = 'TEST_AUC' if 'TEST_AUC' in lr_results_pd.columns else 'test_auc'
        lr_aucs[row[cohort_col]] = row[auc_col]
    
    # Compare with XGBoost
    for result in [xgb_results_new, xgb_results_active, xgb_results_post]:
        cohort = result['cohort_type']
        xgb_auc = result['test_auc']
        lr_auc = lr_aucs.get(cohort, np.nan)
        diff = xgb_auc - lr_auc if not np.isnan(lr_auc) else np.nan
        
        print(f"{cohort:<20} {lr_auc:<15.4f} {xgb_auc:<15.4f} {diff:>+11.4f}")
        
except Exception as e:
    print(f"⚠️  Could not load logistic regression results: {e}")
    print("   Skipping model comparison. Run logistic regression section first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save XGBoost Results to Snowflake

# COMMAND ----------

# Convert to Spark DataFrame
xgb_importance_spark = spark.createDataFrame(xgb_importance_df)

# Save to Snowflake
output_table_xgb = "first_session_second_session_return_xgb_importance"

xgb_importance_spark.write.format("snowflake") \
    .options(**OPTIONS) \
    .option("dbtable", f"proddb.fionafan.{output_table_xgb}") \
    .mode("overwrite") \
    .save()

print(f"\n✅ Saved {len(xgb_importance_df)} rows to proddb.fionafan.{output_table_xgb}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis Complete

# COMMAND ----------

print("\n" + "="*80)
print("ANALYSIS COMPLETE!")
print("="*80)
print(f"\n📊 Summary:")
print(f"   - Logistic models trained: 3 (new, active, post_onboarding)")
print(f"   - XGBoost models trained: 3 (new, active, post_onboarding)")
print(f"   - Target: Second session return")
print(f"   - Features: {len(FEATURES)}")
print(f"\n💾 Results saved to:")
print(f"   - Logistic coefficients: proddb.fionafan.first_session_second_session_return_logit_results")
print(f"   - XGBoost importance: proddb.fionafan.{output_table_xgb}")
print(f"\n🎯 Key Insights:")
print(f"   1. Compare coefficients/importance across cohorts")
print(f"   2. Identify what drives immediate re-engagement")
print(f"   3. Positive features = behaviors that encourage return")
print(f"   4. Use to optimize first session experience")

# COMMAND ----------


