# Databricks notebook source
# MAGIC %md
# MAGIC # Logistic Regression Models for Session Conversion Prediction (PySpark)
# MAGIC 
# MAGIC This notebook builds two logistic regression models using PySpark:
# MAGIC 1. **Pre-Funnel Model**: Uses only browsing/engagement features
# MAGIC 2. **Funnel-Inclusive Model**: Includes funnel progression features
# MAGIC 
# MAGIC **Outputs**:
# MAGIC - Model coefficients with odds ratios and percentage impacts
# MAGIC - Model performance metrics
# MAGIC - ROC curves and feature importance visualizations

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, IntegerType
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vectors, VectorUDT
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

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
# MAGIC ## Define Feature Sets

# COMMAND ----------

def define_feature_sets():
    """Define the two feature sets for modeling"""
    
    # Feature Set 1: Pre-Funnel Features (Early Signal Model)
    pre_funnel_features = [
        # Temporal features
        'DAYS_SINCE_ONBOARDING',
        'DAYS_SINCE_ONBOARDING_MOD7',
        'SESSION_DAY_OF_WEEK',
        'ONBOARDING_DAY_OF_WEEK',
        
        # Overall session engagement
        'TOTAL_EVENTS',
        'SESSION_DURATION_SECONDS',
        'EVENTS_PER_MINUTE',
        'NUM_UNIQUE_DISCOVERY_FEATURES',
        'NUM_UNIQUE_DISCOVERY_SURFACES',
        'NUM_UNIQUE_STORES',
        
        # Impression features
        'IMPRESSION_EVENT_COUNT',
        'IMPRESSION_UNIQUE_STORES',
        'IMPRESSION_UNIQUE_FEATURES',
        'IMPRESSION_HAD_ANY',
        
        # Action features
        'ACTION_EVENT_COUNT',
        'ACTION_NUM_BACKGROUNDS',
        'ACTION_NUM_FOREGROUNDS',
        'ACTION_NUM_TAB_SWITCHES',
        'ACTION_NUM_ADDRESS_ACTIONS',
        'ACTION_NUM_PAYMENT_ACTIONS',
        'ACTION_NUM_AUTH_ACTIONS',
        'ACTION_HAD_TAB_SWITCH',
        'ACTION_HAD_ADDRESS',
        'ACTION_HAD_PAYMENT',
        'ACTION_HAD_AUTH',
        
        # Attribution features
        'ATTRIBUTION_EVENT_COUNT',
        'ATTRIBUTION_NUM_CARD_CLICKS',
        'ATTRIBUTION_UNIQUE_STORES_CLICKED',
        'ATTRIBUTION_HAD_ANY',
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
        'ATTRIBUTION_PCT_FROM_DOMINANT',
        'ATTRIBUTION_ENTROPY',
        'ATTRIBUTION_FIRST_CLICK_FROM_SEARCH',
        
        # Error indicators
        'ERROR_EVENT_COUNT',
        'ERROR_HAD_ANY',
        
        # Timing features
        'TIMING_MEAN_INTER_EVENT_SECONDS',
        'TIMING_MEDIAN_INTER_EVENT_SECONDS',
        'TIMING_STD_INTER_EVENT_SECONDS',
        'TIMING_EVENTS_FIRST_30S',
        'TIMING_EVENTS_FIRST_2MIN',
        'ATTRIBUTION_SECONDS_TO_FIRST_CARD_CLICK',
        'ACTION_SECONDS_TO_FIRST_TAB_SWITCH',
        'ACTION_SECONDS_TO_FIRST_FOREGROUND',
        
        # Behavioral sequences (pre-funnel)
        'SEQUENCE_IMPRESSION_TO_ATTRIBUTION',
        
        # Store context
        'STORE_HAD_NV',
        'STORE_NV_IMPRESSION_OCCURRED',
        'STORE_NV_IMPRESSION_POSITION',
    ]
    
    # Feature Set 2: Funnel-Inclusive Features (Full Journey Model)
    funnel_features = [
        # Funnel progression indicators
        'FUNNEL_EVENT_COUNT',
        'FUNNEL_NUM_ADDS',
        'FUNNEL_NUM_STORE_PAGE',
        'FUNNEL_NUM_CART',
        'FUNNEL_NUM_CHECKOUT',
        'FUNNEL_HAD_ANY',
        'FUNNEL_HAD_ADD',
        'FUNNEL_REACHED_STORE_BOOL',
        'FUNNEL_REACHED_CART_BOOL',
        'FUNNEL_REACHED_CHECKOUT_BOOL',
        
        # Funnel timing
        'FUNNEL_SECONDS_TO_FIRST_ADD',
        'FUNNEL_SECONDS_TO_STORE_PAGE',
        'FUNNEL_SECONDS_TO_CART',
        'FUNNEL_SECONDS_TO_CHECKOUT',
        
        # Conversion rates
        'IMPRESSION_TO_ATTRIBUTION_RATE',
        'ATTRIBUTION_TO_ADD_RATE',
        'FUNNEL_ADD_PER_ATTRIBUTION_CLICK',
        
        # Additional sequences
        'SEQUENCE_ATTRIBUTION_TO_FUNNEL_STORE',
        'SEQUENCE_FUNNEL_STORE_TO_ACTION_ADD',
    ]
    
    full_funnel_features = pre_funnel_features + funnel_features
    
    # Categorical features
    categorical_features = ['SESSION_TYPE', 'COHORT_TYPE', 'PLATFORM', 'ATTRIBUTION_DOMINANT_FEATURE']
    
    return pre_funnel_features, full_funnel_features, categorical_features

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Prepare Data

# COMMAND ----------

# Load data from Snowflake
query = """
SELECT *
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
WHERE funnel_converted_bool IS NOT NULL
"""

df_spark = (
    spark.read.format("snowflake")
    .options(**OPTIONS)
    .option("query", query)
    .load()
)

print(f"Loaded {df_spark.count():,} sessions")

# Get conversion rate
conversion_rate = df_spark.agg(F.avg("FUNNEL_CONVERTED_BOOL")).collect()[0][0]
print(f"Conversion rate: {conversion_rate:.2%}")

# Cache for reuse
df_spark.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Features

# COMMAND ----------

# Get feature sets
pre_funnel_base, full_funnel_base, categorical_features = define_feature_sets()

# One-hot encode categorical features
from pyspark.ml.feature import StringIndexer, OneHotEncoder

categorical_dummy_cols = []
df_encoded = df_spark

for cat_col in categorical_features:
    if cat_col in df_encoded.columns:
        # Fill nulls with 'UNKNOWN'
        df_encoded = df_encoded.withColumn(cat_col, F.coalesce(F.col(cat_col), F.lit("UNKNOWN")))
        
        # String indexer
        indexer = StringIndexer(inputCol=cat_col, outputCol=f"{cat_col}_INDEX", handleInvalid="keep")
        df_encoded = indexer.fit(df_encoded).transform(df_encoded)
        
        # One-hot encoder
        encoder = OneHotEncoder(inputCols=[f"{cat_col}_INDEX"], outputCols=[f"{cat_col}_VEC"], dropLast=True)
        df_encoded = encoder.fit(df_encoded).transform(df_encoded)
        
        categorical_dummy_cols.append(f"{cat_col}_VEC")

# Combine base features with categorical
pre_funnel_features = [f for f in pre_funnel_base if f in df_encoded.columns]
full_funnel_features = [f for f in full_funnel_base if f in df_encoded.columns]

print(f"\nPre-Funnel Model: {len(pre_funnel_features)} base features + {len(categorical_dummy_cols)} categorical")
print(f"Funnel-Inclusive Model: {len(full_funnel_features)} base features + {len(categorical_dummy_cols)} categorical")

# Handle missing values - fill numeric with 0
for feature in pre_funnel_features + full_funnel_features:
    if feature in df_encoded.columns:
        df_encoded = df_encoded.withColumn(feature, F.coalesce(F.col(feature).cast("double"), F.lit(0.0)))

# Split data (stratified by outcome)
train_df, test_df = df_encoded.randomSplit([0.8, 0.2], seed=42)

train_count = train_df.count()
test_count = test_df.count()
train_conversion = train_df.agg(F.avg("FUNNEL_CONVERTED_BOOL")).collect()[0][0]
test_conversion = test_df.agg(F.avg("FUNNEL_CONVERTED_BOOL")).collect()[0][0]

print(f"\nTrain set: {train_count:,} sessions (conversion: {train_conversion:.2%})")
print(f"Test set:  {test_count:,} sessions (conversion: {test_conversion:.2%})")

train_df.cache()
test_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Training Function

# COMMAND ----------

def train_logistic_model(train_df, test_df, features, categorical_cols, label_col, model_name):
    """
    Train a logistic regression model using PySpark ML
    
    Args:
        train_df: Training DataFrame
        test_df: Test DataFrame
        features: List of numeric feature names
        categorical_cols: List of categorical vector column names
        label_col: Target variable name
        model_name: Model name for reporting
        
    Returns:
        model, predictions_df, metrics
    """
    print(f"\n{'='*80}")
    print(f"Training {model_name}")
    print(f"{'='*80}")
    
    # Assemble features
    assembler = VectorAssembler(
        inputCols=features + categorical_cols,
        outputCol="features_raw",
        handleInvalid="keep"
    )
    
    train_assembled = assembler.transform(train_df)
    test_assembled = assembler.transform(test_df)
    
    # Scale features
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,
        withStd=True
    )
    
    scaler_model = scaler.fit(train_assembled)
    train_scaled = scaler_model.transform(train_assembled)
    test_scaled = scaler_model.transform(test_assembled)
    
    # Train logistic regression
    lr = LogisticRegression(
        featuresCol="features",
        labelCol=label_col,
        maxIter=100,
        regParam=0.01,  # L2 regularization (1/C in sklearn)
        elasticNetParam=0.0,  # Pure L2
        family="binomial",
        standardization=False  # Already scaled
    )
    
    lr_model = lr.fit(train_scaled)
    
    # Make predictions
    train_predictions = lr_model.transform(train_scaled)
    test_predictions = lr_model.transform(test_scaled)
    
    # Evaluate
    evaluator_auc = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    
    evaluator_acc = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="accuracy"
    )
    
    evaluator_precision = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="weightedPrecision"
    )
    
    evaluator_recall = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="weightedRecall"
    )
    
    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="f1"
    )
    
    metrics = {
        'model_name': model_name,
        'train_auc': evaluator_auc.evaluate(train_predictions),
        'test_auc': evaluator_auc.evaluate(test_predictions),
        'train_accuracy': evaluator_acc.evaluate(train_predictions),
        'test_accuracy': evaluator_acc.evaluate(test_predictions),
        'train_precision': evaluator_precision.evaluate(train_predictions),
        'test_precision': evaluator_precision.evaluate(test_predictions),
        'train_recall': evaluator_recall.evaluate(train_predictions),
        'test_recall': evaluator_recall.evaluate(test_predictions),
        'train_f1': evaluator_f1.evaluate(train_predictions),
        'test_f1': evaluator_f1.evaluate(test_predictions),
        'num_features': len(features) + len(categorical_cols),
    }
    
    # Print metrics
    print(f"\nModel Performance:")
    print(f"  Train AUC-ROC: {metrics['train_auc']:.4f}")
    print(f"  Test AUC-ROC:  {metrics['test_auc']:.4f}")
    print(f"  Train Accuracy: {metrics['train_accuracy']:.4f}")
    print(f"  Test Accuracy:  {metrics['test_accuracy']:.4f}")
    print(f"  Test Precision: {metrics['test_precision']:.4f}")
    print(f"  Test Recall:    {metrics['test_recall']:.4f}")
    print(f"  Test F1-Score:  {metrics['test_f1']:.4f}")
    
    return lr_model, scaler_model, assembler, test_predictions, metrics, features + categorical_cols

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Model Coefficients

# COMMAND ----------

def extract_coefficients(lr_model, scaler_model, feature_names, numeric_feature_count, model_name):
    """Extract coefficients and calculate odds ratios"""
    
    # Get coefficients and intercept
    coefficients = lr_model.coefficients.toArray()
    intercept = lr_model.intercept
    
    # Get scaling parameters (for numeric features only)
    scale_values = scaler_model.std.toArray()[:numeric_feature_count]
    
    # Create coefficient dataframe (only for numeric features)
    coef_data = []
    for i, feat in enumerate(feature_names[:numeric_feature_count]):
        coef_data.append({
            'model_name': model_name,
            'feature': feat,
            'coefficient': float(coefficients[i]),
            'feature_std': float(scale_values[i]) if i < len(scale_values) else 1.0,
        })
    
    coef_df = pd.DataFrame(coef_data)
    
    # Calculate odds ratios for 1 standard deviation change
    coef_df['odds_ratio_1sd'] = np.exp(coef_df['coefficient'])
    coef_df['pct_impact_1sd'] = (coef_df['odds_ratio_1sd'] - 1) * 100
    
    # Calculate marginal effect at mean
    baseline_prob = 1 / (1 + np.exp(-intercept))
    coef_df['marginal_effect_1sd'] = coef_df['coefficient'] * baseline_prob * (1 - baseline_prob)
    coef_df['pct_point_impact_1sd'] = coef_df['marginal_effect_1sd'] * 100
    
    # Add absolute values for sorting
    coef_df['abs_coefficient'] = np.abs(coef_df['coefficient'])
    coef_df['abs_pct_impact'] = np.abs(coef_df['pct_impact_1sd'])
    
    # Sort by absolute impact
    coef_df = coef_df.sort_values('abs_pct_impact', ascending=False)
    coef_df['rank'] = range(1, len(coef_df) + 1)
    
    print(f"\n{model_name} - Top 20 Features by Impact:")
    print(f"{'Rank':<6} {'Feature':<50} {'Coef':<10} {'Odds Ratio':<12} {'% Impact':<12} {'PP Impact':<12}")
    print("-" * 110)
    
    for idx, row in coef_df.head(20).iterrows():
        print(f"{row['rank']:<6} {row['feature']:<50} {row['coefficient']:>9.4f} "
              f"{row['odds_ratio_1sd']:>11.4f} {row['pct_impact_1sd']:>11.1f}% "
              f"{row['pct_point_impact_1sd']:>11.2f}pp")
    
    return coef_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Models

# COMMAND ----------

# Model 1: Pre-Funnel Features
print("\n" + "="*80)
print("MODEL 1: PRE-FUNNEL MODEL")
print("="*80)

lr_model_pre, scaler_pre, assembler_pre, predictions_pre, metrics_pre, all_features_pre = train_logistic_model(
    train_df, test_df,
    pre_funnel_features,
    categorical_dummy_cols,
    "FUNNEL_CONVERTED_BOOL",
    "Pre-Funnel Model"
)

coef_pre = extract_coefficients(
    lr_model_pre, scaler_pre, all_features_pre, len(pre_funnel_features), "Pre-Funnel Model"
)

# COMMAND ----------

# Model 2: Funnel-Inclusive Features
print("\n" + "="*80)
print("MODEL 2: FUNNEL-INCLUSIVE MODEL")
print("="*80)

lr_model_full, scaler_full, assembler_full, predictions_full, metrics_full, all_features_full = train_logistic_model(
    train_df, test_df,
    full_funnel_features,
    categorical_dummy_cols,
    "FUNNEL_CONVERTED_BOOL",
    "Funnel-Inclusive Model"
)

coef_full = extract_coefficients(
    lr_model_full, scaler_full, all_features_full, len(full_funnel_features), "Funnel-Inclusive Model"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results to Snowflake

# COMMAND ----------

# Combine coefficients
combined_coefs = pd.concat([coef_pre, coef_full], ignore_index=True)
combined_coefs['created_at'] = datetime.now()

# Convert to Spark DataFrame and save
coef_spark_df = spark.createDataFrame(combined_coefs)

coef_spark_df.write.format("snowflake") \
    .options(**OPTIONS) \
    .option("dbtable", "proddb.fionafan.session_conversion_model_coefficients_pyspark") \
    .mode("overwrite") \
    .save()

print(f"\n✅ Saved {len(combined_coefs)} coefficients to Snowflake")

# COMMAND ----------

# Save metrics
metrics_df = pd.DataFrame([metrics_pre, metrics_full])
metrics_df['created_at'] = datetime.now()

metrics_spark_df = spark.createDataFrame(metrics_df)

metrics_spark_df.write.format("snowflake") \
    .options(**OPTIONS) \
    .option("dbtable", "proddb.fionafan.session_conversion_model_metrics_pyspark") \
    .mode("overwrite") \
    .save()

print(f"✅ Saved metrics to Snowflake")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizations

# COMMAND ----------

# Get predictions as pandas for plotting
pred_pre_pd = predictions_pre.select("FUNNEL_CONVERTED_BOOL", "probability").toPandas()
pred_full_pd = predictions_full.select("FUNNEL_CONVERTED_BOOL", "probability").toPandas()

# Extract probability of positive class
pred_pre_pd['prob_positive'] = pred_pre_pd['probability'].apply(lambda x: float(x[1]))
pred_full_pd['prob_positive'] = pred_full_pd['probability'].apply(lambda x: float(x[1]))

# COMMAND ----------

# Plot ROC curves
from sklearn.metrics import roc_curve, auc

fig, ax = plt.subplots(figsize=(10, 8))

colors = ['#1f77b4', '#ff7f0e']

# Pre-Funnel Model
fpr_pre, tpr_pre, _ = roc_curve(pred_pre_pd['FUNNEL_CONVERTED_BOOL'], pred_pre_pd['prob_positive'])
roc_auc_pre = auc(fpr_pre, tpr_pre)
ax.plot(fpr_pre, tpr_pre, color=colors[0], lw=2, label=f'Pre-Funnel Model (AUC = {roc_auc_pre:.4f})')

# Funnel-Inclusive Model
fpr_full, tpr_full, _ = roc_curve(pred_full_pd['FUNNEL_CONVERTED_BOOL'], pred_full_pd['prob_positive'])
roc_auc_full = auc(fpr_full, tpr_full)
ax.plot(fpr_full, tpr_full, color=colors[1], lw=2, label=f'Funnel-Inclusive Model (AUC = {roc_auc_full:.4f})')

# Plot diagonal
ax.plot([0, 1], [0, 1], 'k--', lw=1, label='Random Classifier')

ax.set_xlim([0.0, 1.0])
ax.set_ylim([0.0, 1.05])
ax.set_xlabel('False Positive Rate', fontsize=12)
ax.set_ylabel('True Positive Rate', fontsize=12)
ax.set_title('ROC Curves: Session Conversion Prediction Models', fontsize=14, fontweight='bold')
ax.legend(loc="lower right", fontsize=11)
ax.grid(alpha=0.3)
plt.tight_layout()

display(fig)

# COMMAND ----------

# Plot coefficient comparison
fig, axes = plt.subplots(1, 2, figsize=(18, 10))

top_n = 15

for idx, (model_name, coef_df) in enumerate([('Pre-Funnel Model', coef_pre), ('Funnel-Inclusive Model', coef_full)]):
    ax = axes[idx]
    
    top_features = coef_df.head(top_n).copy()
    
    # Create color based on sign
    colors = ['#d62728' if x < 0 else '#2ca02c' for x in top_features['pct_impact_1sd']]
    
    # Horizontal bar plot
    y_pos = np.arange(len(top_features))
    ax.barh(y_pos, top_features['pct_impact_1sd'], color=colors, alpha=0.7)
    
    # Customize
    ax.set_yticks(y_pos)
    ax.set_yticklabels(top_features['feature'], fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel('% Change in Odds (for 1 SD increase)', fontsize=11, fontweight='bold')
    ax.set_title(f'{model_name}\nTop {top_n} Features by Impact', fontsize=12, fontweight='bold')
    ax.axvline(x=0, color='black', linestyle='-', linewidth=0.8)
    ax.grid(axis='x', alpha=0.3)
    
    # Add value labels
    for i, (idx_val, row) in enumerate(top_features.iterrows()):
        value = row['pct_impact_1sd']
        x_pos = value + (5 if value > 0 else -5)
        ha = 'left' if value > 0 else 'right'
        ax.text(x_pos, i, f'{value:+.1f}%', va='center', ha=ha, fontsize=8)

plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Comparison Summary

# COMMAND ----------

print("\n" + "="*80)
print("MODEL COMPARISON SUMMARY")
print("="*80)
print(f"\n{'Metric':<30} {'Pre-Funnel':<15} {'Funnel-Inclusive':<15} {'Difference':<15}")
print("-"*75)

comparison_metrics = [
    ('Test AUC-ROC', 'test_auc'),
    ('Test Accuracy', 'test_accuracy'),
    ('Test Precision', 'test_precision'),
    ('Test Recall', 'test_recall'),
    ('Test F1-Score', 'test_f1'),
    ('Number of Features', 'num_features'),
]

for metric_name, metric_key in comparison_metrics:
    val_pre = metrics_pre[metric_key]
    val_full = metrics_full[metric_key]
    diff = val_full - val_pre
    
    if metric_key == 'num_features':
        print(f"{metric_name:<30} {val_pre:<15.0f} {val_full:<15.0f} {diff:<+15.0f}")
    else:
        print(f"{metric_name:<30} {val_pre:<15.4f} {val_full:<15.4f} {diff:<+15.4f}")

print("\n" + "="*80)
print("ANALYSIS COMPLETE!")
print("="*80)
print(f"\nResults saved to:")
print(f"  - proddb.fionafan.session_conversion_model_coefficients_pyspark")
print(f"  - proddb.fionafan.session_conversion_model_metrics_pyspark")

# COMMAND ----------



