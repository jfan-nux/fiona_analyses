# Databricks notebook source
# MAGIC %md
# MAGIC # Session Clustering Analysis (PySpark)
# MAGIC 
# MAGIC Clusters sessions based on behavioral features and analyzes cluster characteristics.
# MAGIC Excludes outcome variables (checkout, order placement) from clustering features.
# MAGIC 
# MAGIC **Key Steps**:
# MAGIC 1. Load session features from Snowflake
# MAGIC 2. Select clustering features (excluding outcome variables)
# MAGIC 3. Determine optimal number of clusters
# MAGIC 4. Fit K-Means clustering
# MAGIC 5. Analyze and visualize cluster characteristics

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Set plotting style
plt.style.use('seaborn-v0_8-darkgrid')
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
# MAGIC ## Load Data

# COMMAND ----------

# Load session data from Snowflake
query = """
SELECT *
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
WHERE total_events > 0  -- Exclude empty sessions
"""

df_spark = (
    spark.read.format("snowflake")
    .options(**OPTIONS)
    .option("query", query)
    .load()
)

session_count = df_spark.count()
column_count = len(df_spark.columns)

print(f"Loaded {session_count:,} sessions")
print(f"Columns: {column_count}")

# Cache for reuse
df_spark.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Clustering Features

# COMMAND ----------

def select_clustering_features():
    """
    Select features for clustering, excluding outcome variables.
    
    Excluded outcome variables:
    - funnel_had_order, funnel_converted_bool, funnel_num_orders
    - funnel_num_success, funnel_reached_success
    - funnel_seconds_to_success, funnel_seconds_to_checkout
    """
    
    clustering_features = []
    
    # Temporal features
    clustering_features.extend([
        'DAYS_SINCE_ONBOARDING',
        'DAYS_SINCE_ONBOARDING_MOD7',
        'SESSION_DAY_OF_WEEK',
    ])
    
    # Overall session metrics
    clustering_features.extend([
        'TOTAL_EVENTS',
        'SESSION_DURATION_SECONDS',
        'EVENTS_PER_MINUTE',
        'NUM_UNIQUE_DISCOVERY_FEATURES',
        'NUM_UNIQUE_DISCOVERY_SURFACES',
        'NUM_UNIQUE_STORES',
    ])
    
    # Impression features
    clustering_features.extend([
        'IMPRESSION_EVENT_COUNT',
        'IMPRESSION_UNIQUE_STORES',
        'IMPRESSION_UNIQUE_FEATURES',
        'IMPRESSION_HAD_ANY',
    ])
    
    # Action features
    clustering_features.extend([
        'ACTION_EVENT_COUNT',
        'ACTION_NUM_BACKGROUNDS',
        'ACTION_NUM_FOREGROUNDS',
        'ACTION_NUM_TAB_SWITCHES',
        'ACTION_NUM_ADDRESS_ACTIONS',
        'ACTION_NUM_PAYMENT_ACTIONS',
        'ACTION_NUM_AUTH_ACTIONS',
        'ACTION_HAD_TAB_SWITCH',
        'ACTION_HAD_FOREGROUND',
    ])
    
    # Attribution features
    clustering_features.extend([
        'ATTRIBUTION_EVENT_COUNT',
        'ATTRIBUTION_NUM_CARD_CLICKS',
        'ATTRIBUTION_UNIQUE_STORES_CLICKED',
        'ATTRIBUTION_HAD_ANY',
        'ATTRIBUTION_CNT_TRADITIONAL_CAROUSEL',
        'ATTRIBUTION_CNT_CORE_SEARCH',
        'ATTRIBUTION_CNT_PILL_FILTER',
        'ATTRIBUTION_CNT_HOME_FEED',
    ])
    
    # Error features
    clustering_features.extend([
        'ERROR_EVENT_COUNT',
    ])
    
    # Funnel features (EXCLUDING ORDER/CONVERSION OUTCOMES)
    clustering_features.extend([
        'FUNNEL_NUM_ADDS',
        'FUNNEL_HAD_ADD',
        'FUNNEL_NUM_STORE_PAGE',
        'FUNNEL_NUM_CART',
        'FUNNEL_REACHED_STORE_BOOL',
        'FUNNEL_REACHED_CART_BOOL',
    ])
    
    # Timing features (EXCLUDING order/success timing)
    clustering_features.extend([
        'TIMING_MEAN_INTER_EVENT_SECONDS',
        'TIMING_MEDIAN_INTER_EVENT_SECONDS',
        'TIMING_EVENTS_FIRST_30S',
        'TIMING_EVENTS_FIRST_2MIN',
        'FUNNEL_SECONDS_TO_FIRST_ADD',
        'FUNNEL_SECONDS_TO_STORE_PAGE',
        'FUNNEL_SECONDS_TO_CART',
        'ATTRIBUTION_SECONDS_TO_FIRST_CARD_CLICK',
        'ACTION_SECONDS_TO_FIRST_TAB_SWITCH',
    ])
    
    # Store features
    clustering_features.extend([
        'STORE_HAD_NV',
        'STORE_NV_IMPRESSION_OCCURRED',
    ])
    
    return clustering_features

features = select_clustering_features()
print(f"\nDefined {len(features)} features for clustering")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preprocess Features

# COMMAND ----------

# Filter to features that exist in the dataframe
available_features = [f for f in features if f in df_spark.columns]
missing_features = [f for f in features if f not in df_spark.columns]

if missing_features:
    print(f"\nWarning: {len(missing_features)} features not found in data:")
    print(missing_features[:10])

print(f"\nUsing {len(available_features)} features for clustering")

# Handle missing values - fill with 0
df_processed = df_spark
for feature in available_features:
    df_processed = df_processed.withColumn(
        feature, 
        F.coalesce(F.col(feature).cast("double"), F.lit(0.0))
    )

# Assemble features into vector
assembler = VectorAssembler(
    inputCols=available_features,
    outputCol="features_raw",
    handleInvalid="keep"
)

df_assembled = assembler.transform(df_processed)

# Scale features
scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withMean=True,
    withStd=True
)

scaler_model = scaler.fit(df_assembled)
df_scaled = scaler_model.transform(df_assembled)

print(f"\nPreprocessed {df_scaled.count():,} sessions")

# Cache scaled data
df_scaled.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Determine Optimal Number of Clusters

# COMMAND ----------

# Subsample for faster optimization if dataset is large
if df_scaled.count() > 50000:
    print(f"Subsampling 50,000 sessions for cluster optimization...")
    df_sample = df_scaled.sample(fraction=50000/df_scaled.count(), seed=42)
else:
    df_sample = df_scaled

df_sample.cache()

# Test different k values
max_k = 10
k_range = range(2, max_k + 1)

results = []

for k in k_range:
    print(f"\nTesting k={k}...")
    
    # Fit KMeans
    kmeans = KMeans(
        featuresCol="features",
        predictionCol="prediction",
        k=k,
        seed=42,
        maxIter=20
    )
    
    model = kmeans.fit(df_sample)
    predictions = model.transform(df_sample)
    
    # Calculate silhouette score
    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="prediction",
        metricName="silhouette"
    )
    
    silhouette = evaluator.evaluate(predictions)
    inertia = model.summary.trainingCost
    
    results.append({
        'k': k,
        'silhouette': silhouette,
        'inertia': inertia
    })
    
    print(f"  Silhouette: {silhouette:.4f}, Inertia: {inertia:.2f}")

results_df = pd.DataFrame(results)

# COMMAND ----------

# Plot optimization metrics
fig, axes = plt.subplots(1, 2, figsize=(16, 5))

# Elbow plot
axes[0].plot(results_df['k'], results_df['inertia'], 'bo-', linewidth=2, markersize=8)
axes[0].set_xlabel('Number of Clusters (k)', fontsize=12)
axes[0].set_ylabel('Inertia', fontsize=12)
axes[0].set_title('Elbow Method', fontsize=14, fontweight='bold')
axes[0].grid(True, alpha=0.3)

# Silhouette score
axes[1].plot(results_df['k'], results_df['silhouette'], 'go-', linewidth=2, markersize=8)
axes[1].set_xlabel('Number of Clusters (k)', fontsize=12)
axes[1].set_ylabel('Silhouette Score', fontsize=12)
axes[1].set_title('Silhouette Score (higher is better)', fontsize=14, fontweight='bold')
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
display(fig)

# Find best k
best_k = int(results_df.loc[results_df['silhouette'].idxmax(), 'k'])
best_silhouette = results_df['silhouette'].max()

print(f"\n✅ Recommended k by Silhouette Score: {best_k} (score: {best_silhouette:.3f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fit Final K-Means Model

# COMMAND ----------

print(f"\nFitting K-Means with {best_k} clusters on full dataset...")

# Fit on full dataset
kmeans_final = KMeans(
    featuresCol="features",
    predictionCol="cluster",
    k=best_k,
    seed=42,
    maxIter=50
)

kmeans_model = kmeans_final.fit(df_scaled)
df_clustered = kmeans_model.transform(df_scaled)

# Calculate final silhouette score
evaluator = ClusteringEvaluator(
    featuresCol="features",
    predictionCol="cluster",
    metricName="silhouette"
)

final_silhouette = evaluator.evaluate(df_clustered)
print(f"Final Silhouette Score: {final_silhouette:.3f}")

# Cache clustered data
df_clustered.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize Clusters with PCA

# COMMAND ----------

# Apply PCA for visualization
pca = PCA(
    k=2,
    inputCol="features",
    outputCol="pca_features"
)

pca_model = pca.fit(df_clustered)
df_pca = pca_model.transform(df_clustered)

# Convert to pandas for plotting (subsample if too large)
if df_pca.count() > 20000:
    df_pca_sample = df_pca.sample(fraction=20000/df_pca.count(), seed=42)
else:
    df_pca_sample = df_pca

pca_pd = df_pca_sample.select("pca_features", "cluster").toPandas()

# Extract PC1 and PC2
pca_pd['PC1'] = pca_pd['pca_features'].apply(lambda x: float(x[0]))
pca_pd['PC2'] = pca_pd['pca_features'].apply(lambda x: float(x[1]))

# Plot
fig, ax = plt.subplots(figsize=(14, 10))

for cluster in range(best_k):
    cluster_data = pca_pd[pca_pd['cluster'] == cluster]
    ax.scatter(cluster_data['PC1'], cluster_data['PC2'], 
              label=f'Cluster {cluster}', alpha=0.5, s=20)

explained_variance = pca_model.explainedVariance.toArray()
ax.set_xlabel(f'PC1 ({explained_variance[0]:.1%} variance)', fontsize=12)
ax.set_ylabel(f'PC2 ({explained_variance[1]:.1%} variance)', fontsize=12)
ax.set_title(f'Session Clusters (PCA Visualization)', fontsize=14, fontweight='bold')
ax.legend(fontsize=10)
ax.grid(True, alpha=0.3)

plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze Cluster Characteristics

# COMMAND ----------

# Get cluster sizes
cluster_sizes = df_clustered.groupBy("cluster").count().orderBy("cluster").toPandas()

print("\n" + "="*80)
print("CLUSTER SIZES")
print("="*80)
total_sessions = df_clustered.count()
for _, row in cluster_sizes.iterrows():
    cluster = row['cluster']
    size = row['count']
    pct = size / total_sessions * 100
    print(f"Cluster {cluster}: {size:,} sessions ({pct:.1f}%)")

# COMMAND ----------

# Analyze key metrics by cluster
key_metrics = [
    # Session basics
    'TOTAL_EVENTS',
    'SESSION_DURATION_SECONDS',
    'EVENTS_PER_MINUTE',
    
    # User journey
    'IMPRESSION_EVENT_COUNT',
    'ATTRIBUTION_NUM_CARD_CLICKS',
    'FUNNEL_NUM_ADDS',
    'FUNNEL_NUM_CART',
    
    # Outcome variables (for understanding, not clustering)
    'FUNNEL_HAD_ORDER',
    'FUNNEL_CONVERTED_BOOL',
    
    # Timing
    'TIMING_MEDIAN_INTER_EVENT_SECONDS',
    'FUNNEL_SECONDS_TO_FIRST_ADD',
    'ATTRIBUTION_SECONDS_TO_FIRST_CARD_CLICK',
    
    # Behavior flags
    'ACTION_HAD_TAB_SWITCH',
    'ACTION_NUM_PAYMENT_ACTIONS',
    'ERROR_EVENT_COUNT',
    
    # Store
    'NUM_UNIQUE_STORES',
    'STORE_HAD_NV',
]

# Filter to available metrics
available_metrics = [m for m in key_metrics if m in df_clustered.columns]

# Calculate statistics
cluster_stats_list = []
for cluster_id in range(best_k):
    cluster_data = df_clustered.filter(F.col("cluster") == cluster_id)
    
    stats = {'cluster': cluster_id}
    for metric in available_metrics:
        avg_val = cluster_data.agg(F.avg(metric)).collect()[0][0]
        stats[f"{metric}_mean"] = avg_val
    
    cluster_stats_list.append(stats)

cluster_stats_pd = pd.DataFrame(cluster_stats_list)

# COMMAND ----------

# Print detailed cluster profiles
print("\n" + "="*80)
print("CLUSTER PROFILES")
print("="*80)

for cluster_id in range(best_k):
    cluster_data = df_clustered.filter(F.col("cluster") == cluster_id)
    cluster_count = cluster_data.count()
    cluster_pct = cluster_count / total_sessions * 100
    
    print(f"\n{'='*80}")
    print(f"CLUSTER {cluster_id} - {cluster_count:,} sessions ({cluster_pct:.1f}%)")
    print(f"{'='*80}")
    
    # Session intensity
    print(f"\n📊 Session Intensity:")
    avg_duration = cluster_data.agg(F.avg("SESSION_DURATION_SECONDS")).collect()[0][0] or 0
    median_duration = cluster_data.approxQuantile("SESSION_DURATION_SECONDS", [0.5], 0.01)[0]
    avg_events = cluster_data.agg(F.avg("TOTAL_EVENTS")).collect()[0][0] or 0
    avg_events_per_min = cluster_data.agg(F.avg("EVENTS_PER_MINUTE")).collect()[0][0] or 0
    
    print(f"  - Avg Duration: {avg_duration:.0f} seconds ({avg_duration/60:.1f} min)")
    print(f"  - Median Duration: {median_duration:.0f} seconds")
    print(f"  - Avg Total Events: {avg_events:.1f}")
    print(f"  - Avg Events/Minute: {avg_events_per_min:.1f}")
    
    # User journey
    print(f"\n🛒 User Journey:")
    avg_impressions = cluster_data.agg(F.avg("IMPRESSION_EVENT_COUNT")).collect()[0][0] or 0
    avg_clicks = cluster_data.agg(F.avg("ATTRIBUTION_NUM_CARD_CLICKS")).collect()[0][0] or 0
    avg_adds = cluster_data.agg(F.avg("FUNNEL_NUM_ADDS")).collect()[0][0] or 0
    pct_cart = cluster_data.agg(F.avg("FUNNEL_REACHED_CART_BOOL")).collect()[0][0] or 0
    
    print(f"  - Avg Impressions: {avg_impressions:.1f}")
    print(f"  - Avg Card Clicks: {avg_clicks:.1f}")
    print(f"  - Avg Adds to Cart: {avg_adds:.1f}")
    print(f"  - % Reached Cart: {pct_cart*100:.1f}%")
    
    # Conversion outcomes
    print(f"\n🎯 Outcomes (not used for clustering):")
    pct_order = cluster_data.agg(F.avg("FUNNEL_HAD_ORDER")).collect()[0][0] or 0
    pct_converted = cluster_data.agg(F.avg("FUNNEL_CONVERTED_BOOL")).collect()[0][0] or 0
    
    print(f"  - % Had Order: {pct_order*100:.1f}%")
    print(f"  - % Converted: {pct_converted*100:.1f}%")
    
    # Store interaction
    print(f"\n🏪 Store Interaction:")
    avg_stores = cluster_data.agg(F.avg("NUM_UNIQUE_STORES")).collect()[0][0] or 0
    pct_nv = cluster_data.agg(F.avg("STORE_HAD_NV")).collect()[0][0] or 0
    
    print(f"  - Avg Unique Stores: {avg_stores:.1f}")
    print(f"  - % Had NV Store: {pct_nv*100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster Comparison Visualizations

# COMMAND ----------

# Plot 1: Session Duration and Event Count by Cluster
fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# Collect data for plotting
cluster_duration_data = []
cluster_events_data = []

for cluster_id in range(best_k):
    cluster_data = df_clustered.filter(F.col("cluster") == cluster_id)
    
    # Duration (in minutes, clipped at 30)
    duration_sample = cluster_data.select((F.col("SESSION_DURATION_SECONDS")/60).alias("duration_min")) \
        .filter(F.col("duration_min") <= 30) \
        .sample(fraction=min(1.0, 5000/cluster_data.count()), seed=42) \
        .toPandas()
    cluster_duration_data.append(duration_sample['duration_min'].values)
    
    # Events (clipped at 200)
    events_sample = cluster_data.select("TOTAL_EVENTS") \
        .filter(F.col("TOTAL_EVENTS") <= 200) \
        .sample(fraction=min(1.0, 5000/cluster_data.count()), seed=42) \
        .toPandas()
    cluster_events_data.append(events_sample['TOTAL_EVENTS'].values)

# Duration distribution
for cluster_id, data in enumerate(cluster_duration_data):
    axes[0, 0].hist(data, bins=50, alpha=0.5, label=f'Cluster {cluster_id}')
axes[0, 0].set_xlabel('Session Duration (minutes)', fontsize=11)
axes[0, 0].set_ylabel('Frequency', fontsize=11)
axes[0, 0].set_title('Session Duration Distribution by Cluster', fontsize=12, fontweight='bold')
axes[0, 0].legend()
axes[0, 0].set_xlim(0, 30)

# Event count distribution
for cluster_id, data in enumerate(cluster_events_data):
    axes[0, 1].hist(data, bins=50, alpha=0.5, label=f'Cluster {cluster_id}')
axes[0, 1].set_xlabel('Total Events', fontsize=11)
axes[0, 1].set_ylabel('Frequency', fontsize=11)
axes[0, 1].set_title('Total Events Distribution by Cluster', fontsize=12, fontweight='bold')
axes[0, 1].legend()
axes[0, 1].set_xlim(0, 200)

# Funnel progression by cluster
funnel_metrics = ['FUNNEL_REACHED_STORE_BOOL', 'FUNNEL_HAD_ADD', 
                  'FUNNEL_REACHED_CART_BOOL', 'FUNNEL_CONVERTED_BOOL']

funnel_rates = df_clustered.groupBy("cluster").agg(
    *[F.avg(m).alias(m) for m in funnel_metrics]
).orderBy("cluster").toPandas()

funnel_rates[funnel_metrics] = funnel_rates[funnel_metrics] * 100
funnel_rates.set_index('cluster')[funnel_metrics].plot(kind='bar', ax=axes[1, 0])
axes[1, 0].set_xlabel('Cluster', fontsize=11)
axes[1, 0].set_ylabel('% of Sessions', fontsize=11)
axes[1, 0].set_title('Funnel Progression by Cluster', fontsize=12, fontweight='bold')
axes[1, 0].legend(['Store Page', 'Add to Cart', 'Cart', 'Converted'], fontsize=9)
axes[1, 0].set_xticklabels(axes[1, 0].get_xticklabels(), rotation=0)

# Average adds and clicks by cluster
journey_metrics = df_clustered.groupBy("cluster").agg(
    F.avg("ATTRIBUTION_NUM_CARD_CLICKS").alias("Card Clicks"),
    F.avg("FUNNEL_NUM_ADDS").alias("Cart Adds")
).orderBy("cluster").toPandas()

journey_metrics.set_index('cluster')[["Card Clicks", "Cart Adds"]].plot(kind='bar', ax=axes[1, 1])
axes[1, 1].set_xlabel('Cluster', fontsize=11)
axes[1, 1].set_ylabel('Average Count', fontsize=11)
axes[1, 1].set_title('Card Clicks & Cart Adds by Cluster', fontsize=12, fontweight='bold')
axes[1, 1].set_xticklabels(axes[1, 1].get_xticklabels(), rotation=0)

plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results to Snowflake

# COMMAND ----------

# Save cluster assignments
cluster_assignments = df_clustered.select(
    "USER_ID", "DD_DEVICE_ID", "EVENT_DATE", "SESSION_NUM",
    "COHORT_TYPE", "SESSION_TYPE", "cluster"
)

cluster_assignments.write.format("snowflake") \
    .options(**OPTIONS) \
    .option("dbtable", "proddb.fionafan.session_cluster_assignments_pyspark") \
    .mode("overwrite") \
    .save()

print(f"\n✅ Saved cluster assignments to Snowflake")

# COMMAND ----------

# Save cluster statistics
cluster_stats_spark = spark.createDataFrame(cluster_stats_pd)
cluster_stats_spark = cluster_stats_spark.withColumn("created_at", F.current_timestamp())

cluster_stats_spark.write.format("snowflake") \
    .options(**OPTIONS) \
    .option("dbtable", "proddb.fionafan.session_cluster_statistics_pyspark") \
    .mode("overwrite") \
    .save()

print(f"✅ Saved cluster statistics to Snowflake")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis Summary

# COMMAND ----------

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80)
print(f"\nOptimal number of clusters: {best_k}")
print(f"Final Silhouette Score: {final_silhouette:.3f}")
print(f"\nTotal sessions analyzed: {total_sessions:,}")
print(f"\nResults saved to:")
print(f"  - proddb.fionafan.session_cluster_assignments_pyspark")
print(f"  - proddb.fionafan.session_cluster_statistics_pyspark")

# COMMAND ----------



