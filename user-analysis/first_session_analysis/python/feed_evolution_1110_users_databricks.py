# Databricks notebook source
# MAGIC %md
# MAGIC # Feed Evolution Analysis for 1110 Activity Pattern Users
# MAGIC 
# MAGIC **Compares store embeddings across sessions to latest_week_3 final state**
# MAGIC 
# MAGIC ### Base Table
# MAGIC - `proddb.fionafan.all_user_sessions_with_events_features_gen`
# MAGIC - Creation Logic: `user-analysis/first_session_analysis/sql/sessions_features_gen.sql`
# MAGIC 
# MAGIC ### Analysis Steps:
# MAGIC 1. Samples 50K users from each cohort (new/post_onboarding/active) with 1110 activity pattern
# MAGIC 2. For each user, extracts their store features from different session types
# MAGIC    - first_session, second_session, third_session
# MAGIC    - latest_week_1, latest_week_2
# MAGIC    - latest_week_3 (used as final state reference)
# MAGIC 3. Generates BGE embeddings for store tags, categories, and names (by event type: impression, attribution, funnel_store)
# MAGIC 4. Calculates cosine similarity from each session to the latest_week_3 final state
# MAGIC 5. Outputs results at consumer_id + session_type + cohort level with 3 similarity scores per row

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Required Packages
# MAGIC 
# MAGIC **Note:** Run this cell once at the start to install/upgrade sentence-transformers library

# COMMAND ----------

# MAGIC %pip install -U sentence-transformers scikit-learn

# COMMAND ----------

# Restart Python to load the updated packages
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Setup and Imports

# COMMAND ----------

import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from pyspark.sql import functions as F
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

print("=" * 80)
print("FEED EVOLUTION ANALYSIS FOR 1110 ACTIVITY PATTERN USERS")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Snowflake Connection Setup

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

print("✅ Snowflake connection configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Configuration

# COMMAND ----------

# Configuration
SAMPLE_SIZE = 50000  # Sample size per cohort
OUTPUT_TABLE = "proddb.fionafan.feed_evolution_1110_cosine_similarity"

# Session types to analyze (comparing all to latest_week_3 as final state)
SESSION_TYPES = [
    'first_session',
    'second_session', 
    'third_session',
    'latest_week_1',
    'latest_week_2'
]

# Final state for comparison
FINAL_SESSION_TYPE = 'latest_week_3'

# Store types and their columns
STORE_TYPES = [
    ('impression', 'STORE_TAGS_CONCAT_IMPRESSION', 'STORE_CATEGORIES_CONCAT_IMPRESSION', 'STORE_NAMES_CONCAT_IMPRESSION'),
    ('attribution', 'STORE_TAGS_CONCAT_ATTRIBUTION', 'STORE_CATEGORIES_CONCAT_ATTRIBUTION', 'STORE_NAMES_CONCAT_ATTRIBUTION'),
    ('funnel_store', 'STORE_TAGS_CONCAT_FUNNEL_STORE', 'STORE_CATEGORIES_CONCAT_FUNNEL_STORE', 'STORE_NAMES_CONCAT_FUNNEL_STORE'),
]

print(f"Sample size per cohort: {SAMPLE_SIZE:,}")
print(f"Output table: {OUTPUT_TABLE}")
print(f"Session types: {SESSION_TYPES}")
print(f"Final session type: {FINAL_SESSION_TYPE}")
print(f"Store types: {len(STORE_TYPES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Load Data and Sample Users

# COMMAND ----------

print("\n" + "=" * 80)
print("STEP 1: LOADING AND SAMPLING DATA")
print("=" * 80)

print(f"\n📥 Loading session-level feature generation table...")

# Load data with 1110 activity pattern
query = """
SELECT 
    f.USER_ID AS CONSUMER_ID,
    f.COHORT_TYPE,
    f.SESSION_TYPE,
    f.DD_DEVICE_ID,
    f.EVENT_DATE,
    f.SESSION_NUM,
    f.STORE_TAGS_CONCAT_IMPRESSION,
    f.STORE_CATEGORIES_CONCAT_IMPRESSION,
    f.STORE_NAMES_CONCAT_IMPRESSION,
    f.STORE_TAGS_CONCAT_ATTRIBUTION,
    f.STORE_CATEGORIES_CONCAT_ATTRIBUTION,
    f.STORE_NAMES_CONCAT_ATTRIBUTION,
    f.STORE_TAGS_CONCAT_FUNNEL_STORE,
    f.STORE_CATEGORIES_CONCAT_FUNNEL_STORE,
    f.STORE_NAMES_CONCAT_FUNNEL_STORE
FROM proddb.fionafan.all_user_sessions_with_events_features_gen f
INNER JOIN proddb.fionafan.users_activity_1110 a
    ON f.USER_ID = a.CONSUMER_ID
WHERE 1=1
    AND f.SESSION_TYPE IN ('first_session', 'second_session', 'third_session', 
                           'latest_week_1', 'latest_week_2', 'latest_week_3')
"""

df = (
    spark.read.format("snowflake")
    .options(**OPTIONS)
    .option("query", query)
    .load()
)

print(f"   Total sessions with 1110 activity pattern: {df.count():,}")

# COMMAND ----------

# Sample 50K users from each cohort
print(f"\n🎲 Sampling {SAMPLE_SIZE:,} users from each cohort...")

# Get distinct users by cohort
users_new = df.filter(F.col("COHORT_TYPE") == "new") \
    .select("CONSUMER_ID").distinct().limit(SAMPLE_SIZE)
users_post_onboarding = df.filter(F.col("COHORT_TYPE") == "post_onboarding") \
    .select("CONSUMER_ID").distinct().limit(SAMPLE_SIZE)
users_active = df.filter(F.col("COHORT_TYPE") == "active") \
    .select("CONSUMER_ID").distinct().limit(SAMPLE_SIZE)

print(f"   New users sampled: {users_new.count():,}")
print(f"   Post-onboarding users sampled: {users_post_onboarding.count():,}")
print(f"   Active users sampled: {users_active.count():,}")

# Get all sessions for sampled users
sampled_users = users_new.union(users_post_onboarding).union(users_active)
df_sampled = df.join(sampled_users, on="CONSUMER_ID", how="inner")

print(f"   Total sessions for sampled users: {df_sampled.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Prepare Session-Level Data

# COMMAND ----------

print("\n" + "=" * 80)
print("STEP 2: PREPARING SESSION-LEVEL DATA")
print("=" * 80)

# Data is already at session level with SESSION_TYPE column
# Just split by session type
session_dfs = {}

for session_type in SESSION_TYPES + [FINAL_SESSION_TYPE]:
    print(f"\n📋 Filtering {session_type} data...")
    
    session_df = df_sampled.filter(F.col("SESSION_TYPE") == session_type)
    session_dfs[session_type] = session_df
    
    print(f"   ✅ {session_type}: {session_df.count():,} sessions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Load BGE Model and Generate Week 3 Embeddings

# COMMAND ----------

print("\n" + "=" * 80)
print("STEP 3: GENERATING EMBEDDINGS AND CALCULATING SIMILARITY")
print("=" * 80)

print("\n🤖 Loading BGE sentence transformer model...")
model = SentenceTransformer('BAAI/bge-base-en-v1.5')
print("   ✅ BGE model loaded successfully")

# COMMAND ----------

# Convert latest_week_3 data to pandas for embedding generation
print(f"\n📊 Processing {FINAL_SESSION_TYPE} embeddings (final state)...")
week3_pd = session_dfs[FINAL_SESSION_TYPE].toPandas()

print(f"   {FINAL_SESSION_TYPE} sessions loaded: {len(week3_pd):,}")

week3_embeddings = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Process Week 3 Embeddings by Store Type

# COMMAND ----------

for store_type, tags_col, categories_col, names_col in STORE_TYPES:
    print(f"\n  📊 Processing {FINAL_SESSION_TYPE.upper()} {store_type.upper()} embeddings...")
    
    # Combine store text features (columns are already in the right format from SQL)
    week3_pd[f'STORE_TEXT_{store_type.upper()}'] = (
        week3_pd[tags_col].fillna('') + ' | ' +
        week3_pd[categories_col].fillna('') + ' | ' +
        week3_pd[names_col].fillna('')
    )
    
    # Clean up empty/null combinations more comprehensively
    text_col = f'STORE_TEXT_{store_type.upper()}'
    week3_pd[text_col] = week3_pd[text_col].str.strip()  # Remove leading/trailing spaces
    
    # Replace various empty patterns with placeholder
    empty_patterns = ['', ' | ', ' |  | ', '|', ' |  |', ' | |', ' | | ', '||']
    for pattern in empty_patterns:
        week3_pd[text_col] = week3_pd[text_col].replace(pattern, 'no_store_info')
    
    # Also replace if only whitespace remains
    week3_pd[text_col] = week3_pd[text_col].apply(
        lambda x: 'no_store_info' if not x or x.strip() == '' or set(x.replace(' ', '').replace('|', '')) == set() else x
    )
    
    # Track which sessions have valid store data
    week3_pd[f'HAS_STORE_DATA_{store_type.upper()}'] = (week3_pd[text_col] != 'no_store_info').astype(int)
    
    print(f"    Sessions with valid {store_type} data: {week3_pd[f'HAS_STORE_DATA_{store_type.upper()}'].sum():,} / {len(week3_pd):,}")
    
    print(f"    Generating embeddings for {len(week3_pd):,} users...")
    
    # Generate embeddings in batches
    batch_size = 1000
    embeddings_list = []
    
    for i in range(0, len(week3_pd), batch_size):
        batch = week3_pd[f'STORE_TEXT_{store_type.upper()}'].iloc[i:i+batch_size].tolist()
        batch_embeddings = model.encode(batch, show_progress_bar=False)
        embeddings_list.append(batch_embeddings)
        
        if (i // batch_size + 1) % 10 == 0:
            print(f"      Processed {i:,} / {len(week3_pd):,} users...")
    
    embeddings = np.vstack(embeddings_list)
    print(f"    ✅ Generated embeddings with shape: {embeddings.shape}")
    
    # Store embeddings indexed by consumer_id
    week3_embeddings[store_type] = {
        consumer_id: embeddings[idx] 
        for idx, consumer_id in enumerate(week3_pd['CONSUMER_ID'].values)
    }
    
    # Store validity flag for week3 sessions (whether they have real store data)
    week3_embeddings[f'{store_type}_has_data'] = {
        consumer_id: (week3_pd.iloc[idx][f'HAS_STORE_DATA_{store_type.upper()}'] == 1)
        for idx, consumer_id in enumerate(week3_pd['CONSUMER_ID'].values)
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Calculate Similarity for Each Session Type

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper Functions for Parallel Processing
# MAGIC 
# MAGIC **Parallelization Strategy:**
# MAGIC - **Level 1:** Process up to 3 session types concurrently (first_session, second_session, etc.)
# MAGIC - **Level 2:** Within each session, process 3 store types concurrently (impression, attribution, funnel_store)
# MAGIC - **Result:** Up to 9x speedup (3 sessions × 3 store types) compared to sequential processing
# MAGIC - **Thread-safe:** Uses locks for print statements to avoid garbled output

# COMMAND ----------

# Thread-safe print lock
print_lock = threading.Lock()

def safe_print(*args, **kwargs):
    """Thread-safe print function"""
    with print_lock:
        print(*args, **kwargs)

def process_store_type(session_pd, store_type, tags_col, categories_col, names_col, 
                       session_type, model, week3_embeddings, FINAL_SESSION_TYPE):
    """
    Process a single store type: generate embeddings and calculate similarities
    Returns: (store_type, similarities, valid_comparisons_count)
    """
    safe_print(f"\n  📊 Processing {session_type} {store_type.upper()} embeddings...")
    
    # Create a copy to avoid threading issues
    session_data = session_pd.copy()
    
    # Combine store text features
    session_data[f'STORE_TEXT_{store_type.upper()}'] = (
        session_data[tags_col].fillna('') + ' | ' +
        session_data[categories_col].fillna('') + ' | ' +
        session_data[names_col].fillna('')
    )
    
    # Clean up empty/null combinations more comprehensively
    text_col = f'STORE_TEXT_{store_type.upper()}'
    session_data[text_col] = session_data[text_col].str.strip()
    
    # Replace various empty patterns with placeholder
    empty_patterns = ['', ' | ', ' |  | ', '|', ' |  |', ' | |', ' | | ', '||']
    for pattern in empty_patterns:
        session_data[text_col] = session_data[text_col].replace(pattern, 'no_store_info')
    
    # Also replace if only whitespace remains
    session_data[text_col] = session_data[text_col].apply(
        lambda x: 'no_store_info' if not x or x.strip() == '' or set(x.replace(' ', '').replace('|', '')) == set() else x
    )
    
    # Track which sessions have valid store data
    session_data[f'HAS_STORE_DATA_{store_type.upper()}'] = (session_data[text_col] != 'no_store_info').astype(int)
    
    valid_count = session_data[f'HAS_STORE_DATA_{store_type.upper()}'].sum()
    safe_print(f"    Sessions with valid {store_type} data: {valid_count:,} / {len(session_data):,}")
    
    safe_print(f"    Generating embeddings for {len(session_data):,} users...")
    
    # Generate embeddings in batches
    batch_size = 1000
    embeddings_list = []
    
    for i in range(0, len(session_data), batch_size):
        batch = session_data[f'STORE_TEXT_{store_type.upper()}'].iloc[i:i+batch_size].tolist()
        batch_embeddings = model.encode(batch, show_progress_bar=False)
        embeddings_list.append(batch_embeddings)
        
        if (i // batch_size + 1) % 10 == 0:
            safe_print(f"      [{store_type}] Processed {i:,} / {len(session_data):,} users...")
    
    embeddings = np.vstack(embeddings_list)
    safe_print(f"    ✅ Generated {store_type} embeddings with shape: {embeddings.shape}")
    
    # Calculate similarity to latest_week_3 for each user
    safe_print(f"    🔄 Calculating {store_type} similarity to {FINAL_SESSION_TYPE}...")
    similarities = []
    valid_comparisons = 0
    
    for idx, consumer_id in enumerate(session_data['CONSUMER_ID'].values):
        # Check if both current session and week3 have valid store data
        current_has_data = session_data.iloc[idx][f'HAS_STORE_DATA_{store_type.upper()}'] == 1
        week3_has_data = consumer_id in week3_embeddings[store_type] and \
                        week3_embeddings.get(f'{store_type}_has_data', {}).get(consumer_id, False)
        
        if current_has_data and week3_has_data and consumer_id in week3_embeddings[store_type]:
            # Both sessions have valid store data - calculate cosine similarity
            similarity = cosine_similarity(
                embeddings[idx].reshape(1, -1),
                week3_embeddings[store_type][consumer_id].reshape(1, -1)
            )[0, 0]
            similarities.append(similarity)
            valid_comparisons += 1
        else:
            # At least one session has no store data - mark as NaN
            similarities.append(np.nan)
    
    safe_print(f"    ✅ {store_type}: Valid similarity calculations: {valid_comparisons:,} / {len(session_data):,} ({100*valid_comparisons/len(session_data):.1f}%)")
    
    return store_type, similarities

def process_session_type(session_type, session_dfs, STORE_TYPES, model, 
                        week3_embeddings, FINAL_SESSION_TYPE):
    """
    Process a single session type: convert to pandas and process all store types in parallel
    Returns: (session_type, session_pd_with_similarities)
    """
    safe_print(f"\n📊 Processing {session_type}...")
    
    session_pd = session_dfs[session_type].toPandas()
    
    # Process all store types in parallel for this session
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for store_type, tags_col, categories_col, names_col in STORE_TYPES:
            future = executor.submit(
                process_store_type,
                session_pd, store_type, tags_col, categories_col, names_col,
                session_type, model, week3_embeddings, FINAL_SESSION_TYPE
            )
            futures.append(future)
        
        # Collect results as they complete
        for future in as_completed(futures):
            store_type, similarities = future.result()
            session_pd[f'SIMILARITY_{store_type.upper()}'] = similarities
    
    safe_print(f"   ✅ Completed {session_type}")
    return session_type, session_pd

# COMMAND ----------

print("\n" + "=" * 80)
print(f"STEP 4: CALCULATING COSINE SIMILARITY TO {FINAL_SESSION_TYPE.upper()} (PARALLELIZED)")
print("=" * 80)

results_list = []

# COMMAND ----------

# Process all session types in parallel
print(f"\n🚀 Processing {len(SESSION_TYPES)} session types in parallel (max 3 workers)...")

with ThreadPoolExecutor(max_workers=3) as executor:
    futures = []
    for session_type in SESSION_TYPES:
        future = executor.submit(
            process_session_type,
            session_type, session_dfs, STORE_TYPES, model,
            week3_embeddings, FINAL_SESSION_TYPE
        )
        futures.append(future)
    
    # Collect results as they complete
    for future in as_completed(futures):
        session_type, session_pd = future.result()
        
        # Collect results
        for idx, row in session_pd.iterrows():
            results_list.append({
                'CONSUMER_ID': row['CONSUMER_ID'],
                'COHORT_TYPE': row['COHORT_TYPE'],
                'SESSION_TYPE': session_type,
                'SIMILARITY_IMPRESSION': row.get('SIMILARITY_IMPRESSION', np.nan),
                'SIMILARITY_ATTRIBUTION': row.get('SIMILARITY_ATTRIBUTION', np.nan),
                'SIMILARITY_FUNNEL_STORE': row.get('SIMILARITY_FUNNEL_STORE', np.nan)
            })

print(f"\n✅ All session types processed!")
print(f"   Total results collected: {len(results_list):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Create Final Results DataFrame

# COMMAND ----------

print("\n" + "=" * 80)
print("STEP 5: CREATING FINAL RESULTS TABLE")
print("=" * 80)
print(f"   ⚡ Note: Processing was parallelized with up to 3 session workers and 3 store type workers per session")

print(f"\n📊 Creating results dataframe...")
results_df = pd.DataFrame(results_list)

print(f"   Total records: {len(results_df):,}")
print(f"   Unique consumers: {results_df['CONSUMER_ID'].nunique():,}")
print(f"   Session types: {results_df['SESSION_TYPE'].unique()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Summary Statistics

# COMMAND ----------

# Show summary statistics
print("\n📈 Summary Statistics:")

print("\n📊 Valid Similarity Count (non-NaN) by Session Type and Store Type:")
valid_counts = pd.DataFrame({
    'IMPRESSION': results_df.groupby('SESSION_TYPE')['SIMILARITY_IMPRESSION'].apply(lambda x: x.notna().sum()),
    'ATTRIBUTION': results_df.groupby('SESSION_TYPE')['SIMILARITY_ATTRIBUTION'].apply(lambda x: x.notna().sum()),
    'FUNNEL_STORE': results_df.groupby('SESSION_TYPE')['SIMILARITY_FUNNEL_STORE'].apply(lambda x: x.notna().sum())
})
print(valid_counts)

# COMMAND ----------

print("\n📈 Mean Similarity Scores by Session Type (excluding NaN):")
summary = results_df.groupby('SESSION_TYPE')[
    ['SIMILARITY_IMPRESSION', 'SIMILARITY_ATTRIBUTION', 'SIMILARITY_FUNNEL_STORE']
].mean().round(4)
print(summary)

# COMMAND ----------

print("\n📈 Summary by Cohort (excluding NaN):")
cohort_summary = results_df.groupby(['COHORT_TYPE', 'SESSION_TYPE'])[
    ['SIMILARITY_IMPRESSION', 'SIMILARITY_ATTRIBUTION', 'SIMILARITY_FUNNEL_STORE']
].mean().round(4)
print(cohort_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Save Results to Snowflake

# COMMAND ----------

print("\n" + "=" * 80)
print("STEP 6: SAVING RESULTS TO SNOWFLAKE")
print("=" * 80)

print(f"\n💾 Converting to Spark DataFrame...")
results_spark = spark.createDataFrame(results_df)

print(f"\n💾 Writing to {OUTPUT_TABLE}...")
results_spark.write.format("snowflake") \
    .options(**OPTIONS) \
    .option("dbtable", OUTPUT_TABLE) \
    .mode("overwrite") \
    .save()

print(f"   ✅ Results saved successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Final Summary

# COMMAND ----------

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE!")
print("=" * 80)

print(f"""
📊 Final Summary:
   - Total records: {len(results_df):,}
   - Unique consumers: {results_df['CONSUMER_ID'].nunique():,}
   - Session types analyzed: {len(SESSION_TYPES)}
   - Store types analyzed: {len(STORE_TYPES)}
   - Output table: {OUTPUT_TABLE}

📈 Key Findings:
   - Average similarity scores calculated for each session vs {FINAL_SESSION_TYPE}
   - Separate similarity scores for impression, attribution, and funnel stores
   - Results available for all three cohorts: new, post_onboarding, and active

🎯 Next Steps:
   - Query the results table to analyze feed evolution patterns
   - Compare cohort differences in feed convergence patterns
   - Identify which store types show more/less consistency over time across cohorts
""")

print("\n✅ Script completed successfully!")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Quick Validation Query
# MAGIC 
# MAGIC Run this to validate the output table:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   COHORT_TYPE,
# MAGIC   SESSION_TYPE,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(DISTINCT CONSUMER_ID) as unique_users,
# MAGIC   ROUND(AVG(SIMILARITY_IMPRESSION), 4) as avg_sim_impression,
# MAGIC   ROUND(AVG(SIMILARITY_ATTRIBUTION), 4) as avg_sim_attribution,
# MAGIC   ROUND(AVG(SIMILARITY_FUNNEL_STORE), 4) as avg_sim_funnel_store,
# MAGIC   SUM(CASE WHEN SIMILARITY_IMPRESSION IS NOT NULL THEN 1 ELSE 0 END) as valid_impression_count,
# MAGIC   SUM(CASE WHEN SIMILARITY_ATTRIBUTION IS NOT NULL THEN 1 ELSE 0 END) as valid_attribution_count,
# MAGIC   SUM(CASE WHEN SIMILARITY_FUNNEL_STORE IS NOT NULL THEN 1 ELSE 0 END) as valid_funnel_store_count
# MAGIC FROM proddb.fionafan.feed_evolution_1110_cosine_similarity
# MAGIC GROUP BY COHORT_TYPE, SESSION_TYPE
# MAGIC ORDER BY COHORT_TYPE, SESSION_TYPE


