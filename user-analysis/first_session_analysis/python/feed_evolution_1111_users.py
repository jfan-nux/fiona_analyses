"""
Feed Evolution Analysis for 1111 Activity Pattern Users
Compares store embeddings across sessions to latest_week_4 final state

Base Table: proddb.fionafan.all_user_sessions_with_events_features_gen
Table Creation Logic: user-analysis/first_session_analysis/sql/sessions_features_gen.sql

This script:
1. Samples 50K users from each cohort (new/post_onboarding/active) with 1111 activity pattern
2. For each user, extracts their store features from different session types
   - first_session, second_session, third_session
   - latest_week_1, latest_week_2, latest_week_3
   - latest_week_4 (used as final state reference)
3. Generates embeddings for store tags, categories, and names (by event type: impression, attribution, funnel_store)
4. Calculates cosine similarity from each session to the latest_week_4 final state
5. Outputs results at consumer_id + session_type + cohort level with 3 similarity scores per row
"""

import os
import sys
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.snowflake_connection import get_spark_session

print("=" * 80)
print("FEED EVOLUTION ANALYSIS FOR 1111 ACTIVITY PATTERN USERS")
print("=" * 80)

# Initialize Spark session with Snowflake
print("\n📊 Initializing Spark session with Snowflake...")
spark = get_spark_session()

# ============================================================================
# CONFIGURATION
# ============================================================================

SAMPLE_SIZE = 50000  # Sample size per cohort
OUTPUT_TABLE = "proddb.fionafan.feed_evolution_1111_cosine_similarity"

# Session types to analyze (comparing all to latest_week_4 as final state)
SESSION_TYPES = [
    'first_session',
    'second_session', 
    'third_session',
    'latest_week_1',
    'latest_week_2',
    'latest_week_3'
]

# Final state for comparison
FINAL_SESSION_TYPE = 'latest_week_4'

# Store types and their columns
STORE_TYPES = [
    ('impression', 'STORE_TAGS_CONCAT_IMPRESSION', 'STORE_CATEGORIES_CONCAT_IMPRESSION', 'STORE_NAMES_CONCAT_IMPRESSION'),
    ('attribution', 'STORE_TAGS_CONCAT_ATTRIBUTION', 'STORE_CATEGORIES_CONCAT_ATTRIBUTION', 'STORE_NAMES_CONCAT_ATTRIBUTION'),
    ('funnel_store', 'STORE_TAGS_CONCAT_FUNNEL_STORE', 'STORE_CATEGORIES_CONCAT_FUNNEL_STORE', 'STORE_NAMES_CONCAT_FUNNEL_STORE'),
]

# ============================================================================
# STEP 1: LOAD DATA AND SAMPLE
# ============================================================================

print("\n" + "=" * 80)
print("STEP 1: LOADING AND SAMPLING DATA")
print("=" * 80)

print(f"\n📥 Loading session-level feature generation table...")
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
INNER JOIN proddb.fionafan.users_activity_1111 a
    ON f.USER_ID = a.CONSUMER_ID
WHERE 1=1
    AND f.SESSION_TYPE IN ('first_session', 'second_session', 'third_session', 
                           'latest_week_1', 'latest_week_2', 'latest_week_3', 'latest_week_4')
"""

df = spark.read.format("snowflake").option("query", query).load()

print(f"   Total sessions with 1111 activity pattern: {df.count():,}")

# Sample 50K users from each cohort (get user list first)
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

# ============================================================================
# STEP 2: PREPARE SESSION-LEVEL DATA
# ============================================================================

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

# ============================================================================
# STEP 3: GENERATE EMBEDDINGS AND CALCULATE SIMILARITY
# ============================================================================

print("\n" + "=" * 80)
print("STEP 3: GENERATING EMBEDDINGS AND CALCULATING SIMILARITY")
print("=" * 80)

print("\n🤖 Loading BGE sentence transformer model...")
model = SentenceTransformer('BAAI/bge-base-en-v1.5')
print("   ✅ BGE model loaded successfully")

# Convert latest_week_4 data to pandas for embedding generation
print(f"\n📊 Processing {FINAL_SESSION_TYPE} embeddings (final state)...")
week4_pd = session_dfs[FINAL_SESSION_TYPE].toPandas()

print(f"   {FINAL_SESSION_TYPE} sessions loaded: {len(week4_pd):,}")

week4_embeddings = {}
for store_type, tags_col, categories_col, names_col in STORE_TYPES:
    print(f"\n  📊 Processing {FINAL_SESSION_TYPE.upper()} {store_type.upper()} embeddings...")
    
    # Combine store text features (columns are already in the right format from SQL)
    week4_pd[f'STORE_TEXT_{store_type.upper()}'] = (
        week4_pd[tags_col].fillna('') + ' | ' +
        week4_pd[categories_col].fillna('') + ' | ' +
        week4_pd[names_col].fillna('')
    )
    
    # Clean up empty/null combinations more comprehensively
    text_col = f'STORE_TEXT_{store_type.upper()}'
    week4_pd[text_col] = week4_pd[text_col].str.strip()  # Remove leading/trailing spaces
    
    # Replace various empty patterns with placeholder
    empty_patterns = ['', ' | ', ' |  | ', '|', ' |  |', ' | |', ' | | ', '||']
    for pattern in empty_patterns:
        week4_pd[text_col] = week4_pd[text_col].replace(pattern, 'no_store_info')
    
    # Also replace if only whitespace remains
    week4_pd[text_col] = week4_pd[text_col].apply(
        lambda x: 'no_store_info' if not x or x.strip() == '' or set(x.replace(' ', '').replace('|', '')) == set() else x
    )
    
    # Track which sessions have valid store data
    week4_pd[f'HAS_STORE_DATA_{store_type.upper()}'] = (week4_pd[text_col] != 'no_store_info').astype(int)
    
    print(f"    Sessions with valid {store_type} data: {week4_pd[f'HAS_STORE_DATA_{store_type.upper()}'].sum():,} / {len(week4_pd):,}")
    
    print(f"    Generating embeddings for {len(week4_pd):,} users...")
    
    # Generate embeddings in batches
    batch_size = 1000
    embeddings_list = []
    
    for i in range(0, len(week4_pd), batch_size):
        batch = week4_pd[f'STORE_TEXT_{store_type.upper()}'].iloc[i:i+batch_size].tolist()
        batch_embeddings = model.encode(batch, show_progress_bar=False)
        embeddings_list.append(batch_embeddings)
        
        if (i // batch_size + 1) % 10 == 0:
            print(f"      Processed {i:,} / {len(week4_pd):,} users...")
    
    embeddings = np.vstack(embeddings_list)
    print(f"    ✅ Generated embeddings with shape: {embeddings.shape}")
    
    # Store embeddings indexed by consumer_id
    week4_embeddings[store_type] = {
        consumer_id: embeddings[idx] 
        for idx, consumer_id in enumerate(week4_pd['CONSUMER_ID'].values)
    }
    
    # Store validity flag for week4 sessions (whether they have real store data)
    week4_embeddings[f'{store_type}_has_data'] = {
        consumer_id: (week4_pd.iloc[idx][f'HAS_STORE_DATA_{store_type.upper()}'] == 1)
        for idx, consumer_id in enumerate(week4_pd['CONSUMER_ID'].values)
    }

# ============================================================================
# STEP 4: CALCULATE SIMILARITY FOR EACH SESSION
# ============================================================================

print("\n" + "=" * 80)
print(f"STEP 4: CALCULATING COSINE SIMILARITY TO {FINAL_SESSION_TYPE.upper()}")
print("=" * 80)

results_list = []

for session_type in SESSION_TYPES:
    print(f"\n📊 Processing {session_type}...")
    
    session_pd = session_dfs[session_type].toPandas()
    
    for store_type, tags_col, categories_col, names_col in STORE_TYPES:
        print(f"\n  📊 Processing {session_type} {store_type.upper()} embeddings...")
        
        # Combine store text features
        session_pd[f'STORE_TEXT_{store_type.upper()}'] = (
            session_pd[tags_col].fillna('') + ' | ' +
            session_pd[categories_col].fillna('') + ' | ' +
            session_pd[names_col].fillna('')
        )
        
        # Clean up empty/null combinations more comprehensively
        text_col = f'STORE_TEXT_{store_type.upper()}'
        session_pd[text_col] = session_pd[text_col].str.strip()  # Remove leading/trailing spaces
        
        # Replace various empty patterns with placeholder
        empty_patterns = ['', ' | ', ' |  | ', '|', ' |  |', ' | |', ' | | ', '||']
        for pattern in empty_patterns:
            session_pd[text_col] = session_pd[text_col].replace(pattern, 'no_store_info')
        
        # Also replace if only whitespace remains
        session_pd[text_col] = session_pd[text_col].apply(
            lambda x: 'no_store_info' if not x or x.strip() == '' or set(x.replace(' ', '').replace('|', '')) == set() else x
        )
        
        # Track which sessions have valid store data
        session_pd[f'HAS_STORE_DATA_{store_type.upper()}'] = (session_pd[text_col] != 'no_store_info').astype(int)
        
        print(f"    Sessions with valid {store_type} data: {session_pd[f'HAS_STORE_DATA_{store_type.upper()}'].sum():,} / {len(session_pd):,}")
        
        print(f"    Generating embeddings for {len(session_pd):,} users...")
        
        # Generate embeddings in batches
        batch_size = 1000
        embeddings_list = []
        
        for i in range(0, len(session_pd), batch_size):
            batch = session_pd[f'STORE_TEXT_{store_type.upper()}'].iloc[i:i+batch_size].tolist()
            batch_embeddings = model.encode(batch, show_progress_bar=False)
            embeddings_list.append(batch_embeddings)
            
            if (i // batch_size + 1) % 10 == 0:
                print(f"      Processed {i:,} / {len(session_pd):,} users...")
        
        embeddings = np.vstack(embeddings_list)
        print(f"    ✅ Generated embeddings with shape: {embeddings.shape}")
        
        # Calculate similarity to latest_week_4 for each user
        # Note: We only calculate similarity when BOTH sessions have valid store data
        # If either session has "no_store_info" placeholder, we set similarity to NaN
        # This ensures we're only comparing meaningful store features
        print(f"    🔄 Calculating similarity to {FINAL_SESSION_TYPE}...")
        similarities = []
        valid_comparisons = 0
        
        for idx, consumer_id in enumerate(session_pd['CONSUMER_ID'].values):
            # Check if both current session and week4 have valid store data
            current_has_data = session_pd.iloc[idx][f'HAS_STORE_DATA_{store_type.upper()}'] == 1
            week4_has_data = consumer_id in week4_embeddings[store_type] and \
                            week4_embeddings.get(f'{store_type}_has_data', {}).get(consumer_id, False)
            
            if current_has_data and week4_has_data and consumer_id in week4_embeddings[store_type]:
                # Both sessions have valid store data - calculate cosine similarity
                similarity = cosine_similarity(
                    embeddings[idx].reshape(1, -1),
                    week4_embeddings[store_type][consumer_id].reshape(1, -1)
                )[0, 0]
                similarities.append(similarity)
                valid_comparisons += 1
            else:
                # At least one session has no store data - mark as NaN
                # This handles cases where store columns are empty/null
                similarities.append(np.nan)
        
        session_pd[f'SIMILARITY_{store_type.upper()}'] = similarities
        print(f"    Valid similarity calculations: {valid_comparisons:,} / {len(session_pd):,} ({100*valid_comparisons/len(session_pd):.1f}%)")
    
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
    
    print(f"   ✅ Completed {session_type}")

# ============================================================================
# STEP 5: CREATE FINAL RESULTS TABLE
# ============================================================================

print("\n" + "=" * 80)
print("STEP 5: CREATING FINAL RESULTS TABLE")
print("=" * 80)

print(f"\n📊 Creating results dataframe...")
results_df = pd.DataFrame(results_list)

print(f"   Total records: {len(results_df):,}")
print(f"   Unique consumers: {results_df['CONSUMER_ID'].nunique():,}")
print(f"   Session types: {results_df['SESSION_TYPE'].unique()}")

# Show summary statistics
print("\n📈 Summary Statistics:")

print("\n📊 Valid Similarity Count (non-NaN) by Session Type and Store Type:")
valid_counts = pd.DataFrame({
    'IMPRESSION': results_df.groupby('SESSION_TYPE')['SIMILARITY_IMPRESSION'].apply(lambda x: x.notna().sum()),
    'ATTRIBUTION': results_df.groupby('SESSION_TYPE')['SIMILARITY_ATTRIBUTION'].apply(lambda x: x.notna().sum()),
    'FUNNEL_STORE': results_df.groupby('SESSION_TYPE')['SIMILARITY_FUNNEL_STORE'].apply(lambda x: x.notna().sum())
})
print(valid_counts)

print("\n📈 Mean Similarity Scores by Session Type (excluding NaN):")
summary = results_df.groupby('SESSION_TYPE')[
    ['SIMILARITY_IMPRESSION', 'SIMILARITY_ATTRIBUTION', 'SIMILARITY_FUNNEL_STORE']
].mean().round(4)
print(summary)

print("\n📈 Summary by Cohort (excluding NaN):")
cohort_summary = results_df.groupby(['COHORT_TYPE', 'SESSION_TYPE'])[
    ['SIMILARITY_IMPRESSION', 'SIMILARITY_ATTRIBUTION', 'SIMILARITY_FUNNEL_STORE']
].mean().round(4)
print(cohort_summary)

# ============================================================================
# STEP 6: SAVE TO SNOWFLAKE
# ============================================================================

print("\n" + "=" * 80)
print("STEP 6: SAVING RESULTS TO SNOWFLAKE")
print("=" * 80)

print(f"\n💾 Converting to Spark DataFrame...")
results_spark = spark.createDataFrame(results_df)

print(f"\n💾 Writing to {OUTPUT_TABLE}...")
results_spark.write \
    .format("snowflake") \
    .mode("overwrite") \
    .option("dbtable", OUTPUT_TABLE) \
    .save()

print(f"   ✅ Results saved successfully!")

# ============================================================================
# FINAL SUMMARY
# ============================================================================

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

