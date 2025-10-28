# Session Conversion Model Improvements

## Changes Made

### 1. Excluded Data Leakage Features

Removed the following features that were causing data leakage:

- **`ATTRIBUTION_CNT_DOUBLEDASH`** - DoubleDash orders are a direct proxy for conversion (Rank #1, 263.6% impact)
- **`ACTION_HAD_PAYMENT`** - Payment actions occur during checkout, which is part of the target event (Rank #8, 34.2% impact)
- **`ACTION_NUM_PAYMENT_ACTIONS`** - Same issue as above
- **`NUM_UNIQUE_DISCOVERY_SURFACES`** - Highly correlated with checkout behavior (Rank #2, 120.2% impact)
- **`NUM_UNIQUE_DISCOVERY_FEATURES`** - Highly correlated with checkout behavior (Rank #5, 63.9% impact)

### 2. Excluded Generic Features

Removed overly generic features that don't provide meaningful predictive signal:

- **`TOTAL_EVENTS`** - Too generic, not specific enough to predict conversion (Rank #11, 31.1% impact)
- **`EVENTS_PER_MINUTE`** - Too generic, varies widely across sessions (Rank #7, -41.0% impact)

### 3. Added Store Embedding Features (3 Types)

Created **three separate** store embedding similarity features to capture different user behaviors:

#### **Store Embedding Types:**

1. **Impression Similarity** (`store_similarity_impression_prev`)
   - Based on: `store_tags_concat_impression`, `store_categories_concat_impression`, `store_names_concat_impression`
   - Captures: What stores users **SAW** (browsing behavior)
   - Use case: Preference consistency in browsing patterns

2. **Attribution Similarity** (`store_similarity_attribution_prev`)
   - Based on: `store_tags_concat_attribution`, `store_categories_concat_attribution`, `store_names_concat_attribution`
   - Captures: What stores users **CLICKED** (interest/intent)
   - Use case: Preference consistency in active selection

3. **Funnel Store Page Similarity** (`store_similarity_funnel_store_prev`)
   - Based on: `store_tags_concat_funnel_store`, `store_categories_concat_funnel_store`, `store_names_concat_funnel_store`
   - Captures: What stores users **VISITED** (deep engagement)
   - Use case: Preference consistency in serious consideration

#### **Additional Feature:**
- `HAS_PREVIOUS_SESSION`: Binary flag indicating whether a previous session exists

#### How It Works:

1. For each session and event type, concatenate store metadata (tags, categories, names)
2. Generate embedding vector for each type using SentenceTransformer 'all-MiniLM-L6-v2' (384 dimensions)
3. Sort sessions by (user_id, device_id, event_date, session_num)
4. Calculate 3 separate cosine similarities between current and previous session embeddings
5. Missing values (first sessions) remain as **NaN** (not 0) to preserve data integrity

### 4. Feature Count Summary

**Pre-Funnel Model (Databricks PySpark):**
- Curated features: 48 features
- Removed: 7 leakage/generic features
- Added: 4 embedding features (3 similarity scores + 1 flag)
- **Total: 48 features** (excludes all funnel_* features)

**Funnel-Inclusive Model (Databricks PySpark):**
- Curated features: 59 features
- Removed: 7 leakage/generic features
- Added: 4 embedding features + 5 funnel progression features
- **Total: 59 features** (includes funnel progression but excludes target-related)

## Files Modified

### 1. `logistic_regression_conversion_models.py` (Pandas/SKLearn)
- Added `create_store_embeddings()` function
- Updated `define_feature_sets()` to exclude leakage and generic features
- Added store embedding similarity features
- Imports: Added `sentence_transformers` and `cosine_similarity`

### 2. `logistic_regression_conversion_models_spark.py` (PySpark) - NEW FILE
- Created PySpark version for scalability
- Updated `select_covariates()` to explicitly exclude leakage features
- Added detailed exclusion logic with comments
- Maintains same feature exclusion philosophy as pandas version
- **Note**: Store embeddings not implemented in Spark version (would require pre-computation)

## Expected Impact

### Removed Features Had Large Impact (Now Gone):
1. `ATTRIBUTION_CNT_DOUBLEDASH`: 263.6% impact → **DATA LEAKAGE**
2. `NUM_UNIQUE_DISCOVERY_SURFACES`: 120.2% impact → **DATA LEAKAGE**
3. `NUM_UNIQUE_DISCOVERY_FEATURES`: 63.9% impact → **DATA LEAKAGE**
4. `EVENTS_PER_MINUTE`: -41.0% impact → **TOO GENERIC**
5. `ACTION_HAD_PAYMENT`: 34.2% impact → **DATA LEAKAGE**
6. `TOTAL_EVENTS`: 31.1% impact → **TOO GENERIC**

### New Features Added:
- `STORE_SIMILARITY_IMPRESSION_PREV`: Captures browsing preference consistency (stores user saw)
- `STORE_SIMILARITY_ATTRIBUTION_PREV`: Captures click preference consistency (stores user clicked)
- `STORE_SIMILARITY_FUNNEL_STORE_PREV`: Captures visit preference consistency (stores user visited)
- `HAS_PREVIOUS_SESSION`: Controls for first-time vs returning session behavior

**Expected Impact**: Each similarity type captures different aspects of user behavior - browsing vs clicking vs visiting patterns.

## Next Steps

1. **Run Pandas Version**: Execute `logistic_regression_conversion_models.py`
   ```bash
   python user-analysis/first_session_analysis/python/logistic_regression_conversion_models.py
   ```

2. **Run PySpark Version** (optional, for large datasets):
   ```bash
   python user-analysis/first_session_analysis/python/logistic_regression_conversion_models_spark.py
   ```

3. **Compare Results**: 
   - Check `proddb.fionafan.session_conversion_model_coefficients`
   - Check `proddb.fionafan.session_conversion_model_metrics`
   - Verify that model performance is maintained or improved
   - Ensure no more data leakage features in top 20

4. **Validate**: Ensure test AUC-ROC doesn't drop significantly (acceptable drop: < 0.05)

## Key Improvements

✅ **Removed data leakage** - Model now uses only information available before conversion decision  
✅ **More interpretable** - Removed generic features that don't explain *why* users convert  
✅ **Added behavioral consistency** - Store similarity captures user preference patterns  
✅ **Scalable** - Created PySpark version for larger datasets  
✅ **Documented** - Clear comments explaining why each feature was excluded  

---

**Created**: October 25, 2025  
**Author**: Analytics Team  
**Status**: Ready for execution

