# Databricks Logistic Regression Guide

## Overview

This notebook trains two logistic regression models to predict session conversion using PySpark on Databricks:

1. **Pre-Funnel Model** (49 features) - Uses only browsing behavior before funnel entry + 3 store embedding similarities
2. **Funnel-Inclusive Model** (60 features) - Includes funnel progression metrics + 3 store embedding similarities

**Note**: First sessions are excluded from modeling since they don't have previous session data for embedding similarity. However, they are included during embedding calculation so that second sessions can calculate similarity to first sessions.

## File Location

**Notebook**: `user-analysis/first_session_analysis/python/logistic_regression_conversion_models_databricks.py`

## Features Used (60 total)

### Curated Feature List (No Data Leakage)

✅ **Included Features**:
- Temporal (4): `days_since_onboarding`, `days_since_onboarding_mod7`, `session_day_of_week`, `onboarding_day_of_week`
- Session duration (1): `session_duration_seconds`
- Impression (3): `impression_unique_stores`, `impression_unique_features`, `impression_to_attribution_rate`
- Action (10): Background, foreground, tab switches, address, auth actions (counts + booleans)
- Attribution (13): Card clicks, discovery features (carousel, search, filters, etc.)
- Error (1): `error_event_count`
- Funnel progression (5): `funnel_num_adds`, `funnel_num_store_page`, `funnel_num_cart`, `funnel_num_checkout`, `funnel_add_per_attribution_click`
- Timing (15): Inter-event timing, time-to-actions, early engagement
- Sequence (3): Behavioral sequences (impression→attribution, attribution→store, store→add)
- Store (6): NV impression occurrence and position, **3 store embedding similarities** (NEW), has previous session flag (NEW)

**NEW Store Embedding Features (3 Types)**:

Each captures similarity for different user behaviors:

1. **`store_similarity_impression_prev`** - Browsing similarity
   - What stores users **SAW** in current vs previous session
   - Captures browsing preference consistency
   
2. **`store_similarity_attribution_prev`** - Click similarity
   - What stores users **CLICKED** in current vs previous session
   - Captures interest/intent consistency
   
3. **`store_similarity_funnel_store_prev`** - Visit similarity
   - What stores users **VISITED** (store page) in current vs previous session
   - Captures deep engagement consistency

4. **`has_previous_session`** - Binary flag
   - 1 if user has a previous session to compare against
   - 0 for first session (similarity scores are NaN)

**Similarity Range**: -1 to 1, where 1 = identical stores, 0 = unrelated, -1 = opposite

❌ **Excluded Features (Data Leakage)**:
- `attribution_cnt_doubledash` - DoubleDash orders proxy for conversion
- `action_had_payment`, `action_num_payment_actions` - Payment during checkout
- `num_unique_discovery_surfaces`, `num_unique_discovery_features` - Correlated with checkout
- `total_events`, `events_per_minute` - Too generic

❌ **Excluded Features (Target-Related)**:
- `funnel_converted_bool` (target)
- `funnel_num_orders`, `funnel_had_order` (same as target)
- `funnel_num_success`, `funnel_seconds_to_success` (leakage)
- `funnel_order_per_add` (uses target)

## How to Run in Databricks

### Step 1: Import Notebook

1. Open Databricks workspace
2. Navigate to **Workspace** → **Users** → **Your Name**
3. Click **Import**
4. Select the file: `logistic_regression_conversion_models_databricks.py`
5. Click **Import**

### Step 2: Set Up Secrets (if not already configured)

```python
# Secrets should be set in Databricks:
# - Scope: fionafan-scope
# - Keys: snowflake-user, snowflake-password
```

If secrets are not set, configure them:

1. In Databricks, go to **Settings** → **User Settings** → **Developer**
2. Create or use existing scope: `fionafan-scope`
3. Add keys:
   - `snowflake-user`: Your Snowflake username
   - `snowflake-password`: Your Snowflake password

### Step 3: Attach to Cluster

1. Open the imported notebook
2. Click **Attach** at the top
3. Select a cluster with:
   - **Databricks Runtime**: 12.2 LTS or higher
   - **Spark version**: 3.3+
   - **Python version**: 3.10+

### Step 4: Run the Notebook

**Option A: Run All Cells**
- Click **Run All** at the top
- ⚠️ **Note**: Embedding calculation can take 5-15 minutes for large datasets

**Option B: Run Cell by Cell**
- Press `Shift + Enter` to run each cell sequentially
- Review outputs and plots as you go

**Processing Steps**:
1. **Load Data** (~1-2 min) - Loads all sessions from Snowflake
2. **Calculate Embeddings** (~5-15 min) - Generates store embeddings and similarity scores
   - Converts Spark → Pandas for embedding calculation
   - Uses SentenceTransformer 'all-MiniLM-L6-v2'
   - Calculates cosine similarity with previous session
   - Filters out first_session types
   - Converts back to Spark
3. **Define Features** (~1 min) - Validates 59 features
4. **Train Models** (~5-10 min) - Fits both logistic regression models
5. **Visualize & Save** (~2-3 min) - Creates plots and saves to Snowflake

**Total Runtime**: ~15-30 minutes depending on data size

### Step 5: Review Results

The notebook will output:

1. **Feature Selection Summary** - Shows which features are available
2. **Model Training Progress** - AUC-ROC, convergence status
3. **Top 20 Features** - Ranked by percentage impact on odds
4. **Coefficient Visualizations** - Bar charts showing feature importance
5. **Model Comparison** - Pre-funnel vs Funnel-inclusive performance

## Output Tables

Results are saved to Snowflake:

**Table**: `proddb.fionafan.session_conversion_model_results_databricks`

**Columns**:
- `model_name`: "Pre-Funnel Model" or "Funnel-Inclusive Model"
- `target`: "funnel_converted_bool"
- `covariate`: Feature name or "const" for intercept
- `coefficient`: Logistic regression coefficient (β)
- `odds_ratio_1sd`: exp(β) - Odds ratio for 1 SD increase
- `pct_impact_1sd`: (OR - 1) × 100 - Percentage change in odds
- `n_obs`: Number of observations used
- `test_count`: Test set size
- `test_auc`: Test AUC-ROC score
- `converged`: Whether model converged
- `iterations`: Number of iterations used
- `created_at`: Timestamp

## Query Results

```sql
-- Get top features by model
SELECT 
    model_name,
    covariate,
    coefficient,
    odds_ratio_1sd,
    pct_impact_1sd,
    test_auc
FROM proddb.fionafan.session_conversion_model_results_databricks
WHERE covariate != 'const'
    AND model_name = 'Pre-Funnel Model'
ORDER BY ABS(pct_impact_1sd) DESC
LIMIT 20;

-- Compare model performance
SELECT 
    model_name,
    MAX(test_auc) as test_auc,
    MAX(CASE WHEN covariate = 'const' THEN converged END) as converged,
    MAX(CASE WHEN covariate = 'const' THEN iterations END) as iterations,
    COUNT(*) - 1 as num_features  -- Subtract 1 for intercept
FROM proddb.fionafan.session_conversion_model_results_databricks
GROUP BY model_name;
```

## Expected Results

### Pre-Funnel Model
- **Features**: 49 (no funnel progression, includes 3 store embedding similarities + 1 flag)
- **Expected AUC**: 0.65-0.75
- **Use Case**: Early prediction before user enters purchase funnel
- **Key Features**: Attribution and impression similarity should rank in top 20 if effective

### Funnel-Inclusive Model
- **Features**: 60 (includes funnel progression + 3 store embedding similarities + 1 flag)
- **Expected AUC**: 0.75-0.85
- **Use Case**: Prediction during funnel progression
- **Key Features**: Funnel progression (adds, cart, checkout) should rank highest, followed by store visit similarity

### Store Embedding Features
- **Impact**: Each similarity type typically shows 5-20% impact on odds
  - **Impression similarity**: Browsing consistency (5-10% impact expected)
  - **Attribution similarity**: Click consistency (10-15% impact expected)
  - **Funnel store similarity**: Visit consistency (15-20% impact expected)
- **Interpretation**: Higher similarity → user has consistent store preferences → higher conversion likelihood
- **Validation**: At least one similarity feature should appear in top 20 features

## Validation Checklist

✅ **No Data Leakage**: Top 20 features should NOT include:
- `attribution_cnt_doubledash`
- `action_had_payment` / `action_num_payment_actions`
- `num_unique_discovery_surfaces` / `num_unique_discovery_features`

✅ **Model Performance**: 
- Test AUC should be > 0.65 for pre-funnel
- Test AUC should be > 0.75 for funnel-inclusive
- Models should converge (iterations < max_iter)

✅ **Feature Importance Makes Sense**:
- Funnel progression features should rank high in funnel-inclusive model
- Attribution features should show meaningful impact
- Timing features should have interpretable signs

## Troubleshooting

### Issue: SentenceTransformer not installed
```
ModuleNotFoundError: No module named 'sentence_transformers'
```

**Solution**: Install on cluster:
```python
%pip install sentence-transformers
```

### Issue: Secrets not found
```
Error: KeyError: 'snowflake-user'
```

**Solution**: Configure Databricks secrets (see Step 2 above)

### Issue: Out of memory during embedding calculation
```
OutOfMemoryError: Java heap space
```

**Solution**: 
- Use a cluster with more memory (16GB+ driver)
- Or reduce batch_size in `create_store_embeddings()` from 1000 to 500
- Or sample the data before embedding calculation

### Issue: Features not found in data
```
⚠️ Warning: X features not found in data
```

**Solution**: Check that `all_user_sessions_with_events_features_gen` table exists and has required columns

### Issue: Model doesn't converge
```
Converged: False (iterations: 1000/1000)
```

**Solution**: 
- Increase `max_iter` parameter (e.g., 2000)
- Reduce `reg_param` (e.g., 0.1)
- Check for multicollinearity in features

### Issue: Low AUC score
```
Test AUC-ROC: 0.52
```

**Solution**:
- Check conversion rate balance in data
- Verify features are properly scaled
- Consider feature engineering or selection

## Configuration Parameters

Located in "Train Pre-Funnel Model" section:

```python
target = "funnel_converted_bool"  # Target variable
max_iter = 1000                    # Maximum iterations
reg_param = 1.0                    # L2 regularization (higher = more regularization)
elastic_net = 0.0                  # 0 = L2 only, 1 = L1 only
```

**Recommended values**:
- `max_iter`: 500-2000 (increase if not converging)
- `reg_param`: 0.1-10.0 (increase to reduce overfitting)
- `elastic_net`: 0.0 (pure L2 regularization)

## Next Steps

1. ✅ Run notebook and validate no data leakage
2. 📊 Compare with previous model results
3. 📈 Analyze feature importance changes
4. 🔍 Investigate surprising coefficients
5. 📝 Document insights in experiment readout

---

**Created**: October 26, 2025  
**Last Updated**: October 26, 2025  
**Status**: Ready to run

