# Transformer Models for Session Conversion Attribution

## Overview

These scripts use **PyTorch Transformers with attention mechanisms** to predict session conversion and identify which events/actions drive conversions through **attention-based attribution**.

---

## Files

### **Primary Models (No Data Leakage)**

#### 1. `transformer_attribution_no_funnel.py` ⭐
**Local Python version - NO funnel events**

- **Purpose**: Predict conversion WITHOUT data leakage
- **Excludes**: add_item, cart, checkout, place_order events
- **Includes**: impressions, clicks, store_page_load, search, tab switches
- **Expected AUC**: 0.55-0.75 (realistic predictive power)
- **Use this for**: True predictive insights

#### 2. `transformer_attribution_no_funnel_pyspark.py` ⭐
**Databricks version - NO funnel events**

- Same logic as above, optimized for PySpark/Databricks
- Spark preprocessing → PyTorch training
- Saves results to Snowflake

### **Comparison Models (With Data Leakage)**

#### 3. `transformer_attribution_with_funnel.py` ⚠️
**Local Python version - WITH funnel events**

- **Warning**: Includes funnel events (data leakage!)
- **Expected AUC**: 0.95-0.99 (inflated due to leakage)
- **Use this for**: Comparison baseline only

#### 4. `transformer_attribution_with_funnel_pyspark.py` ⚠️
**Databricks version - WITH funnel events**

- Same as above for Databricks

---

## Architecture

### **Transformer Design**

```
Event Sequence:
[impression] → [click] → [store_page] → [search] → [click] → ...

Each event is represented by 7 embeddings:
1. event_type embedding (e.g., m_card_click)
2. discovery_feature embedding (e.g., Core Search)
3. store_id embedding
4. store_name embedding
5. primary_tag embedding (e.g., "Fast Food")
6. primary_category embedding (e.g., "American")
7. continuous features (event_rank, time_delta, is_nv)

→ Concatenate → Project to d_model=192
→ Add positional encoding
→ Add CLS token
→ 3 Transformer Encoder Layers (6 heads each)
→ CLS token pooling
→ Classification head → Conversion probability
```

### **Attention Extraction**

- Uses standard `nn.TransformerEncoder`
- Captures attention weights via forward hooks
- Aggregates attention across layers and heads
- Maps attention back to events and stores

---

## Key Outputs

### **Attribution Metrics**

1. **Attention Lift**: `attention_converted - attention_not_converted`
   - Which events get MORE attention when predicting conversions

2. **Event-Level Attribution**: Attention weight per event in sequence

3. **Store-Level Attribution**: Aggregated attention per store

4. **Event Type Importance**: Which types of events matter most

---

## Data Leakage Prevention

### **How Labels Are Created (Both Versions)**
Both versions use the SAME label creation:
```sql
-- Conversion = 1 if session has place_order or checkout_success
MAX(CASE 
    WHEN event_type = 'funnel' 
    AND event IN ('action_place_order', 'system_checkout_success') 
    THEN 1 ELSE 0 
END) AS converted
```

### **The Key Difference**

**NO FUNNEL Version** (Recommended):
```sql
-- Excludes ALL funnel events from the event sequence
WHERE event_type != 'funnel'
```
- Only learns from: impressions, clicks, search, navigation
- True predictive power (what happens BEFORE user decides to convert)

**WITH FUNNEL Version** (Comparison):
```sql
-- Includes ALL events including funnel
WHERE 1=1  -- no exclusion
```
- Learns from: impressions, clicks, AND add_item, cart, checkout
- Data leakage! (model sees conversion path)

### **Why This Matters**
Including `event_type='funnel'` events creates leakage:
- `action_add_item` → User added to cart (obvious signal)
- `checkout_page_load` → User reached checkout (almost guaranteed conversion)
- `cart_page_load` → User viewing cart (strong signal)

The model will achieve AUC 0.99 but won't generalize!

---

## Usage

### **Local Python**

```bash
# Recommended: No data leakage
python transformer_attribution_no_funnel.py

# Comparison only: With leakage
python transformer_attribution_with_funnel.py
```

### **Databricks**

```python
# Upload to Databricks workspace:
# 1. transformer_attribution_no_funnel_pyspark.py (recommended)
# 2. transformer_attribution_with_funnel_pyspark.py (comparison)

# Run on GPU cluster for faster training
```

---

## Expected Results

### **NO FUNNEL Version (Recommended)**
```
Expected Val AUC: 0.55-0.75
Top attributed events:
- m_card_click (store clicks)
- store_page_load (browsing)
- Search events
- Early engagement patterns
```

### **WITH FUNNEL Version (Comparison)**
```
Expected Val AUC: 0.95-0.99 (data leakage!)
Top attributed events:
- action_add_item (⚠️ leakage)
- checkout_page_load (⚠️ leakage)
- cart events (⚠️ leakage)
```

---

## Key Insights

### **What Makes This Different from Traditional Models?**

1. **Sequence modeling**: Captures temporal patterns, not just aggregated counts
2. **Attention attribution**: Shows WHICH specific events matter, not just WHAT types
3. **Store-level insights**: Identifies which stores get attention in conversion paths
4. **Event position matters**: Early vs late events have different importance

### **Actionable Insights You'll Get**

- **"Users who click on store X in first 5 events are 2x more likely to convert"**
- **"Search events in converted sessions get 30% more attention"**
- **"NV stores appearing early get more attention than later"**

---

## Technical Notes

### **Why nn.TransformerEncoder?**
- Standard PyTorch implementation
- Well-tested and optimized
- Attention extraction via hooks (cleaner than custom layers)

### **Sampling Strategy**
```sql
-- Sample USERS first (not events)
WITH sampled_users AS (
    SELECT DISTINCT USER_ID
    FROM events
    ORDER BY RANDOM()
    LIMIT 10000
)
-- Then get ALL events for sampled users
```

This ensures complete session histories.

### **Store Metadata Embeddings**
For events without stores (e.g., tab switches):
- `store_name = 'NO_STORE'`
- `primary_tag_name = 'NO_STORE'`  
- `primary_category_name = 'NO_STORE'`

Embeddings learn to represent "no store context" naturally.

---

## Next Steps

1. **Run NO FUNNEL version first** to get baseline attribution
2. **Compare with WITH FUNNEL** to quantify data leakage impact
3. **Analyze attention weights** to identify key conversion drivers
4. **Validate findings** with A/B test experiments

---

## Contact

Questions? Check the code comments or reach out to the Growth Analytics team.

