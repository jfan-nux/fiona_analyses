# Transformer Attribution Analysis - Complete Guide

## **What You'll Learn From These Models**

The transformer models answer: **"Which events in a session drive conversion predictions?"**

---

## **Attribution Granularities**

### **1. Event-Type Level** (Most Actionable) ⭐

**Question**: Which types of user actions predict conversion?

**Output**:
```
Event Type              | Attribution Lift | Interpretation
------------------------|------------------|----------------------------------
m_card_click            | +0.15           | Clicking stores is a strong signal
store_page_load         | +0.12           | Viewing stores predicts conversion
search                  | +0.08           | Active search indicates intent
m_card_view             | -0.02           | Passive impressions don't matter
m_app_background        | -0.10           | Leaving app is negative signal
```

**Actionable Insight**: "Optimize features that drive clicks and store views"

---

### **2. Position-Based Level** (Temporal Importance)

**Question**: When in the session do important events need to happen?

**Output**:
```
Position    | Attribution Lift | Interpretation
------------|------------------|----------------------------------
1-5 (early) | +0.12           | Early engagement critical!
6-10        | +0.05           | Still important
11-20       | -0.02           | Late events less predictive
21-50       | -0.05           | Very late events weak signal
```

**Actionable Insight**: "Focus on improving the first 5 events - they're most predictive"

---

### **3. Store-Level** (Business Impact)

**Question**: Which stores drive conversion when users interact with them?

**Output**:
```
Store ID | Store Name      | Avg Attribution | Event Count
---------|-----------------|-----------------|-------------
12345    | McDonald's      | 0.42            | 250
67890    | Chipotle        | 0.38            | 180
54321    | Starbucks       | 0.35            | 150
```

**Actionable Insight**: "Promote high-attribution stores in discovery features"

---

### **4. Event-Specific Level** (Session Debugging)

**Question**: In a specific session, which exact event mattered most?

**Output for Session ID "user123_device456_2025-01-15_1"**:
```
Position | Event Type        | Store          | Attribution
---------|-------------------|----------------|-------------
2        | store_page_load   | McDonald's     | 0.45  ⭐⭐
1        | m_card_click      | McDonald's     | 0.30  ⭐
0        | m_card_view       | McDonald's     | 0.15
3        | search            | (no store)     | 0.10
```

**Actionable Insight**: "Model focused most on store_page_load at position 2"

---

## **How Attribution Works**

### **Method: Gradient × Input (Saliency)**

```python
1. Forward pass: Session → Transformer → Conversion Probability
2. Backward pass: Compute ∂(prediction)/∂(event_embedding)
3. Multiply: gradient × input embedding
4. Result: How much each event contributed to the prediction
```

**Why this is better than just attention weights:**

| Method | What It Measures | Reliability |
|--------|------------------|-------------|
| **Attention Weights** | What model "looked at" | Medium (can look at irrelevant things) |
| **Gradient × Input** | What actually affected output | **High** ⭐ |

**Example:**
- Model might attend to `m_app_background` (high attention weight)
- But gradient shows it contributes negatively (lowers conversion prob)
- Gradient × Input correctly identifies this as a negative signal

---

## **File Outputs**

### **NO FUNNEL Version** (Recommended)

#### **Snowflake Tables:**
```sql
-- Event type importance
SELECT * FROM proddb.fionafan.transformer_event_attribution_no_funnel
ORDER BY attribution_lift DESC;

-- Position importance
SELECT * FROM proddb.fionafan.transformer_position_attribution_no_funnel;

-- Store importance
SELECT * FROM proddb.fionafan.transformer_store_attribution_no_funnel
WHERE label = 1  -- Converted sessions only
ORDER BY mean DESC
LIMIT 20;
```

#### **Visualizations:**
1. **Event Type Attribution Lift** - Which events get more attention in conversions?
2. **Position Attribution** - Early vs late event importance
3. **Converted vs Not Converted Comparison** - Side-by-side event importance
4. **Heatmap: Event Type × Position** - Which events matter at which positions?

---

### **WITH FUNNEL Version** (Comparison Baseline)

Same outputs but with ⚠️ warnings about data leakage:

```sql
SELECT * FROM proddb.fionafan.transformer_event_attribution_with_funnel
ORDER BY attribution_lift DESC;

-- Top events will be:
-- 1. action_add_item       +0.40  ⚠️ DATA LEAKAGE
-- 2. checkout_page_load    +0.35  ⚠️ DATA LEAKAGE  
-- 3. cart_page_load        +0.30  ⚠️ DATA LEAKAGE
```

---

## **Expected Results Comparison**

### **NO FUNNEL (True Predictive Power)**

```
AUC: 0.55 - 0.75  ← Realistic

Top Events:
1. m_card_click              +0.15  ✅ Actionable insight
2. store_page_load           +0.12  ✅ Actionable insight
3. search events             +0.08  ✅ Actionable insight
4. early_position (1-5)      +0.10  ✅ Temporal insight
5. specific_store_categories +0.07  ✅ Business insight
```

### **WITH FUNNEL (Data Leakage)**

```
AUC: 0.95 - 0.99  ← Inflated!

Top Events:
1. action_add_item           +0.40  ⚠️ Obvious (already adding to cart)
2. checkout_page_load        +0.35  ⚠️ Obvious (almost converted)
3. cart_page_load            +0.30  ⚠️ Obvious (viewing cart)
4. m_card_click              +0.05  ← Buried under funnel events
5. search                    +0.02  ← Buried under funnel events
```

**The problem**: Funnel events dominate attribution because they're perfect predictors (but useless for prediction on new data)

---

## **How to Use Results**

### **Step 1: Run NO FUNNEL Version**
```bash
# Databricks - upload and run:
transformer_attribution_no_funnel_pyspark.py
```

### **Step 2: Query Attribution Results**
```sql
-- Find top predictive events
SELECT 
    event_type,
    converted,
    not_converted,
    attribution_lift,
    CASE 
        WHEN attribution_lift > 0.10 THEN 'Strong positive signal'
        WHEN attribution_lift > 0.05 THEN 'Moderate positive signal'
        WHEN attribution_lift < -0.05 THEN 'Negative signal'
        ELSE 'Neutral'
    END as signal_strength
FROM proddb.fionafan.transformer_event_attribution_no_funnel
ORDER BY attribution_lift DESC
LIMIT 20;
```

### **Step 3: Validate with Business Logic**

Example validation:
```
Finding: "m_card_click has +0.15 attribution lift"

Validation questions:
- Do we see higher conversion rates when users click more stores?
- Is click-through rate correlated with conversion in historical data?
- Does this align with user research findings?
```

### **Step 4: Design Interventions**

```
High Attribution Event: m_card_click (+0.15)
Position: Early (position 1-5, +0.12)

Intervention Ideas:
1. Improve store card visibility in first 5 impressions
2. Add quick-action buttons to reduce friction to clicking
3. Personalize top 5 stores to increase click probability
4. A/B test: Does increasing early clicks → increase conversion?
```

---

## **Common Questions**

### **Q: Why is my NO FUNNEL AUC only 0.65?**
A: That's expected! Without funnel events, the model only sees:
- Impressions
- Clicks  
- Page views
- Search

These are **early signals** before conversion decision. AUC 0.65 means the model learned meaningful patterns!

### **Q: Should I trust attention weights or gradients?**
A: **Gradients** (Gradient × Input) are more reliable:
- Attention = "where model looked"
- Gradients = "what affected the output"

Use gradients for attribution decisions.

### **Q: How do I interpret attribution lift of +0.15?**
A: It means:
- In converted sessions: this event gets 0.30 attribution
- In not converted sessions: this event gets 0.15 attribution
- Difference: +0.15

**Interpretation**: "This event is 2x more important for conversion prediction"

### **Q: Can I use this for real-time scoring?**
A: Yes! The trained model can score new sessions:
```python
# New session comes in
new_session = preprocess_session(raw_events)
conversion_prob = model.predict(new_session)  # 0.0 to 1.0

# Use for ranking, filtering, personalization, etc.
```

---

## **Technical Notes**

### **Why nn.TransformerEncoder?**
- Standard PyTorch (well-tested, optimized)
- Easy to deploy
- Compatible with TorchScript for production

### **Embedding Architecture**
```
7 embeddings per event:
- event_type: "m_card_click"
- discovery_feature: "Core Search"  
- store_id: "12345"
- store_name: "McDonald's"
- primary_tag: "Fast Food"
- primary_category: "American"
- continuous: [event_rank, time_delta, is_nv]

→ Concatenate → Project to d_model=192
→ Add positional encoding
→ Transformer (3 layers, 6 heads)
```

### **Sampling Strategy**
```sql
-- Sample 10,000 users (not events!)
WITH sampled_users AS (
    SELECT DISTINCT USER_ID
    FROM events
    ORDER BY RANDOM()
    LIMIT 10000
)

-- Then get ALL events for sampled users
SELECT e.*
FROM events e
INNER JOIN sampled_users su ON e.USER_ID = su.USER_ID
```

This ensures complete session histories.

---

## **Comparison: NO FUNNEL vs WITH FUNNEL**

Run both versions and compare:

```sql
-- Compare top events
SELECT 
    nf.event_type,
    nf.attribution_lift as no_funnel_lift,
    wf.attribution_lift as with_funnel_lift,
    wf.attribution_lift - nf.attribution_lift as leakage_amount
FROM proddb.fionafan.transformer_event_attribution_no_funnel nf
LEFT JOIN proddb.fionafan.transformer_event_attribution_with_funnel wf
    ON nf.event_type = wf.event_type
ORDER BY leakage_amount DESC
LIMIT 20;
```

**Expected Result**:
```
Event Type          | No Funnel | With Funnel | Leakage
--------------------|-----------|-------------|----------
action_add_item     | N/A       | +0.40       | HUGE ⚠️
checkout_page_load  | N/A       | +0.35       | HUGE ⚠️
m_card_click        | +0.15     | +0.05       | -0.10 (buried!)
store_page_load     | +0.12     | +0.04       | -0.08 (buried!)
```

**Insight**: Funnel events dominate and bury the real signals!

---

## **Next Steps**

1. ✅ Run `transformer_attribution_no_funnel_pyspark.py` on Databricks
2. ✅ Query results from Snowflake tables
3. ✅ Validate top events with business logic
4. ✅ Design product interventions based on high-attribution events
5. ✅ A/B test: Does increasing high-attribution behaviors → increase conversion?
6. ✅ Monitor: Track if attribution patterns change over time

---

## **Files Created**

**Local Versions:**
- `transformer_attribution_no_funnel.py` ⭐
- `transformer_attribution_with_funnel.py`

**Databricks Versions:**
- `transformer_attribution_no_funnel_pyspark.py` ⭐
- `transformer_attribution_with_funnel_pyspark.py`

**Documentation:**
- `TRANSFORMER_MODELS_README.md` - Architecture & usage
- `ATTENTION_EXTRACTION_GUIDE.md` - How attention works
- `ATTRIBUTION_ANALYSIS_SUMMARY.md` - This file

---

## **Quick Reference**

### **Run Analysis**
```bash
# Local
python transformer_attribution_no_funnel.py

# Databricks
# Upload transformer_attribution_no_funnel_pyspark.py
# Run all cells
```

### **Query Results**
```sql
-- Top events
SELECT * FROM proddb.fionafan.transformer_event_attribution_no_funnel
ORDER BY attribution_lift DESC LIMIT 20;

-- Position importance
SELECT * FROM proddb.fionafan.transformer_position_attribution_no_funnel;

-- Top stores
SELECT * FROM proddb.fionafan.transformer_store_attribution_no_funnel
WHERE label = 1 ORDER BY mean DESC LIMIT 20;
```

### **Interpret Results**
- **Positive Lift (+0.10)**: Event drives conversion
- **Negative Lift (-0.10)**: Event prevents conversion
- **Near Zero**: Event is neutral

---

**Ready to uncover which actions drive conversions!** 🚀

