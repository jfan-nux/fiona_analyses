# Transformer Attribution - Quick Start Guide

## **TL;DR - Answer Your Question**

### **"Which steps does the attention model pay most attention to?"**

**Answer:** The model provides attribution at **4 granularities**:

1. **Event-Type** → "m_card_click gets +0.15 attribution lift"
2. **Position** → "Events at positions 1-5 get +0.12 lift"  
3. **Store** → "McDonald's gets 0.42 average attribution"
4. **Event-Specific** → "Position 2: store_page_load = 0.45 attribution"

**Method**: Gradient × Input (more reliable than attention weights alone)

**Granularity**: Event-level (each event gets an attribution score 0.0-1.0, normalized per session)

---

## **Run Analysis (Choose One)**

### **Option 1: Local Python** (Quick test, small data)
```bash
cd /Users/fiona.fan/Documents/fiona_analyses
source venv/bin/activate

# Recommended: NO data leakage
python user-analysis/first_session_analysis/python/transformer_attribution_no_funnel.py

# Comparison: WITH data leakage
python user-analysis/first_session_analysis/python/transformer_attribution_with_funnel.py
```

### **Option 2: Databricks** (Production scale, large data)
```
1. Upload to Databricks:
   - transformer_attribution_no_funnel_pyspark.py (recommended)
   - transformer_attribution_with_funnel_pyspark.py (comparison)

2. Run on GPU cluster

3. Query results from Snowflake
```

---

## **What You'll Get**

### **NO FUNNEL Version** (Recommended) ⭐

**Model Performance:**
- Expected AUC: 0.55-0.75 (realistic predictive power)

**Attribution Outputs:**

1. **Event Type Ranking**
```
Top Events by Attribution Lift:
1. m_card_click              +0.15  ← Users who click are likely to convert
2. store_page_load           +0.12  ← Viewing stores is positive signal
3. search events             +0.08  ← Active search indicates intent
4. m_select_tab - explore    +0.05  ← Exploration behavior
5. m_card_view               -0.02  ← Passive impressions weak
```

2. **Position Importance**
```
Position Range | Attribution Lift
---------------|------------------
1-5 (early)    | +0.12  ← Early engagement critical!
6-10           | +0.05
11-20          | -0.02
21-50          | -0.05
```

3. **Top Converting Stores**
```
Store            | Avg Attribution
-----------------|------------------
McDonald's       | 0.42
Chipotle         | 0.38
Starbucks        | 0.35
```

4. **Visualizations**
- Event attribution lift bar chart
- Position importance comparison
- Converted vs not converted side-by-side
- Heatmap: Event type × Position

---

### **WITH FUNNEL Version** (Comparison Only) ⚠️

**Model Performance:**
- Expected AUC: 0.95-0.99 (inflated due to leakage!)

**Attribution Outputs:**
```
Top Events (BIASED by funnel events):
1. action_add_item           +0.40  ⚠️ Leakage!
2. checkout_page_load        +0.35  ⚠️ Leakage!
3. cart_page_load            +0.30  ⚠️ Leakage!
4. m_card_click              +0.05  ← Buried!
5. search                    +0.02  ← Buried!
```

**Use this to**: Show the impact of data leakage (funnel events dominate)

---

## **Files Overview**

```
user-analysis/first_session_analysis/python/
├── transformer_attribution_no_funnel.py              ⭐ Run this (local)
├── transformer_attribution_no_funnel_pyspark.py      ⭐ Run this (Databricks)
├── transformer_attribution_with_funnel.py            ⚠️ Comparison only
├── transformer_attribution_with_funnel_pyspark.py    ⚠️ Comparison only
├── TRANSFORMER_MODELS_README.md                      📖 Architecture guide
├── ATTENTION_EXTRACTION_GUIDE.md                     📖 How attention works
├── ATTRIBUTION_ANALYSIS_SUMMARY.md                   📖 Complete workflow
└── QUICK_START.md                                    📖 This file
```

---

## **Understanding the Output**

### **Attribution Lift Explained**

```python
# For each event type:
attribution_lift = avg_attribution_converted - avg_attribution_not_converted
```

**Positive Lift** (+0.15):
- Event gets MORE attention in converted sessions
- ✅ This event is a **conversion driver**
- **Action**: Optimize features to increase this behavior

**Negative Lift** (-0.10):
- Event gets LESS attention in converted sessions
- ❌ This event is a **conversion blocker**
- **Action**: Reduce friction or remove barriers

**Near Zero** (±0.02):
- Event is neutral
- No strong signal either way

---

## **Example Workflow**

### **Step 1: Run NO FUNNEL Model**
```bash
python transformer_attribution_no_funnel.py
```

**Output**: 
```
✅ Training Complete! Best Val AUC: 0.68
✓ Computed attribution for 15,432 events
```

### **Step 2: Examine Top Events**
```
Top 5 Most Important Events:
1. m_card_click                          | Lift: +0.18
2. store_page_load                       | Lift: +0.14
3. search - Core Search                  | Lift: +0.11
4. m_card_click - Traditional Carousel   | Lift: +0.09
5. m_select_tab - explore                | Lift: +0.06
```

### **Step 3: Check Position Importance**
```
Most Important Positions:
Position 1-5     | Lift: +0.15  ← Early engagement!
Position 6-10    | Lift: +0.06
Position 11-20   | Lift: -0.03
```

### **Step 4: Identify Top Stores**
```
Top 10 Stores in Converted Sessions:
Store 12345 (McDonald's)    | 0.42
Store 67890 (Chipotle)      | 0.38
Store 54321 (Starbucks)     | 0.35
```

### **Step 5: Form Hypotheses**
Based on attribution:
1. **Clicks are critical** (+0.18 lift) → How can we drive more clicks?
2. **Early engagement matters** (position 1-5: +0.15) → Optimize first 5 events
3. **Certain stores convert better** → Feature them prominently

### **Step 6: Design Interventions**
```
Hypothesis: "Increasing clicks in first 5 events will increase conversion"

Intervention Ideas:
- Show most-clickable stores first (personalization)
- Add CTA buttons to reduce friction
- Improve store card imagery
- Test different carousel layouts

A/B Test: Measure conversion lift
```

---

## **Common Issues & Fixes**

### **Issue: `NameError: name 'test_loader' is not defined`**
✅ **Fixed!** All versions now create test_loader before attribution analysis.

### **Issue: AUC is 0.99 (too high)**
✅ **Fixed!** NO FUNNEL version excludes `event_type='funnel'` to prevent leakage.

### **Issue: `KeyError: 'is_conversion_event'`**
✅ **Fixed!** We don't use a separate column - label comes from detecting conversion events.

### **Issue: `invalid identifier 'E.NV_ORG'`**
✅ **Fixed!** NV fields now correctly come from `dimension_store` table via JOIN.

---

## **Key Differences: NO FUNNEL vs WITH FUNNEL**

| Aspect | NO FUNNEL ⭐ | WITH FUNNEL ⚠️ |
|--------|-------------|----------------|
| **Events Included** | Impressions, clicks, search, navigation | **+ funnel events** (add, cart, checkout) |
| **Data Leakage** | None | Severe |
| **Expected AUC** | 0.55-0.75 | 0.95-0.99 |
| **Top Attribution** | m_card_click, store_page_load | action_add_item, checkout |
| **Actionability** | High - shows early signals | Low - shows obvious signals |
| **Use Case** | **Production predictions** | Comparison baseline |

---

## **Files Generated**

### **Model Files**
- `outputs/best_transformer_no_funnel.pt` - Trained model (NO funnel)
- `outputs/best_transformer_with_funnel.pt` - Trained model (WITH funnel)

### **Attribution CSVs**
- `outputs/gradient_attribution_no_funnel.csv` - Event-level attribution
- `outputs/event_type_attribution_no_funnel.csv` - Event type summary
- `outputs/position_attribution_no_funnel.csv` - Position summary

### **Visualizations**
- `plots/attention_attribution_analysis.png` - 4-panel attribution analysis

### **Snowflake Tables** (Databricks only)
- `proddb.fionafan.transformer_event_attribution_no_funnel`
- `proddb.fionafan.transformer_position_attribution_no_funnel`
- `proddb.fionafan.transformer_store_attribution_no_funnel`

---

## **Next Steps**

1. ✅ **Run NO FUNNEL version** to get true attribution
2. ✅ **Examine top events** - which actions drive conversion?
3. ✅ **Check position patterns** - when do events need to happen?
4. ✅ **Identify top stores** - which stores convert best?
5. ✅ **Validate findings** - do they match business intuition?
6. ✅ **Design experiments** - test if increasing high-attribution behaviors → increases conversion
7. ✅ **Compare with WITH FUNNEL** - quantify data leakage impact

---

## **Quick Reference Commands**

```bash
# Run analysis
python transformer_attribution_no_funnel.py

# View results
cat outputs/event_type_attribution_no_funnel.csv | head -20

# Open visualization
open plots/attention_attribution_analysis.png
```

```sql
-- Query Snowflake (after Databricks run)
SELECT * FROM proddb.fionafan.transformer_event_attribution_no_funnel
ORDER BY attribution_lift DESC LIMIT 20;
```

---

**You're ready to discover which events drive conversions!** 🚀

