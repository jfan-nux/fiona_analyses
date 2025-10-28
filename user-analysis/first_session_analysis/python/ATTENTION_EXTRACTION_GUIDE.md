# Attention Extraction Guide - Understanding Event Attribution

## Question: How Do We Uncover Which Steps the Model Pays Attention To?

---

## **Attribution Granularities**

The transformer model provides attribution at **multiple granularities**:

### **1. Event-Level Attribution** (Finest Granularity)
**Granularity**: Individual event instances
**Question**: "Which specific event at which position matters most?"

```
Example session:
Position 0: m_card_view on McDonald's     → Attribution: 0.05
Position 1: m_card_click on McDonald's    → Attribution: 0.35  ⭐ Important!
Position 2: store_page_load               → Attribution: 0.40  ⭐⭐ Most important!
Position 3: search event                  → Attribution: 0.10
Position 4: m_card_click on Chipotle      → Attribution: 0.10
```

**Insight**: "The model paid most attention to store_page_load at position 2"

---

### **2. Event-Type Attribution** (Medium Granularity)
**Granularity**: Event types (aggregated across all instances)
**Question**: "Which types of events drive conversion prediction?"

```
Event Type          | Avg Attribution (Converted) | Avg Attribution (Not Converted) | Lift
--------------------|-----------------------------|---------------------------------|-------
m_card_click        | 0.25                        | 0.15                            | +0.10
store_page_load     | 0.30                        | 0.20                            | +0.10
m_card_view         | 0.10                        | 0.12                            | -0.02
search              | 0.15                        | 0.08                            | +0.07
```

**Insight**: "Clicks and store page loads get 10% more attention in converted sessions"

---

### **3. Position-Based Attribution** (Temporal Granularity)
**Granularity**: Event position buckets
**Question**: "Do early events matter more than late events?"

```
Position Range | Avg Attribution (Converted) | Avg Attribution (Not Converted) | Lift
---------------|-----------------------------|---------------------------------|-------
1-5 (early)    | 0.35                        | 0.25                            | +0.10  ⭐
6-10           | 0.25                        | 0.20                            | +0.05
11-20          | 0.20                        | 0.25                            | -0.05
21-50          | 0.15                        | 0.20                            | -0.05
50+ (late)     | 0.05                        | 0.10                            | -0.05
```

**Insight**: "Early events (first 5) get significantly more attention in conversions"

---

### **4. Store-Level Attribution** (Business Granularity)
**Granularity**: Specific stores
**Question**: "Which stores drive conversion when clicked/viewed?"

```
Store ID    | Store Name      | Avg Attribution | Event Count
------------|-----------------|-----------------|-------------
12345       | McDonald's      | 0.42            | 250
67890       | Chipotle        | 0.38            | 180
54321       | Starbucks       | 0.35            | 150
```

**Insight**: "McDonald's interactions receive the highest attribution"

---

## **How We Extract Attention**

### **Method Used: Gradient × Input (Saliency)**

```python
# For each session:
1. Forward pass through model
2. Compute gradient of prediction w.r.t. input embeddings
3. Multiply gradient × input (saliency map)
4. Take absolute value and normalize
5. Map back to events
```

**Why this method?**
- ✅ More reliable than attention weights for attribution
- ✅ Captures full gradient flow through all layers
- ✅ Shows which inputs actually impact the output
- ✅ Standard in interpretability literature

### **Alternative: Direct Attention Extraction**

For pure attention weights from transformer:

```python
# Register hooks on nn.MultiheadAttention modules
def register_attention_hooks(model):
    attention_weights = []
    
    def hook(module, input, output):
        # Capture attention weights from output[1]
        attention_weights.append(output[1])
    
    for module in model.transformer.modules():
        if isinstance(module, nn.MultiheadAttention):
            module.register_forward_hook(hook)
    
    return attention_weights

# After forward pass:
# attention_weights contains (B, num_heads, L, L) tensors
# - B: batch size
# - num_heads: 6 attention heads
# - L: sequence length
# - Matrix[i,j] = attention FROM position i TO position j

# For CLS-based pooling:
# Take attention FROM CLS (position 0) TO events (positions 1:L)
cls_to_events_attention = attention_weights[0][0, :, 0, 1:]  # (num_heads, L-1)
avg_attention = cls_to_events_attention.mean(dim=0)  # Average across heads
```

---

## **What The Code Produces**

### **Output Files**

1. **`gradient_attribution_no_funnel.csv`**
   - Event-level attribution for every event in test sessions
   - Columns: session_id, label, event_position, gradient_attribution, event_type, store_id

2. **`event_type_attribution_no_funnel.csv`**
   - Event type importance (aggregated)
   - Columns: event_type, not_converted, converted, attribution_lift

3. **`position_attribution_no_funnel.csv`**
   - Position-based importance
   - Columns: position_bin, not_converted, converted, attribution_lift

4. **`attention_attribution_analysis.png`**
   - 4-panel visualization:
     - Top events by lift
     - Position importance
     - Converted vs not converted comparison
     - Event type × Position heatmap

---

## **Interpreting Results**

### **Attribution Lift Metric**
```
Attribution Lift = Attribution_Converted - Attribution_Not_Converted
```

**Positive Lift (+0.10)**:
→ This event gets MORE attention when predicting conversions
→ **Conversion drivers**: Users who do this are more likely to convert

**Negative Lift (-0.05)**:
→ This event gets LESS attention when predicting conversions
→ **Non-conversion signals**: Users who do this are less likely to convert

### **Example Interpretation**

```
Event: m_card_click
- Converted sessions: 0.30 attribution
- Not converted sessions: 0.15 attribution
- Lift: +0.15

INTERPRETATION: "Store card clicks are a strong conversion signal. 
When the model sees card clicks, it increases conversion probability."
```

```
Event: m_app_background
- Converted sessions: 0.08 attribution  
- Not converted sessions: 0.18 attribution
- Lift: -0.10

INTERPRETATION: "Backgrounding the app is a negative signal.
Users who background frequently are less likely to convert."
```

---

## **Granularity Trade-offs**

| Granularity | Precision | Actionability | Stability |
|-------------|-----------|---------------|-----------|
| Event-Level | Highest | Low | Low (noisy) |
| Event-Type | Medium | **High** ⭐ | High |
| Position | Low | Medium | Highest |
| Store-Level | Medium | **High** ⭐ | Medium |

**Recommendation**: Focus on **Event-Type** and **Store-Level** attribution for actionable insights.

---

## **How to Use in Practice**

### **Step 1: Event-Type Attribution**
```python
# Find: Which types of events matter most?
event_pivot.head(10)

# Example output:
# m_card_click         +0.15  ← Drive this behavior!
# store_page_load      +0.12  ← Important conversion signal
# search               +0.08
```

**Action**: Optimize features that drive high-attribution events

### **Step 2: Position Attribution**
```python
# Find: When do important events need to happen?
pos_pivot

# Example output:
# Position 1-5:   +0.10  ← Early engagement matters!
# Position 6-10:  +0.05
# Position 11-20: -0.02  ← Late events less predictive
```

**Action**: Focus on improving early session experience

### **Step 3: Store Attribution**
```python
# Find: Which stores drive conversions?
store_attr.nlargest(20, 'mean')

# Example output:
# McDonald's:  0.42  ← High-converting stores
# Chipotle:    0.38
# Starbucks:   0.35
```

**Action**: Promote high-attribution stores, understand why they convert

---

## **Advanced: Session-Specific Attribution**

For individual session analysis:

```python
# Filter to specific session
session_attr = df_grad_attribution[df_grad_attribution['session_id'] == 'user123_device456_2025-01-15_1']

# See which events in THIS session got attention
print(session_attr[['event_position', 'event_type', 'gradient_attribution']].sort_values('gradient_attribution', ascending=False))

# Example output:
# Position 2: store_page_load     0.45  ← Model focused here!
# Position 1: m_card_click         0.30
# Position 0: m_card_view          0.15
# Position 3: search               0.10
```

**Use case**: Debug specific sessions, understand model behavior

---

## **Technical Details**

### **Why Gradient × Input?**

**Attention weights alone are incomplete** because:
1. Attention ≠ importance (you can attend to something irrelevant)
2. Gradients show what actually affects the output
3. Input magnitude matters (high attention × low magnitude = low impact)

**Gradient × Input combines both:**
- Gradient: How much output changes w.r.t. this input
- Input: The actual embedding values
- Product: Actual contribution to prediction

### **Normalization**

We normalize attribution per session:
```python
saliency = saliency / (saliency.sum() + 1e-12)
```

So attribution sums to 1.0 across all events in a session.

This makes attribution **comparable across sessions** of different lengths.

---

## **Quick Start**

Run the NO FUNNEL model to get attribution:

```bash
python transformer_attribution_no_funnel.py
```

This produces:
1. **4 visualizations** showing attribution at all granularities
2. **Event type ranking** - which events matter most
3. **Position analysis** - when events need to happen
4. **Store rankings** - which stores drive conversion

Then examine outputs:
```bash
# Event-level details
cat outputs/gradient_attribution_no_funnel.csv

# Summary by event type  
cat outputs/event_type_attribution_no_funnel.csv

# Summary by position
cat outputs/position_attribution_no_funnel.csv
```

---

## **Expected Insights (No Funnel Version)**

Since funnel events are excluded, you'll likely find:

**High Attribution Events:**
- `m_card_click` - Users clicking on stores
- `store_page_load` - Actually viewing store pages
- Search-related events - Active store discovery
- Early position events (1-5)

**Low Attribution Events:**
- `m_app_background` - Leaving the app
- Late position events (50+)
- Passive impressions without clicks

**Store Patterns:**
- Popular QSR restaurants (McDonald's, Chipotle)
- Stores in user's typical categories
- Geographically close stores

These insights are **actionable** - you can optimize the app to drive high-attribution behaviors!

