# Uplift Model Comparison Guide

## Overview

This script compares three uplift modeling approaches on the **Direct channel** using a **train/validation split** (90%/10%) and evaluates them using **Qini scores**.

## Three Models Compared

### 1. T-Learner (Two-Model Baseline)
**Algorithm**: Simple two-model approach from `original.py`

```
Treatment Model: μ₁(X) → P(Y=1 | T=1, X)
Control Model:   μ₀(X) → P(Y=1 | T=0, X)
Uplift Score:    μ₁(X) - μ₀(X)
```

**Models trained**: 2
- One on treatment group
- One on control group

**Pros:**
- Simple and fast
- Easy to interpret
- Good baseline

**Cons:**
- No propensity weighting
- Doesn't leverage cross-group information
- Can be unstable with imbalanced groups

---

### 2. X-Learner (Full with Propensity Weighting)
**Algorithm**: Four-model approach with propensity weighting

**Stage 1** - Response models:
```
μ₀: Control response model
μ₁: Treatment response model
```

**Stage 2** - Impute treatment effects:
```
For control:   τ̂ᶜ = μ₁(X) - Y
For treatment: τ̂ᵗ = Y - μ₀(X)
```

**Stage 3** - Train CATE models:
```
τ₀: Control CATE model (predicts τ̂ᶜ from X)
τ₁: Treatment CATE model (predicts τ̂ᵗ from X)
```

**Stage 4** - Propensity model:
```
g(X): Propensity score P(T=1 | X)
```

**Final uplift**:
```
Uplift = g(X) · τ₁(X) + (1 - g(X)) · τ₀(X)
```

**Models trained**: 5 (μ₀, μ₁, τ₀, τ₁, g)

**Pros:**
- Most theoretically sound
- Handles imbalanced data well
- Uses both groups to inform predictions
- Propensity weighting reduces bias

**Cons:**
- Most complex
- Longest training time
- Requires most computation

---

### 3. X-Learner V2 (Simplified)
**Algorithm**: Stages 1-3 same as X-Learner, but **no propensity weighting**

**Stages 1-3**: Same as X-Learner (train μ₀, μ₁, τ₀, τ₁)

**Final uplift** (simplified):
```
Uplift = (τ₀(X) + τ₁(X)) / 2
```

**Models trained**: 4 (μ₀, μ₁, τ₀, τ₁ - no propensity model)

**Pros:**
- Simpler than full X-Learner
- Still leverages cross-group information
- Faster than full X-Learner (no propensity model)

**Cons:**
- No propensity weighting (may have bias)
- Simple average may not be optimal

---

## Data Split

### Train Set (90%)
- Used to fit all models
- Stratified by treatment/control

### Validation Set (10%)
- **Holdout set** - never seen during training
- Used **only** for calculating Qini scores
- Ensures unbiased performance comparison

## Evaluation Metric: Qini Score

### What is Qini Score?

The **Qini coefficient** measures how well an uplift model identifies individuals who benefit most from treatment.

**Formula**:
```
Qini Score = (Area under model curve - Area under random curve) / 
              (Area under perfect curve - Area under random curve)
```

**Interpretation**:
- **1.0**: Perfect model (captures all uplift)
- **> 0.3**: Strong model
- **0.1-0.3**: Moderate model
- **~0**: No better than random
- **< 0**: Worse than random (targeting wrong people)

### Why Qini Instead of AUC?

| Metric | What it Measures | Good for |
|--------|------------------|----------|
| **AUC** | Classification accuracy | Predicting Y given X |
| **Qini** | Incremental lift from treatment | **Uplift modeling** ✅ |

**Example**: A model with high AUC in both treatment/control groups might have **zero uplift** if both groups have similar predictions.

## Expected Output

```
================================================================================
🏆 FINAL COMPARISON RESULTS - DIRECT CHANNEL
================================================================================

                       Model  Qini_Score  Avg_Uplift  Positive_Uplift_Pct
       X-Learner V2 (Simplified)      0.3521      0.0234               0.6821
           X-Learner (Full)      0.3489      0.0228               0.6752
                     T-Learner      0.3102      0.0198               0.6423

🥇 **Winner**: X-Learner V2 (Simplified) with Qini Score = 0.3521
📈 Improvement over T-Learner baseline: 13.5%
```

### Metrics Explained

| Metric | Description |
|--------|-------------|
| **Qini_Score** | Primary metric - how well model ranks uplift |
| **Avg_Uplift** | Average predicted uplift across population |
| **Positive_Uplift_Pct** | % of users with positive predicted uplift |

## How to Run

### In Databricks Notebook

```python
# Run all cells in sequence
%run /path/to/xlearner_pyspark
```

The script will automatically:
1. Load data from Snowflake
2. Filter to Direct channel
3. Split train/validation (90/10)
4. Train all three models
5. Evaluate on validation set
6. Display comparison results
7. Save results to Snowflake

### Output Table

Results saved to: `proddb.fionafan.uplift_model_comparison_results`

Schema:
```
Model (string)
Qini_Score (double)
Avg_Uplift (double)
Positive_Uplift_Pct (double)
channel (string) = "Direct"
evaluated_at (timestamp)
train_samples (int)
val_samples (int)
```

## Interpretation Guide

### Scenario 1: X-Learner Wins
```
X-Learner (Full): Qini = 0.35
X-Learner V2:     Qini = 0.32
T-Learner:        Qini = 0.28
```

**Interpretation**:
- Propensity weighting helps
- Treatment assignment is imbalanced or confounded
- **Recommendation**: Use X-Learner (Full) for production

---

### Scenario 2: X-Learner V2 Wins
```
X-Learner V2:     Qini = 0.36
X-Learner (Full): Qini = 0.34
T-Learner:        Qini = 0.29
```

**Interpretation**:
- Cross-group information helps
- But propensity weighting doesn't add much (treatment is random)
- **Recommendation**: Use X-Learner V2 (simpler and faster)

---

### Scenario 3: T-Learner Wins
```
T-Learner:        Qini = 0.32
X-Learner V2:     Qini = 0.30
X-Learner (Full): Qini = 0.29
```

**Interpretation**:
- Treatment and control groups are very different
- Cross-group modeling introduces noise
- **Recommendation**: Stick with T-Learner (simpler is better)

---

### Scenario 4: All Models Similar
```
X-Learner (Full): Qini = 0.31
X-Learner V2:     Qini = 0.31
T-Learner:        Qini = 0.30
```

**Interpretation**:
- Signal is weak or data is noisy
- Model choice doesn't matter much
- **Recommendation**: Use T-Learner (fastest and simplest)

---

### Scenario 5: All Qini Scores Near Zero or Negative
```
All models: Qini < 0.05
```

**Interpretation**:
- **No uplift signal** in the data
- Treatment may have no effect or inconsistent effect
- **Recommendation**: Don't deploy any model - treatment doesn't work

## Visualizing Results

To plot Qini curves for visual comparison:

```python
from sklift.viz import plot_qini_curves
import matplotlib.pyplot as plt

plot_qini_curves(
    y_true=df_eval_tlearner[label_col].values,
    uplift={
        'T-Learner': df_eval_tlearner['uplift_score'].values,
        'X-Learner': df_eval_xlearner['uplift_score'].values,
        'X-Learner V2': df_eval_xlearner_v2['uplift_score'].values
    },
    treatment=df_eval_tlearner[treatment_col].values
)
plt.title('Qini Curves - Model Comparison')
plt.show()
```

## Next Steps After Comparison

### 1. If X-Learner wins significantly (>10% improvement):
- Deploy X-Learner for Direct channel
- Consider training for other channels
- Monitor performance over time

### 2. If results are close (<5% difference):
- Use simplest model (T-Learner)
- Focus on feature engineering instead
- Consider ensemble approaches

### 3. If all models perform poorly (<0.1 Qini):
- Check if treatment actually works
- Look for heterogeneous effects by segments
- Gather more data or better features

## Training for Other Channels

To run comparison on other channels, modify:

```python
# Change this line:
channel_name = "Direct"

# To any of:
channel_name = "Organic_Search"
channel_name = "Paid_Media"
channel_name = "Email"
channel_name = "Partners"
channel_name = "Affiliate"
channel_name = "All"  # All users
channel_name = "Null"  # No channel attribution
```

## Performance Notes

### Training Time (Approximate)

For Direct channel (~50K samples):

| Model | Training Time | # Models | Complexity |
|-------|---------------|----------|------------|
| T-Learner | ~3 min | 2 | Low |
| X-Learner V2 | ~7 min | 4 | Medium |
| X-Learner (Full) | ~10 min | 5 | High |

*Times on 2-worker Spark cluster with CPUs*

### Memory Usage

| Model | Spark Memory | Notes |
|-------|--------------|-------|
| T-Learner | Low | Minimal shuffling |
| X-Learner V2 | Medium | More transformations |
| X-Learner (Full) | Medium-High | Most transformations |

## Troubleshooting

### Issue: "Skipping channel due to small sample size"
**Solution**: Lower the `min_sample_size` threshold or combine channels

### Issue: Training is very slow
**Solution**: 
- Increase `num_workers` parameter
- Use GPU if available (`device="cuda"`)
- Reduce `n_estimators` in XGBoost

### Issue: Qini scores are negative
**Solution**: 
- Check if treatment actually has positive effect
- Verify features are predictive
- Try different time periods or segments

### Issue: Out of memory
**Solution**:
- Increase Spark executor memory
- Reduce data size (sample or filter)
- Use smaller validation split

## References

1. **X-Learner**: Künzel et al. (2019). "Metalearners for estimating heterogeneous treatment effects"
2. **Qini Curve**: Radcliffe (2007). "Using control groups to target on predicted lift"
3. **scikit-uplift**: https://www.uplift-modeling.com/
4. **XGBoost PySpark**: https://xgboost.readthedocs.io/

## Summary

This comparison framework allows you to:
- ✅ Objectively compare uplift modeling approaches
- ✅ Use proper holdout validation (no training data leakage)
- ✅ Evaluate with uplift-specific metrics (Qini score)
- ✅ Make data-driven decisions about which model to deploy

**Key takeaway**: The best model is the one that maximizes Qini score on the validation set!


