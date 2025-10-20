# Quick Start - Uplift Model Comparison

## What You Have

Three uplift models comparing on **Direct channel** with **train/validation split**:

1. **T-Learner** - Two-model baseline (from original.py)
2. **X-Learner (Full)** - Four-model + propensity weighting
3. **X-Learner V2** - Four-model without propensity (simplified)

All evaluated on **10% holdout validation set** using **Qini score**.

## How to Run

### In Databricks

```python
# Just run the notebook
%run /path/to/xlearner_pyspark
```

## What It Does

```
1. Load data from Snowflake ✓
2. Filter to Direct channel ✓
3. Split 90% train, 10% validation ✓
4. Train T-Learner → evaluate on validation
5. Train X-Learner → evaluate on validation
6. Train X-Learner V2 → evaluate on validation
7. Display comparison table
8. Save results to Snowflake
```

## Expected Output

```
================================================================================
🏆 FINAL COMPARISON RESULTS - DIRECT CHANNEL
================================================================================

                       Model  Qini_Score  Avg_Uplift  Positive_Uplift_Pct
           X-Learner (Full)      0.3489      0.0228               0.6752
  X-Learner V2 (Simplified)      0.3521      0.0234               0.6821
                     T-Learner      0.3102      0.0198               0.6423

🥇 **Winner**: X-Learner V2 (Simplified) with Qini Score = 0.3521
📈 Improvement over T-Learner baseline: 13.5%
```

## What the Metrics Mean

| Metric | Meaning |
|--------|---------|
| **Qini_Score** | How well model ranks users by uplift (0-1, higher = better) |
| **Avg_Uplift** | Average predicted increase in conversion probability |
| **Positive_Uplift_Pct** | % of users predicted to benefit from treatment |

## Decision Tree

```
Qini Score > 0.3?
├─ Yes → Strong model, deploy winner
└─ No
   ├─ Qini 0.1-0.3 → Moderate, consider deploying
   └─ Qini < 0.1 → Weak signal, don't deploy
```

## Quick Interpretation

**If X-Learner wins by >10%**:
- Use X-Learner for production
- Worth the extra complexity

**If all models within 5%**:
- Use T-Learner (simplest)
- Focus on better features instead

**If all Qini < 0.1**:
- Treatment may not work
- Need more data or different features

## Files Created

| File | Purpose |
|------|---------|
| `xlearner_pyspark.py` | Main comparison script |
| `MODEL_COMPARISON_GUIDE.md` | Detailed guide (read this!) |
| `QUICK_START.md` | This file |
| `BUGFIX_NOTES.md` | Technical fixes applied |

## Results Saved To

**Snowflake Table**: `proddb.fionafan.uplift_model_comparison_results`

Contains:
- Model name
- Qini score
- Uplift metrics
- Channel
- Train/val sample sizes
- Timestamp

## Next Steps

1. **Run the comparison** in Databricks
2. **Check Qini scores** in output
3. **Read MODEL_COMPARISON_GUIDE.md** for detailed interpretation
4. **Deploy winner** if Qini > 0.3

## Need Help?

See `MODEL_COMPARISON_GUIDE.md` for:
- Detailed algorithm explanations
- Interpretation scenarios
- Troubleshooting guide
- How to run on other channels

## Key Differences from Original

| Original | New Comparison |
|----------|----------------|
| Single method (T-Learner) | Three methods |
| No validation split | 10% holdout validation |
| Trained on all data | Trained on 90% |
| Evaluated on training data | Evaluated on validation |
| AUC + LogLoss metrics | Qini score (uplift-specific) |
| All channels | Direct channel only |
| Scored by decile | Full comparison |

## TL;DR

Run `xlearner_pyspark.py` → Compare Qini scores → Deploy winner if > 0.3 → Profit! 🎉


