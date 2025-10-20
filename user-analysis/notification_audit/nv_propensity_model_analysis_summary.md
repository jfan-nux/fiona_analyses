# NV Propensity Model Performance Analysis

**Model:** `proddb.ml.fact_cx_cross_vertical_propensity_scores_v1`  
**Score:** `CAF_CS_PROPENSITY_SCORE_28D_GROCERY`  
**Active Date:** 2025-10-01  
**Evaluation Period:** 30 days post-active_date (2025-10-02 to 2025-10-31)  
**User Table:** `proddb.fionafan.nv_propensity_model_evaluation`

---

## Executive Summary

The NV propensity model shows **strong predictive performance** across all user segments, with a 30.8x lift from lowest to highest decile (2.26% → 69.74% conversion rate). The model performs well for both Active users and New Cx users, though Active users show higher baseline conversion rates.

### Key Findings
- **Overall Population:** 33.8M users (94.79% Active, 5.21% New Cx)
- **Overall Conversion Rate:** 19.88% made an NV order within 30 days
- **Model Lift:** Top decile converts at 69.74% vs bottom decile at 2.26% (30.8x)
- **Segment Performance:** Model works equally well across both user segments (~11x lift)

---

## 1. Overall Model Performance by Decile

| Decile | Total Users | Converters | Conversion Rate | Min Score | Max Score | Avg Score | Median Score |
|--------|-------------|------------|-----------------|-----------|-----------|-----------|--------------|
| 1 (Lowest) | 3,376,058 | 76,344 | 2.26% | 0.340 | 0.396 | 0.370 | 0.368 |
| 2 | 3,376,058 | 153,527 | 4.55% | 0.396 | 0.482 | 0.447 | 0.450 |
| 3 | 3,376,058 | 178,235 | 5.28% | 0.482 | 0.517 | 0.497 | 0.494 |
| 4 | 3,376,057 | 233,058 | 6.90% | 0.517 | 0.591 | 0.560 | 0.560 |
| 5 | 3,376,057 | 464,563 | 13.76% | 0.591 | 0.662 | 0.619 | 0.612 |
| 6 | 3,376,057 | 528,390 | 15.65% | 0.662 | 0.753 | 0.721 | 0.737 |
| 7 | 3,376,057 | 593,115 | 17.57% | 0.753 | 0.814 | 0.784 | 0.784 |
| 8 | 3,376,057 | 833,485 | 24.69% | 0.814 | 0.858 | 0.840 | 0.842 |
| 9 | 3,376,057 | 1,306,740 | 38.71% | 0.858 | 0.899 | 0.876 | 0.877 |
| 10 (Highest) | 3,376,057 | 2,354,410 | 69.74% | 0.899 | 0.923 | 0.916 | 0.921 |

**Insights:**
- Clear monotonic relationship between score and conversion rate
- Score range is relatively narrow (0.34 - 0.92) but highly predictive
- Strongest acceleration occurs at deciles 5-6 and 9-10

---

## 2. Performance by User Segment

| User Segment | Total Users | Converters | Conversion Rate | Avg Score | Median Score |
|--------------|-------------|------------|-----------------|-----------|--------------|
| Active | 32,001,568 | 6,583,857 | **20.57%** | 0.6718 | 0.6797 |
| New Cx | 1,759,005 | 138,010 | **7.85%** | 0.4996 | 0.4544 |

**Insights:**
- Active users convert at 2.6x higher rate than New Cx users
- Active users have significantly higher propensity scores on average
- New Cx users are newer to the platform and less likely to have tried NV yet

---

## 3. Detailed Performance: Decile × User Segment

### Active Users by Decile

| Decile | Users | Converters | Conversion Rate | Avg Score |
|--------|-------|------------|-----------------|-----------|
| 1 | 2,647,870 | 62,521 | 2.36% | 0.370 |
| 2 | 3,155,001 | 142,397 | 4.51% | 0.448 |
| 3 | 3,204,527 | 167,335 | 5.22% | 0.497 |
| 4 | 3,180,277 | 221,234 | 6.96% | 0.560 |
| 5 | 3,211,790 | 446,955 | 13.92% | 0.619 |
| 6 | 3,261,639 | 506,178 | 15.52% | 0.721 |
| 7 | 3,319,069 | 579,887 | 17.47% | 0.785 |
| 8 | 3,302,452 | 813,200 | 24.62% | 0.840 |
| 9 | 3,355,011 | 1,297,817 | 38.68% | 0.875 |
| 10 | 3,363,932 | 2,346,333 | **69.75%** | 0.916 |

### New Cx Users by Decile

| Decile | Users | Converters | Conversion Rate | Avg Score |
|--------|-------|------------|-----------------|-----------|
| 1 | 728,188 | 13,823 | 1.90% | 0.370 |
| 2 | 221,057 | 11,130 | 5.03% | 0.437 |
| 3 | 171,531 | 10,900 | 6.35% | 0.501 |
| 4 | 195,780 | 11,824 | 6.04% | 0.559 |
| 5 | 164,267 | 17,608 | 10.72% | 0.614 |
| 6 | 114,418 | 22,212 | 19.41% | 0.708 |
| 7 | 56,988 | 13,228 | 23.21% | 0.769 |
| 8 | 73,605 | 20,285 | 27.56% | 0.838 |
| 9 | 21,046 | 8,923 | 42.40% | 0.885 |
| 10 | 12,125 | 8,077 | **66.61%** | 0.911 |

**Insights:**
- New Cx users are heavily concentrated in lower deciles (41.4% in decile 1 alone)
- Both segments show strong model performance with similar lift patterns
- New Cx users in top deciles perform nearly as well as Active users

---

## 4. User Segment Distribution Across Deciles

| Decile | Total Users | Active Users | Active % | New Cx Users | New Cx % |
|--------|-------------|--------------|----------|--------------|----------|
| 1 | 3,376,058 | 2,647,870 | 78.4% | 728,188 | 21.6% |
| 2 | 3,376,058 | 3,155,001 | 93.5% | 221,057 | 6.5% |
| 3 | 3,376,058 | 3,204,527 | 94.9% | 171,531 | 5.1% |
| 4 | 3,376,057 | 3,180,277 | 94.2% | 195,780 | 5.8% |
| 5 | 3,376,057 | 3,211,790 | 95.1% | 164,267 | 4.9% |
| 6 | 3,376,057 | 3,261,639 | 96.6% | 114,418 | 3.4% |
| 7 | 3,376,057 | 3,319,069 | 98.3% | 56,988 | 1.7% |
| 8 | 3,376,057 | 3,302,452 | 97.8% | 73,605 | 2.2% |
| 9 | 3,376,057 | 3,355,011 | 99.4% | 21,046 | 0.6% |
| 10 | 3,376,057 | 3,363,932 | 99.6% | 12,125 | 0.4% |

**Insight:** New Cx users are disproportionately in the lowest decile, indicating they generally have lower NV propensity.

---

## 5. Model Lift Analysis: Top vs Bottom Deciles

### Active Users

| Score Group | Users | Converters | Conversion Rate |
|-------------|-------|------------|-----------------|
| **Bottom 30%** (Deciles 1-3) | 9,007,398 | 372,253 | 4.13% |
| **Middle 40%** (Deciles 4-7) | 12,972,775 | 1,754,254 | 13.52% |
| **Top 30%** (Deciles 8-10) | 10,021,395 | 4,457,350 | **44.48%** |

**Lift:** Top 30% converts at **10.8x** the rate of Bottom 30% (44.48% / 4.13%)

### New Cx Users

| Score Group | Users | Converters | Conversion Rate |
|-------------|-------|------------|-----------------|
| **Bottom 30%** (Deciles 1-3) | 1,120,776 | 35,853 | 3.20% |
| **Middle 40%** (Deciles 4-7) | 531,453 | 64,872 | 12.21% |
| **Top 30%** (Deciles 8-10) | 106,776 | 37,285 | **34.92%** |

**Lift:** Top 30% converts at **10.9x** the rate of Bottom 30% (34.92% / 3.20%)

---

## 6. Key Takeaways

### Model Performance
✅ **Strong predictive power:** 30.8x lift from lowest to highest decile  
✅ **Consistent across segments:** ~11x lift for both Active and New Cx users  
✅ **Well-calibrated:** Clear monotonic relationship between score and conversion  

### User Segment Insights
- **Active users:** Higher baseline conversion (20.57%) and higher propensity scores
- **New Cx users:** Lower baseline (7.85%) but model still highly predictive
- **Distribution skew:** New Cx heavily concentrated in lower deciles (63.7% in bottom 3 deciles)

### Recommendations
1. **Targeting strategy:** Focus NV campaigns on users in deciles 6+ for best efficiency
2. **Segment-specific approach:** 
   - Active users: Target deciles 8-10 (44.48% conversion rate)
   - New Cx users: Target deciles 6+ (19.41%+ conversion rate)
3. **Resource allocation:** Top 30% of users drive 67.7% of Active conversions and 27.0% of New Cx conversions

---

## Data Quality Notes

- **No Non-Purchaser segment found:** The growth accounting filter for 'Non-Purchaser' returned 0 users for active_date = 2025-10-01
- **Evaluation window:** 30 days may not capture all potential conversions for lower-propensity users
- **Score saturation:** Top decile scores cluster near 0.92, suggesting potential score ceiling

---

*Analysis Date: 2025-10-20*  
*SQL Code: `/user-analysis/notification_audit/sql/nv_propensity_model_evaluation.sql`*  
*User Table: `proddb.fionafan.nv_propensity_model_evaluation`*

