# Badge Experiment Analysis by Store Volume Deciles

## Overview
`store_decile_analysis.sql` provides a complementary analysis to the main badge experiment, breaking down treatment effects by **store volume deciles** instead of item volume bins.

## Purpose
While the original `volume_bin_analysis_50.sql` analyzes treatment effects by item volume bins (10, 50, 100 orders), this analysis examines how the badge treatment performs across different **store volume tiers** to understand:

- Which types of stores show strongest badge treatment effects?  
- Should rollout prioritize high-volume or low-volume stores?
- Are treatment effects consistent across store volume tiers?

## Methodology

### Store Volume Segmentation
- **Baseline Period**: June 25 - July 24, 2025 (30 days prior to experiment start)
- **Segmentation Method**: `NTILE(10)` based on store order volume
- **Decile Labels**: D1 (Lowest Volume) through D10 (Highest Volume)

### Analysis Framework
1. **Badge Filtering**: Still uses badge bins to ensure we analyze badged items only
2. **Store Context**: Adds store volume decile information to all experiment data  
3. **Treatment Comparison**: Compares treatment vs control within each store volume decile

## Key Output Metrics

### Store Context
- `volume_decile` - Store volume tier (1-10)
- `volume_tier_label` - Descriptive labels (D1_Lowest_Volume, etc.)  
- `unique_stores` - Number of stores in this decile-treatment combination
- `avg_store_baseline_volume` - Average baseline daily orders for stores in decile

### Treatment Effects
- `pct_diff_order_count` - Treatment vs control % difference in order count  
- `pct_diff_revenue_usd` - Treatment vs control % difference in revenue
- `order_count_ratio` - Treatment/control ratio (1.0 = no effect, >1.0 = positive effect)
- `revenue_ratio` - Revenue treatment/control ratio

### Raw Metrics  
- `treatment_order_count` / `control_order_count` - Absolute order counts
- `treatment_revenue_usd` / `control_revenue_usd` - Absolute revenue amounts

## Expected Store Tier Characteristics

| Decile | Label | Typical Store Profile | Expected Badge Impact |
|--------|-------|----------------------|---------------------|
| D1-D3 | Low Volume | Small restaurants, limited menu | Lower absolute impact |
| D4-D7 | Medium Volume | Popular local spots, moderate selection | Potential highest relative uplift |
| D8-D10 | High Volume | Major chains, food courts, high traffic | Lower relative uplift, high absolute impact |

## Strategic Questions Answered

### 1. **Treatment Heterogeneity**
- Do low-volume stores show different treatment response than high-volume stores?
- Which deciles show statistically significant treatment effects?

### 2. **Rollout Prioritization**  
- Should initial rollout focus on high-volume stores (broader reach)?
- Or medium-volume stores (potentially higher relative uplift)?

### 3. **Resource Allocation**
- Where should engineering effort focus badge feature improvements?
- Which store partnerships would maximize badge impact?

## Usage Examples

### Basic Analysis
```sql
-- Run the full analysis
-- Look for patterns in pct_diff_order_count across volume_decile
-- Identify deciles with strongest positive treatment effects
```

### Key Patterns to Look For
- **Monotonic effects**: Treatment effect increasing/decreasing with volume
- **Sweet spot**: Specific deciles with optimal treatment response  
- **Threshold effects**: Treatment working above/below certain volume levels

## Comparison to Original Analysis

### Original `volume_bin_analysis_50.sql`
- **Focus**: Item-level volume bins (how popular is this specific item?)
- **Question**: Do badges work better on high-volume vs low-volume items?
- **Strategic Use**: Item selection and badge targeting

### New `store_decile_analysis.sql`  
- **Focus**: Store-level volume deciles (how busy is this store overall?)
- **Question**: Do badges work better at high-volume vs low-volume stores?
- **Strategic Use**: Store partnerships and rollout strategy

## Files Structure
```
sql/
├── volume_bin_analysis_50.sql     # Original: Treatment effects by item volume bins
├── store_decile_analysis.sql      # New: Treatment effects by store volume deciles  
└── [other analysis files]
```

## Expected Business Impact

### High-Volume Stores (D8-D10)
- **Pro**: Massive reach, affects many customers
- **Con**: May show smaller relative treatment effects
- **Strategy**: Focus if absolute impact matters most

### Medium-Volume Stores (D4-D7)
- **Pro**: Likely strongest treatment effects  
- **Con**: Medium reach compared to high-volume stores
- **Strategy**: Focus if efficiency/ROI matters most

### Low-Volume Stores (D1-D3)
- **Pro**: May have untapped potential
- **Con**: Limited reach and possibly limited badge exposure
- **Strategy**: Lowest priority unless strong effects detected

## Next Steps After Analysis
1. **Execute** `store_decile_analysis.sql` to get actual results
2. **Compare** patterns with `volume_bin_analysis_50.sql` results  
3. **Identify** optimal store volume tiers for badge rollout
4. **Develop** decile-specific badge optimization strategy

---
**Created**: To provide store-level strategic insights for badge experiment optimization
**Complements**: Original item-level volume bin analysis
**Strategic Value**: Informs store partnership and rollout prioritization decisions
