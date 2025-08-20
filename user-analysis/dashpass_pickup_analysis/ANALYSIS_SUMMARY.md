# DashPass vs Pickup Analysis - Summary

**Research Question**: Do DashPass Cx have lower % pickup Cx?

**Answer**: ✅ **YES** - DashPass customers have significantly lower pickup rates

## Key Results

### Pickup Rates by Customer Type
- **DashPass customers**: 3.01% of orders are pickup
- **Non-DashPass customers**: 6.23% of orders are pickup  
- **Difference**: 3.22 percentage points lower for DashPass customers
- **Relative difference**: DashPass customers are 2.1x less likely to use pickup

### Data Summary (July 1 - Aug 16, 2025)
| Metric | Non-DashPass | DashPass | 
|--------|-------------|----------|
| Total Orders | 93.4M | 192.3M |
| Unique Customers | 24.2M | 18.7M |
| Pickup Orders | 5.8M | 5.8M |
| Orders per Customer | 3.9 | 10.3 |

### Statistical Significance
- **Z-statistic**: 1,293.53
- **P-value**: < 0.001 (highly significant)
- **95% CI for DashPass**: 3.008% - 3.012%
- **95% CI for Non-DashPass**: 6.225% - 6.235%

## Business Insights

1. **Value Proposition Alignment**: Lower pickup rates among DashPass customers align with the subscription's core benefit of convenient, free delivery
2. **Customer Behavior**: DashPass customers are more engaged (10.3 vs 3.9 orders per customer) and show strong preference for delivery
3. **Market Share**: DashPass represents 67% of order volume but only 44% of unique customers

## Files Generated
- `sql/dashpass_pickup_comparison.sql` - SQL analysis query
- `dashpass_pickup_analysis.py` - Python analysis script
- Raw data from Snowflake query execution

**Analysis Period**: July 1 - August 16, 2025
**Data Source**: proddb.public.dimension_deliveries
**Sample Size**: 285.8M orders, 41.2M unique customers
