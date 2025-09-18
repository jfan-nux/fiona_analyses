# NV/DP New User Analysis: Technical Methodology

## Overview
This document details the technical methodology, data definitions, and analytical framework used to analyze the impact of early NV (Non-Verticals) and DashPass exposure on new customer behavior.

---

## Cohort Definition & Study Design

### Primary Cohort
- **Population**: New customers who first joined DoorDash in **April 2024**
- **Cohort Size**: 1.53MM unique users
- **Tracking Period**: 90 days post-registration
- **Geographic Scope**: All active DoorDash markets (scope inferred from data scale)

### Study Design
- **Type**: Observational cohort study with retrospective analysis
- **Comparison Groups**: Users with vs. without early NV/DP exposure
- **Time Horizon**: April 2024 registration through September 2024 (orders tallied until September)

---

## Data Tables & Metric Definitions

### 1. Session-Level Order Performance
**Table**: `proddb.fionafan.nv_dp_new_user_session_order_rates`

#### Cohort Definition
- **Granularity**: Session number (1, 2, 3) × NV impression status (0/1) × Order type
- **Session Definition**: Distinct user engagement periods (likely app/web sessions)
- **Order Types**: `overall`, `nv_order`, `non_nv_order`

#### Key Metrics
- **`consumers_started_session`**: Count of users who initiated each session number
- **`consumers_retained_Xh`**: Users who placed orders within X hours after session start
  - Time windows: 1h, 4h, 24h post-session
- **`distinct_deliveries_Xh`**: Count of unique deliveries within X hours
- **`order_rate_Xh`**: `distinct_deliveries_Xh / consumers_started_session`
- **`consumer_ordered_Xh`**: `consumers_retained_Xh / consumers_started_session`

#### Ratio Calculations
- **`ratio_order_rate_Xh_nv_over_non_nv`**: Order rate for users with NV impressions ÷ Order rate for users without NV impressions
- **`ratio_consumer_ordered_Xh_nv_over_non_nv`**: Consumer ordering rate with NV ÷ without NV

### 2. Join-Time Order Performance  
**Table**: `proddb.fionafan.nv_dp_new_user_join_order_rates`

#### Cohort Definition
- **Granularity**: NV exposure window × Order type × NV impression status (0/1)
- **NV Windows**: `1h`, `4h`, `12h`, `24h`, `7d`, `14d`, `30d` (time window for NV exposure measurement)
- **Order Types**: `overall`, `nv_order`, `non_nv_order`

#### Key Metrics
- **`consumers_joined`**: Count of users in each NV window cohort
- **`consumers_retained_Xd`**: Users who placed orders X days after joining
  - Time windows: 7d, 12d, 14d, 21d, 28d, 60d, 90d post-join
- **`distinct_deliveries_Xd`**: Unique deliveries within X days of joining
- **`order_rate_Xd`**: `distinct_deliveries_Xd / consumers_joined`
- **`consumer_ordered_Xd`**: `consumers_retained_Xd / consumers_joined`

#### Window Definition Logic
- **Hour windows (1h-24h)**: Users who received NV impressions within X hours of joining
- **Day windows (7d-30d)**: Users who received NV impressions within X days of joining
- **Mutually exclusive**: Each user appears in only their earliest qualifying window

### 3. Device-Level Performance Cohorts
**Table**: `proddb.fionafan.nv_dp_orders_performance_from_device`

#### Cohort Definition
- **Granularity**: Cohort type × Month bucket × Order type × Cohort membership (0/1)
- **Device-Level**: Analysis unit is unique devices (likely device IDs)
- **Cohorts**: `nv_first_order`, `dp_first_order`, `dp_second_order`, etc.

#### Key Metrics
- **`devices`**: Count of unique devices in cohort
- **`consumers_retained`**: Users who remained active in the month bucket
- **`distinct_deliveries`**: Orders placed by cohort in time period
- **`order_rate`**: `distinct_deliveries / devices`
- **`retention`**: `consumers_retained / devices`

#### Time Bucketing
- **Month Buckets**: `month1`, `month2`, `month3`, `month4` (post-cohort-entry)
- **Cohort vs. Non-Cohort Ratios**: Each metric calculated as cohort performance ÷ non-cohort performance

### 4. Notification Performance
**Table**: `proddb.fionafan.nv_dp_notif_performance_from_daily`

#### Cohort Definition  
- **Granularity**: Notification window × Order type × NV notification status (0/1)
- **Windows**: Various time periods for notification exposure measurement

#### Key Metrics
- **Daily-level aggregation**: Performance measured at daily granularity
- **Notification impact**: Comparison of users who received NV notifications vs. those who didn't

### 5. DashPass Performance Tables
**Tables**: 
- `proddb.fionafan.dp_new_user_notif_performance`
- `proddb.fionafan.dp_new_user_orders_performance`

#### Cohort Definition
- **DP Cohorts**: Users grouped by DashPass interaction type
  - `dp_first_order`: First order was DP-eligible
  - `dp_second_order`: Second order was DP-eligible  
  - `dp_signup_24h`: Signed up for DP within 24h
  - `dp_signup_7d`: Signed up for DP within 7d
  - `dp_signup_30d`: Signed up for DP within 30d

#### Baseline Definition
- **Baseline Group**: Users with no DashPass interaction in respective time windows
- **Ratio Calculations**: All metrics calculated as cohort rate ÷ baseline rate

---

## Regression Analysis Framework

### Target Variables Definition
**Table**: `proddb.fionafan.nv_dp_logit_results_0_5`

#### Target Definitions
1. **`target_first_order_new_cx`**: Binary indicator (0/1) for whether user placed first order
2. **`target_ordered_24h`**: Binary indicator for order within 24 hours of joining  
3. **`target_ordered_month1`**: Binary indicator for any order in first month post-join
4. **`target_ordered_month2`**: Binary indicator for any order in second month post-join
5. **`target_ordered_month3`**: Binary indicator for any order in third month post-join
6. **`target_ordered_month4`**: Binary indicator for any order in fourth month post-join

### Feature Engineering & Definitions

#### NV Impression Features
- **`had_nv_imp_Xh/Xd`**: Binary indicators for NV impression within time window
  - Windows: `1h`, `4h`, `12h`, `24h`, `7d`, `30d`
  - **Measurement Period**: All impressions tracked for 30 days after join date for each user
- **`had_nv_imp_first_session`**: NV impression in first user session
- **`had_nv_imp_second_session`**: NV impression in second user session
- **`minutes_to_first_nv_impression`**: Continuous time until first NV impression (normalized)

#### Non-NV Impression Features  
- **`had_non_nv_imp_Xh/Xd`**: Binary indicators for non-NV (traditional vertical) impressions
  - Windows: `1h`, `4h`, `12h`, `24h`, `7d`, `30d`
- **`had_non_nv_imp_first_session`**: Non-NV impression in first session
- **`had_non_nv_imp_second_session`**: Non-NV impression in second session
- **`minutes_to_first_non_nv_impression`**: Continuous time until first non-NV impression (normalized)

#### DashPass Features
- **`dp_has_notif_30d`**: Binary indicator for DP notification within 30 days
- **`dp_has_notif_any_day`**: Binary indicator for any DP notification
- **`dp_first_order`**: Binary indicator for first order being DP-eligible
- **`nv_first_order`**: Binary indicator for first order being NV-related
- **`nv_second_order`**: Binary indicator for second order being NV-related

#### Notification Features
- **`has_nv_notif_30d`**: Binary indicator for NV-related notifications within 30 days
- **`has_nv_notif_60d`**: Binary indicator for NV-related notifications within 60 days
- **Measurement Period**: All notifications tracked for 60 days after join date for each user

### Model Specification

#### Logistic Regression Setup
- **Model Type**: Binary logistic regression with maximum likelihood estimation
- **Threshold**: 0.5 probability cutoff (implied by table name `_0_5`)
- **Sample Size**: 1.53MM observations across all models
- **Regression Sample Rate**: 0.5 (regression analysis conducted on 50% random sample)
- **Convergence**: All models converged successfully

#### Statistical Outputs
- **`coef`**: Log-odds coefficient estimates
- **`std_err`**: Standard errors of coefficients
- **`p_value`**: Statistical significance tests
- **`conf_low/conf_high`**: 95% confidence intervals for coefficients
- **`pseudo_r2`**: McFadden's pseudo R-squared (model fit metric)

#### Average Marginal Effects (AME)
- **`ame`**: Average marginal effect (change in probability from coefficient)
- **`ame_se`**: Standard error of marginal effect
- **`ame_z`**: Z-statistic for marginal effect significance
- **Interpretation**: AME represents percentage point change in probability from 1-unit increase in feature

### Feature Selection & Engineering Approach

#### Time Window Strategy
- **Multiple windows tested**: 1h, 4h, 12h, 24h, 7d, 30d to capture different exposure effects
- **Session-based features**: Capture timing relative to user engagement patterns
- **Normalization**: Time-to-event features normalized (likely minutes ÷ max or z-score)

#### Feature Interaction Approach
- **Separate NV/non-NV**: Captures differential effects of vertical vs. non-vertical content
- **Separate DP interaction types**: Distinguishes notification exposure from order behavior
- **Session timing**: Captures critical early engagement windows

---

## Analytical Framework & Limitations

### Causal Inference Approach
- **Observational study**: No randomized treatment assignment
- **Selection bias**: Users who receive longer exposure windows may differ systematically
- **Confounding control**: Regression models control for observable characteristics

### Time Window Logic
1. **Exposure Windows**: Time periods for measuring treatment (NV/DP impressions)
2. **Outcome Windows**: Time periods for measuring effects (order/retention behavior)  
3. **Non-overlapping**: Exposure measurement precedes outcome measurement

### Statistical Considerations
- **Multiple comparisons**: 6 targets × 20+ features require adjustment consideration
- **Sample size**: Large N (746K+) provides strong statistical power
- **Model fit**: Pseudo R² ~0.10 suggests moderate explanatory power
- **Significance testing**: Z-tests used for marginal effect significance

### Data Quality Assumptions
- **Complete tracking**: All user sessions and impressions captured
- **Consistent definitions**: Metrics defined consistently across time periods
- **Representative cohort**: April 2024 represents typical user behavior patterns
- **Impression tracking**: 30-day window for NV impressions post-join
- **Notification tracking**: 60-day window for notifications post-join
- **Order tracking**: Orders tallied through September 2024

---

## Reproducibility Notes

### SQL Table Creation
- **Feature tables**: Created via `sql/feature_generate.sql` and related scripts
- **Performance tables**: Generated through `sql/orders_performance_from_device_features.sql`
- **Session analysis**: Built via `sql/nv_session_nv_coverage.sql`

### Python Analysis Pipeline
- **Data extraction**: Snowflake connection via `utils/snowflake_connection.py`
- **Visualization**: Matplotlib/Seaborn plots in `python/` directory
- **Regression analysis**: Logistic regression via `python/build_logit_models.py`

### Model Validation
- **Cross-validation**: Not explicitly implemented (single-split analysis)
- **Holdout testing**: No separate test set (all data used for final model)
- **Stability**: Single cohort month limits generalizability testing

This methodology enables answering key business questions about NV/DP impact while acknowledging the observational nature of the data and potential for selection bias in exposure assignment.