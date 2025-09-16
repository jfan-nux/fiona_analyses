now, I want to create a document on the deep dives we have done so far in nv_dp_intro_deepdive. 

The methodology is we look at new customers that onboarded during april, and look at the impact of early nv and dp impression, notif and order on their later order and retention's impact. 

read the data  we constructed here:
select * from proddb.fionafan.nv_dp_new_user_session_order_rates order by 1, 2 desc;
select * from proddb.fionafan.nv_dp_new_user_join_order_rates order by 1, 2 ;
select * from proddb.fionafan.nv_dp_orders_performance_from_device order by cohort_name, month_bucket, order_type, cohort_flag desc;
select * from proddb.fionafan.nv_dp_notif_performance_from_daily order by window_name, order_type, has_nv_notif desc;
select * from proddb.fionafan.dp_new_user_notif_performance order by 1,2;
select * from proddb.fionafan.dp_new_user_orders_performance order by 1,2;

then read from these plots that plots from the tbales above.
## Analysis artifacts: SQL ↔ Python ↔ Plot

| Plot | Python | SQL source (table) | What it shows (short) |
| --- | --- | --- | --- |
| `dp_new_user_rates_by_month_cohort.png` | `python/plot_dp_new_user_orders_performance.py` | `proddb.fionafan.dp_new_user_orders_performance` | Ratios vs baseline by cohort and month: order rate and MAU rate (two subplots). |
| `nv_dp_orders_perf_device_ratios.png` | `python/plot_nv_dp_orders_performance_from_device.py` | `proddb.fionafan.nv_dp_orders_performance_from_device` | 3×2 grid by order_type (rows) and metric (cols): cohort/non order-rate and retention ratios by month. |
| `nv_session_impact_ratios.png` | `python/plot_nv_session_ratios.py` | `proddb.fionafan.nv_dp_new_user_session_order_rates` | NV vs non-NV order-rate ratios across sessions and 1h/4h/24h windows. |
| `nv_join_order_rate_ratios.png` | `python/plot_nv_join_order_rates.py` | `proddb.fionafan.nv_dp_new_user_join_order_rates` | NV vs non-NV order-rate ratios across NV exposure windows over 7–90d post-join. |
| `session_nv_coverage_analysis.png` | `python/plot_session_nv_coverage_analysis.py` | `proddb.fionafan.nv_dp_new_user_session_nv_coverage` | NV coverage by session: stacked users with NV vs non-NV impressions; total active users line. |
| `covariates_targets_heatmap.png` | `python/eda_covariates_targets_heatmap.py` | `proddb.fionafan.nv_dp_features_targets` | Correlation heatmap of selected covariates vs targets (EDA). |
| `coef_heatmap_YYYYMMDD_HHMMSS.png` | `python/visualize_logit_results.py` | `proddb.fionafan.nv_dp_logit_results_0_5` | Logistic regression coefficients by covariate × target (heatmap). |
| `ame_heatmap_YYYYMMDD_HHMMSS.png` | `python/visualize_logit_results.py` | `proddb.fionafan.nv_dp_logit_results_0_5` | Average marginal effects by covariate × target (heatmap). |
| `top_covariates_per_target_coef_YYYYMMDD_HHMMSS.png` | `python/visualize_logit_results.py` | `proddb.fionafan.nv_dp_logit_results_0_5` | Top |coef| covariates per target (small multiples). |

Notes:
- Where present, creation logic lives in `sql/` (e.g., `sql/nv_session_nv_coverage.sql`, `sql/orders_performance_from_device_features.sql`), but plots read the final Snowflake tables above.

also read the regression results in top_covariates_per_target_ame_20250912_225218.png and ame_heatmap_20250912_225218.png and covariates_targets_heatmap.png
The coefficient data is in select * from  proddb.fionafan.nv_dp_logit_results_0_5 order by target, abs(coef) desc;

understand really the logic we used to create the data in the table and coefficients. sometimes the results contradict with each toher, in which case explain the nuances and why. 

you are an expert data scientist trying to investigate the problems in readme, using all the analyses above, create a document on the deep dive, and give solid insights, recommendations and next steps. 