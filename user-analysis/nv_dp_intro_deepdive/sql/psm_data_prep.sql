-- Data preparation for Propensity Score Matching
-- Goal: Find matched controls (dp_first_order=0) for treated units (dp_first_order=1)
-- by controlling for NV and DP related covariates

SELECT 
    device_id,
    consumer_id,
    join_day,
    
    -- Treatment variable
    dp_first_order,
    
    -- Covariates: NV impressions
    had_nv_imp_1h,
    had_nv_imp_4h,
    had_nv_imp_12h,
    had_nv_imp_24h,
    had_nv_imp_7d,
    had_nv_imp_30d,
    had_nv_imp_first_session,
    had_nv_imp_second_session,
    
    -- Covariates: NV orders
    nv_first_order,
    nv_second_order,
    
    -- Covariates: NV notifications
    has_nv_notif_30d,
    has_nv_notif_60d,
    
    -- Covariates: Time to impressions
    minutes_to_first_non_nv_impression,
    minutes_to_first_nv_impression,
    
    -- Covariates: Non-NV impressions
    had_non_nv_imp_1h,
    had_non_nv_imp_4h,
    had_non_nv_imp_12h,
    had_non_nv_imp_24h,
    had_non_nv_imp_7d,
    had_non_nv_imp_30d,
    had_non_nv_imp_first_session,
    had_non_nv_imp_second_session,
    
    -- Covariates: DP notifications
    dp_has_notif_30d,
    dp_has_notif_any_day,
    
    -- Additional target variables for downstream analysis
    target_ordered_24h,
    target_first_order_new_cx,
    target_ordered_month1,
    target_ordered_month2,
    target_ordered_month3,
    target_ordered_month4

FROM proddb.fionafan.nv_dp_features_targets

-- Optional: Add any filters if needed
-- WHERE join_day >= '2024-01-01'

ORDER BY dp_first_order DESC, device_id
;

