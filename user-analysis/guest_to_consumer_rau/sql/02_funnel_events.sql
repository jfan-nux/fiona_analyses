/*
Purpose: Capture funnel events from fact_unique_visitors_full_pt
Funnel stages: app visit -> store content -> store page -> add to cart -> checkout -> place order
Creates: proddb.fionafan.guest_vs_onboarding_august_funnel_events
*/

CREATE OR REPLACE TABLE proddb.fionafan.guest_vs_onboarding_august_funnel_events AS
SELECT 
    c.dd_device_id_filtered,
    c.cohort_date,
    c.cohort_type,
    c.platform,
    f.event_date,
    f.local_event_date,
    DATEDIFF(day, c.cohort_date, f.event_date) as days_since_cohort,
    
    -- Funnel events
    MAX(f.unique_visitor) as had_app_visit,
    MAX(f.unique_store_content_page_visitor) as had_store_content_visit,
    MAX(f.unique_store_page_visitor) as had_store_page_visit,
    MAX(f.action_add_item_visitor) as had_add_to_cart,
    MAX(f.unique_order_cart_page_visitor) as had_cart_page_visit,
    MAX(f.unique_checkout_page_visitor) as had_checkout_page_visit,
    MAX(f.action_place_order_visitor) as had_place_order_action,
    MAX(f.unique_purchaser) as had_purchase
    
FROM proddb.fionafan.guest_vs_onboarding_august_cohorts c
LEFT JOIN proddb.public.fact_unique_visitors_full_pt f
    ON c.dd_device_id_filtered = replace(lower(CASE WHEN f.dd_device_id like 'dx_%' then f.dd_device_id
                                                    else 'dx_'||f.dd_device_id end), '-')
    AND f.event_date >= c.cohort_date
    AND f.event_date <= DATEADD(day, 28, c.cohort_date)  -- Look at 28 days post-cohort
    AND LOWER(f.platform) = c.platform
GROUP BY 
    c.dd_device_id_filtered,
    c.cohort_date,
    c.cohort_type,
    c.platform,
    f.event_date,
    f.local_event_date;

-- Verify sample
SELECT COUNT(*) as total_records,
       COUNT(DISTINCT dd_device_id_filtered) as unique_users
FROM proddb.fionafan.guest_vs_onboarding_august_funnel_events;



    select distinct platform from proddb.public.fact_unique_visitors_full_pt where event_date >= current_Date-2;