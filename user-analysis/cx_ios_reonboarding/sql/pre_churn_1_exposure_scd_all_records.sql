-- Step 1: Get all exposure records with ALL SCD history records
-- This keeps every SCD record (all changes over time) for each exposed user
-- Using existing exposure table: proddb.fionafan.cx_ios_reonboarding_experiment_exposures

CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_exposure_scd_all_records AS (
SELECT 
    e.tag,
    e.result,
    -- e.dd_device_id_filtered,
    e.exposure_day,
    e.exposure_time,
    e.consumer_id,
    scd.consumer_id AS scd_consumer_id,
    scd.scd_current_record,
    scd.scd_start_date,
    scd.scd_end_date,
    scd.signup_date,
    scd.first_order_date,
    scd.last_order_date,
    scd.prev_last_order_date,
    scd.lifestage,
    scd.lifestage_bucket,
    scd.prev_lifestage,
    scd.prev_lifestage_bucket,
    scd.experience,
    scd.business_vertical_line,
    scd.country_id,
    scd.region_name,
    scd.submarket_id,
    scd.updated_at,
    ROW_NUMBER() OVER (PARTITION BY e.consumer_id ORDER BY scd.scd_start_date DESC) AS scd_rank
FROM (select distinct consumer_id, min(exposure_time) as exposure_time
, min(day) as EXPOSURE_DAY
,max(tag) as tag
,max(result) as result
from proddb.fionafan.cx_ios_reonboarding_experiment_exposures group by all) e
LEFT JOIN edw.growth.consumer_growth_accounting_scd3 scd
    ON e.consumer_id::string = scd.consumer_id::string
);

select distinct lifestage from proddb.fionafan.cx_ios_reonboarding_exposure_scd_all_records;

-- Quick summary of records per user
SELECT 
    'All SCD Records' AS record_type,
    COUNT(DISTINCT consumer_id) AS unique_users,
    COUNT(*) AS total_scd_records,
    COUNT(*) / COUNT(DISTINCT consumer_id) AS avg_records_per_user,
    COUNT(DISTINCT CASE WHEN scd_current_record THEN consumer_id END) AS users_with_current_record,
    COUNT(CASE WHEN scd_current_record THEN 1 END) AS current_records
FROM proddb.fionafan.cx_ios_reonboarding_exposure_scd_all_records;



select 
    lifestage, 
    count(1) as cnt,
    count(1) * 100.0 / sum(count(1)) over() as cnt_pct,
    count(distinct consumer_id) as cnt_consumer_id,
    count(distinct consumer_id) * 100.0 / sum(count(distinct consumer_id)) over() as cnt_consumer_id_pct
from proddb.fionafan.cx_ios_reonboarding_exposure_scd_all_records 
where exposure_day between scd_start_date and scd_end_date 
group by all
order by cnt desc;


-- Create consumer-level lifetime metrics table
CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_consumer_lifetime_metrics AS (

WITH scd_with_actual_days AS (
    SELECT 
        *,
        -- Calculate actual days for each SCD period
        -- For current records (end_date = 9999-12-31), use CURRENT_DATE as the end
        DATEDIFF(day, 
            scd_start_date, 
            CASE WHEN scd_end_date = '9999-12-31' THEN CURRENT_DATE ELSE scd_end_date END
        ) + 1 AS period_days
    FROM proddb.fionafan.cx_ios_reonboarding_exposure_scd_all_records
    WHERE scd_consumer_id IS NOT NULL and scd_end_date<exposure_day
)
SELECT 
    consumer_id,
    tag,
    exposure_day,
    exposure_time,
    -- 2. Number of days as churned (Very Churned, Dormant, Churned)
    SUM(CASE 
        WHEN lifestage IN ('Very Churned', 'Dormant', 'Churned') 
        THEN period_days
        ELSE 0 
    END) AS days_churned,
    
    -- 2a. Breakdown by specific churn status
    MAX(CASE WHEN lifestage = 'Very Churned' THEN 1 ELSE 0 END) AS has_been_very_churned,
    SUM(CASE WHEN lifestage = 'Very Churned' THEN period_days ELSE 0 END) AS days_very_churned,
    MAX(CASE WHEN lifestage = 'Dormant' THEN 1 ELSE 0 END) AS has_been_dormant,
    SUM(CASE WHEN lifestage = 'Dormant' THEN period_days ELSE 0 END) AS days_dormant,
    MAX(CASE WHEN lifestage = 'Churned' THEN 1 ELSE 0 END) AS has_been_churned,
    SUM(CASE WHEN lifestage = 'Churned' THEN period_days ELSE 0 END) AS days_churned_status,
    
    -- 3. Whether has been resurrected
    MAX(CASE WHEN lifestage = 'Resurrected' THEN 1 ELSE 0 END) AS has_been_resurrected,
    SUM(CASE WHEN lifestage = 'Resurrected' THEN period_days ELSE 0 END) AS days_resurrected,
    
    -- 4. Whether was New Cx
    MAX(CASE WHEN lifestage = 'New Cx' THEN 1 ELSE 0 END) AS was_new_cx,
    SUM(CASE WHEN lifestage = 'New Cx' THEN period_days ELSE 0 END) AS days_new_cx,
    
    -- 5. Whether has been Non-Purchaser throughout (only Non-Purchaser status)
    CASE 
        WHEN COUNT(DISTINCT lifestage) = 1 AND MAX(lifestage) = 'Non-Purchaser' THEN 1 
        ELSE 0 
    END AS non_purchaser_only,
    
    -- 6. Whether has been Non-Purchaser
    MAX(CASE WHEN lifestage = 'Non-Purchaser' THEN 1 ELSE 0 END) AS has_been_non_purchaser,
    SUM(CASE WHEN lifestage = 'Non-Purchaser' THEN period_days ELSE 0 END) AS days_non_purchaser,
    
    -- 7. Whether has been Active
    MAX(CASE WHEN lifestage = 'Active' THEN 1 ELSE 0 END) AS has_been_active,
    SUM(CASE WHEN lifestage = 'Active' THEN period_days ELSE 0 END) AS days_active,
    

    -- Tenure: days from signup to exposure
    DATEDIFF(day, MIN(signup_date), MAX(exposure_day)) AS tenure_days,
    
    -- Purchase behavior
    CASE 
        WHEN MAX(first_order_date) IS NOT NULL 
        AND MAX(first_order_date) != '9999-12-31' 
        THEN 1 
        ELSE 0 
    END AS has_purchased,
    DATEDIFF(day, MAX(last_order_date), MAX(exposure_day)) AS order_recency_days,
    
    -- Additional useful metrics
    COUNT(*) AS num_scd_records,
    COUNT(DISTINCT lifestage) AS num_distinct_lifestages,
    MIN(scd_start_date) AS first_scd_date,
    MAX(CASE WHEN scd_end_date != '9999-12-31' THEN scd_end_date ELSE NULL END) AS last_completed_scd_date,
    MIN(signup_date) AS signup_date,
    MAX(first_order_date) AS first_order_date,
    MAX(last_order_date) AS last_order_date
    
FROM scd_with_actual_days
GROUP BY consumer_id, tag, exposure_day, exposure_time
);

-- Summary stats of the consumer-level table
SELECT 
    'Consumer Lifetime Metrics' AS metric_type,
    COUNT(*) AS total_consumers,
    
    -- Average days metrics
    AVG(days_churned) AS avg_days_churned,
    AVG(days_very_churned) AS avg_days_very_churned,
    AVG(days_dormant) AS avg_days_dormant,
    AVG(days_churned_status) AS avg_days_churned_status,
    AVG(days_resurrected) AS avg_days_resurrected,
    AVG(days_new_cx) AS avg_days_new_cx,
    AVG(days_non_purchaser) AS avg_days_non_purchaser,
    AVG(days_active) AS avg_days_active,
    AVG(tenure_days) AS avg_tenure_days,
    AVG(order_recency_days) AS avg_order_recency_days,
    
    -- Count of consumers with specific characteristics
    SUM(has_purchased) AS consumers_have_purchased,
    SUM(has_been_very_churned) AS consumers_been_very_churned,
    SUM(has_been_dormant) AS consumers_been_dormant,
    SUM(has_been_churned) AS consumers_been_churned,
    SUM(has_been_resurrected) AS consumers_resurrected,
    SUM(was_new_cx) AS consumers_were_new_cx,
    SUM(has_been_non_purchaser) AS consumers_been_non_purchaser,
    SUM(non_purchaser_only) AS consumers_non_purchaser_only,
    SUM(has_been_active) AS consumers_been_active,
    
    -- Average number of records/lifestages
    AVG(num_scd_records) AS avg_num_scd_records,
    AVG(num_distinct_lifestages) AS avg_num_distinct_lifestages
FROM proddb.fionafan.cx_ios_reonboarding_consumer_lifetime_metrics;