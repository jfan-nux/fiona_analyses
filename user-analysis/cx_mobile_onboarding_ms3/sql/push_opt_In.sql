create or replace table proddb.fionafan.cx_mobile_onboarding_ms3_exposures as (

      SELECT
    ee.tag,
    ee.result,
    ee.bucket_key,
    REPLACE(LOWER(CASE WHEN bucket_key LIKE 'dx_%' THEN bucket_key ELSE 'dx_'||bucket_key END), '-') AS dd_device_id_filtered,
    ee.segment,
    ee.custom_attributes:userId AS consumer_id,
    MIN(CONVERT_TIMEZONE('UTC','America/Los_Angeles', ee.exposure_time)::DATE) AS day,
    MIN(CONVERT_TIMEZONE('UTC','America/Los_Angeles', ee.exposure_time)) AS exposure_time
  FROM proddb.public.fact_dedup_experiment_exposure ee
  WHERE ee.experiment_name = 'cx_mobile_onboarding_ms3'
    AND ee.experiment_version::INT = 1
    AND ee.segment = 'iOS'
    AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', ee.exposure_time)
          BETWEEN '2025-09-23' AND '2025-10-03'
  GROUP BY 1,2,3,4,5,6
);
WITH exposure AS (
select * from proddb.fionafan.cx_mobile_onboarding_ms3_exposures
),

-- Current (as-of now) push settings; include device_id to link to exposure
push_current AS (
  SELECT DISTINCT
    CAST(consumer_id AS VARCHAR) AS consumer_id,
      CASE
  WHEN LOWER(device_id) LIKE 'dx\\_%' ESCAPE '\\'
    THEN device_id
  ELSE CONCAT('dx_', device_id)
END AS device_id ,

    REPLACE(LOWER(CASE WHEN device_id LIKE 'dx_%' THEN device_id ELSE 'dx_'||device_id END), '-') AS dd_device_id_filtered,
    system_level_status,
    doordash_offers_status,
    order_updates_status AS push_order_updates_status,
    product_updates_news_status,
    recommendations_status,
    reminders_status,
    store_offers_status
  FROM edw.consumer.dimension_consumer_device_push_settings_scd3


  where scd_current_record
),

-- Current (as-of now) SMS settings; join by consumer_id
sms_current AS (
  SELECT DISTINCT
    CAST(consumer_id AS VARCHAR) AS consumer_id,
    order_updates_status AS sms_order_updates_status,
    marketing_status     AS sms_marketing_status
  FROM edw.consumer.dimension_consumer_sms_settings_scd3

  where scd_current_record
),

joined AS (
  SELECT
    e.tag,
    e.dd_device_id_filtered,
    e.bucket_key as DEVICE_ID,
    e.day,
    e.consumer_id,
    pc.system_level_status,
    pc.doordash_offers_status,
    pc.push_order_updates_status,
    pc.product_updates_news_status,
    pc.recommendations_status,
    pc.reminders_status,
    pc.store_offers_status,
    sc.sms_order_updates_status,
    sc.sms_marketing_status
  FROM exposure e
  LEFT JOIN push_current pc
    ON pc.device_id = e.bucket_key
  LEFT JOIN sms_current sc
    ON sc.consumer_id = e.consumer_id
),

agg AS (
  SELECT
    tag,
    COUNT(DISTINCT device_id) AS exposure_devices,
    COUNT(DISTINCT CASE WHEN LOWER(system_level_status) = 'on' THEN device_id END)       AS push_system_level_users,
    COUNT(DISTINCT CASE WHEN LOWER(doordash_offers_status) = 'on' THEN device_id END)    AS push_doordash_offers_users,
    COUNT(DISTINCT CASE WHEN LOWER(push_order_updates_status) = 'on' THEN device_id END) AS push_order_updates_users,
    COUNT(DISTINCT CASE WHEN LOWER(product_updates_news_status) = 'on' THEN device_id END) AS push_product_updates_news_users,
    COUNT(DISTINCT CASE WHEN LOWER(recommendations_status) = 'on' THEN device_id END)    AS push_recommendations_users,
    COUNT(DISTINCT CASE WHEN LOWER(reminders_status) = 'on' THEN device_id END)          AS push_reminders_users,
    COUNT(DISTINCT CASE WHEN LOWER(store_offers_status) = 'on' THEN device_id END)       AS push_store_offers_users,
    COUNT(DISTINCT CASE WHEN LOWER(sms_order_updates_status) = 'on' THEN consumer_id END)            AS sms_order_updates_users,
    COUNT(DISTINCT CASE WHEN LOWER(sms_marketing_status) = 'on' THEN consumer_id END)                AS sms_marketing_users
  FROM joined
  GROUP BY 1
)
SELECT
  tag,
  exposure_devices,
  push_system_level_users,
  push_doordash_offers_users,
  push_order_updates_users,
  push_product_updates_news_users,
  push_recommendations_users,
  push_reminders_users,
  push_store_offers_users,
  sms_order_updates_users,
  sms_marketing_users,
  push_system_level_users / NULLIF(exposure_devices, 0)         AS push_system_level_rate,
  push_doordash_offers_users / NULLIF(exposure_devices, 0)      AS push_doordash_offers_rate,
  push_order_updates_users / NULLIF(exposure_devices, 0)        AS push_order_updates_rate,
  push_product_updates_news_users / NULLIF(exposure_devices, 0) AS push_product_updates_news_rate,
  push_recommendations_users / NULLIF(exposure_devices, 0)      AS push_recommendations_rate,
  push_reminders_users / NULLIF(exposure_devices, 0)            AS push_reminders_rate,
  push_store_offers_users / NULLIF(exposure_devices, 0)         AS push_store_offers_rate,
  sms_order_updates_users / NULLIF(exposure_devices, 0)         AS sms_order_updates_rate,
  sms_marketing_users / NULLIF(exposure_devices, 0)             AS sms_marketing_rate
FROM agg
ORDER BY tag;


create or replace table proddb.fionafan.cx_mobile_onboarding_ms3__iOS_push_optin as (
  WITH exposure AS (
    SELECT
      *
    FROM METRICS_REPO.PUBLIC.cx_mobile_onboarding_ms3__iOS_exposures

  ), push_email_sms_optin /* Fetch metric events */ AS (
    SELECT
      *
    FROM metrics_repo.public.push_email_sms_optin AS push_email_sms_optin
    WHERE
      CAST(push_email_sms_optin.event_ts AS DATETIME) BETWEEN CAST('2025-09-23T18:19:00' AS DATETIME) AND CAST('2025-11-18T18:19:00' AS DATETIME)
      AND (
        event_ts >= CURRENT_DATE - 1
      )
  ), push_email_sms_optin_NonWindow_exp_metrics /* Aggregate events and compute metrics and covariates */ AS (
    SELECT
      TO_CHAR(exposure.bucket_key) AS bucket_key,
      exposure.experiment_group,
      COUNT(DISTINCT push_recommendations_device_id) AS optin_push_recommendations_numerator,
      COUNT(*) AS optin_push_recommendations_numerator_count
    FROM exposure
    LEFT JOIN push_email_sms_optin
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(push_email_sms_optin.device_id)
      AND exposure.first_exposure_time <= DATEADD(SECOND, 10, push_email_sms_optin.event_ts)
    WHERE
      1 = 1
    GROUP BY ALL
  )
  select * from push_email_sms_optin_NonWindow_exp_metrics
  );
select experiment_group, count(1) as count
, sum(optin_push_recommendations_numerator) as optin_push_recommendations_numerator
, sum(optin_push_recommendations_numerator_count) as optin_push_recommendations_numerator_count
from  
proddb.fionafan.cx_mobile_onboarding_ms3__iOS_push_optin 
group by all;


select experiment_group, count(1) as device_count,
count(distinct bucket_key),
count(distinct push_recommendations_device_id)
-- count(distinct push_recommendations_consumer_id)
from METRICS_REPO.PUBLIC.cx_mobile_onboarding_ms3__iOS_exposures a
left join (select * from metrics_repo.public.push_email_sms_optin where event_ts >= CURRENT_DATE - 1) b
on a.bucket_key = b.device_id
where 1=1
group by all;


select experiment_group, count(1) as device_count,
count(distinct a.bucket_key),
count(distinct b.device_id),
count(distinct b.consumer_id)
from METRICS_REPO.PUBLIC.cx_mobile_onboarding_ms3__iOS_exposures a
left join (select consumer_id, device_id FROM edw.consumer.dimension_consumer_device_push_settings_scd3
  where scd_current_record and system_level_status = 'on') b
on a.bucket_key = b.device_id
where 1=1
group by all;

select tag, count(1) as device_count,
count(distinct a.bucket_key),
count(distinct b.device_id),
count(distinct b.consumer_id)
from proddb.fionafan.cx_mobile_onboarding_ms3_exposures a
left join (select consumer_id, device_id FROM edw.consumer.dimension_consumer_device_push_settings_scd3
  where scd_current_record and system_level_status = 'on') b
on a.bucket_key = b.device_id
where 1=1
group by all;

select count(1) from METRICS_REPO.PUBLIC.cx_mobile_onboarding_ms3__iOS_exposures;
select 452222005/155932289;


select count(distinct device_id), count(distinct consumer_id) FROM edw.consumer.dimension_consumer_device_push_settings_scd3

  where scd_current_record and recommendations_status = 'on';


452222005	155932289
452496446	155982492
;

with latest as (
  select max(FETCHED_AT) as FETCHED_AT
  from proddb.fionafan.nux_curie_result_daily
), base as (
  select *
  from proddb.fionafan.nux_curie_result_daily
  where FETCHED_AT = (select FETCHED_AT from latest)
), control as (
  select
    ANALYSIS_NAME,
    ANALYSIS_ID,
    METRIC_NAME,
    DIMENSION_NAME,
    DIMENSION_CUT_NAME,
    METRIC_VALUE,
    UNIT_COUNT,
    SAMPLE_SIZE
  from base
  where lower(VARIANT_NAME) = 'control'
), non_control as (
  select *
  from base
  where lower(VARIANT_NAME) <> 'control'
),
metrics as (
select
  nc.*,
  c.METRIC_VALUE as CONTROL_METRIC_VALUE,
  c.UNIT_COUNT as CONTROL_UNIT_COUNT,
  c.SAMPLE_SIZE as CONTROL_SAMPLE_SIZE
from non_control nc
left join control c
  on c.ANALYSIS_NAME = nc.ANALYSIS_NAME
 and c.ANALYSIS_ID = nc.ANALYSIS_ID
 and c.METRIC_NAME = nc.METRIC_NAME
 and c.DIMENSION_NAME = nc.DIMENSION_NAME
 and c.DIMENSION_CUT_NAME = nc.DIMENSION_CUT_NAME
)
SELECT
    *,
    CASE
        -- PRIMARY metrics
        WHEN metric_name IN (
            'cng_order_rate_nc',
            'consumer_order_frequency_l_28_d',
            'consumers_mau',
            'dashpass_signup',
            'dsmp_gov',
            'dsmp_order_frequency_7d',
            'dsmp_order_rate',
            'dsmp_order_rate_14d',
            'dsmp_order_rate_7d',
            'gov_per_order_curie',
            'nux_onboarding_page_notification_click',
            'nv_mau',
            'order_frequency_per_entity_7d',
            'order_rate_per_entity',
            'order_rate_per_entity_7d',
            'variable_profit_per_order'
        ) THEN 'primary'

        -- GUARDRAIL metrics
        WHEN metric_name IN (
            'ads_promotion_promotion_cx_discount',
            'ads_revenue',
            'consumer_mto',
            'core_quality_aotw',
            'core_quality_asap',
            'core_quality_botw',
            'core_quality_cancellation',
            'core_quality_late20',
            'core_quality_otw',
            'cx_app_quality_action_load_latency_android',
            'cx_app_quality_action_load_latency_ios',
            'cx_app_quality_action_load_latency_web',
            'cx_app_quality_crash_android',
            'cx_app_quality_crash_ios',
            'cx_app_quality_crash_web',
            'cx_app_quality_hitch_android',
            'cx_app_quality_hitch_ios',
            'cx_app_quality_inp_web',
            'cx_app_quality_page_action_error_android',
            'cx_app_quality_page_action_error_ios',
            'cx_app_quality_page_action_error_web',
            'cx_app_quality_page_load_error_android',
            'cx_app_quality_page_load_error_ios',
            'cx_app_quality_page_load_error_web',
            'cx_app_quality_page_load_latency_android',
            'cx_app_quality_page_load_latency_ios',
            'cx_app_quality_page_load_latency_web',
            'cx_app_quality_single_metric_ios',
            'cx_app_quality_tbt_web',
            'nux_onboarding_page_end_page_view',
            'nux_onboarding_page_preference_view',
            'ox_subtotal_combined'
        ) THEN 'guardrail'

        -- Everything else is secondary
        ELSE 'secondary'
    END AS metric_type
from metrics
;

