select top 10 * from proddb.public.fact_unique_visitors_full_pt ;
SELECT DISTINCT
  cast(iguazu_timestamp as date) AS day,
  consumer_id,
  DD_DEVICE_ID,
  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID else 'dx_'||DD_DEVICE_ID end), '-') as dd_device_id_filtered,
  dd_platform,
  lower(onboarding_type) as onboarding_type,
  promo_title,
  'start_page' as onboarding_page
FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice
WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM (SELECT current_date -14 as start_dt)) AND (SELECT end_dt FROM (SELECT current_date as end_dt))
  AND ((lower(onboarding_type) = 'new_user') 