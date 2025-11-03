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
  AND ((lower(onboarding_type) = 'new_user') ;



  SELECT 
    s.user_id,
    s.dd_device_id,
    s.cohort_type,
    CASE WHEN e.second_session_id IS NOT NULL THEN 1 ELSE 0 END as had_second_session
FROM proddb.fionafan.all_user_sessions_with_events_features_gen s
LEFT JOIN proddb.fionafan.all_user_sessions_enriched e 
    ON s.user_id = e.consumer_id
WHERE s.session_type = 'first_session'
    AND s.cohort_type IS NOT NULL
    AND s.user_id IS NOT NULL;

select cohort_type, count(1) cnt, avg(CASE WHEN second_session_id IS NOT NULL THEN 1 ELSE 0 END) as avg_had_second_session from (
  select s.*, e.second_session_id
    FROM proddb.fionafan.all_user_sessions_with_events_features_gen s
LEFT JOIN proddb.fionafan.all_user_sessions_enriched e 
    ON s.user_id = e.consumer_id
WHERE s.session_type = 'first_session' and session_num = 1
    AND s.cohort_type IS NOT NULL
    AND s.user_id IS NOT NULL) group by all;



  select *
    FROM proddb.fionafan.all_user_sessions_with_events_features_gen s
LEFT JOIN proddb.fionafan.all_user_sessions_enriched e 
    ON s.user_id = e.consumer_id
WHERE s.session_type = 'first_session' and session_num = 1
and user_id = '1261327083';
