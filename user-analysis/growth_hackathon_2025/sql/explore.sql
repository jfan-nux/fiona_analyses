
-- Overlap of specified audience tags with NUX reonboarding eligible set
create or replace table proddb.fionafan.post_onboarding_end_promo_audience_tags as (
WITH tags(tag_name) AS (
  SELECT column1 FROM VALUES
    ('can-js-npws-promo-test-h2-25-trt-40off1'),
    ('AUS-npws-promo-treatment'),
    ('ep_aus_F28D_treatment2'),
    ('can-js-npws-promo-test-h2-25-trt-40off1'),
    ('can-js-npws-promo-v4-0-orders'),
    ('can-js-npws-promo-test-h2-25-trt-40off3'),
    ('can-js-npws-promo-test-h2-25-trt-50off1'),
    ('NZ_NewCx_30'),
    ('NZ_NewCx_50'),
    ('npws_45d_t1'),
    ('npws_30d_t2'),
    ('npws_14d_t3'),
    ('npws_no_siw_t1'),
    ('npws_siw_t2'),
    ('npws-fobt-interim-40off1'),
    ('ep_au_rx_resurrection_automation_90d_treatment'),
    ('ep_au_rx_resurrection_automation_120d_treatment'),
    ('ep_au_rx_resurrection_automation_150d_treatment'),
    ('ep_nz_resurrection_automation_180d'),
    ('ep_nz_resurrection_automation_150d_treatment'),
    ('ep_consumer_very_churned_low_vp_us_v1_t1'),
    ('ep_consumer_very_churned_med_vp_us_v1_t1'),
    ('ep_consumer_super_churned_low_vp_us_v1_t1'),
    ('ep_consumer_super_churned_low_vp_us_v1_t1'),
    ('ep_consumer_churned_low_vp_us_v1_t1'),
    ('ep_consumer_churned_med_vp_us_v1_t1'),
    ('ep_consumer_dormant_late_bloomers_us_v1_t1'),
    ('ep_consumer_dormant_winback_us_v1_t1'),
    ('ep_consumer_dewo_phase2_us_v1_t1'),
    ('ep_consumer_dewo_phase1_retarget_us_v1_t1'),
    ('ep_consumer_ml_churn_prevention_us_v2_p1_active_t1'),
    ('ep_consumer_ml_churn_prevention_us_v2_p1_dormant_t1'),
    ('ep_consumer_ml_churn_prevention_us_v2_p2_active_active_t1'),
    ('ep_consumer_ml_churn_prevention_us_v2_p2_active_dormant_t1'),
    ('ep_consumer_ml_churn_prevention_us_v2_p2_dormant_dormant_t1'),
    ('ep_consumer_enhanced_rxauto_90d_us_v1_t1'),
    ('ep_consumer_enhanced_rxauto_120d_test_us_v1_t2'),
    ('ep_consumer_enhanced_rxauto_150day_test_us_v1_t1'),
    ('ep_consumer_enhanced_rxauto_180day_test_us_v1_t1'),
    ('ep_consumer_churned_btm_pickup_exclude_test_us_v1_t2'),
    ('ep_consumer_churned_latebloomers_auto_ctc_test_us_v1_t1'),
    ('ep_consumer_rx_reachability_auto_us_t1')
),
audience_targets AS (
  SELECT LOWER(tag_name) AS tag_name, CAST(target_id AS STRING) AS target_id
  FROM audience_service.public.cassandra_tags_by_target

  WHERE LOWER(tag_name) IN (SELECT LOWER(tag_name) FROM tags)
)
select * from audience_targets);

SELECT consumer_id -- consumer id, can use to join with creator_id from dimension_deliveries or consumer_id from dimension_users
, v3_sensitivity_cohort -- PSM Cx Level Price Sensitivity tag. Cx are categorized into 10 cohorts: (dp, classic) X (very insensitive, insensitive, middle, sensitive, very sensitive), with each cohort comprising 20% of Cx.
, active_date -- date of PSM snapshot

FROM proddb.ml.cx_sensitivity_v3
;
SELECT * FROM proddb.ml.cx_sensitivity_v3
WHERE active_date = '2025-11-01' -- Use a date here to get PSM tags per Cx on that date
limit 10;

select v3_sensitivity_cohort, count(1) cnt from proddb.ml.cx_sensitivity_v3 WHERE active_date = '2025-11-01'  group by all;

create or replace table proddb.fionafan.reonboading_eligible as (
  SELECT DISTINCT CAST(target_id AS STRING) AS target_id
  FROM audience_service.public.cassandra_tags_by_target
  WHERE LOWER(tag_name) = 'nux_reonboarding_ms1_eligible'
);

-- 80m with price sensitivity
-- 3.2m with genai profile shadow
-- 96K with promo tags

create or replace table proddb.public.reonboading_cx_features as (
select a.target_id::varchar as consumer_id
, b.country_id
, b.daf_p365d_order_cnt
, b.is_paid_active_dp
, b.is_trial_active_dp
, b.is_dashpass_active
, b.v3_sens_ptile_all_cx
, b.v3_sensitivity_cohort, c.profile as genai_cx_profile
, case when d.tag_names is not null then 1 else 0 end as promo_eligible
, d.tag_names as promo_tags
from proddb.fionafan.reonboading_eligible a
inner join proddb.ml.cx_sensitivity_v3 b
on a.target_id = to_varchar(b.consumer_id)
and b.active_date = '2025-11-02'
inner join proddb.ml.genai_cx_profile_shadow c
on a.target_id = to_varchar(c.consumer_id)
left join (select target_id, listagg(tag_name, ',') within group (order by tag_name) as tag_names from proddb.fionafan.post_onboarding_end_promo_audience_tags group by all) d
on a.target_id = d.target_id
)
;
grant all privileges on proddb.fionafan.reonboading_cx_features to public;
select v3_sensitivity_cohort, count(1), avg(v3_sens_ptile_all_cx) from proddb.fionafan.reonboading_cx_features group by all;


create or replace table proddb.public.reonboading_cx_features_all as (
select b.consumer_id::varchar as consumer_id
, b.country_id
, b.daf_p365d_order_cnt
, b.is_paid_active_dp
, b.is_trial_active_dp
, b.is_dashpass_active
, b.v3_sens_ptile_all_cx
, b.v3_sensitivity_cohort, c.profile as genai_cx_profile
, case when d.tag_names is not null then 1 else 0 end as promo_eligible
, d.tag_names as promo_tags
from (select * from proddb.ml.cx_sensitivity_v3 b where b.active_date = '2025-11-03') b
inner join proddb.ml.genai_cx_profile_shadow c
on to_varchar(b.consumer_id) = to_varchar(c.consumer_id)
left join (select target_id, listagg(tag_name, ',') within group (order by tag_name) as tag_names from proddb.fionafan.post_onboarding_end_promo_audience_tags group by all) d
on to_varchar(b.consumer_id) = d.target_id
)
;
create or replace table proddb.public.reonboading_cx_features_nux as (


select *,CASE 
        WHEN consumer_id = '867644899' THEN 'shivani.poddar@doordash.com'
        WHEN consumer_id = '1114297252' THEN 'carissa.sun@doordash.com'
        WHEN consumer_id = '1125900312735189' THEN 'cayla.dorsey@doordash.com'
        WHEN consumer_id = '1890774073' THEN 'connor.haskins@doordash.com'
        WHEN consumer_id = '1125900194789693' THEN 'mai.ngo@doordash.com'
        WHEN consumer_id = '685698687' THEN 'omung.goyal@doordash.com'
        WHEN consumer_id = '1076294071' THEN 'ran.jiang@doordash.com'
        WHEN consumer_id = '847059343' THEN 'saur.vasil@doordash.com'
        WHEN consumer_id = '1917314864' THEN 'scott.doerrfeld@doordash.com'
        WHEN consumer_id = '1125900258685514' THEN 'baolu.shen@doordash.com'
        WHEN consumer_id = '754149862' THEN 'danni.liang@doordash.com'
        WHEN consumer_id = '606759055' THEN 'brian.hale@doordash.com'
        WHEN consumer_id = '1210867622' THEN 'will.rosato@doordash.com'
        WHEN consumer_id = '1062926387' THEN 'sara.nordstrom@doordash.com'
        WHEN consumer_id = '1125900281188900' THEN 'fiona.fan@doordash.com'
    END AS email_consumer_map
from proddb.public.reonboading_cx_features_all
where consumer_id IN (
    '867644899','1114297252','1125900312735189','1890774073',
    '1125900194789693','685698687','1076294071','847059343','1917314864',
    '1125900258685514','754149862','606759055','1210867622','1062926387',
    '1125900281188900'
)
);

select * from proddb.public.reonboading_cx_features_all  where consumer_id = '1125900281188900';

select * from proddb.public.reonboading_cx_features_nux;

