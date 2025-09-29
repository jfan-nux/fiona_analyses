select count(distinct consumer_id), count(distinct dd_device_id)
FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice
  WHERE iguazu_timestamp::DATE BETWEEN '2025-09-23'::DATE AND '2025-09-24'::DATE
    AND (
    --   lower(onboarding_type) = 'new_user'
      (lower(dd_platform) = 'android' AND lower(onboarding_type) = 'resurrected_user')
    );
-- Start Page View (ios resurrected welcomeback)
  SELECT count(distinct consumer_id), count(distinct dd_device_id)
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp::DATE BETWEEN '2025-09-23'::DATE AND '2025-09-24'::DATE
    AND lower(onboarding_type) = 'resurrected_user'
    AND lower(page) = 'welcomeback'
    AND lower(dd_platform) = 'ios'    
    
    ;

    select 22338+40162;

    select 22341+40160;




    proddb.static.action_dimension
