-- Create table with all notification data plus campaign/message tagging
CREATE OR REPLACE TABLE proddb.fionafan.all_user_notifications_with_braze_with_tag AS


WITH source AS (
  SELECT
    *
  FROM proddb.fionafan.all_user_notifications_with_braze
  WHERE 1=1
    AND notification_channel = 'PUSH'
    -- AND notification_source = 'Braze'
    AND notification_message_type_overall != 'TRANSACTIONAL'
    AND notification_message_type != 'TRANSACTIONAL'
    AND COALESCE(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
    AND is_valid_send = 1
),

flagged AS (
  SELECT
    *,
    
    -- cart tokens (convenience, alcohol, 3p, groceries, grocery)
    CASE WHEN lower(master_campaign_name) LIKE '%convenience%' THEN 1
         WHEN lower(master_campaign_name) LIKE '%alcohol%' THEN 1
         WHEN lower(master_campaign_name) LIKE '%3p%' THEN 1
         WHEN lower(master_campaign_name) LIKE '%groceries%' THEN 1
         WHEN lower(master_campaign_name) LIKE '%grocery%' THEN 1
         WHEN lower(master_campaign_name) LIKE '%nv%' THEN 1 
         ELSE 0 END AS is_nv,

    -- fmx family: fmu / fmx / fm / adpt
    CASE WHEN lower(master_campaign_name) LIKE '%fmu%' THEN 1
         WHEN lower(master_campaign_name) LIKE '%fmx%' THEN 1
         WHEN lower(master_campaign_name) LIKE '%fm%'  THEN 1
         WHEN lower(master_campaign_name) LIKE '%adpt%' THEN 1
         ELSE 0 END AS is_fmx,

    CASE WHEN lower(master_campaign_name) LIKE '%new40off%' THEN 1 ELSE 0 END AS is_npws,
    CASE WHEN lower(master_campaign_name) LIKE '%challenge%' THEN 1 ELSE 0 END AS is_challenge,

    -- dashpass: contains 'dashpass' OR contains 'dp' and NOT 'adpt'
    CASE WHEN lower(master_campaign_name) LIKE '%dashpass%' THEN 1
         WHEN lower(master_campaign_name) LIKE '%dp%' AND lower(master_campaign_name) NOT LIKE '%adpt%' THEN 1
         ELSE 0 END AS is_dashpass,

    CASE WHEN lower(master_campaign_name) LIKE '%abandon%' THEN 1 ELSE 0 END AS is_abandon_campaign,

    CASE WHEN lower(master_campaign_name) LIKE '%post_order%' THEN 1
         WHEN lower(master_campaign_name) LIKE '%postcheckout%' THEN 1
         ELSE 0 END AS is_post_order,

    -- fixed typo: reorder
    CASE WHEN lower(master_campaign_name) LIKE '%reorder%' THEN 1 ELSE 0 END AS is_reorder,

    CASE WHEN lower(master_campaign_name) LIKE '%gift%' THEN 1 ELSE 0 END AS is_gift_card_campaign,

    -- message-type based flags
    CASE WHEN lower(notification_message_type) LIKE '%recommendation%' THEN 1 ELSE 0 END AS is_recommendation,
    CASE WHEN lower(notification_message_type) LIKE '%doordash_offer%' THEN 1 ELSE 0 END AS is_doordash_offer,
    CASE WHEN lower(notification_message_type) LIKE '%reminder%' THEN 1 ELSE 0 END AS is_reminder,
    CASE WHEN lower(notification_message_type) LIKE '%store_offer%' THEN 1 ELSE 0 END AS is_store_offer,

    -- notification source flags
    CASE WHEN notification_source = 'Braze' THEN 1 ELSE 0 END AS is_braze,
    CASE WHEN notification_source = 'FPN Postal Service' THEN 1 ELSE 0 END AS is_fpn

  FROM source
),

with_is_new AS (
  SELECT
    *,
    -- is_new if contains 'new' OR is_npws OR is_fmx
    CASE WHEN lower(master_campaign_name) LIKE '%new%' THEN 1
         WHEN is_npws = 1 THEN 1
         WHEN is_fmx = 1 THEN 1
         ELSE 0 END AS is_new
  FROM flagged
)

SELECT * FROM with_is_new;



-- Distribution of notification tags by cohort type and days since onboarding
-- Shows percentage of messages with each tag across the lifecycle
create or replace table proddb.fionafan.all_user_notifications_cohort_by_day_sent_pct as (
WITH base AS (
  SELECT
    cohort_type,
    days_since_onboarding,
    deduped_message_id,
    opened_at,
    unsubscribed_at,
    uninstalled_at,
    is_nv,
    is_fmx,
    is_npws,
    is_challenge,
    is_dashpass,
    is_abandon_campaign,
    is_post_order,
    is_reorder,
    is_gift_card_campaign,
    is_recommendation,
    is_doordash_offer,
    is_reminder,
    is_store_offer,
    is_new,
    is_braze,
    is_fpn
  FROM proddb.fionafan.all_user_notifications_with_braze_with_tag
  WHERE days_since_onboarding IS NOT NULL
),

metrics AS (
  SELECT 
    cohort_type,
    days_since_onboarding,
    'is_nv' as pivot_by,
    COUNT(DISTINCT CASE WHEN is_nv = 1 THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as pct_messages_with_tag,
    COUNT(DISTINCT CASE WHEN is_nv = 1 THEN deduped_message_id END) as messages_with_tag,
    COUNT(DISTINCT deduped_message_id) as total_messages,
    COUNT(DISTINCT CASE WHEN is_nv = 1 AND opened_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_nv = 1 THEN deduped_message_id END), 0) as open_rate,
    COUNT(DISTINCT CASE WHEN is_nv = 1 AND unsubscribed_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_nv = 1 THEN deduped_message_id END), 0) as unsub_rate,
    COUNT(DISTINCT CASE WHEN is_nv = 1 AND uninstalled_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_nv = 1 THEN deduped_message_id END), 0) as uninstall_rate
  FROM base
  GROUP BY cohort_type, days_since_onboarding

  UNION ALL

  SELECT 
    cohort_type,
    days_since_onboarding,
    'is_fmx' as pivot_by,
    COUNT(DISTINCT CASE WHEN is_fmx = 1 THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as pct_messages_with_tag,
    COUNT(DISTINCT CASE WHEN is_fmx = 1 THEN deduped_message_id END) as messages_with_tag,
    COUNT(DISTINCT deduped_message_id) as total_messages,
    COUNT(DISTINCT CASE WHEN is_fmx = 1 AND opened_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_fmx = 1 THEN deduped_message_id END), 0) as open_rate,
    COUNT(DISTINCT CASE WHEN is_fmx = 1 AND unsubscribed_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_fmx = 1 THEN deduped_message_id END), 0) as unsub_rate,
    COUNT(DISTINCT CASE WHEN is_fmx = 1 AND uninstalled_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_fmx = 1 THEN deduped_message_id END), 0) as uninstall_rate
  FROM base
  GROUP BY cohort_type, days_since_onboarding

  UNION ALL

  SELECT 
    cohort_type,
    days_since_onboarding,
    'is_npws' as pivot_by,
    COUNT(DISTINCT CASE WHEN is_npws = 1 THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as pct_messages_with_tag,
    COUNT(DISTINCT CASE WHEN is_npws = 1 THEN deduped_message_id END) as messages_with_tag,
    COUNT(DISTINCT deduped_message_id) as total_messages,
    COUNT(DISTINCT CASE WHEN is_npws = 1 AND opened_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_npws = 1 THEN deduped_message_id END), 0) as open_rate,
    COUNT(DISTINCT CASE WHEN is_npws = 1 AND unsubscribed_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_npws = 1 THEN deduped_message_id END), 0) as unsub_rate,
    COUNT(DISTINCT CASE WHEN is_npws = 1 AND uninstalled_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_npws = 1 THEN deduped_message_id END), 0) as uninstall_rate
  FROM base
  GROUP BY cohort_type, days_since_onboarding

  UNION ALL

  SELECT 
    cohort_type,
    days_since_onboarding,
    'is_challenge' as pivot_by,
    COUNT(DISTINCT CASE WHEN is_challenge = 1 THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as pct_messages_with_tag,
    COUNT(DISTINCT CASE WHEN is_challenge = 1 THEN deduped_message_id END) as messages_with_tag,
    COUNT(DISTINCT deduped_message_id) as total_messages,
    COUNT(DISTINCT CASE WHEN is_challenge = 1 AND opened_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_challenge = 1 THEN deduped_message_id END), 0) as open_rate,
    COUNT(DISTINCT CASE WHEN is_challenge = 1 AND unsubscribed_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_challenge = 1 THEN deduped_message_id END), 0) as unsub_rate,
    COUNT(DISTINCT CASE WHEN is_challenge = 1 AND uninstalled_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_challenge = 1 THEN deduped_message_id END), 0) as uninstall_rate
  FROM base
  GROUP BY cohort_type, days_since_onboarding

  UNION ALL

  SELECT 
    cohort_type,
    days_since_onboarding,
    'is_dashpass' as pivot_by,
    COUNT(DISTINCT CASE WHEN is_dashpass = 1 THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as pct_messages_with_tag,
    COUNT(DISTINCT CASE WHEN is_dashpass = 1 THEN deduped_message_id END) as messages_with_tag,
    COUNT(DISTINCT deduped_message_id) as total_messages,
    COUNT(DISTINCT CASE WHEN is_dashpass = 1 AND opened_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_dashpass = 1 THEN deduped_message_id END), 0) as open_rate,
    COUNT(DISTINCT CASE WHEN is_dashpass = 1 AND unsubscribed_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_dashpass = 1 THEN deduped_message_id END), 0) as unsub_rate,
    COUNT(DISTINCT CASE WHEN is_dashpass = 1 AND uninstalled_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_dashpass = 1 THEN deduped_message_id END), 0) as uninstall_rate
  FROM base
  GROUP BY cohort_type, days_since_onboarding

  UNION ALL

  SELECT 
    cohort_type,
    days_since_onboarding,
    'is_abandon_campaign' as pivot_by,
    COUNT(DISTINCT CASE WHEN is_abandon_campaign = 1 THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as pct_messages_with_tag,
    COUNT(DISTINCT CASE WHEN is_abandon_campaign = 1 THEN deduped_message_id END) as messages_with_tag,
    COUNT(DISTINCT deduped_message_id) as total_messages,
    COUNT(DISTINCT CASE WHEN is_abandon_campaign = 1 AND opened_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_abandon_campaign = 1 THEN deduped_message_id END), 0) as open_rate,
    COUNT(DISTINCT CASE WHEN is_abandon_campaign = 1 AND unsubscribed_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_abandon_campaign = 1 THEN deduped_message_id END), 0) as unsub_rate,
    COUNT(DISTINCT CASE WHEN is_abandon_campaign = 1 AND uninstalled_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_abandon_campaign = 1 THEN deduped_message_id END), 0) as uninstall_rate
  FROM base
  GROUP BY cohort_type, days_since_onboarding

  UNION ALL

  SELECT 
    cohort_type,
    days_since_onboarding,
    'is_post_order' as pivot_by,
    COUNT(DISTINCT CASE WHEN is_post_order = 1 THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as pct_messages_with_tag,
    COUNT(DISTINCT CASE WHEN is_post_order = 1 THEN deduped_message_id END) as messages_with_tag,
    COUNT(DISTINCT deduped_message_id) as total_messages,
    COUNT(DISTINCT CASE WHEN is_post_order = 1 AND opened_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_post_order = 1 THEN deduped_message_id END), 0) as open_rate,
    COUNT(DISTINCT CASE WHEN is_post_order = 1 AND unsubscribed_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_post_order = 1 THEN deduped_message_id END), 0) as unsub_rate,
    COUNT(DISTINCT CASE WHEN is_post_order = 1 AND uninstalled_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_post_order = 1 THEN deduped_message_id END), 0) as uninstall_rate
  FROM base
  GROUP BY cohort_type, days_since_onboarding

  UNION ALL

  SELECT 
    cohort_type,
    days_since_onboarding,
    'is_reorder' as pivot_by,
    COUNT(DISTINCT CASE WHEN is_reorder = 1 THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as pct_messages_with_tag,
    COUNT(DISTINCT CASE WHEN is_reorder = 1 THEN deduped_message_id END) as messages_with_tag,
    COUNT(DISTINCT deduped_message_id) as total_messages,
    COUNT(DISTINCT CASE WHEN is_reorder = 1 AND opened_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_reorder = 1 THEN deduped_message_id END), 0) as open_rate,
    COUNT(DISTINCT CASE WHEN is_reorder = 1 AND unsubscribed_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_reorder = 1 THEN deduped_message_id END), 0) as unsub_rate,
    COUNT(DISTINCT CASE WHEN is_reorder = 1 AND uninstalled_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_reorder = 1 THEN deduped_message_id END), 0) as uninstall_rate
  FROM base
  GROUP BY cohort_type, days_since_onboarding

  UNION ALL

  SELECT 
    cohort_type,
    days_since_onboarding,
    'is_gift_card_campaign' as pivot_by,
    COUNT(DISTINCT CASE WHEN is_gift_card_campaign = 1 THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as pct_messages_with_tag,
    COUNT(DISTINCT CASE WHEN is_gift_card_campaign = 1 THEN deduped_message_id END) as messages_with_tag,
    COUNT(DISTINCT deduped_message_id) as total_messages,
    COUNT(DISTINCT CASE WHEN is_gift_card_campaign = 1 AND opened_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_gift_card_campaign = 1 THEN deduped_message_id END), 0) as open_rate,
    COUNT(DISTINCT CASE WHEN is_gift_card_campaign = 1 AND unsubscribed_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_gift_card_campaign = 1 THEN deduped_message_id END), 0) as unsub_rate,
    COUNT(DISTINCT CASE WHEN is_gift_card_campaign = 1 AND uninstalled_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_gift_card_campaign = 1 THEN deduped_message_id END), 0) as uninstall_rate
  FROM base
  GROUP BY cohort_type, days_since_onboarding

  UNION ALL

  SELECT 
    cohort_type,
    days_since_onboarding,
    'is_recommendation' as pivot_by,
    COUNT(DISTINCT CASE WHEN is_recommendation = 1 THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as pct_messages_with_tag,
    COUNT(DISTINCT CASE WHEN is_recommendation = 1 THEN deduped_message_id END) as messages_with_tag,
    COUNT(DISTINCT deduped_message_id) as total_messages,
    COUNT(DISTINCT CASE WHEN is_recommendation = 1 AND opened_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_recommendation = 1 THEN deduped_message_id END), 0) as open_rate,
    COUNT(DISTINCT CASE WHEN is_recommendation = 1 AND unsubscribed_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_recommendation = 1 THEN deduped_message_id END), 0) as unsub_rate,
    COUNT(DISTINCT CASE WHEN is_recommendation = 1 AND uninstalled_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_recommendation = 1 THEN deduped_message_id END), 0) as uninstall_rate
  FROM base
  GROUP BY cohort_type, days_since_onboarding

  UNION ALL

  SELECT 
    cohort_type,
    days_since_onboarding,
    'is_doordash_offer' as pivot_by,
    COUNT(DISTINCT CASE WHEN is_doordash_offer = 1 THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as pct_messages_with_tag,
    COUNT(DISTINCT CASE WHEN is_doordash_offer = 1 THEN deduped_message_id END) as messages_with_tag,
    COUNT(DISTINCT deduped_message_id) as total_messages,
    COUNT(DISTINCT CASE WHEN is_doordash_offer = 1 AND opened_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_doordash_offer = 1 THEN deduped_message_id END), 0) as open_rate,
    COUNT(DISTINCT CASE WHEN is_doordash_offer = 1 AND unsubscribed_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_doordash_offer = 1 THEN deduped_message_id END), 0) as unsub_rate,
    COUNT(DISTINCT CASE WHEN is_doordash_offer = 1 AND uninstalled_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_doordash_offer = 1 THEN deduped_message_id END), 0) as uninstall_rate
  FROM base
  GROUP BY cohort_type, days_since_onboarding

  UNION ALL

  SELECT 
    cohort_type,
    days_since_onboarding,
    'is_reminder' as pivot_by,
    COUNT(DISTINCT CASE WHEN is_reminder = 1 THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as pct_messages_with_tag,
    COUNT(DISTINCT CASE WHEN is_reminder = 1 THEN deduped_message_id END) as messages_with_tag,
    COUNT(DISTINCT deduped_message_id) as total_messages,
    COUNT(DISTINCT CASE WHEN is_reminder = 1 AND opened_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_reminder = 1 THEN deduped_message_id END), 0) as open_rate,
    COUNT(DISTINCT CASE WHEN is_reminder = 1 AND unsubscribed_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_reminder = 1 THEN deduped_message_id END), 0) as unsub_rate,
    COUNT(DISTINCT CASE WHEN is_reminder = 1 AND uninstalled_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_reminder = 1 THEN deduped_message_id END), 0) as uninstall_rate
  FROM base
  GROUP BY cohort_type, days_since_onboarding

  UNION ALL

  SELECT 
    cohort_type,
    days_since_onboarding,
    'is_store_offer' as pivot_by,
    COUNT(DISTINCT CASE WHEN is_store_offer = 1 THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as pct_messages_with_tag,
    COUNT(DISTINCT CASE WHEN is_store_offer = 1 THEN deduped_message_id END) as messages_with_tag,
    COUNT(DISTINCT deduped_message_id) as total_messages,
    COUNT(DISTINCT CASE WHEN is_store_offer = 1 AND opened_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_store_offer = 1 THEN deduped_message_id END), 0) as open_rate,
    COUNT(DISTINCT CASE WHEN is_store_offer = 1 AND unsubscribed_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_store_offer = 1 THEN deduped_message_id END), 0) as unsub_rate,
    COUNT(DISTINCT CASE WHEN is_store_offer = 1 AND uninstalled_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_store_offer = 1 THEN deduped_message_id END), 0) as uninstall_rate
  FROM base
  GROUP BY cohort_type, days_since_onboarding

  UNION ALL

  SELECT 
    cohort_type,
    days_since_onboarding,
    'is_new' as pivot_by,
    COUNT(DISTINCT CASE WHEN is_new = 1 THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as pct_messages_with_tag,
    COUNT(DISTINCT CASE WHEN is_new = 1 THEN deduped_message_id END) as messages_with_tag,
    COUNT(DISTINCT deduped_message_id) as total_messages,
    COUNT(DISTINCT CASE WHEN is_new = 1 AND opened_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_new = 1 THEN deduped_message_id END), 0) as open_rate,
    COUNT(DISTINCT CASE WHEN is_new = 1 AND unsubscribed_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_new = 1 THEN deduped_message_id END), 0) as unsub_rate,
    COUNT(DISTINCT CASE WHEN is_new = 1 AND uninstalled_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_new = 1 THEN deduped_message_id END), 0) as uninstall_rate
  FROM base
  GROUP BY cohort_type, days_since_onboarding

  UNION ALL

  SELECT 
    cohort_type,
    days_since_onboarding,
    'is_braze' as pivot_by,
    COUNT(DISTINCT CASE WHEN is_braze = 1 THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as pct_messages_with_tag,
    COUNT(DISTINCT CASE WHEN is_braze = 1 THEN deduped_message_id END) as messages_with_tag,
    COUNT(DISTINCT deduped_message_id) as total_messages,
    COUNT(DISTINCT CASE WHEN is_braze = 1 AND opened_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_braze = 1 THEN deduped_message_id END), 0) as open_rate,
    COUNT(DISTINCT CASE WHEN is_braze = 1 AND unsubscribed_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_braze = 1 THEN deduped_message_id END), 0) as unsub_rate,
    COUNT(DISTINCT CASE WHEN is_braze = 1 AND uninstalled_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_braze = 1 THEN deduped_message_id END), 0) as uninstall_rate
  FROM base
  GROUP BY cohort_type, days_since_onboarding

  UNION ALL

  SELECT 
    cohort_type,
    days_since_onboarding,
    'is_fpn' as pivot_by,
    COUNT(DISTINCT CASE WHEN is_fpn = 1 THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as pct_messages_with_tag,
    COUNT(DISTINCT CASE WHEN is_fpn = 1 THEN deduped_message_id END) as messages_with_tag,
    COUNT(DISTINCT deduped_message_id) as total_messages,
    COUNT(DISTINCT CASE WHEN is_fpn = 1 AND opened_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_fpn = 1 THEN deduped_message_id END), 0) as open_rate,
    COUNT(DISTINCT CASE WHEN is_fpn = 1 AND unsubscribed_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_fpn = 1 THEN deduped_message_id END), 0) as unsub_rate,
    COUNT(DISTINCT CASE WHEN is_fpn = 1 AND uninstalled_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT CASE WHEN is_fpn = 1 THEN deduped_message_id END), 0) as uninstall_rate
  FROM base
  GROUP BY cohort_type, days_since_onboarding

  UNION ALL

  -- Overall metrics (not filtered by any tag)
  SELECT 
    cohort_type,
    days_since_onboarding,
    'overall' as pivot_by,
    1.0 as pct_messages_with_tag,
    COUNT(DISTINCT deduped_message_id) as messages_with_tag,
    COUNT(DISTINCT deduped_message_id) as total_messages,
    COUNT(DISTINCT CASE WHEN opened_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as open_rate,
    COUNT(DISTINCT CASE WHEN unsubscribed_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as unsub_rate,
    COUNT(DISTINCT CASE WHEN uninstalled_at IS NOT NULL THEN deduped_message_id END)::FLOAT / 
      NULLIF(COUNT(DISTINCT deduped_message_id), 0) as uninstall_rate
  FROM base
  GROUP BY cohort_type, days_since_onboarding
)

-- Final SELECT with threshold comparisons
SELECT 
  cohort_type,
  days_since_onboarding,
  pivot_by,
  pct_messages_with_tag,
  messages_with_tag,
  total_messages,
  open_rate,
  unsub_rate,
  uninstall_rate,
  -- Threshold flags based on benchmark cohorts (Active, New, Non-Purchaser/post_onboarding)
  CASE 
    WHEN cohort_type = 'active' AND unsub_rate > 0.0002 THEN 1
    WHEN cohort_type = 'new' AND unsub_rate > 0.0008 THEN 1
    WHEN cohort_type = 'post_onboarding' AND unsub_rate > 0.0002 THEN 1
    ELSE 0 
  END as is_high_unsub,
  CASE 
    WHEN cohort_type = 'active' AND uninstall_rate > 0.0025 THEN 1
    WHEN cohort_type = 'new' AND uninstall_rate > 0.0060 THEN 1
    WHEN cohort_type = 'post_onboarding' AND uninstall_rate > 0.0022 THEN 1
    ELSE 0 
  END as is_high_uninstall,
  CASE 
    WHEN cohort_type = 'active' AND open_rate < 0.0162 THEN 1
    WHEN cohort_type = 'new' AND open_rate < 0.0227 THEN 1
    WHEN cohort_type = 'post_onboarding' AND open_rate < 0.0057 THEN 1ß
    ELSE 0 
  END as is_low_open
FROM metrics
ORDER BY cohort_type, days_since_onboarding, pivot_by

);

select * from proddb.fionafan.all_user_notifications_cohort_by_day_sent_pct order by 1,2,3;
-- grant select on proddb.fionafan.all_user_notifications_cohort_by_day_sent_pct to role public;