-- Daily MAU calculation by experiment group since 10/12
WITH exposure AS (
  SELECT
    bucket_key,
    experiment_group
  FROM METRICS_REPO.PUBLIC.cx_ios_guest_conversion_fix__Users_exposures
), 

date_spine AS (
  -- Generate all dates from 10/12 to current date
  SELECT 
    DATEADD(DAY, SEQ4(), '2025-10-12') AS analysis_date
  FROM TABLE(GENERATOR(ROWCOUNT => 30))
  WHERE DATEADD(DAY, SEQ4(), '2025-10-12') <= CURRENT_DATE
),

daily_mau AS (
  SELECT
    d.analysis_date,
    e.experiment_group,
    e.bucket_key,
    -- Count as MAU if user was active in the 28 days before the analysis date
    CASE 
      WHEN COUNT(DISTINCT CASE 
        WHEN c.dte BETWEEN DATEADD(DAY, -28, d.analysis_date) 
                      AND DATEADD(DAY, -1, d.analysis_date)
        THEN c.active_consumer_volume_curie 
      END) > 0 
      THEN 1 
      ELSE 0 
    END AS is_mau
  FROM date_spine d
  CROSS JOIN exposure e
  LEFT JOIN proddb.public.firefly_consumer_volume_curie_measure c
    ON TO_CHAR(e.bucket_key) = TO_CHAR(c.device_id)
    AND c.dte BETWEEN DATEADD(DAY, -28, d.analysis_date) 
                  AND DATEADD(DAY, -1, d.analysis_date)
  GROUP BY 
    d.analysis_date,
    e.experiment_group,
    e.bucket_key
)

SELECT
  analysis_date,
  experiment_group,
  COUNT(DISTINCT bucket_key) AS total_users,
  SUM(is_mau) AS mau_count,
  SUM(is_mau)::FLOAT / COUNT(DISTINCT bucket_key) AS mau_rate
FROM daily_mau
GROUP BY 
  analysis_date,
  experiment_group
ORDER BY 
  analysis_date,
  experiment_group

