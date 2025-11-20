
-- Create search terms table
CREATE OR REPLACE TABLE proddb.fionafan.search_terms AS
SELECT * FROM (
  SELECT 1 AS ordering, 'fast food' AS search_term, 62621659 AS search_sessions UNION ALL
  SELECT 2, 'pizza', 33150320 UNION ALL
  SELECT 3, 'breakfast', 25417133 UNION ALL
  SELECT 4, 'burgers', 23726661 UNION ALL
  SELECT 5, 'mexican', 23952493 UNION ALL
  SELECT 6, 'chicken', 19382966 UNION ALL
  SELECT 7, 'desserts', 16590060 UNION ALL
  SELECT 8, 'chinese', 18925214 UNION ALL
  SELECT 9, 'healthy', 15912768 UNION ALL
  SELECT 10, 'sandwiches', 12034670 UNION ALL
  SELECT 11, 'comfort food', 10833441 UNION ALL
  SELECT 12, 'coffee', 7316016 UNION ALL
  SELECT 13, 'sushi', 7133486 UNION ALL
  SELECT 14, 'asian', 6253811 UNION ALL
  SELECT 15, 'italian', 5151465 UNION ALL
  SELECT 16, 'seafood', 3985465 UNION ALL
  SELECT 17, 'indian', 3558474 UNION ALL
  SELECT 18, 'steak', 3555529 UNION ALL
  SELECT 19, 'soup', 3535883 UNION ALL
  SELECT 20, 'thai', 3612238 UNION ALL
  SELECT 21, 'barbecue', 1962027 UNION ALL
  SELECT 22, 'salad', 1807113 UNION ALL
  SELECT 23, 'ramen', 2302550 UNION ALL
  SELECT 24, 'vegan', 1349364 UNION ALL
  SELECT 25, 'pho', 1792534 UNION ALL
  SELECT 26, 'japanese', 1760403 UNION ALL
  SELECT 27, 'noodles', 1405550 UNION ALL
  SELECT 28, 'bubble tea', 1093204 UNION ALL
  SELECT 29, 'smoothie', 1121964 UNION ALL
  SELECT 30, 'american', 1233694 UNION ALL
  SELECT 31, 'korean', 1159493 UNION ALL
  SELECT 32, 'greek', 983495 UNION ALL
  SELECT 33, 'southern', 959016 UNION ALL
  SELECT 34, 'halal', 878305 UNION ALL
  SELECT 35, 'poke', 729064 UNION ALL
  SELECT 36, 'bakery', 596908 UNION ALL
  SELECT 37, 'latin american', 506611 UNION ALL
  SELECT 38, 'vietnamese', 505632 UNION ALL
  SELECT 39, 'middle eastern', 412650 UNION ALL
  SELECT 40, 'filipino', 337363 UNION ALL
  SELECT 41, 'african', 371140 UNION ALL
  SELECT 42, 'spanish', 319082 UNION ALL
  SELECT 43, 'brazilian', 185115 UNION ALL
  SELECT 44, 'gastropubs', 199811 UNION ALL
  SELECT 45, 'peruvian', 132549 UNION ALL
  SELECT 46, 'pakistani', 158705 UNION ALL
  SELECT 47, 'german', 145763 UNION ALL
  SELECT 48, 'russian', 170403 UNION ALL
  SELECT 49, 'british', 128134 UNION ALL
  SELECT 50, 'european', 106898 UNION ALL
  SELECT 51, 'belgian', 107452 UNION ALL
  SELECT 52, 'ethiopian', 79604 UNION ALL
  SELECT 53, 'french', 65730 UNION ALL
  SELECT 54, 'poutineries', 70798 UNION ALL
  SELECT 55, 'burmese', 75940 UNION ALL
  SELECT 56, 'snacks', 165920 UNION ALL
  SELECT 57, 'flowers', 75229 UNION ALL
  SELECT 58, 'irish', 44044 UNION ALL
  SELECT 59, 'argentine', 37395 UNION ALL
  SELECT 60, 'tapas', 43624 UNION ALL
  SELECT 61, 'australian', 96568 UNION ALL
  SELECT 62, 'drinks', 212400 UNION ALL
  SELECT 63, 'kosher', 29664 UNION ALL
  SELECT 64, 'convenience', 7621 UNION ALL
  SELECT 65, 'dessert', 505 UNION ALL
  SELECT 66, 'burger', 418 UNION ALL
  SELECT 67, 'sandwich', 341 UNION ALL
  SELECT 68, 'pasta', 135 UNION ALL
  SELECT 69, 'cake', 130 UNION ALL
  SELECT 70, 'donut', 113 UNION ALL
  SELECT 71, 'cookie', 105 UNION ALL
  SELECT 72, 'ice cream', 92 UNION ALL
  SELECT 73, 'milkshake', 74 UNION ALL
  SELECT 74, 'chicken wings', 77 UNION ALL
  SELECT 75, 'taco', 83 UNION ALL
  SELECT 76, 'burrito', 75 UNION ALL
  SELECT 77, 'wine', 8822 UNION ALL
  SELECT 78, 'cheesecake', 46 UNION ALL
  SELECT 79, 'waffle', 47 UNION ALL
  SELECT 80, 'wings', 47 UNION ALL
  SELECT 81, 'bagel', 37 UNION ALL
  SELECT 82, 'acai bowl', 30 UNION ALL
  SELECT 83, 'breakfast burrito', 19 UNION ALL
  SELECT 84, 'shrimp', 19 UNION ALL
  SELECT 85, 'sonic', 34 UNION ALL
  SELECT 86, 'cheesesteak', 33 UNION ALL
  SELECT 87, 'nachos', 27 UNION ALL
  SELECT 88, 'fried chicken', 25 UNION ALL
  SELECT 89, 'food', 26 UNION ALL
  SELECT 90, 'birria', 23 UNION ALL
  SELECT 91, 'soul food', 23 UNION ALL
  SELECT 92, 'tacos', 25 UNION ALL
  SELECT 93, 'enchilada', 22 UNION ALL
  SELECT 94, 'fried rice', 20 UNION ALL
  SELECT 95, 'bbq', 26 UNION ALL
  SELECT 96, 'submarine sandwich', 18 UNION ALL
  SELECT 97, 'pancake', 20 UNION ALL
  SELECT 98, 'club sandwich', 17 UNION ALL
  SELECT 99, 'cupcake', 17 UNION ALL
  SELECT 100, 'dumplings', 14 UNION ALL
  SELECT 101, 'brownie', 15 UNION ALL
  SELECT 102, 'breakfast sandwich', 13 UNION ALL
  SELECT 103, 'donuts', 30 UNION ALL
  SELECT 104, 'olive garden', 63 UNION ALL
  SELECT 105, 'waffle house', 39 UNION ALL
  SELECT 106, 'taco bell', 55 UNION ALL
  SELECT 107, 'chicken noodle soup', 16 UNION ALL
  SELECT 108, 'biryani', 15 UNION ALL
  SELECT 109, 'starbucks', 22 UNION ALL
  SELECT 110, 'blt', 13 UNION ALL
  SELECT 111, 'chilis', 13 UNION ALL
  SELECT 112, 'gyro', 11 UNION ALL
  SELECT 113, 'quesadilla', 17 UNION ALL
  SELECT 114, 'fajita', 15 UNION ALL
  SELECT 115, 'frappe', 11 UNION ALL
  SELECT 116, 'boba', 18 UNION ALL
  SELECT 117, 'fish', 19 UNION ALL
  SELECT 118, 'fries', 9 UNION ALL
  SELECT 119, 'kfc', 18 UNION ALL
  SELECT 120, 'fried fish', 8 UNION ALL
  SELECT 121, 'pad thai', 12 UNION ALL
  SELECT 122, 'fish and chips', 11 UNION ALL
  SELECT 123, 'curry', 10 UNION ALL
  SELECT 124, 'chicken sandwich', 10 UNION ALL
  SELECT 125, 'chicken nuggets', 8 UNION ALL
  SELECT 126, 'mcdonald''s', 35 UNION ALL
  SELECT 127, 'lunch', 8 UNION ALL
  SELECT 128, 'alcohol', 48 UNION ALL
  SELECT 129, 'greek salad', 9 UNION ALL
  SELECT 130, 'panini', 9 UNION ALL
  SELECT 131, 'chicken parmesan', 10 UNION ALL
  SELECT 132, 'pizza hut', 27 UNION ALL
  SELECT 133, 'dairy queen', 12 UNION ALL
  SELECT 134, 'burger king', 18 UNION ALL
  SELECT 135, 'cook out', 8 UNION ALL
  SELECT 136, 'thai curry', 6 UNION ALL
  SELECT 137, 'general tso''s chicken', 7 UNION ALL
  SELECT 138, 'pancakes', 7 UNION ALL
  SELECT 139, 'margarita', 8 UNION ALL
  SELECT 140, 'asian soup', 6 UNION ALL
  SELECT 141, 'orange chicken', 7 UNION ALL
  SELECT 142, 'jack in the box', 6 UNION ALL
  SELECT 143, 'hummus', 8 UNION ALL
  SELECT 144, 'chocolate', 6 UNION ALL
  SELECT 145, 'cheese curds', 5 UNION ALL
  SELECT 146, 'wendys', 22 UNION ALL
  SELECT 147, 'panera', 13 UNION ALL
  SELECT 148, 'brussel sprouts', 4 UNION ALL
  SELECT 149, 'shawarma', 5 UNION ALL
  SELECT 150, 'alfredo', 6 UNION ALL
  SELECT 151, 'udon', 7 UNION ALL
  SELECT 152, 'hash browns', 4 UNION ALL
  SELECT 153, 'macchiato', 6 UNION ALL
  SELECT 154, 'subs', 8 UNION ALL
  SELECT 155, 'cupcakes', 6 UNION ALL
  SELECT 156, 'fruit', 6 UNION ALL
  SELECT 157, 'sub', 7 UNION ALL
  SELECT 158, 'queso', 5 UNION ALL
  SELECT 159, 'thai soup', 8 UNION ALL
  SELECT 160, 'tomato soup', 9 UNION ALL
  SELECT 161, 'korean fried chicken', 5 UNION ALL
  SELECT 162, 'cookies', 9 UNION ALL
  SELECT 163, 'dunkin', 9 UNION ALL
  SELECT 164, 'loaded fries', 4 UNION ALL
  SELECT 165, 'gyros', 3 UNION ALL
  SELECT 166, 'spam', 3 UNION ALL
  SELECT 167, 'cauliflower', 3 UNION ALL
  SELECT 168, 'restaurants', 6 UNION ALL
  SELECT 169, 'deals', 8 UNION ALL
  SELECT 170, 'chick', 4 UNION ALL
  SELECT 171, 'milk tea', 5 UNION ALL
  SELECT 172, 'vegetarian', 2333 UNION ALL
  SELECT 173, 'cafe', 4 UNION ALL
  SELECT 174, 'latte', 6 UNION ALL
  SELECT 175, 'thai fried rice', 3
);

-- Create target cuisines/tags table for analysis
CREATE OR REPLACE TABLE proddb.fionafan.ffs_target_cuisines AS
SELECT * FROM (
  SELECT 'american' AS primary_tag UNION ALL
  SELECT 'mexican' UNION ALL
  SELECT 'convenience_store' UNION ALL
  SELECT 'pizza' UNION ALL
  SELECT 'italian' UNION ALL
  SELECT 'burgers' UNION ALL
  SELECT 'sub' UNION ALL
  SELECT 'chinese_food' UNION ALL
  SELECT 'japanese' UNION ALL
  SELECT 'sandwiches' UNION ALL
  SELECT 'chicken' UNION ALL
  SELECT 'grocery' UNION ALL
  SELECT 'indian' UNION ALL
  SELECT 'coffee_tea' UNION ALL
  SELECT 'fast_food' UNION ALL
  SELECT 'breakfast' UNION ALL
  SELECT 'dessert' UNION ALL
  SELECT 'thai' UNION ALL
  SELECT 'seafood' UNION ALL
  SELECT 'asian' UNION ALL
  SELECT 'sushi' UNION ALL
  SELECT 'healthy' UNION ALL
  SELECT 'smoothies' UNION ALL
  SELECT 'comfort_food'
);

-- Analyze percentage of consumers seeing 0 restaurants by primary tag
-- Similar structure to the ETA analysis but focused on restaurant availability
CREATE OR REPLACE TABLE proddb.fionafan.ffs_cuisine_filter_zero_results_analysis AS (
WITH target_tags AS (
  SELECT primary_tag FROM proddb.fionafan.ffs_target_cuisines
),
consumer_tag_combinations AS (
  -- For each target tag, count stores per consumer that match the tag
  SELECT 
    tc.primary_tag,
    base.consumer_id,
    COUNT(DISTINCT base.store_id) AS store_count
  FROM target_tags tc
  CROSS JOIN (
    SELECT DISTINCT consumer_id, store_id, primary_tag_name
    FROM proddb.fionafan.ffs_static_eta_events_w_store_cuisine
    WHERE primary_tag_name IS NOT NULL
  ) base
  WHERE base.primary_tag_name ILIKE '%' || tc.primary_tag || '%'
  GROUP BY tc.primary_tag, base.consumer_id
),
all_consumers AS (
  -- Get all unique consumers to include those with 0 stores for each tag
  SELECT DISTINCT consumer_id
  FROM proddb.fionafan.ffs_static_eta_events_w_store_cuisine
),
consumer_tag_full AS (
  -- Create full combination of consumers x tags, filling in 0 for missing combinations
  SELECT 
    tc.primary_tag,
    ac.consumer_id,
    COALESCE(ctc.store_count, 0) AS store_count
  FROM target_tags tc
  CROSS JOIN all_consumers ac
  LEFT JOIN consumer_tag_combinations ctc
    ON tc.primary_tag = ctc.primary_tag
    AND ac.consumer_id = ctc.consumer_id
),
tag_summary AS (
  -- Calculate metrics by tag
  SELECT 
    primary_tag,
    COUNT(DISTINCT consumer_id) AS total_consumers,
    COUNT(DISTINCT CASE WHEN store_count = 0 THEN consumer_id END) AS consumers_with_0_stores,
    COUNT(DISTINCT CASE WHEN store_count > 0 THEN consumer_id END) AS consumers_with_stores,
    AVG(store_count) AS avg_stores_per_consumer,
    MEDIAN(store_count) AS median_stores_per_consumer,
    -- Percentage seeing 0 stores
    (COUNT(DISTINCT CASE WHEN store_count = 0 THEN consumer_id END)::FLOAT / 
     NULLIF(COUNT(DISTINCT consumer_id), 0) * 100) AS pct_consumers_seeing_0_stores,
    -- Percentage seeing at least 1 store
    (COUNT(DISTINCT CASE WHEN store_count > 0 THEN consumer_id END)::FLOAT / 
     NULLIF(COUNT(DISTINCT consumer_id), 0) * 100) AS pct_consumers_seeing_stores
  FROM consumer_tag_full
  GROUP BY primary_tag
)
SELECT 
  primary_tag,
  total_consumers,
  consumers_with_0_stores,
  consumers_with_stores,
  pct_consumers_seeing_0_stores,
  pct_consumers_seeing_stores,
  avg_stores_per_consumer,
  median_stores_per_consumer
FROM tag_summary
ORDER BY pct_consumers_seeing_0_stores DESC, total_consumers DESC
);

-- View results with threshold flags (similar to the ETA analysis threshold checks)
SELECT 
  primary_tag,
  total_consumers,
  pct_consumers_seeing_0_stores,
  -- Threshold flags for consumers seeing 0 restaurants
  (pct_consumers_seeing_0_stores >= 50) AS pct_zero_stores_exceed_50,
  (pct_consumers_seeing_0_stores >= 25) AS pct_zero_stores_exceed_25,
  (pct_consumers_seeing_0_stores >= 10) AS pct_zero_stores_exceed_10,
  pct_consumers_seeing_stores,
  avg_max_stores_per_consumer,
  median_max_stores_per_consumer,
  consumers_with_0_stores,
  consumers_with_stores
FROM proddb.fionafan.ffs_cuisine_filter_zero_results_analysis
ORDER BY pct_consumers_seeing_0_stores DESC;


select * from proddb.fionafan.ffs_cuisine_filter_zero_results_analysis;

-- Create extended target cuisines/tags table for comprehensive analysis
CREATE OR REPLACE TABLE proddb.fionafan.ffs_target_cuisines_extended AS
SELECT * FROM (
  SELECT 'american' AS primary_tag UNION ALL
  SELECT 'mexican' UNION ALL
  SELECT 'convenience_store' UNION ALL
  SELECT 'pizza' UNION ALL
  SELECT 'italian' UNION ALL
  SELECT 'burgers' UNION ALL
  SELECT 'sub' UNION ALL
  SELECT 'chinese_food' UNION ALL
  SELECT 'japanese' UNION ALL
  SELECT 'sandwiches' UNION ALL
  SELECT 'chicken_shop' UNION ALL
  SELECT 'grocery' UNION ALL
  SELECT 'beer_and_wine' UNION ALL
  SELECT 'indian' UNION ALL
  SELECT 'coffee_tea' UNION ALL
  SELECT 'fast-food' UNION ALL
  SELECT 'fast_food' UNION ALL
  SELECT 'breakfast' UNION ALL
  SELECT 'chicken_wings' UNION ALL
  SELECT 'dessert_and_fast-food' UNION ALL
  SELECT 'donut_shop' UNION ALL
  SELECT 'thai' UNION ALL
  SELECT 'seafood' UNION ALL
  SELECT 'home_improvement' UNION ALL
  SELECT 'flowers' UNION ALL
  SELECT 'home_goods' UNION ALL
  SELECT 'drinks' UNION ALL
  SELECT 'ice_cream' UNION ALL
  SELECT 'cafe' UNION ALL
  SELECT 'mediterranean' UNION ALL
  SELECT 'vietnamese' UNION ALL
  SELECT 'bakery' UNION ALL
  SELECT 'asian_fusion' UNION ALL
  SELECT 'deli' UNION ALL
  SELECT 'beauty' UNION ALL
  SELECT 'asian' UNION ALL
  SELECT 'korean' UNION ALL
  SELECT 'pickup' UNION ALL
  SELECT 'pet_stores' UNION ALL
  SELECT 'beer' UNION ALL
  SELECT 'takeout' UNION ALL
  SELECT 'sushi' UNION ALL
  SELECT 'middle_eastern' UNION ALL
  SELECT 'snap_/_ebt' UNION ALL
  SELECT 'healthy' UNION ALL
  SELECT 'smoothies' UNION ALL
  SELECT 'greek' UNION ALL
  SELECT 'salads' UNION ALL
  SELECT 'tacos' UNION ALL
  SELECT 'electronics' UNION ALL
  SELECT 'wings' UNION ALL
  SELECT 'barbecue' UNION ALL
  SELECT 'apparel' UNION ALL
  SELECT 'desserts' UNION ALL
  SELECT 'breakfast_sandwiches' UNION ALL
  SELECT 'fried_chicken' UNION ALL
  SELECT '_wine_spirits' UNION ALL
  SELECT 'latin_american' UNION ALL
  SELECT 'liquor' UNION ALL
  SELECT 'arts_and_crafts' UNION ALL
  SELECT 'hot_dogs' UNION ALL
  SELECT 'taiwanese' UNION ALL
  SELECT 'lunch' UNION ALL
  SELECT 'comfort_food' UNION ALL
  SELECT 'halal' UNION ALL
  SELECT 'bars' UNION ALL
  SELECT 'retail' UNION ALL
  SELECT 'donuts' UNION ALL
  SELECT 'steak' UNION ALL
  SELECT 'sandwich' UNION ALL
  SELECT 'caribbean' UNION ALL
  SELECT 'office_supplies' UNION ALL
  SELECT 'chicken_tenders' UNION ALL
  SELECT 'pasta' UNION ALL
  SELECT 'australian' UNION ALL
  SELECT 'burritos' UNION ALL
  SELECT 'halloween' UNION ALL
  SELECT 'french_fries' UNION ALL
  SELECT 'bubble_tea' UNION ALL
  SELECT 'american_new' UNION ALL
  SELECT 'pharmacy' UNION ALL
  SELECT 'appetizers' UNION ALL
  SELECT 'miami_deli' UNION ALL
  SELECT 'pet' UNION ALL
  SELECT 'dinner' UNION ALL
  SELECT 'bagels' UNION ALL
  SELECT 'hawaiian' UNION ALL
  SELECT 'cheesesteaks' UNION ALL
  SELECT 'cajun' UNION ALL
  SELECT 'french' UNION ALL
  SELECT 'soul_food' UNION ALL
  SELECT 'vegetarian' UNION ALL
  SELECT 'snacks' UNION ALL
  SELECT 'jamaican' UNION ALL
  SELECT 'cakes' UNION ALL
  SELECT 'southern' UNION ALL
  SELECT 'tea' UNION ALL
  SELECT 'cookies' UNION ALL
  SELECT 'vegan' UNION ALL
  SELECT 'coffee_shop' UNION ALL
  SELECT 'family_meals' UNION ALL
  SELECT 'melts' UNION ALL
  SELECT 'african' UNION ALL
  SELECT 'turkish' UNION ALL
  SELECT 'noodles' UNION ALL
  SELECT 'sweets' UNION ALL
  SELECT 'soup' UNION ALL
  SELECT 'canadian' UNION ALL
  SELECT 'catering' UNION ALL
  SELECT 'bbq' UNION ALL
  SELECT 'tex-mex' UNION ALL
  SELECT 'brazilian' UNION ALL
  SELECT 'candy' UNION ALL
  SELECT 'peruvian' UNION ALL
  SELECT 'pretzels' UNION ALL
  SELECT 'asian_food' UNION ALL
  SELECT 'cuban' UNION ALL
  SELECT 'milk_shakes' UNION ALL
  SELECT 'caters' UNION ALL
  SELECT 'gift' UNION ALL
  SELECT 'coffee_tea_and_food' UNION ALL
  SELECT 'sushi_bars' UNION ALL
  SELECT 'florist' UNION ALL
  SELECT 'lebanese' UNION ALL
  SELECT 'sporting_goods' UNION ALL
  SELECT 'wraps' UNION ALL
  SELECT 'casual_dining' UNION ALL
  SELECT 'filipino' UNION ALL
  SELECT 'steakhouses' UNION ALL
  SELECT 'poke_bowl' UNION ALL
  SELECT 'baby' UNION ALL
  SELECT 'popcorn' UNION ALL
  SELECT 'kebabs' UNION ALL
  SELECT 'spanish' UNION ALL
  SELECT 'milk_tea' UNION ALL
  SELECT 'colombian' UNION ALL
  SELECT 'liquor_store' UNION ALL
  SELECT 'dominican' UNION ALL
  SELECT 'brunch' UNION ALL
  SELECT 'acai' UNION ALL
  SELECT 'quesadillas' UNION ALL
  SELECT 'puerto_rican' UNION ALL
  SELECT 'ramen' UNION ALL
  SELECT 'boba' UNION ALL
  SELECT 'balloons' UNION ALL
  SELECT 'cafes' UNION ALL
  SELECT 'accessories' UNION ALL
  SELECT 'american_traditional' UNION ALL
  SELECT 'venezuelan' UNION ALL
  SELECT 'fish_chips' UNION ALL
  SELECT 'bowls' UNION ALL
  SELECT 'juice_bars_smoothies' UNION ALL
  SELECT 'gifts' UNION ALL
  SELECT 'chocolates' UNION ALL
  SELECT 'salvadorian' UNION ALL
  SELECT 'makeup' UNION ALL
  SELECT 'frozen_food' UNION ALL
  SELECT 'fruit_tea' UNION ALL
  SELECT 'gluten-free' UNION ALL
  SELECT 'pastries' UNION ALL
  SELECT 'himalayan_nepalese' UNION ALL
  SELECT 'sports_bars' UNION ALL
  SELECT 'chicken' UNION ALL
  SELECT 'frozen_yogurt' UNION ALL
  SELECT 'chocolate' UNION ALL
  SELECT 'burger' UNION ALL
  SELECT 'floral' UNION ALL
  SELECT 'fast_casual' UNION ALL
  SELECT 'fish' UNION ALL
  SELECT 'pubs' UNION ALL
  SELECT 'hemp_thc' UNION ALL
  SELECT 'pakistani' UNION ALL
  SELECT 'pho' UNION ALL
  SELECT 'curry' UNION ALL
  SELECT 'persian_iranian' UNION ALL
  SELECT 'box_lunch' UNION ALL
  SELECT 'plant_based' UNION ALL
  SELECT 'brisket' UNION ALL
  SELECT 'fried_rice' UNION ALL
  SELECT 'delivery' UNION ALL
  SELECT 'mexican_food' UNION ALL
  SELECT 'ethiopian' UNION ALL
  SELECT 'sports_bar' UNION ALL
  SELECT 'candy_stores' UNION ALL
  SELECT 'party' UNION ALL
  SELECT 'gyro' UNION ALL
  SELECT 'craft_beer' UNION ALL
  SELECT 'Storefront Catering' UNION ALL
  SELECT 'modern_european' UNION ALL
  SELECT 'pets' UNION ALL
  SELECT 'middle_east' UNION ALL
  SELECT 'irish' UNION ALL
  SELECT 'dumplings' UNION ALL
  SELECT 'personal_care' UNION ALL
  SELECT 'shawarma' UNION ALL
  SELECT 'malaysian' UNION ALL
  SELECT 'hawaiian_bbq' UNION ALL
  SELECT 'chicken_sandwiches' UNION ALL
  SELECT 'creperies' UNION ALL
  SELECT 'haitian'
);

-- Extended analysis for all tags - percentage of consumers seeing 0 restaurants
CREATE OR REPLACE TABLE proddb.fionafan.ffs_cuisine_filter_zero_results_analysis_extended AS (
WITH target_tags AS (
  SELECT primary_tag FROM proddb.fionafan.ffs_target_cuisines_extended
),
consumer_tag_combinations AS (
  -- For each target tag, count stores per consumer that match the tag
  SELECT 
    tc.primary_tag,
    base.consumer_id,
    COUNT(DISTINCT base.store_id) AS store_count
  FROM target_tags tc
  CROSS JOIN (
    SELECT DISTINCT consumer_id, store_id, primary_tag_name
    FROM proddb.fionafan.ffs_static_eta_events_w_store_cuisine
    WHERE primary_tag_name IS NOT NULL
  ) base
  WHERE base.primary_tag_name ILIKE '%' || tc.primary_tag || '%'
  GROUP BY tc.primary_tag, base.consumer_id
),
all_consumers AS (
  -- Get all unique consumers to include those with 0 stores for each tag
  SELECT DISTINCT consumer_id
  FROM proddb.fionafan.ffs_static_eta_events_w_store_cuisine
),
consumer_tag_full AS (
  -- Create full combination of consumers x tags, filling in 0 for missing combinations
  SELECT 
    tc.primary_tag,
    ac.consumer_id,
    COALESCE(ctc.store_count, 0) AS store_count
  FROM target_tags tc
  CROSS JOIN all_consumers ac
  LEFT JOIN consumer_tag_combinations ctc
    ON tc.primary_tag = ctc.primary_tag
    AND ac.consumer_id = ctc.consumer_id
),
tag_summary AS (
  -- Calculate metrics by tag
  SELECT 
    primary_tag,
    COUNT(DISTINCT consumer_id) AS total_consumers,
    COUNT(DISTINCT CASE WHEN store_count = 0 THEN consumer_id END) AS consumers_with_0_stores,
    COUNT(DISTINCT CASE WHEN store_count > 0 THEN consumer_id END) AS consumers_with_stores,
    AVG(store_count) AS avg_stores_per_consumer,
    MEDIAN(store_count) AS median_stores_per_consumer,
    -- Percentage seeing 0 stores
    (COUNT(DISTINCT CASE WHEN store_count = 0 THEN consumer_id END)::FLOAT / 
     NULLIF(COUNT(DISTINCT consumer_id), 0) * 100) AS pct_consumers_seeing_0_stores,
    -- Percentage seeing at least 1 store
    (COUNT(DISTINCT CASE WHEN store_count > 0 THEN consumer_id END)::FLOAT / 
     NULLIF(COUNT(DISTINCT consumer_id), 0) * 100) AS pct_consumers_seeing_stores
  FROM consumer_tag_full
  GROUP BY primary_tag
)
SELECT 
  primary_tag,
  total_consumers,
  consumers_with_0_stores,
  consumers_with_stores,
  pct_consumers_seeing_0_stores,
  pct_consumers_seeing_stores,
  avg_stores_per_consumer,
  median_stores_per_consumer
FROM tag_summary
ORDER BY pct_consumers_seeing_0_stores DESC, total_consumers DESC
);

-- View extended results
SELECT 
  primary_tag,
  total_consumers,
  pct_consumers_seeing_0_stores,
  pct_consumers_seeing_stores,
  avg_stores_per_consumer,
  median_stores_per_consumer,
  consumers_with_0_stores,
  consumers_with_stores
FROM proddb.fionafan.ffs_cuisine_filter_zero_results_analysis_extended
ORDER BY pct_consumers_seeing_0_stores DESC;

