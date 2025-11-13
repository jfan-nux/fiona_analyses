# Databricks notebook source
# MAGIC %pip install scikit-uplift xgboost matplotlib

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, ArrayType, StringType
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from xgboost.spark import SparkXGBClassifier, SparkXGBRegressor
import numpy as np
import pandas as pd
from typing import Dict, Tuple, List
from sklift.metrics import qini_auc_score
from sklift.viz import plot_qini_curve
import matplotlib.pyplot as plt
import os
from datetime import datetime

# UDF to extract probability of class 1 from probability vector
@F.udf(DoubleType())
def extract_prob_class_1(probability):
    """Extract probability of class 1 from Vector."""
    if probability is not None:
        # Convert to dense vector and get second element (class 1)
        return float(probability[1])
    return 0.0

# COMMAND ----------

# Snowflake connection setup
your_scope = "fionafan-scope"
user = dbutils.secrets.get(scope=your_scope, key="snowflake-user")
password = dbutils.secrets.get(scope=your_scope, key="snowflake-password")
default_schema = "fionafan"

OPTIONS = dict(
    sfurl="doordash.snowflakecomputing.com/",
    sfuser=user,
    sfpassword=password,
    sfaccount="DOORDASH",
    sfdatabase="PRODDB",
    sfrole=default_schema,
    sfwarehouse="TEAM_DATA_ANALYTICS_ETL",
)

# COMMAND ----------

# Define categorical features to be manually encoded to binary
categorical_features_to_encode = [
    "PLATFORM",
    "DEVICE_ID_GENERAL_TRAFFIC_TYPE_REAL_TIME",
    "DEVICE_ID_GENERAL_TRAFFIC_TYPE_MOST_RECENT_SESSION",
    "DEVICE_ID_BROWSER_MOST_RECENT_SESSION",
    "CX360_COUNTRY",
    "CX360_LIFESTAGE",
    "CX360_RECENCY_ORDER_FREQUENCY_CATEGORY",
    "CX360_ORDER_RECENCY_CATEGORY",
    "CX360_FREQ_CATEGORY",
    "CX360_SESSIONS_COUNT_L28D_CATEGORY",
    "DEVICE_ID_SUBMARKET_TIER_REAL_TIME",
    "CX360_ML_ACTIVE_CX_CHURN_PROPENSITY_SCORE_QUINTILE_BUCKET",
]

# Binary features created from categorical encoding
categorical_binary_features = [
    # Platform (3)
    "PLATFORM_MOBILE", "PLATFORM_DESKTOP", "PLATFORM_MISSING",
    
    # Traffic type real-time (6)
    "TRAFFIC_RT_ORGANIC_SEARCH", "TRAFFIC_RT_DIRECT", "TRAFFIC_RT_PAID_MEDIA",
    "TRAFFIC_RT_MX_SHARE", "TRAFFIC_RT_OTHER", "TRAFFIC_RT_MISSING",
    
    # Traffic type most recent session (5)
    "TRAFFIC_MRS_DIRECT", "TRAFFIC_MRS_ORGANIC_SEARCH", "TRAFFIC_MRS_PAID_MEDIA",
    "TRAFFIC_MRS_OTHER", "TRAFFIC_MRS_MISSING",
    
    # Browser (6)
    "BROWSER_SAFARI", "BROWSER_CHROME", "BROWSER_INSTAGRAM_INAPP",
    "BROWSER_CHROME_IOS", "BROWSER_OTHER", "BROWSER_MISSING",
    
    # Country (5)
    "COUNTRY_US", "COUNTRY_CANADA", "COUNTRY_AUSTRALIA",
    "COUNTRY_OTHER", "COUNTRY_MISSING",
    
    # Lifestage (8)
    "LIFESTAGE_ACTIVE", "LIFESTAGE_DORMANT", "LIFESTAGE_CHURN",
    "LIFESTAGE_NON_PURCHASER", "LIFESTAGE_SUPER_CHURN", "LIFESTAGE_NEW",
    "LIFESTAGE_VERY_CHURN", "LIFESTAGE_MISSING",
    
    # Recency frequency (4)
    "RECENCY_FREQ_LOW", "RECENCY_FREQ_HIGH", "RECENCY_FREQ_MEDIUM", "RECENCY_FREQ_MISSING",
    
    # Order recency (4)
    "ORDER_RECENCY_ACTIVE", "ORDER_RECENCY_LAPSED", "ORDER_RECENCY_INACTIVE", "ORDER_RECENCY_MISSING",
    
    # Sessions count L28D category (4)
    "SESSIONS_L28D_LOW", "SESSIONS_L28D_MEDIUM", "SESSIONS_L28D_HIGH", "SESSIONS_L28D_MISSING",
    
    # Submarket tier (6 - tiers 1-5 + missing)
    "SUBMARKET_TIER_1", "SUBMARKET_TIER_2", "SUBMARKET_TIER_3", "SUBMARKET_TIER_4", "SUBMARKET_TIER_5", "SUBMARKET_TIER_MISSING",
    
    # Churn propensity quintile (6 - quintiles 1-5 + missing)
    "CHURN_QUINTILE_1", "CHURN_QUINTILE_2", "CHURN_QUINTILE_3", "CHURN_QUINTILE_4", "CHURN_QUINTILE_5", "CHURN_QUINTILE_MISSING",
    
    # Favorite cuisine (14)
    "FREQ_CAT_BURGERS", "FREQ_CAT_MEXICAN", "FREQ_CAT_AMERICAN", "FREQ_CAT_BREAKFAST",
    "FREQ_CAT_COFFEE_TEA", "FREQ_CAT_ASIAN", "FREQ_CAT_PIZZA", "FREQ_CAT_CHICKEN",
    "FREQ_CAT_SANDWICHES_SUBS", "FREQ_CAT_ITALIAN", "FREQ_CAT_INDIAN",
    "FREQ_CAT_CONVENIENCE", "FREQ_CAT_OTHER", "FREQ_CAT_MISSING",
]

# Define numerical features - comprehensive feature set from v5
numerical_features = [
    # Core device features
    "HAS_ASSOCIATED_CONSUMER_ID",
    
    # Platform indicators
    "IS_ANDROID",
    "IS_IOS",
    "IS_WEB",
    
    # Device activity - 28 days
    "DEVICE_ID_IS_ACTIVE_RECENT_28_DAYS",
    "DEVICE_ID_SESSIONS_RECENT_28_DAYS",
    "DEVICE_ID_HAS_LOGGED_IN_RECENT_28_DAYS",
    "DEVICE_ID_ORDERS_RECENT_28_DAYS",
    "DEVICE_ID_HAS_LOGGED_OUT_ADDRESS_ENTRY_RECENT_28_DAYS",
    "DEVICE_ID_HAS_LOGGED_IN_ADDRESS_ENTRY_RECENT_28_DAYS",
    
    # Device activity - 90 days
    "DEVICE_ID_IS_ACTIVE_RECENT_90_DAYS",
    "DEVICE_ID_SESSIONS_RECENT_90_DAYS",
    "DEVICE_ID_HAS_LOGGED_IN_RECENT_90_DAYS",
    "DEVICE_ID_ORDERS_RECENT_90_DAYS",
    "DEVICE_ID_HAS_LOGGED_OUT_ADDRESS_ENTRY_RECENT_90_DAYS",
    "DEVICE_ID_HAS_LOGGED_IN_ADDRESS_ENTRY_RECENT_90_DAYS",
    
    # Most recent session behavior
    "DEVICE_ID_RECENCY_MOST_RECENT_SESSION",
    "DEVICE_ID_PLACED_ORDER_MOST_RECENT_SESSION",
    "DEVICE_ID_IS_AUTO_LOGGED_IN_MOST_RECENT_SESSION",
    "DEVICE_ID_HAS_LOGGED_OUT_ADDRESS_ENTRY_MOST_RECENT_SESSION",
    "DEVICE_ID_HAS_LOGGED_IN_ADDRESS_ENTRY_MOST_RECENT_SESSION",
    "DEVICE_ID_HAS_LOGGED_IN_RECENT_MOST_RECENT_SESSION",
    
    # Real-time channels (one-hot encoded - the channel at exposure time)
    "DEVICE_ID_CHANNEL_IS_DIRECT_REAL_TIME",
    "DEVICE_ID_CHANNEL_IS_ORGANIC_SEARCH_REAL_TIME",
    "DEVICE_ID_CHANNEL_IS_PAID_MEDIA_REAL_TIME",
    "DEVICE_ID_CHANNEL_IS_EMAIL_REAL_TIME",
    "DEVICE_ID_CHANNEL_IS_PARTNERS_REAL_TIME",
    "DEVICE_ID_CHANNEL_IS_AFFILIATE_REAL_TIME",
    "DEVICE_ID_CHANNEL_IS_MX_SHARE_REAL_TIME",
    
    # Exposure time features (hour of day)
    "DEVICE_ID_FIRST_EXPOSURE_TIME_IS_0_TO_6_REAL_TIME",
    "DEVICE_ID_FIRST_EXPOSURE_TIME_IS_6_TO_10_REAL_TIME",
    "DEVICE_ID_FIRST_EXPOSURE_TIME_IS_10_TO_14_REAL_TIME",
    "DEVICE_ID_FIRST_EXPOSURE_TIME_IS_14_TO_17_REAL_TIME",
    "DEVICE_ID_FIRST_EXPOSURE_TIME_IS_17_TO_21_REAL_TIME",
    "DEVICE_ID_FIRST_EXPOSURE_TIME_IS_21_TO_24_REAL_TIME",
    
    # Consumer order features - 28 days
    "CONSUMER_ID_IN_APP_ORDERS_RECENT_28_DAYS",
    "CONSUMER_ID_DESKTOP_WEB_ORDERS_RECENT_28_DAYS",
    "CONSUMER_ID_MOBILE_WEB_ORDERS_RECENT_28_DAYS",
    "CONSUMER_ID_TOTAL_ORDERS_RECENT_28_DAYS",
    
    # Consumer order features - 90 days
    "CONSUMER_ID_IN_APP_ORDERS_RECENT_90_DAYS",
    "CONSUMER_ID_DESKTOP_WEB_ORDERS_RECENT_90_DAYS",
    "CONSUMER_ID_MOBILE_WEB_ORDERS_RECENT_90_DAYS",
    "CONSUMER_ID_TOTAL_ORDERS_RECENT_90_DAYS",
    
    # Device visitor type flags - 28 days
    "DEVICE_ID_UNIQUE_VISITOR_RECENT_28_DAYS",
    "DEVICE_ID_UNIQUE_STORE_CONTENT_PAGE_VISITOR_RECENT_28_DAYS",
    "DEVICE_ID_UNIQUE_STORE_PAGE_VISITOR_RECENT_28_DAYS",
    "DEVICE_ID_UNIQUE_ORDER_CART_PAGE_VISITOR_RECENT_28_DAYS",
    "DEVICE_ID_UNIQUE_CHECKOUT_PAGE_VISITOR_RECENT_28_DAYS",
    "DEVICE_ID_UNIQUE_PURCHASER_RECENT_28_DAYS",
    "DEVICE_ID_UNIQUE_APP_INSTALLER_RECENT_28_DAYS",
    "DEVICE_ID_UNIQUE_CORE_VISITOR_RECENT_28_DAYS",
    "DEVICE_ID_HOME_PAGE_VISITOR_RECENT_28_DAYS",
    "DEVICE_ID_MOBILE_SPLASH_PAGE_VISITOR_RECENT_28_DAYS",
    "DEVICE_ID_MULTI_STORE_VISITOR_RECENT_28_DAYS",
    
    # Device visitor type flags - 90 days
    "DEVICE_ID_UNIQUE_VISITOR_RECENT_90_DAYS",
    "DEVICE_ID_UNIQUE_STORE_CONTENT_PAGE_VISITOR_RECENT_90_DAYS",
    "DEVICE_ID_UNIQUE_STORE_PAGE_VISITOR_RECENT_90_DAYS",
    "DEVICE_ID_UNIQUE_ORDER_CART_PAGE_VISITOR_RECENT_90_DAYS",
    "DEVICE_ID_UNIQUE_CHECKOUT_PAGE_VISITOR_RECENT_90_DAYS",
    "DEVICE_ID_UNIQUE_PURCHASER_RECENT_90_DAYS",
    "DEVICE_ID_UNIQUE_APP_INSTALLER_RECENT_90_DAYS",
    "DEVICE_ID_UNIQUE_CORE_VISITOR_RECENT_90_DAYS",
    "DEVICE_ID_HOME_PAGE_VISITOR_RECENT_90_DAYS",
    "DEVICE_ID_MOBILE_SPLASH_PAGE_VISITOR_RECENT_90_DAYS",
    "DEVICE_ID_MULTI_STORE_VISITOR_RECENT_90_DAYS",
    
    # Deep link and Singular events
    "DEVICE_ID_DEEP_LINK_EVENT_COUNT_TOTAL",
    "DEVICE_ID_SINGULAR_EVENT_COUNT_TOTAL",
    "DEVICE_ID_DEEP_LINK_RECENCY_DAYS",
    "DEVICE_ID_SINGULAR_RECENCY_DAYS",
    
    # CX360 consumer lifetime features - boolean flags
    "CX360_IS_CURRENT_DASHPASS",
    "CX360_IS_EVER_DASHPASS",
    "CX360_IS_FRAUD",
    "CX360_IS_SUMA_CLUSTER_ACCOUNT_MOST_ACTIVE_RECENT",
    
    # CX360 consumer lifetime features - numeric
    "CX360_TOTAL_MAIN_VISITOR_COUNT_LIFETIME",
    "CX360_STORE_PAGE_VISITOR_COUNT_LIFETIME",
    "CX360_ADD_ITEM_VISITOR_COUNT_LIFETIME",
    "CX360_MON_THU_ORDER_COUNT_RATIO_LIFETIME",
    "CX360_MON_THU_ORDER_COUNT_LIFETIME",
    "CX360_FRI_ORDER_COUNT_RATIO_LIFETIME",
    "CX360_FRI_ORDER_COUNT_LIFETIME",
    "CX360_SAT_ORDER_COUNT_RATIO_LIFETIME",
    "CX360_SAT_ORDER_COUNT_LIFETIME",
    "CX360_SUN_ORDER_COUNT_RATIO_LIFETIME",
    "CX360_SUN_ORDER_COUNT_LIFETIME",
    "CX360_LUNCH_COUNT_LIFETIME",
    "CX360_BREAKFAST_COUNT_LIFETIME",
    "CX360_DINNER_COUNT_LIFETIME",
    "CX360_EARLY_MORNING_COUNT_LIFETIME",
    "CX360_LATE_NIGHT_COUNT_LIFETIME",
    "CX360_LIFETIME_DAYS",
    "CX360_AVG_ORDER_INTERVAL_DAYS",
    "CX360_ORDER_COUNT_LIFETIME",
    "CX360_AVG_VP_LIFETIME",
    "CX360_NV_ORDERS_COUNT_LIFETIME",
    
    # CX360 additional features - spend and order metrics
    "CX360_TOTAL_SPEND_LIFETIME",
    "CX360_TOTAL_SPEND_L12M",
    "CX360_ORDER_COUNT_L28D",
    "CX360_ORDER_COUNT_L14D",
    "CX360_ORDER_COUNT_L90D",
    "CX360_CHECKOUT_RATIO_LIFETIME",
    "CX360_AVG_SPEND_LIFETIME",
    "CX360_AVG_TIP_LIFETIME",
    
    # CX360 DashPass features
    "CX360_IS_CURRENT_PAID_DASHPASS",
    "CX360_IS_CURRENT_ENTITLED_DASHPASS",
    "CX360_DASHPASS_POTENTIAL_SAVINGS_L30D_AMT",
    
    # CX360 marketing reachability
    "CX360_IS_REACHABLE_EMAIL_MARKETING",
    "CX360_IS_EMAIL_ENGAGED_L365D",
    "CX360_IS_REACHABLE_PUSH_MARKETING",
    
    # CX360 promo usage
    "CX360_PROMO_DELIVERY_COUNT_L28D",
    "CX360_PROMO_DELIVERY_RATIO_L365D",
    "CX360_AVG_PROMO_SAVING_PER_ORDER_L90D",
    
    # CX360 quality metrics
    "CX360_CANCEL_COUNT_LIFETIME",
    "CX360_CANCEL_COUNT_L28D",
    "CX360_LATENESS_COUNT_LIFETIME",
    "CX360_LATENESS_COUNT_L28D",
    "CX360_IS_GUEST",
    "CX360_IS_LOYAL_TOP_MX",
    
    # CX360 ML model scores
    "CX360_PRICE_SENSITIVITY_MODEL_SCORE_V2",
    "CX360_ML_CX_LTV_PREDICTION",
    
    # CX360 visitor metrics
    "CX360_TOTAL_MAIN_VISITOR_COUNT_L28D",
    "CX360_HOMEPAGE_SESSION_COUNT_L28D",
    
    # CX360 VP and GOV metrics
    "CX360_TOTAL_VP_L12M",
    "CX360_TOTAL_VP_L90D",
    "CX360_TOTAL_VP_L28D",
    "CX360_TOTAL_VP_L7D",
    "CX360_TOTAL_GOV_L12M",
    "CX360_TOTAL_GOV_L90D",
    "CX360_TOTAL_GOV_L28D",
    "CX360_TOTAL_GOV_L7D",
    
    # CX360 engagement metrics
    "CX360_DAYS_ACTIVE_LIFETIME",
    "CX360_RECENCY_DAYS",
    "CX360_ORDER_FREQUENCY_MONTHLY",
    "CX360_BUSINESS_COUNT_LIFETIME",
    "CX360_STORE_COUNT_LIFETIME",
    "CX360_DELIVERY_ADDRESS_COUNT_LIFETIME",
    
    # CX360 flags
    "CX360_IS_EMPLOYEE",
    "CX360_IS_BLACKLISTED",
    
    # CX360 delivery quality
    "CX360_DEFECT_DELIVERY_COUNT_LIFETIME",
    "CX360_NEVER_DELIVERED_COUNT_LIFETIME",
    "CX360_MISSING_INCORRECT_COUNT_LIFETIME",
    "CX360_HIGH_QUALITY_DELIVERY_COUNT_LIFETIME",
    
    # CX360 calculated features
    "CX360_AVG_VISITS_PER_ACTIVE_DAY",
    "CX360_AVG_SPEND_PER_ORDER",
]

# Combined feature list - numerical + manually encoded categorical
all_features = numerical_features + categorical_binary_features

label_col = "DEVICE_ID_PLACED_AN_ORDER_REAL_TIME"
treatment_col = "DEVICE_ID_SHOW_APP_DOWNLOAD_BOTTOM_SHEET_REAL_TIME"

# COMMAND ----------

def create_categorical_binary_features(df: "DataFrame") -> "DataFrame":
    """
    Manually create binary features from categorical columns based on domain knowledge.
    
    Args:
        df: PySpark DataFrame with categorical columns
    
    Returns:
        DataFrame with binary categorical features added
    """
    # PLATFORM
    df = df.withColumn("PLATFORM_MOBILE", (F.col("PLATFORM") == "mobile").cast("int"))
    df = df.withColumn("PLATFORM_DESKTOP", (F.col("PLATFORM") == "desktop").cast("int"))
    df = df.withColumn("PLATFORM_MISSING", F.col("PLATFORM").isNull().cast("int"))
    
    # DEVICE_ID_GENERAL_TRAFFIC_TYPE_REAL_TIME
    df = df.withColumn("TRAFFIC_RT_ORGANIC_SEARCH", (F.col("DEVICE_ID_GENERAL_TRAFFIC_TYPE_REAL_TIME") == "Organic Search").cast("int"))
    df = df.withColumn("TRAFFIC_RT_DIRECT", (F.col("DEVICE_ID_GENERAL_TRAFFIC_TYPE_REAL_TIME") == "Direct").cast("int"))
    df = df.withColumn("TRAFFIC_RT_PAID_MEDIA", (F.col("DEVICE_ID_GENERAL_TRAFFIC_TYPE_REAL_TIME") == "Paid Media").cast("int"))
    df = df.withColumn("TRAFFIC_RT_MX_SHARE", (F.col("DEVICE_ID_GENERAL_TRAFFIC_TYPE_REAL_TIME") == "Mx Share").cast("int"))
    df = df.withColumn("TRAFFIC_RT_OTHER", F.col("DEVICE_ID_GENERAL_TRAFFIC_TYPE_REAL_TIME").isin(["Partners", "Affiliate", "Other", "Email"]).cast("int"))
    df = df.withColumn("TRAFFIC_RT_MISSING", F.col("DEVICE_ID_GENERAL_TRAFFIC_TYPE_REAL_TIME").isNull().cast("int"))
    
    # DEVICE_ID_GENERAL_TRAFFIC_TYPE_MOST_RECENT_SESSION
    df = df.withColumn("TRAFFIC_MRS_DIRECT", (F.col("DEVICE_ID_GENERAL_TRAFFIC_TYPE_MOST_RECENT_SESSION") == "Direct").cast("int"))
    df = df.withColumn("TRAFFIC_MRS_ORGANIC_SEARCH", (F.col("DEVICE_ID_GENERAL_TRAFFIC_TYPE_MOST_RECENT_SESSION") == "Organic Search").cast("int"))
    df = df.withColumn("TRAFFIC_MRS_PAID_MEDIA", (F.col("DEVICE_ID_GENERAL_TRAFFIC_TYPE_MOST_RECENT_SESSION") == "Paid Media").cast("int"))
    df = df.withColumn("TRAFFIC_MRS_OTHER", F.col("DEVICE_ID_GENERAL_TRAFFIC_TYPE_MOST_RECENT_SESSION").isin(["Other", "Paid Social", "Partners", "Affiliate", "Email"]).cast("int"))
    df = df.withColumn("TRAFFIC_MRS_MISSING", F.col("DEVICE_ID_GENERAL_TRAFFIC_TYPE_MOST_RECENT_SESSION").isNull().cast("int"))
    
    # DEVICE_ID_BROWSER_MOST_RECENT_SESSION
    df = df.withColumn("BROWSER_SAFARI", (F.col("DEVICE_ID_BROWSER_MOST_RECENT_SESSION") == "Safari").cast("int"))
    df = df.withColumn("BROWSER_CHROME", (F.col("DEVICE_ID_BROWSER_MOST_RECENT_SESSION") == "Chrome").cast("int"))
    df = df.withColumn("BROWSER_INSTAGRAM_INAPP", (F.col("DEVICE_ID_BROWSER_MOST_RECENT_SESSION") == "Instagram In-App").cast("int"))
    df = df.withColumn("BROWSER_CHROME_IOS", (F.col("DEVICE_ID_BROWSER_MOST_RECENT_SESSION") == "Chrome iOS").cast("int"))
    df = df.withColumn("BROWSER_OTHER", F.col("DEVICE_ID_BROWSER_MOST_RECENT_SESSION").isin(["Other", "Facebook In-App", "Firefox", "Firefox iOS", "Edge", "Opera", "Snapchat In-App", "Pinterest In-App", "UC Browser", "Twitter In-App"]).cast("int"))
    df = df.withColumn("BROWSER_MISSING", F.col("DEVICE_ID_BROWSER_MOST_RECENT_SESSION").isNull().cast("int"))
    
    # CX360_COUNTRY
    df = df.withColumn("COUNTRY_US", (F.col("CX360_COUNTRY") == "US").cast("int"))
    df = df.withColumn("COUNTRY_CANADA", (F.col("CX360_COUNTRY") == "CANADA").cast("int"))
    df = df.withColumn("COUNTRY_AUSTRALIA", (F.col("CX360_COUNTRY") == "AUSTRALIA").cast("int"))
    df = df.withColumn("COUNTRY_OTHER", (~F.col("CX360_COUNTRY").isin(["US", "CANADA", "AUSTRALIA"]) & F.col("CX360_COUNTRY").isNotNull()).cast("int"))
    df = df.withColumn("COUNTRY_MISSING", F.col("CX360_COUNTRY").isNull().cast("int"))
    
    # CX360_LIFESTAGE
    df = df.withColumn("LIFESTAGE_ACTIVE", (F.col("CX360_LIFESTAGE") == "Active").cast("int"))
    df = df.withColumn("LIFESTAGE_DORMANT", (F.col("CX360_LIFESTAGE") == "Dormant").cast("int"))
    df = df.withColumn("LIFESTAGE_CHURN", (F.col("CX360_LIFESTAGE") == "Churn").cast("int"))
    df = df.withColumn("LIFESTAGE_NON_PURCHASER", (F.col("CX360_LIFESTAGE") == "Non-Purchaser").cast("int"))
    df = df.withColumn("LIFESTAGE_SUPER_CHURN", (F.col("CX360_LIFESTAGE") == "Super Churn").cast("int"))
    df = df.withColumn("LIFESTAGE_NEW", (F.col("CX360_LIFESTAGE") == "New").cast("int"))
    df = df.withColumn("LIFESTAGE_VERY_CHURN", (F.col("CX360_LIFESTAGE") == "Very Churn").cast("int"))
    df = df.withColumn("LIFESTAGE_MISSING", F.col("CX360_LIFESTAGE").isNull().cast("int"))
    
    # CX360_RECENCY_ORDER_FREQUENCY_CATEGORY
    df = df.withColumn("RECENCY_FREQ_LOW", (F.col("CX360_RECENCY_ORDER_FREQUENCY_CATEGORY") == "Low").cast("int"))
    df = df.withColumn("RECENCY_FREQ_HIGH", (F.col("CX360_RECENCY_ORDER_FREQUENCY_CATEGORY") == "High").cast("int"))
    df = df.withColumn("RECENCY_FREQ_MEDIUM", (F.col("CX360_RECENCY_ORDER_FREQUENCY_CATEGORY") == "Medium").cast("int"))
    df = df.withColumn("RECENCY_FREQ_MISSING", F.col("CX360_RECENCY_ORDER_FREQUENCY_CATEGORY").isNull().cast("int"))
    
    # CX360_ORDER_RECENCY_CATEGORY
    df = df.withColumn("ORDER_RECENCY_ACTIVE", (F.col("CX360_ORDER_RECENCY_CATEGORY") == "Active").cast("int"))
    df = df.withColumn("ORDER_RECENCY_LAPSED", (F.col("CX360_ORDER_RECENCY_CATEGORY") == "Lapsed").cast("int"))
    df = df.withColumn("ORDER_RECENCY_INACTIVE", (F.col("CX360_ORDER_RECENCY_CATEGORY") == "In-Active").cast("int"))
    df = df.withColumn("ORDER_RECENCY_MISSING", F.col("CX360_ORDER_RECENCY_CATEGORY").isNull().cast("int"))
    
    # CX360_SESSIONS_COUNT_L28D_CATEGORY (Low, Medium, High, Missing)
    df = df.withColumn("SESSIONS_L28D_LOW", (F.col("CX360_SESSIONS_COUNT_L28D_CATEGORY") == "Low").cast("int"))
    df = df.withColumn("SESSIONS_L28D_MEDIUM", (F.col("CX360_SESSIONS_COUNT_L28D_CATEGORY") == "Medium").cast("int"))
    df = df.withColumn("SESSIONS_L28D_HIGH", (F.col("CX360_SESSIONS_COUNT_L28D_CATEGORY") == "High").cast("int"))
    df = df.withColumn("SESSIONS_L28D_MISSING", F.col("CX360_SESSIONS_COUNT_L28D_CATEGORY").isNull().cast("int"))
    
    # DEVICE_ID_SUBMARKET_TIER_REAL_TIME (1-5, Missing)
    df = df.withColumn("SUBMARKET_TIER_1", (F.col("DEVICE_ID_SUBMARKET_TIER_REAL_TIME") == 1).cast("int"))
    df = df.withColumn("SUBMARKET_TIER_2", (F.col("DEVICE_ID_SUBMARKET_TIER_REAL_TIME") == 2).cast("int"))
    df = df.withColumn("SUBMARKET_TIER_3", (F.col("DEVICE_ID_SUBMARKET_TIER_REAL_TIME") == 3).cast("int"))
    df = df.withColumn("SUBMARKET_TIER_4", (F.col("DEVICE_ID_SUBMARKET_TIER_REAL_TIME") == 4).cast("int"))
    df = df.withColumn("SUBMARKET_TIER_5", (F.col("DEVICE_ID_SUBMARKET_TIER_REAL_TIME") == 5).cast("int"))
    df = df.withColumn("SUBMARKET_TIER_MISSING", F.col("DEVICE_ID_SUBMARKET_TIER_REAL_TIME").isNull().cast("int"))
    
    # CX360_ML_ACTIVE_CX_CHURN_PROPENSITY_SCORE_QUINTILE_BUCKET
    df = df.withColumn("CHURN_QUINTILE_1", (F.col("CX360_ML_ACTIVE_CX_CHURN_PROPENSITY_SCORE_QUINTILE_BUCKET") == "quintile_1").cast("int"))
    df = df.withColumn("CHURN_QUINTILE_2", (F.col("CX360_ML_ACTIVE_CX_CHURN_PROPENSITY_SCORE_QUINTILE_BUCKET") == "quintile_2").cast("int"))
    df = df.withColumn("CHURN_QUINTILE_3", (F.col("CX360_ML_ACTIVE_CX_CHURN_PROPENSITY_SCORE_QUINTILE_BUCKET") == "quintile_3").cast("int"))
    df = df.withColumn("CHURN_QUINTILE_4", (F.col("CX360_ML_ACTIVE_CX_CHURN_PROPENSITY_SCORE_QUINTILE_BUCKET") == "quintile_4").cast("int"))
    df = df.withColumn("CHURN_QUINTILE_5", (F.col("CX360_ML_ACTIVE_CX_CHURN_PROPENSITY_SCORE_QUINTILE_BUCKET") == "quintile_5").cast("int"))
    df = df.withColumn("CHURN_QUINTILE_MISSING", F.col("CX360_ML_ACTIVE_CX_CHURN_PROPENSITY_SCORE_QUINTILE_BUCKET").isNull().cast("int"))
    
    # CX360_FREQ_CATEGORY - top categories
    top_cuisines = ["burgers", "mexican", "american", "convenience_store", "breakfast_sandwiches", 
                    "coffee_tea", "chinese_food", "pizza", "chicken_wings", "japanese", "chicken_shop",
                    "grocery", "italian", "sandwiches", "dessert_and_fast-food", "indian", "thai",
                    "fast_food", "breakfast", "salads", "pickup", "sub", "seafood"]
    
    df = df.withColumn("FREQ_CAT_BURGERS", (F.col("CX360_FREQ_CATEGORY") == "burgers").cast("int"))
    df = df.withColumn("FREQ_CAT_MEXICAN", (F.col("CX360_FREQ_CATEGORY") == "mexican").cast("int"))
    df = df.withColumn("FREQ_CAT_AMERICAN", (F.col("CX360_FREQ_CATEGORY") == "american").cast("int"))
    df = df.withColumn("FREQ_CAT_BREAKFAST", (F.col("CX360_FREQ_CATEGORY").isin(["breakfast_sandwiches", "breakfast"])).cast("int"))
    df = df.withColumn("FREQ_CAT_COFFEE_TEA", (F.col("CX360_FREQ_CATEGORY") == "coffee_tea").cast("int"))
    df = df.withColumn("FREQ_CAT_ASIAN", (F.col("CX360_FREQ_CATEGORY").isin(["chinese_food", "japanese", "thai"])).cast("int"))
    df = df.withColumn("FREQ_CAT_PIZZA", (F.col("CX360_FREQ_CATEGORY") == "pizza").cast("int"))
    df = df.withColumn("FREQ_CAT_CHICKEN", (F.col("CX360_FREQ_CATEGORY").isin(["chicken_wings", "chicken_shop"])).cast("int"))
    df = df.withColumn("FREQ_CAT_SANDWICHES_SUBS", (F.col("CX360_FREQ_CATEGORY").isin(["sandwiches", "sub"])).cast("int"))
    df = df.withColumn("FREQ_CAT_ITALIAN", (F.col("CX360_FREQ_CATEGORY") == "italian").cast("int"))
    df = df.withColumn("FREQ_CAT_INDIAN", (F.col("CX360_FREQ_CATEGORY") == "indian").cast("int"))
    df = df.withColumn("FREQ_CAT_CONVENIENCE", (F.col("CX360_FREQ_CATEGORY").isin(["convenience_store", "grocery"])).cast("int"))
    df = df.withColumn("FREQ_CAT_OTHER", (~F.col("CX360_FREQ_CATEGORY").isin(top_cuisines) & F.col("CX360_FREQ_CATEGORY").isNotNull()).cast("int"))
    df = df.withColumn("FREQ_CAT_MISSING", F.col("CX360_FREQ_CATEGORY").isNull().cast("int"))
    
    return df

# COMMAND ----------

# Load data from Snowflake
query = """
SELECT * 
FROM proddb.fionafan.logged_out_personalization_training_comprehensive_v5
"""

df_spark = (
    spark.read.format("snowflake")
    .options(**OPTIONS)
    .option("query", query)
    .load()
)

# Convert boolean columns to integers for modeling
df_spark = df_spark.withColumn(label_col, F.col(label_col).cast("int"))
df_spark = df_spark.withColumn(treatment_col, F.col(treatment_col).cast("int"))

# Binary categorical features are already created in SQL - just convert to double and handle nulls
print("🔧 Converting features to double and handling nulls...")
for feat in all_features:
    df_spark = df_spark.withColumn(feat, F.coalesce(F.col(feat).cast("double"), F.lit(0.0)))

# Cache the dataframe for reuse
df_spark.cache()

print(f"Total records loaded: {df_spark.count()}")
print(f"Base numerical features: {len(numerical_features)}")
print(f"Binary categorical features: {len(categorical_binary_features)}")
print(f"Total features: {len(all_features)}")

final_feature_count = len(all_features)

print(f"\n✅ Final Feature Count: {final_feature_count}")
print(f"   - Numerical: {len(numerical_features)}")
print(f"   - Categorical (binary encoded): {len(categorical_binary_features)}")

# COMMAND ----------

def train_xgboost_model(
    df: "DataFrame",
    features: List[str],
    label_col: str,
    task: str = "classification",
    num_workers: int = 2
) -> Tuple["Model", "VectorAssembler"]:
    """
    Train an XGBoost model using PySpark.
    
    Args:
        df: PySpark DataFrame
        features: List of feature column names (already encoded as binary/numeric)
        label_col: Label column name
        task: 'classification' or 'regression'
        num_workers: Number of workers for distributed training
    
    Returns:
        Trained model and vector assembler
    """
    # Drop features column if it already exists to avoid conflicts
    if "features" in df.columns:
        df = df.drop("features")
    
    # Simple VectorAssembler for already-encoded features
    assembler = VectorAssembler(
        inputCols=features,
        outputCol="features",
        handleInvalid="keep"
    )
    df_assembled = assembler.transform(df)
    
    # Configure XGBoost
    if task == "classification":
        model = SparkXGBClassifier(
            features_col="features",
            label_col=label_col,
            num_workers=num_workers,
            n_estimators=100,
            max_depth=4,
            learning_rate=0.05,
            device="cpu",  # Change to "cuda" if GPU available
            random_state=42,
            verbosity=0,  # Suppress training output (0=silent, 1=warning, 2=info, 3=debug)
            verbose_eval=False,  # Disable per-iteration logging
        )
    else:  # regression for uplift model
        model = SparkXGBRegressor(
            features_col="features",
            label_col=label_col,
            num_workers=num_workers,
            n_estimators=100,
            max_depth=4,
            learning_rate=0.05,
            device="cpu",  # Change to "cuda" if GPU available
            random_state=42,
            verbosity=0,  # Suppress training output
            verbose_eval=False,  # Disable per-iteration logging
        )
    
    # Fit model
    fitted_model = model.fit(df_assembled)
    
    return fitted_model, assembler


def calculate_qini_score(
    y_true: np.ndarray,
    uplift: np.ndarray,
    treatment: np.ndarray
) -> float:
    """
    Calculate Qini AUC score.
    
    Args:
        y_true: True labels (binary)
        uplift: Predicted uplift scores
        treatment: Treatment indicator (0/1)
    
    Returns:
        Qini AUC score
    """
    return qini_auc_score(y_true, uplift, treatment, negative_effect=True)


def calculate_auc(df: "DataFrame", prediction_col: str = "probability", label_col: str = "label") -> float:
    """
    Calculate AUC for a binary classification model.
    
    Args:
        df: PySpark DataFrame with predictions
        prediction_col: Column name for predictions (probability vector)
        label_col: Column name for true labels
    
    Returns:
        AUC score
    """
    evaluator = BinaryClassificationEvaluator(
        rawPredictionCol="rawPrediction",
        labelCol=label_col,
        metricName="areaUnderROC"
    )
    return evaluator.evaluate(df)


def get_feature_importance(model, feature_names: List[str], top_n: int = 20) -> pd.DataFrame:
    """
    Extract and format feature importance from XGBoost model.
    
    Args:
        model: Fitted XGBoost model
        feature_names: List of feature names
        top_n: Number of top features to return
    
    Returns:
        DataFrame with feature importance sorted by importance
    """
    # Get feature importance scores
    importance_dict = model.get_feature_importances()
    
    # Create dataframe
    importance_df = pd.DataFrame([
        {"feature": feature_names[int(k.replace("f", ""))], "importance": v}
        for k, v in importance_dict.items()
    ])
    
    # Sort by importance and return top N
    importance_df = importance_df.sort_values("importance", ascending=False).head(top_n)
    importance_df = importance_df.reset_index(drop=True)
    
    return importance_df


# COMMAND ----------

class XLearnerPySpark:
    """
    X-Learner implementation using PySpark and XGBoost.
    
    The X-Learner algorithm:
    1. Train μ0 on control group to predict Y
    2. Train μ1 on treatment group to predict Y
    3. Impute CATE for control: τ̂C = μ1(X) - Y
    4. Impute CATE for treatment: τ̂T = Y - μ0(X)
    5. Train τ0 on control (X, τ̂C) to predict treatment effect
    6. Train τ1 on treatment (X, τ̂T) to predict treatment effect
    7. Final uplift = propensity * τ1(X) + (1 - propensity) * τ0(X)
    """
    
    def __init__(self, features: List[str], label_col: str, treatment_col: str, num_workers: int = 2):
        self.features = features
        self.label_col = label_col
        self.treatment_col = treatment_col
        self.num_workers = num_workers
        
        # Stage 1 models (response models)
        self.mu0_model = None  # Control response model
        self.mu1_model = None  # Treatment response model
        self.mu0_assembler = None
        self.mu1_assembler = None
        
        # Stage 2 models (treatment effect models)
        self.tau0_model = None  # Control CATE model
        self.tau1_model = None  # Treatment CATE model
        self.tau0_assembler = None
        self.tau1_assembler = None
        
        # Propensity model
        self.propensity_model = None
        self.propensity_assembler = None
        
        # Feature importance
        self.tau1_feature_importance = None
        self.tau0_feature_importance = None
    
    def fit(self, df: "DataFrame"):
        """
        Fit X-Learner on the dataset.
        
        Args:
            df: PySpark DataFrame with features, label, and treatment indicator
        """
        print("🔥 Stage 1: Training response models (μ0 and μ1)")
        
        # Split into treatment and control groups
        df_control = df.filter(F.col(self.treatment_col) == 0)
        df_treatment = df.filter(F.col(self.treatment_col) == 1)
        
        print(f"  Control size: {df_control.count()}")
        print(f"  Treatment size: {df_treatment.count()}")
        
        # Train μ0 (control response model)
        print("  Training μ0 (control response model)...")
        self.mu0_model, self.mu0_assembler = train_xgboost_model(
            df_control, self.features, self.label_col, "classification", self.num_workers
        )
        
        # Train μ1 (treatment response model)
        print("  Training μ1 (treatment response model)...")
        self.mu1_model, self.mu1_assembler = train_xgboost_model(
            df_treatment, self.features, self.label_col, "classification", self.num_workers
        )
        
        print("🔥 Stage 2: Computing imputed treatment effects")
        
        # Assemble features for predictions
        df_control_feat = self.mu0_assembler.transform(df_control)
        df_treatment_feat = self.mu1_assembler.transform(df_treatment)
        
        # Predict responses
        df_control_pred = self.mu1_model.transform(df_control_feat).withColumnRenamed(
            "prediction", "mu1_pred"
        )
        df_treatment_pred = self.mu0_model.transform(df_treatment_feat).withColumnRenamed(
            "prediction", "mu0_pred"
        )
        
        # Impute treatment effects
        # For control: τ̂C = μ1(X) - Y (what would happen if treated)
        df_control_tau = df_control_pred.withColumn(
            "imputed_tau",
            F.col("mu1_pred") - F.col(self.label_col)
        )
        
        # For treatment: τ̂T = Y - μ0(X) (what would happen if not treated)
        df_treatment_tau = df_treatment_pred.withColumn(
            "imputed_tau",
            F.col(self.label_col) - F.col("mu0_pred")
        )
        
        # Drop all intermediate feature columns to avoid conflicts
        cols_to_drop = ["features", "mu1_pred", "mu0_pred", "rawPrediction", "probability", "prediction"]
        
        for col in cols_to_drop:
            if col in df_control_tau.columns:
                df_control_tau = df_control_tau.drop(col)
            if col in df_treatment_tau.columns:
                df_treatment_tau = df_treatment_tau.drop(col)
        
        print("🔥 Stage 3: Training CATE models (τ0 and τ1)")
        
        # Train τ0 (control CATE model)
        print("  Training τ0 (control CATE model)...")
        self.tau0_model, self.tau0_assembler = train_xgboost_model(
            df_control_tau, self.features, "imputed_tau", "regression", self.num_workers
        )
        
        # Train τ1 (treatment CATE model)
        print("  Training τ1 (treatment CATE model)...")
        self.tau1_model, self.tau1_assembler = train_xgboost_model(
            df_treatment_tau, self.features, "imputed_tau", "regression", self.num_workers
        )
        
        # Store feature importance for tau models (residual prediction)
        self.tau1_feature_importance = get_feature_importance(self.tau1_model, self.features, top_n=20)
        self.tau0_feature_importance = get_feature_importance(self.tau0_model, self.features, top_n=20)
        
        print("🔥 Stage 4: Training propensity model")
        
        # Train propensity score model
        self.propensity_model, self.propensity_assembler = train_xgboost_model(
            df, self.features, self.treatment_col, "classification", self.num_workers
        )
        
        print("✅ X-Learner training complete!")
        
        return self
    
    def predict_uplift(self, df: "DataFrame") -> "DataFrame":
        """
        Predict uplift scores for new data.
        
        Args:
            df: PySpark DataFrame with features
            
        Returns:
            DataFrame with uplift predictions
        """
        # Drop features column if it exists
        if "features" in df.columns:
            df = df.drop("features")
        
        # Add row ID for joining later
        df_with_id = df.withColumn("_row_id", F.monotonically_increasing_id())
        
        # Get propensity scores
        df_prop = self.propensity_assembler.transform(df_with_id)
        df_prop = self.propensity_model.transform(df_prop)
        # Use UDF to extract probability of class 1 (treatment)
        df_prop_id = df_prop.select("_row_id", "probability").withColumn(
            "propensity", extract_prob_class_1(F.col("probability"))
        ).select("_row_id", "propensity")
        
        # Get tau0 predictions (control CATE)
        # Each assembler creates its own "features" column, so work from original df_with_id
        df_tau0 = self.tau0_assembler.transform(df_with_id)
        df_tau0 = self.tau0_model.transform(df_tau0).withColumnRenamed(
            "prediction", "tau0_pred"
        )
        df_tau0_id = df_tau0.select("_row_id", "tau0_pred")
        
        # Get tau1 predictions (treatment CATE)
        # Each assembler creates its own "features" column, so work from original df_with_id
        df_tau1 = self.tau1_assembler.transform(df_with_id)
        df_tau1 = self.tau1_model.transform(df_tau1).withColumnRenamed(
            "prediction", "tau1_pred"
        )
        df_tau1_id = df_tau1.select("_row_id", "tau1_pred")
        
        # Join all predictions
        df_result = df_with_id
        df_result = df_result.join(df_prop_id, "_row_id", "left")
        df_result = df_result.join(df_tau0_id, "_row_id", "left")
        df_result = df_result.join(df_tau1_id, "_row_id", "left")
        
        # Calculate final uplift: g(x) * τ1(x) + (1 - g(x)) * τ0(x)
        df_result = df_result.withColumn(
            "uplift_score",
            F.col("propensity") * F.col("tau1_pred") + 
            (1 - F.col("propensity")) * F.col("tau0_pred")
        )
        
        # Drop temporary columns
        df_result = df_result.drop("_row_id", "propensity", "tau0_pred", "tau1_pred")
        
        return df_result


# COMMAND ----------

class TLearner:
    """
    T-Learner (Two-Model Approach) - Simple baseline for uplift modeling.
    
    Trains separate models for treatment and control groups:
    - μ₁: Treatment response model
    - μ₀: Control response model
    - Uplift = μ₁(X) - μ₀(X)
    """
    
    def __init__(self, features: List[str], label_col: str, treatment_col: str, num_workers: int = 2):
        self.features = features
        self.label_col = label_col
        self.treatment_col = treatment_col
        self.num_workers = num_workers
        
        self.treatment_model = None
        self.control_model = None
        self.treatment_assembler = None
        self.control_assembler = None
        
        # Feature importance
        self.treatment_feature_importance = None
        self.control_feature_importance = None
    
    def fit(self, df: "DataFrame"):
        """
        Fit T-Learner on the dataset.
        
        Args:
            df: PySpark DataFrame with features, label, and treatment indicator
        """
        print("🔥 Training T-Learner (Two-Model Approach)")
        
        # Split into treatment and control groups
        df_control = df.filter(F.col(self.treatment_col) == 0)
        df_treatment = df.filter(F.col(self.treatment_col) == 1)
        
        print(f"  Control size: {df_control.count()}")
        print(f"  Treatment size: {df_treatment.count()}")
        
        # Train treatment model
        print("  Training treatment model...")
        self.treatment_model, self.treatment_assembler = train_xgboost_model(
            df_treatment, self.features, self.label_col, "classification", self.num_workers
        )
        
        # Train control model
        print("  Training control model...")
        self.control_model, self.control_assembler = train_xgboost_model(
            df_control, self.features, self.label_col, "classification", self.num_workers
        )
        
        # Store feature importance
        self.treatment_feature_importance = get_feature_importance(self.treatment_model, self.features, top_n=20)
        self.control_feature_importance = get_feature_importance(self.control_model, self.features, top_n=20)
        
        print("✅ T-Learner training complete!")
        return self
    
    def predict_uplift(self, df: "DataFrame") -> "DataFrame":
        """
        Predict uplift scores for new data.
        
        Args:
            df: PySpark DataFrame with features
            
        Returns:
            DataFrame with uplift predictions
        """
        # Drop features column if it exists
        if "features" in df.columns:
            df = df.drop("features")
        
        # Add row ID for joining
        df_with_id = df.withColumn("_row_id", F.monotonically_increasing_id())
        
        # Get treatment predictions
        df_treat = self.treatment_assembler.transform(df_with_id)
        df_treat = self.treatment_model.transform(df_treat)
        df_treat_prob = df_treat.select("_row_id", "probability").withColumn(
            "prob_treat", extract_prob_class_1(F.col("probability"))
        ).select("_row_id", "prob_treat")
        
        # Get control predictions
        df_control = self.control_assembler.transform(df_with_id)
        df_control = self.control_model.transform(df_control)
        df_control_prob = df_control.select("_row_id", "probability").withColumn(
            "prob_control", extract_prob_class_1(F.col("probability"))
        ).select("_row_id", "prob_control")
        
        # Join and calculate uplift
        df_result = df_with_id
        df_result = df_result.join(df_treat_prob, "_row_id", "left")
        df_result = df_result.join(df_control_prob, "_row_id", "left")
        
        df_result = df_result.withColumn(
            "uplift_score",
            F.col("prob_treat") - F.col("prob_control")
        )
        
        # Drop temporary columns
        df_result = df_result.drop("_row_id", "prob_treat", "prob_control")
        
        return df_result


# COMMAND ----------

class XLearnerV2:
    """
    X-Learner V2 - Simplified version that uses tau predictions directly.
    
    Stage 1-3 are the same as X-Learner, but:
    - No propensity weighting in final prediction
    - Uses average of τ₀(X) and τ₁(X) as final uplift score
    """
    
    def __init__(self, features: List[str], label_col: str, treatment_col: str, num_workers: int = 2):
        self.features = features
        self.label_col = label_col
        self.treatment_col = treatment_col
        self.num_workers = num_workers
        
        # Stage 1 models
        self.mu0_model = None
        self.mu1_model = None
        self.mu0_assembler = None
        self.mu1_assembler = None
        
        # Stage 2 models (CATE)
        self.tau0_model = None
        self.tau1_model = None
        self.tau0_assembler = None
        self.tau1_assembler = None
        
        # Feature importance
        self.tau1_feature_importance = None
        self.tau0_feature_importance = None
    
    def fit(self, df: "DataFrame"):
        """
        Fit X-Learner V2 on the dataset.
        """
        print("🔥 Training X-Learner V2 (Simplified)")
        
        # Stage 1: Train response models
        print("🔥 Stage 1: Training response models (μ0 and μ1)")
        
        df_control = df.filter(F.col(self.treatment_col) == 0)
        df_treatment = df.filter(F.col(self.treatment_col) == 1)
        
        print(f"  Control size: {df_control.count()}")
        print(f"  Treatment size: {df_treatment.count()}")
        
        print("  Training μ0 (control response model)...")
        self.mu0_model, self.mu0_assembler = train_xgboost_model(
            df_control, self.features, self.label_col, "classification", self.num_workers
        )
        
        print("  Training μ1 (treatment response model)...")
        self.mu1_model, self.mu1_assembler = train_xgboost_model(
            df_treatment, self.features, self.label_col, "classification", self.num_workers
        )
        
        # Stage 2: Compute imputed treatment effects
        print("🔥 Stage 2: Computing imputed treatment effects")
        
        df_control_feat = self.mu0_assembler.transform(df_control)
        df_treatment_feat = self.mu1_assembler.transform(df_treatment)
        
        df_control_pred = self.mu1_model.transform(df_control_feat).withColumnRenamed(
            "prediction", "mu1_pred"
        )
        df_treatment_pred = self.mu0_model.transform(df_treatment_feat).withColumnRenamed(
            "prediction", "mu0_pred"
        )
        
        df_control_tau = df_control_pred.withColumn(
            "imputed_tau",
            F.col("mu1_pred") - F.col(self.label_col)
        )
        
        df_treatment_tau = df_treatment_pred.withColumn(
            "imputed_tau",
            F.col(self.label_col) - F.col("mu0_pred")
        )
        
        # Drop all intermediate feature columns to avoid conflicts
        cols_to_drop = ["features", "mu1_pred", "mu0_pred", "rawPrediction", "probability", "prediction"]
        
        for col in cols_to_drop:
            if col in df_control_tau.columns:
                df_control_tau = df_control_tau.drop(col)
            if col in df_treatment_tau.columns:
                df_treatment_tau = df_treatment_tau.drop(col)
        
        # Stage 3: Train CATE models
        print("🔥 Stage 3: Training CATE models (τ0 and τ1)")
        
        print("  Training τ0 (control CATE model)...")
        self.tau0_model, self.tau0_assembler = train_xgboost_model(
            df_control_tau, self.features, "imputed_tau", "regression", self.num_workers
        )
        
        print("  Training τ1 (treatment CATE model)...")
        self.tau1_model, self.tau1_assembler = train_xgboost_model(
            df_treatment_tau, self.features, "imputed_tau", "regression", self.num_workers
        )
        
        # Store feature importance for tau models (residual prediction)
        self.tau1_feature_importance = get_feature_importance(self.tau1_model, self.features, top_n=20)
        self.tau0_feature_importance = get_feature_importance(self.tau0_model, self.features, top_n=20)
        
        print("✅ X-Learner V2 training complete!")
        return self
    
    def predict_uplift(self, df: "DataFrame") -> "DataFrame":
        """
        Predict uplift scores using averaged tau predictions (no propensity weighting).
        """
        # Drop features column if it exists
        if "features" in df.columns:
            df = df.drop("features")
        
        # Add row ID for joining
        df_with_id = df.withColumn("_row_id", F.monotonically_increasing_id())
        
        # Get tau0 predictions
        df_tau0 = self.tau0_assembler.transform(df_with_id)
        df_tau0 = self.tau0_model.transform(df_tau0).withColumnRenamed(
            "prediction", "tau0_pred"
        )
        df_tau0_id = df_tau0.select("_row_id", "tau0_pred")
        
        # Get tau1 predictions
        df_tau1 = self.tau1_assembler.transform(df_with_id)
        df_tau1 = self.tau1_model.transform(df_tau1).withColumnRenamed(
            "prediction", "tau1_pred"
        )
        df_tau1_id = df_tau1.select("_row_id", "tau1_pred")
        
        # Join and calculate average uplift
        df_result = df_with_id
        df_result = df_result.join(df_tau0_id, "_row_id", "left")
        df_result = df_result.join(df_tau1_id, "_row_id", "left")
        
        # Use average of tau0 and tau1 as uplift score
        df_result = df_result.withColumn(
            "uplift_score",
            (F.col("tau0_pred") + F.col("tau1_pred")) / 2.0
        )
        
        # Drop temporary columns
        df_result = df_result.drop("_row_id", "tau0_pred", "tau1_pred")
        
        return df_result


# COMMAND ----------

# COMPARISON SCRIPT - Train/Validation Split and Model Comparison
# Train across ALL channels (channel is a one-hot encoded feature)

print("="*80)
print("🎯 UPLIFT MODEL COMPARISON: T-Learner vs X-Learner vs X-Learner V2")
print(f"📊 Using COMPREHENSIVE V5 FEATURE SET:")
print(f"   - {len(numerical_features)} numerical features")
print(f"   - {len(categorical_binary_features)} binary categorical features")
print(f"   - Total features: {len(all_features)}")
print("📊 Includes: Platform, 90-day metrics, visitor flags, CX360 features, and more")
print("📊 Training models across ALL channels (channel as feature)")
print("="*80)

# Use all data (no channel filtering)
print(f"\n📊 Dataset Summary:")
print("   Loading and counting data (this triggers Spark execution)...")

# Optimize: Get all counts in one aggregation instead of 3 separate operations
count_df = df_spark.groupBy().agg(
    F.count("*").alias("total"),
    F.sum(F.when(F.col(treatment_col) == 1, 1).otherwise(0)).alias("treatment"),
    F.sum(F.when(F.col(treatment_col) == 0, 1).otherwise(0)).alias("control")
).collect()[0]

total_count = count_df["total"]
treatment_count = count_df["treatment"]
control_count = count_df["control"]

print(f"Total samples: {total_count}")
print(f"Treatment: {treatment_count}, Control: {control_count}")

# Split into train (90%) and validation (10%)
print("\n🔪 Splitting data: 90% train, 10% validation")
df_train, df_val = df_spark.randomSplit([0.9, 0.1], seed=42)

train_count = df_train.count()
val_count = df_val.count()

print(f"Training set: {train_count} samples")
print(f"Validation set: {val_count} samples")

# Cache for performance
df_train.cache()
df_val.cache()

# Store comparison results
comparison_results = []

# Model 1: T-Learner
print("\n" + "="*80)
print("📈 Model 1: T-LEARNER (Two-Model Approach)")
print("="*80)

tlearner = TLearner(
    features=all_features,
    label_col=label_col,
    treatment_col=treatment_col,
    num_workers=2
)

tlearner.fit(df_train)

# Predict on validation set
print("🔮 Predicting on validation set...")
df_val_tlearner = tlearner.predict_uplift(df_val)

# Calculate Qini score
print("📊 Calculating Qini score...")
df_eval_tlearner = df_val_tlearner.select(
    label_col, "uplift_score", treatment_col
).toPandas()

qini_tlearner = calculate_qini_score(
    y_true=df_eval_tlearner[label_col].values,
    uplift=df_eval_tlearner["uplift_score"].values,
    treatment=df_eval_tlearner[treatment_col].values
)

avg_uplift_tlearner = df_eval_tlearner["uplift_score"].mean()
positive_uplift_pct_tlearner = (df_eval_tlearner["uplift_score"] > 0).mean()

print(f"\n✅ T-Learner Results:")
print(f"   Qini Score: {qini_tlearner:.4f}")
print(f"   Average Uplift: {avg_uplift_tlearner:.4f}")
print(f"   Positive Uplift %: {positive_uplift_pct_tlearner:.2%}")

comparison_results.append({
    "Model": "T-Learner",
    "Qini_Score": round(qini_tlearner, 4),
    "Avg_Uplift": round(avg_uplift_tlearner, 4),
    "Positive_Uplift_Pct": round(positive_uplift_pct_tlearner, 4),
})

# Model 2: X-Learner (Full)
print("\n" + "="*80)
print("📈 Model 2: X-LEARNER (Full with Propensity Weighting)")
print("="*80)

xlearner_full = XLearnerPySpark(
    features=all_features,
    label_col=label_col,
    treatment_col=treatment_col,
    num_workers=2
)

xlearner_full.fit(df_train)

# Predict on validation set
print("🔮 Predicting on validation set...")
df_val_xlearner = xlearner_full.predict_uplift(df_val)

# Calculate Qini score
print("📊 Calculating Qini score...")
df_eval_xlearner = df_val_xlearner.select(
    label_col, "uplift_score", treatment_col
).toPandas()

qini_xlearner = calculate_qini_score(
    y_true=df_eval_xlearner[label_col].values,
    uplift=df_eval_xlearner["uplift_score"].values,
    treatment=df_eval_xlearner[treatment_col].values
)

avg_uplift_xlearner = df_eval_xlearner["uplift_score"].mean()
positive_uplift_pct_xlearner = (df_eval_xlearner["uplift_score"] > 0).mean()

print(f"\n✅ X-Learner Results:")
print(f"   Qini Score: {qini_xlearner:.4f}")
print(f"   Average Uplift: {avg_uplift_xlearner:.4f}")
print(f"   Positive Uplift %: {positive_uplift_pct_xlearner:.2%}")

comparison_results.append({
    "Model": "X-Learner (Full)",
    "Qini_Score": round(qini_xlearner, 4),
    "Avg_Uplift": round(avg_uplift_xlearner, 4),
    "Positive_Uplift_Pct": round(positive_uplift_pct_xlearner, 4),
})

# Model 3: X-Learner V2 (Simplified)
print("\n" + "="*80)
print("📈 Model 3: X-LEARNER V2 (Simplified - Average Tau)")
print("="*80)

xlearner_v2 = XLearnerV2(
    features=all_features,
    label_col=label_col,
    treatment_col=treatment_col,
    num_workers=2
)

xlearner_v2.fit(df_train)

# Predict on validation set
print("🔮 Predicting on validation set...")
df_val_xlearner_v2 = xlearner_v2.predict_uplift(df_val)

# Calculate Qini score
print("📊 Calculating Qini score...")
df_eval_xlearner_v2 = df_val_xlearner_v2.select(
    label_col, "uplift_score", treatment_col
).toPandas()

qini_xlearner_v2 = calculate_qini_score(
    y_true=df_eval_xlearner_v2[label_col].values,
    uplift=df_eval_xlearner_v2["uplift_score"].values,
    treatment=df_eval_xlearner_v2[treatment_col].values
)

avg_uplift_xlearner_v2 = df_eval_xlearner_v2["uplift_score"].mean()
positive_uplift_pct_xlearner_v2 = (df_eval_xlearner_v2["uplift_score"] > 0).mean()

print(f"\n✅ X-Learner V2 Results:")
print(f"   Qini Score: {qini_xlearner_v2:.4f}")
print(f"   Average Uplift: {avg_uplift_xlearner_v2:.4f}")
print(f"   Positive Uplift %: {positive_uplift_pct_xlearner_v2:.2%}")

comparison_results.append({
    "Model": "X-Learner V2 (Simplified)",
    "Qini_Score": round(qini_xlearner_v2, 4),
    "Avg_Uplift": round(avg_uplift_xlearner_v2, 4),
    "Positive_Uplift_Pct": round(positive_uplift_pct_xlearner_v2, 4),
})

# Display final comparison
print("\n" + "="*80)
print("🏆 FINAL COMPARISON RESULTS")
print("="*80)

comparison_df = pd.DataFrame(comparison_results)
comparison_df = comparison_df.sort_values("Qini_Score", ascending=False)

print("\n" + comparison_df.to_string(index=False))

# Find best model
best_model = comparison_df.iloc[0]["Model"]
best_qini = comparison_df.iloc[0]["Qini_Score"]

print(f"\n🥇 **Winner**: {best_model} with Qini Score = {best_qini:.4f}")

# Calculate improvements
if len(comparison_df) > 1:
    baseline_qini = comparison_df[comparison_df["Model"] == "T-Learner"]["Qini_Score"].values[0]
    if best_model != "T-Learner" and baseline_qini != 0:
        improvement = ((best_qini - baseline_qini) / abs(baseline_qini) * 100)
        print(f"📈 Improvement over T-Learner baseline: {improvement:.1f}%")

# Save results to Snowflake
results_spark_df = spark.createDataFrame(comparison_df)
results_spark_df = results_spark_df.withColumn("evaluated_at", F.current_timestamp())
results_spark_df = results_spark_df.withColumn("train_samples", F.lit(train_count))
results_spark_df = results_spark_df.withColumn("val_samples", F.lit(val_count))
results_spark_df = results_spark_df.withColumn("num_numerical_features", F.lit(len(numerical_features)))
results_spark_df = results_spark_df.withColumn("num_categorical_binary_features", F.lit(len(categorical_binary_features)))
results_spark_df = results_spark_df.withColumn("num_total_features", F.lit(len(all_features)))
results_spark_df = results_spark_df.withColumn("feature_set", F.lit("comprehensive_v5_binary_categorical"))

results_spark_df.write.format("snowflake") \
    .options(**OPTIONS) \
    .option("dbtable", "proddb.fionafan.uplift_model_comparison_results_comprehensive_v5") \
    .mode("append") \
    .save()

print("\n✅ Comparison results saved to Snowflake: proddb.fionafan.uplift_model_comparison_results_comprehensive_v5")
print(f"✅ Total features: {len(all_features)}")
print(f"   - Numerical features: {len(numerical_features)}")
print(f"   - Binary categorical features: {len(categorical_binary_features)}")
print(f"\n📋 Categorical Features Encoded to Binary:")
print(f"   - Platform (3), Traffic Type RT (6), Traffic Type MRS (5)")
print(f"   - Browser (6), Country (5), Lifestage (8)")
print(f"   - Recency Frequency (4), Order Recency (4), Favorite Cuisine (14)")
print(f"\n📋 Feature Categories Included:")
print(f"   - Platform indicators: IS_ANDROID, IS_IOS, IS_WEB")
print(f"   - 28 & 90 day device metrics with address entry flags")
print(f"   - Visitor type flags (28 & 90 days)")
print(f"   - Consumer order features by platform (28 & 90 days)")
print(f"   - Deep link & Singular event metrics")
print(f"   - CX360 consumer lifetime features (ordering patterns, lifestage, VP, etc.)")
print(f"   - Binary categorical: Platform, Traffic Type, Browser, Country, Lifestage, Cuisine, etc.")

# Unpersist cached dataframes
df_train.unpersist()
df_val.unpersist()

# COMMAND ----------

# ====================================================================
# FEATURE IMPORTANCE ANALYSIS
# ====================================================================

print("\n" + "="*80)
print("📊 FEATURE IMPORTANCE ANALYSIS")
print("="*80)

# T-Learner: Treatment Model Feature Importance
print("\n🔹 T-LEARNER - Treatment Model (Top 20 Features):")
print("-" * 80)
print(tlearner.treatment_feature_importance.to_string(index=False))

# X-Learner Full: τ1 Model Feature Importance (predicts treatment effect)
print("\n\n🔹 X-LEARNER (Full) - τ1 Model / Treatment CATE (Top 20 Features):")
print("-" * 80)
print(xlearner_full.tau1_feature_importance.to_string(index=False))

# X-Learner V2: τ1 Model Feature Importance
print("\n\n🔹 X-LEARNER V2 - τ1 Model / Treatment CATE (Top 20 Features):")
print("-" * 80)
print(xlearner_v2.tau1_feature_importance.to_string(index=False))

print("\n" + "="*80)

# COMMAND ----------

# ====================================================================
# PLOT QINI CURVES
# ====================================================================

print("\n📊 Generating Qini Curve Comparison Plot...")

# Create three separate subplots to avoid matplotlib compatibility issues
fig, axes = plt.subplots(1, 3, figsize=(18, 5))

# Plot T-Learner
plot_qini_curve(
    df_eval_tlearner[label_col].values,
    df_eval_tlearner["uplift_score"].values,
    df_eval_tlearner[treatment_col].values,
    ax=axes[0],
    perfect=False
)
axes[0].set_title(f"T-Learner\nQini AUC = {qini_tlearner:.4f}", fontsize=12, fontweight='bold')
axes[0].grid(True, alpha=0.3)

# Plot X-Learner Full
plot_qini_curve(
    df_eval_xlearner[label_col].values,
    df_eval_xlearner["uplift_score"].values,
    df_eval_xlearner[treatment_col].values,
    ax=axes[1],
    perfect=False
)
axes[1].set_title(f"X-Learner (Full)\nQini AUC = {qini_xlearner:.4f}", fontsize=12, fontweight='bold')
axes[1].grid(True, alpha=0.3)

# Plot X-Learner V2
plot_qini_curve(
    df_eval_xlearner_v2[label_col].values,
    df_eval_xlearner_v2["uplift_score"].values,
    df_eval_xlearner_v2[treatment_col].values,
    ax=axes[2],
    perfect=False
)
axes[2].set_title(f"X-Learner V2\nQini AUC = {qini_xlearner_v2:.4f}", fontsize=12, fontweight='bold')
axes[2].grid(True, alpha=0.3)

# Overall title
fig.suptitle(f"Qini Curve Comparison - Comprehensive V5 ({final_feature_count} features)", 
             fontsize=14, fontweight='bold', y=1.02)

# Save plot
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
plot_dir = "/dbfs/FileStore/plots"
os.makedirs(plot_dir, exist_ok=True)
plot_path = f"{plot_dir}/qini_curve_comparison_v5_{timestamp}.png"
plt.tight_layout()
plt.savefig(plot_path, dpi=150, bbox_inches='tight')
print(f"✅ Qini curve saved to: {plot_path}")
print(f"   View at: /FileStore/plots/qini_curve_comparison_v5_{timestamp}.png")

plt.show()

# ====================================================================
# SUCCINCT PERFORMANCE REPORT
# ====================================================================

print("\n" + "="*80)
print("📊 FINAL PERFORMANCE SUMMARY")
print("="*80)

# Feature Summary
print(f"\n1️⃣  FEATURE SET:")
print(f"    Total features: {final_feature_count}")
print(f"    └─ Numerical: {len(numerical_features)}")
print(f"    └─ Binary categorical: {len(categorical_binary_features)}")
print(f"       (Platform: 3, Traffic: 11, Browser: 6, Country: 5,")
print(f"        Lifestage: 8, Recency: 8, Cuisine: 14)")

# Model Performance
print(f"\n2️⃣  MODEL PERFORMANCE (Qini AUC):")
for result in comparison_results:
    model_name = result["Model"].ljust(25)
    qini = result["Qini_Score"]
    avg_uplift = result["Avg_Uplift"]
    pos_pct = result["Positive_Uplift_Pct"]
    print(f"    {model_name} Qini: {qini:.4f}  |  Avg Uplift: {avg_uplift:+.4f}  |  Pos%: {pos_pct:.1%}")

# Winner
print(f"\n3️⃣  BEST MODEL: {best_model} (Qini = {best_qini:.4f})")
if len(comparison_df) > 1:
    baseline_qini = comparison_df[comparison_df["Model"] == "T-Learner"]["Qini_Score"].values[0]
    if best_model != "T-Learner" and baseline_qini != 0:
        improvement = ((best_qini - baseline_qini) / abs(baseline_qini) * 100)
        print(f"    Improvement vs T-Learner: +{improvement:.1f}%")

print(f"\n💾 Results saved to: proddb.fionafan.uplift_model_comparison_results_comprehensive_v5")
print(f"📊 Qini curve saved to: {plot_path}")
print("="*80)

# COMMAND ----------

