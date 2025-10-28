# Databricks notebook source
import pandas as pd
import numpy as np
from lightgbm import LGBMClassifier
from sklearn.metrics import roc_auc_score, log_loss
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


# COMMAND ----------


your_scope = "fionafan-scope"
user = dbutils.secrets.get(scope=your_scope, key="snowflake-user")
password = dbutils.secrets.get(scope=your_scope, key="snowflake-password")
default_schema = "fionafan"


# COMMAND ----------


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

def get_data_legacy(q):
    with snowflake.connector.connect(**OPTIONS) as ctx:
        data = pd.read_sql(q, ctx)
        data.columns = [i.lower() for i in data.columns]
        return data

def get_data(q):
    with snowflake.connector.connect(**OPTIONS) as ctx:
        cur = ctx.cursor()
        cur.execute(q)
        data = cur.fetch_pandas_all()
    
    data.columns = [i.lower() for i in data.columns]
    return data


def execute_query(q):
    with snowflake.connector.connect(**PARAMS,  client_session_keep_alive=True) as ctx:
        cursor = ctx.cursor()
        cursor.execute(q)

# COMMAND ----------



# Use the static historical data for training
query = """
SELECT * 
FROM proddb.markwu.logged_out_personalization_web_device_id_v1
"""

df = (
    spark.read.format("snowflake")
    .options(**OPTIONS)
    .option("query", query)
    .load()
    .toPandas()
)

df.columns = [x for x in df.columns]

channel_flags = {
    "Direct": "DEVICE_ID_CHANNEL_IS_DIRECT_REAL_TIME",
    "Organic_Search": "DEVICE_ID_CHANNEL_IS_ORGANIC_SEARCH_REAL_TIME",
    "Paid_Media": "DEVICE_ID_CHANNEL_IS_PAID_MEDIA_REAL_TIME",
    "Email": "DEVICE_ID_CHANNEL_IS_EMAIL_REAL_TIME",
    "Partners": "DEVICE_ID_CHANNEL_IS_PARTNERS_REAL_TIME",
    "Affiliate": "DEVICE_ID_CHANNEL_IS_AFFILIATE_REAL_TIME",
}

features = [
    "DEVICE_ID_HAS_RECORDS_LAST_180_DAYS",
    "HAS_ASSOCIATED_CONSUMER_ID",
    "DEVICE_ID_IS_ACTIVE_RECENT_28_DAYS",
    "DEVICE_ID_PLACED_ORDER_MOST_RECENT_28_DAYS",
    "DEVICE_ID_CHANNEL_IS_DIRECT_MOST_RECENT_SESSION",
    "DEVICE_ID_CHANNEL_IS_ORGANIC_SEARCH_MOST_RECENT_SESSION",
    "DEVICE_ID_CHANNEL_IS_PAID_MEDIA_MOST_RECENT_SESSION",
    "DEVICE_ID_CHANNEL_IS_EMAIL_MOST_RECENT_SESSION",
    "DEVICE_ID_CHANNEL_IS_PARTNERS_MOST_RECENT_SESSION",
    "DEVICE_ID_CHANNEL_IS_AFFILIATE_MOST_RECENT_SESSION",
    "DEVICE_ID_PLACED_ORDER_MOST_RECENT_SESSION",
    "DEVICE_ID_IS_AUTO_LOGGED_IN_MOST_RECENT_SESSION",
    "CONSUMER_ID_HAS_IN_APP_ORDERS_RECENT_28_DAYS",
    "CONSUMER_ID_HAS_MOBILE_ORDERS_RECENT_28_DAYS",
    "CONSUMER_ID_HAS_DESKTOP_ORDERS_RECENT_28_DAYS",
    "CONSUMER_ID_HAS_ORDERS_RECENT_28_DAYS",
    "CONSUMER_ID_HAS_IN_APP_ORDERS_RECENT_90_DAYS",
    "CONSUMER_ID_HAS_MOBILE_ORDERS_RECENT_90_DAYS",
    "CONSUMER_ID_HAS_DESKTOP_ORDERS_RECENT_90_DAYS",
    "CONSUMER_ID_HAS_ORDERS_RECENT_90_DAYS",
    "CONSUMER_ID_HAS_PLACED_ORDER_LIFETIME",
    "CONSUMER_ID_TENANCY_0_28_LIFETIME",
    "CONSUMER_ID_TENANCY_29_TO_90_LIFETIME",
    "CONSUMER_ID_TENANCY_91_TO_180_LIFETIME",
    "CONSUMER_ID_TENANCY_181_OR_MORE_LIFETIME",
]

uplift_threshold = 0.01
results = []
channel_models = {}
uplift_feature_importance_summary = {}  # Initialize once before loops


# COMMAND ----------



# COMMAND ----------

all_flags = list(channel_flags.keys()) + ["All", "Null"]

# COMMAND ----------

all_flags

# COMMAND ----------

channel_name = 'Direct'

# COMMAND ----------



# COMMAND ----------



print(f"🔄 Evaluating channel: {channel_name}")

if channel_name == "All":
    df_channel = df.copy()  # ✅ Updated: include every row
elif channel_name == "Null":
    df_channel = df[df[list(channel_flags.values())].sum(axis=1) == 0].copy()
else:
    df_channel = df[df[channel_flags[channel_name]] == 1].copy()

df_treat = df_channel[
    df_channel["DEVICE_ID_SHOW_GOOGLE_ONE_TAP_MODAL_REAL_TIME"] == True
]
df_control = df_channel[
    df_channel["DEVICE_ID_SHOW_GOOGLE_ONE_TAP_MODAL_REAL_TIME"] == False
]

if len(df_treat) < 300 or len(df_control) < 300:
    print(
        f"⚠️ Skipping {channel_name} due to small sample size (T: {len(df_treat)}, C: {len(df_control)})"
    )
    


# COMMAND ----------

df_treat

# COMMAND ----------


# Ensure numeric features and labels
X_treat = df_treat[features].apply(pd.to_numeric, errors="coerce").fillna(0)
y_treat = df_treat["DEVICE_ID_PLACED_AN_ORDER_REAL_TIME"].astype(int)
X_control = df_control[features].apply(pd.to_numeric, errors="coerce").fillna(0)
y_control = df_control["DEVICE_ID_PLACED_AN_ORDER_REAL_TIME"].astype(int)

treat_model = LGBMClassifier(
    n_estimators=100,
    max_depth=-1,
    num_leaves=31,
    learning_rate=0.1,
    random_state=42,
)
control_model = LGBMClassifier(
    n_estimators=100,
    max_depth=-1,
    num_leaves=31,
    learning_rate=0.1,
    random_state=42,
)

treat_model.fit(X_treat, y_treat)
control_model.fit(X_control, y_control)

X_full = df_channel[features].apply(pd.to_numeric, errors="coerce").fillna(0)
df_channel["P_order_treat"] = treat_model.predict_proba(X_full)[:, 1]
df_channel["P_order_control"] = control_model.predict_proba(X_full)[:, 1]
df_channel["uplift_score"] = (
    df_channel["P_order_treat"] - df_channel["P_order_control"]
)
df_channel["Model_Shows_Modal"] = df_channel["uplift_score"] > uplift_threshold

auc_treat = roc_auc_score(y_treat, treat_model.predict_proba(X_treat)[:, 1])
auc_control = roc_auc_score(y_control, control_model.predict_proba(X_control)[:, 1])
logloss_treat = log_loss(y_treat, treat_model.predict_proba(X_treat)[:, 1])
logloss_control = log_loss(y_control, control_model.predict_proba(X_control)[:, 1])

targeted = df_channel[df_channel["Model_Shows_Modal"]]
incremental_conversions = (
    targeted["P_order_treat"].sum() - targeted["P_order_control"].sum()
)
baseline_orders = df_channel["P_order_control"].sum()
new_orders = baseline_orders + incremental_conversions
total_sample = len(df_channel)
modal_targeted = len(targeted)
avg_cr_control = targeted["P_order_control"].mean()
avg_cr_treat = targeted["P_order_treat"].mean()

results.append(
    {
        "Channel": channel_name,
        "Sample Size": total_sample,
        "Targeted Users": modal_targeted,
        "Incremental Conversions": round(incremental_conversions, 1),
        "Baseline_Orders": round(baseline_orders, 1),
        "Projected_Orders": round(new_orders, 1),
        "Treat AUC": round(auc_treat, 3),
        "Control AUC": round(auc_control, 3),
        "Treat LogLoss": round(logloss_treat, 3),
        "Control LogLoss": round(logloss_control, 3),
        "CR Control": round(avg_cr_control, 4),
        "CR Treat": round(avg_cr_treat, 4),
    }
)

feature_importance = pd.DataFrame(
    {
        "Feature": X_full.columns,
        "Importance_Treat": treat_model.feature_importances_,
        "Importance_Control": control_model.feature_importances_,
    }
)
feature_importance["Normalized_Treat"] = (
    feature_importance["Importance_Treat"]
    / feature_importance["Importance_Treat"].sum()
)
feature_importance["Normalized_Control"] = (
    feature_importance["Importance_Control"]
    / feature_importance["Importance_Control"].sum()
)
feature_importance["Uplift_Importance"] = abs(
    feature_importance["Normalized_Treat"]
    - feature_importance["Normalized_Control"]
)
feature_importance.sort_values("Uplift_Importance", ascending=False, inplace=True)

uplift_feature_importance_summary[channel_name] = feature_importance.reset_index(
    drop=True
)

channel_models[channel_name] = {
    "treat_model": treat_model,
    "control_model": control_model,
}

print(f"\n=== Channel: {channel_name} ===")
print(f"Sample Size: {total_sample}")
print(f"Targeted Users: {modal_targeted}")
print(f"Incremental Conversions: {incremental_conversions:.1f}")
print(f"AUC (Treat): {auc_treat:.3f} | AUC (Control): {auc_control:.3f}")
print(
    f"LogLoss (Treat): {logloss_treat:.3f} | LogLoss (Control): {logloss_control:.3f}"
)
print(
    f"Targeted CR (Control): {avg_cr_control:.3%} | CR (Treat): {avg_cr_treat:.3%}"
)
# display(feature_importance[['Feature', 'Normalized_Treat', 'Normalized_Control', 'Uplift_Importance']])


# COMMAND ----------

results

# COMMAND ----------



# ---------------------------------------------
# Final Results Summary
# ---------------------------------------------
results_df = pd.DataFrame(results)
print("\n✅ Channel-Specific Model Results")
print(results_df)

# Print uplift-driving feature importance summary across all channels
print("\n✅ Uplift-Driving Feature Importance Summary Across All Channels")
for channel, importance in uplift_feature_importance_summary.items():
    print(f"\n===== {channel} Channel =====")
    print(importance)

q_template = """
SELECT * 
FROM seo.public.logged_out_personalization_historical_web_device_id_for_labelling
WHERE decile_1_to_10 = {}
"""

target_table_name = "proddb.fionafan.personalization_data_v1_mw_direct"

# Clear the table if it exists
clear_table_query = f"Drop table if exists {target_table_name}"
spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(OPTIONS, clear_table_query)
print(f"Table {target_table_name} cleared")

for decile in range(1, 5):
    print(f"🚀 Processing decile {decile}/10")

    q = q_template.format(decile)

    df_with_uplift = (
        spark.read.format("snowflake")
        .options(**OPTIONS)
        .option("query", q)
        .load()
        .toPandas()
    )

    # Loop and compute uplift for every channel
    for channel in channel_models:
        print(f"📈 Scoring uplift for channel: {channel}")

        # Predict uplift directly on raw data (no flag reset)
        treat_model = channel_models[channel]["treat_model"]
        control_model = channel_models[channel]["control_model"]

        df_with_uplift[f"uplift_{channel}"] = (
            treat_model.predict_proba(df_with_uplift[features])[:, 1]
            - control_model.predict_proba(df_with_uplift[features])[:, 1]
        )

    # Convert the Pandas DataFrame back to a Spark DataFrame
    spark_df_with_uplift = spark.createDataFrame(df_with_uplift).withColumn(
        "updated_at", current_timestamp()
    )
    print("Spark dataframe is created based on the result df")

    if decile == 1:
        # Generate the CREATE TABLE statement based on the DataFrame schema
        schema = spark_df_with_uplift.schema
        columns = ", ".join(
            [
                f"{field.name} {field.dataType.simpleString().upper()}"
                for field in schema
            ]
        )
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {target_table_name} (
            {columns}
        )
        """
        print(create_table_query)

    # Write the DataFrame to Snowflake
    spark_df_with_uplift.write.format("snowflake").options(**OPTIONS).option(
        "dbtable", target_table_name
    ).mode("append").save()

print("🎉 All deciles processed and uploaded successfully!")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

