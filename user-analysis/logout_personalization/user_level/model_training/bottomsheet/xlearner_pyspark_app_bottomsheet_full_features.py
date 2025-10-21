# Databricks notebook source
# MAGIC %pip install scikit-uplift xgboost

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, ArrayType, StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT
from xgboost.spark import SparkXGBClassifier, SparkXGBRegressor
import numpy as np
import pandas as pd
from typing import Dict, Tuple
from sklift.metrics import qini_auc_score

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

# Define features - including real-time channel as one-hot encoded variables
features = [
    # Historical device-level features
    "DEVICE_ID_HAS_RECORDS_LAST_180_DAYS",
    "HAS_ASSOCIATED_CONSUMER_ID",
    "DEVICE_ID_IS_ACTIVE_RECENT_28_DAYS",
    "DEVICE_ID_SESSIONS_RECENT_28_DAYS",
    "DEVICE_ID_ORDERS_RECENT_28_DAYS",
    "DEVICE_ID_HAS_LOGGED_OUT_ADDRESS_ENTRY_RECENT_28_DAYS",
    "DEVICE_ID_HAS_LOGGED_IN_RECENT_28_DAYS",
    "DEVICE_ID_PLACED_ORDER_MOST_RECENT_28_DAYS",
    
    # Most recent session channels (historical)
    "DEVICE_ID_CHANNEL_IS_DIRECT_MOST_RECENT_SESSION",
    "DEVICE_ID_CHANNEL_IS_ORGANIC_SEARCH_MOST_RECENT_SESSION",
    "DEVICE_ID_CHANNEL_IS_PAID_MEDIA_MOST_RECENT_SESSION",
    "DEVICE_ID_CHANNEL_IS_EMAIL_MOST_RECENT_SESSION",
    "DEVICE_ID_CHANNEL_IS_PARTNERS_MOST_RECENT_SESSION",
    "DEVICE_ID_CHANNEL_IS_AFFILIATE_MOST_RECENT_SESSION",
    
    # Real-time channels (one-hot encoded - the channel at exposure time)
    "DEVICE_ID_CHANNEL_IS_DIRECT_REAL_TIME",
    "DEVICE_ID_CHANNEL_IS_ORGANIC_SEARCH_REAL_TIME",
    "DEVICE_ID_CHANNEL_IS_PAID_MEDIA_REAL_TIME",
    "DEVICE_ID_CHANNEL_IS_EMAIL_REAL_TIME",
    "DEVICE_ID_CHANNEL_IS_PARTNERS_REAL_TIME",
    "DEVICE_ID_CHANNEL_IS_AFFILIATE_REAL_TIME",
    
    # Most recent session behavior
    "DEVICE_ID_RECENCY_MOST_RECENT_SESSION",
    "DEVICE_ID_PLACED_ORDER_MOST_RECENT_SESSION",
    "DEVICE_ID_IS_AUTO_LOGGED_IN_MOST_RECENT_SESSION",
    
    # Consumer-level order flags (28 days)
    "CONSUMER_ID_HAS_IN_APP_ORDERS_RECENT_28_DAYS",
    "CONSUMER_ID_HAS_MOBILE_ORDERS_RECENT_28_DAYS",
    "CONSUMER_ID_HAS_DESKTOP_ORDERS_RECENT_28_DAYS",
    "CONSUMER_ID_HAS_ORDERS_RECENT_28_DAYS",
    
    # Consumer-level order flags (90 days)
    "CONSUMER_ID_HAS_IN_APP_ORDERS_RECENT_90_DAYS",
    "CONSUMER_ID_HAS_MOBILE_ORDERS_RECENT_90_DAYS",
    "CONSUMER_ID_HAS_DESKTOP_ORDERS_RECENT_90_DAYS",
    "CONSUMER_ID_HAS_ORDERS_RECENT_90_DAYS",
    
    # Consumer lifetime features
    "CONSUMER_ID_HAS_PLACED_ORDER_LIFETIME",
    "CONSUMER_ID_TENANCY_LIFETIME",
    
    # Consumer tenancy buckets
    "CONSUMER_ID_TENANCY_0_28_LIFETIME",
    "CONSUMER_ID_TENANCY_29_TO_90_LIFETIME",
    "CONSUMER_ID_TENANCY_91_TO_180_LIFETIME",
    "CONSUMER_ID_TENANCY_181_OR_MORE_LIFETIME",
    
    # Exposure time features (hour of day)
    "DEVICE_ID_FIRST_EXPOSURE_TIME_IS_0_TO_3_REAL_TIME",
    "DEVICE_ID_FIRST_EXPOSURE_TIME_IS_4_TO_7_REAL_TIME",
    "DEVICE_ID_FIRST_EXPOSURE_TIME_IS_8_TO_11_REAL_TIME",
    "DEVICE_ID_FIRST_EXPOSURE_TIME_IS_12_TO_15_REAL_TIME",
    "DEVICE_ID_FIRST_EXPOSURE_TIME_IS_16_TO_19_REAL_TIME",
    "DEVICE_ID_FIRST_EXPOSURE_TIME_IS_20_TO_23_REAL_TIME",
]

label_col = "DEVICE_ID_PLACED_AN_ORDER_REAL_TIME"
treatment_col = "DEVICE_ID_SHOW_APP_DOWNLOAD_BOTTOM_SHEET_REAL_TIME"

# COMMAND ----------

# Load data from Snowflake
query = """
SELECT * 
FROM proddb.fionafan.logged_out_personalization_training_v4
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

# Convert all feature columns to double type and handle nulls
for feat in features:
    df_spark = df_spark.withColumn(feat, F.coalesce(F.col(feat).cast("double"), F.lit(0.0)))

# Cache the dataframe for reuse
df_spark.cache()

print(f"Total records loaded: {df_spark.count()}")

# COMMAND ----------

def train_xgboost_model(
    df: "DataFrame",
    features: list,
    label_col: str,
    task: str = "classification",
    num_workers: int = 2
) -> Tuple["Model", "VectorAssembler"]:
    """
    Train an XGBoost model using PySpark.
    
    Args:
        df: PySpark DataFrame
        features: List of feature column names
        label_col: Label column name
        task: 'classification' or 'regression'
        num_workers: Number of workers for distributed training
    
    Returns:
        Trained model and vector assembler
    """
    # Drop features column if it already exists to avoid conflicts
    if "features" in df.columns:
        df = df.drop("features")
    
    # Assemble features into vector
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
            max_depth=6,
            learning_rate=0.1,
            device="cpu",  # Change to "cuda" if GPU available
            random_state=42,
        )
    else:  # regression for uplift model
        model = SparkXGBRegressor(
            features_col="features",
            label_col=label_col,
            num_workers=num_workers,
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            device="cpu",  # Change to "cuda" if GPU available
            random_state=42,
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
    
    def __init__(self, features: list, label_col: str, treatment_col: str, num_workers: int = 2):
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
        ).drop("features", "mu1_pred")  # Drop to avoid conflicts in next stage
        
        # For treatment: τ̂T = Y - μ0(X) (what would happen if not treated)
        df_treatment_tau = df_treatment_pred.withColumn(
            "imputed_tau",
            F.col(self.label_col) - F.col("mu0_pred")
        ).drop("features", "mu0_pred")  # Drop to avoid conflicts in next stage
        
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
    
    def __init__(self, features: list, label_col: str, treatment_col: str, num_workers: int = 2):
        self.features = features
        self.label_col = label_col
        self.treatment_col = treatment_col
        self.num_workers = num_workers
        
        self.treatment_model = None
        self.control_model = None
        self.treatment_assembler = None
        self.control_assembler = None
    
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
    
    def __init__(self, features: list, label_col: str, treatment_col: str, num_workers: int = 2):
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
        ).drop("features", "mu1_pred")
        
        df_treatment_tau = df_treatment_pred.withColumn(
            "imputed_tau",
            F.col(self.label_col) - F.col("mu0_pred")
        ).drop("features", "mu0_pred")
        
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
print(f"📊 Using FULL FEATURE SET: {len(features)} features")
print("📊 Training models across ALL channels (channel as feature)")
print("="*80)

# Use all data (no channel filtering)
print(f"\n📊 Dataset Summary:")
total_count = df_spark.count()
treatment_count = df_spark.filter(F.col(treatment_col) == 1).count()
control_count = df_spark.filter(F.col(treatment_col) == 0).count()

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
    features=features,
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
    features=features,
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
    features=features,
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
results_spark_df = results_spark_df.withColumn("num_features", F.lit(len(features)))
results_spark_df = results_spark_df.withColumn("feature_set", F.lit("with_channel_one_hot"))

results_spark_df.write.format("snowflake") \
    .options(**OPTIONS) \
    .option("dbtable", "proddb.fionafan.uplift_model_comparison_results_full_features") \
    .mode("append") \
    .save()

print("\n✅ Comparison results saved to Snowflake: proddb.fionafan.uplift_model_comparison_results_full_features")
print(f"✅ Total features used: {len(features)} (includes 6 real-time channel one-hot flags)")

# Unpersist cached dataframes
df_train.unpersist()
df_val.unpersist()

# COMMAND ----------

