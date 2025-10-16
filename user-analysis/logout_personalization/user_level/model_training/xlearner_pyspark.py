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

# Define channels and features
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

label_col = "DEVICE_ID_PLACED_AN_ORDER_REAL_TIME"
treatment_col = "DEVICE_ID_SHOW_GOOGLE_ONE_TAP_MODAL_REAL_TIME"

# COMMAND ----------

# Load data from Snowflake
query = """
SELECT * 
FROM proddb.markwu.logged_out_personalization_web_device_id_v1
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
# Focus on Direct channel only

print("="*80)
print("🎯 UPLIFT MODEL COMPARISON: T-Learner vs X-Learner vs X-Learner V2")
print("="*80)

# Filter to Direct channel only
channel_name = "Direct"
df_channel = df_spark.filter(F.col(channel_flags[channel_name]) == 1)

print(f"\n📊 Channel: {channel_name}")
total_count = df_channel.count()
treatment_count = df_channel.filter(F.col(treatment_col) == 1).count()
control_count = df_channel.filter(F.col(treatment_col) == 0).count()

print(f"Total samples: {total_count}")
print(f"Treatment: {treatment_count}, Control: {control_count}")

# Split into train (90%) and validation (10%)
print("\n🔪 Splitting data: 90% train, 10% validation")
df_train, df_val = df_channel.randomSplit([0.9, 0.1], seed=42)

train_count = df_train.count()
val_count = df_val.count()

print(f"Training set: {train_count} samples")
print(f"Validation set: {val_count} samples")

# Cache for performance
df_train.cache()
df_val.cache()

# COMMAND ----------

# Train all three models
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

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

# Display final comparison
print("\n" + "="*80)
print("🏆 FINAL COMPARISON RESULTS - DIRECT CHANNEL")
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
    if best_model != "T-Learner":
        improvement = ((best_qini - baseline_qini) / abs(baseline_qini) * 100) if baseline_qini != 0 else 0
        print(f"📈 Improvement over T-Learner baseline: {improvement:.1f}%")

# COMMAND ----------

# Save comparison results to Snowflake
results_spark_df = spark.createDataFrame(comparison_df)
results_spark_df = results_spark_df.withColumn("channel", F.lit(channel_name))
results_spark_df = results_spark_df.withColumn("evaluated_at", F.current_timestamp())
results_spark_df = results_spark_df.withColumn("train_samples", F.lit(train_count))
results_spark_df = results_spark_df.withColumn("val_samples", F.lit(val_count))

results_spark_df.write.format("snowflake") \
    .options(**OPTIONS) \
    .option("dbtable", "proddb.fionafan.uplift_model_comparison_results") \
    .mode("overwrite") \
    .save()

print("\n✅ Comparison results saved to Snowflake: proddb.fionafan.uplift_model_comparison_results")

# COMMAND ----------

# Plot both Qini and Uplift curves for visual comparison
print("\n📊 Plotting Qini and Uplift curves for each model...")

from sklift.viz import plot_qini_curve, plot_uplift_curve
from sklift.metrics import uplift_curve, uplift_auc_score
import matplotlib.pyplot as plt

# Since all dataframes have the same y_true and treatment (same validation set),
# we can use any of them for these values
y_true = df_eval_tlearner[label_col].values
treatment = df_eval_tlearner[treatment_col].values

# Calculate Uplift AUC scores
uplift_auc_tlearner = uplift_auc_score(
    y_true=y_true,
    uplift=df_eval_tlearner['uplift_score'].values,
    treatment=treatment
)
uplift_auc_xlearner = uplift_auc_score(
    y_true=y_true,
    uplift=df_eval_xlearner['uplift_score'].values,
    treatment=treatment
)
uplift_auc_xlearner_v2 = uplift_auc_score(
    y_true=y_true,
    uplift=df_eval_xlearner_v2['uplift_score'].values,
    treatment=treatment
)

print(f"\n📊 Uplift AUC Scores:")
print(f"   T-Learner: {uplift_auc_tlearner:.4f}")
print(f"   X-Learner (Full): {uplift_auc_xlearner:.4f}")
print(f"   X-Learner V2: {uplift_auc_xlearner_v2:.4f}")

# Plot Qini and Uplift curves - 3 models x 2 curve types
fig, axes = plt.subplots(2, 3, figsize=(20, 12))

# Row 1: Qini Curves for each model
# T-Learner
plot_qini_curve(
    y_true=y_true,
    uplift=df_eval_tlearner['uplift_score'].values,
    treatment=treatment,
    random=True,
    perfect=True,
    ax=axes[0, 0],
    name='T-Learner'
)
axes[0, 0].set_title(f'T-Learner - Qini Curve\nQini AUC: {qini_tlearner:.4f}', 
                     fontsize=12, fontweight='bold')
axes[0, 0].grid(True, alpha=0.3)

# X-Learner (Full)
plot_qini_curve(
    y_true=y_true,
    uplift=df_eval_xlearner['uplift_score'].values,
    treatment=treatment,
    random=True,
    perfect=True,
    ax=axes[0, 1],
    name='X-Learner (Full)'
)
axes[0, 1].set_title(f'X-Learner (Full) - Qini Curve\nQini AUC: {qini_xlearner:.4f}', 
                     fontsize=12, fontweight='bold')
axes[0, 1].grid(True, alpha=0.3)

# X-Learner V2
plot_qini_curve(
    y_true=y_true,
    uplift=df_eval_xlearner_v2['uplift_score'].values,
    treatment=treatment,
    random=True,
    perfect=True,
    ax=axes[0, 2],
    name='X-Learner V2'
)
axes[0, 2].set_title(f'X-Learner V2 - Qini Curve\nQini AUC: {qini_xlearner_v2:.4f}', 
                     fontsize=12, fontweight='bold')
axes[0, 2].grid(True, alpha=0.3)

# Row 2: Uplift Curves for each model
# T-Learner
plot_uplift_curve(
    y_true=y_true,
    uplift=df_eval_tlearner['uplift_score'].values,
    treatment=treatment,
    random=True,
    perfect=True,
    ax=axes[1, 0],
    name='T-Learner'
)
axes[1, 0].set_title(f'T-Learner - Uplift Curve\nUplift AUC: {uplift_auc_tlearner:.4f}', 
                     fontsize=12, fontweight='bold')
axes[1, 0].set_ylabel('Uplift (Treatment - Control rate)', fontsize=10)
axes[1, 0].grid(True, alpha=0.3)

# X-Learner (Full)
plot_uplift_curve(
    y_true=y_true,
    uplift=df_eval_xlearner['uplift_score'].values,
    treatment=treatment,
    random=True,
    perfect=True,
    ax=axes[1, 1],
    name='X-Learner (Full)'
)
axes[1, 1].set_title(f'X-Learner (Full) - Uplift Curve\nUplift AUC: {uplift_auc_xlearner:.4f}', 
                     fontsize=12, fontweight='bold')
axes[1, 1].set_ylabel('Uplift (Treatment - Control rate)', fontsize=10)
axes[1, 1].grid(True, alpha=0.3)

# X-Learner V2
plot_uplift_curve(
    y_true=y_true,
    uplift=df_eval_xlearner_v2['uplift_score'].values,
    treatment=treatment,
    random=True,
    perfect=True,
    ax=axes[1, 2],
    name='X-Learner V2'
)
axes[1, 2].set_title(f'X-Learner V2 - Uplift Curve\nUplift AUC: {uplift_auc_xlearner_v2:.4f}', 
                     fontsize=12, fontweight='bold')
axes[1, 2].set_ylabel('Uplift (Treatment - Control rate)', fontsize=10)
axes[1, 2].grid(True, alpha=0.3)

plt.suptitle('Qini and Uplift Curves Comparison - Direct Channel', 
             fontsize=16, fontweight='bold', y=0.995)
plt.tight_layout()
display(fig)

# COMMAND ----------

# Analyze uplift score distributions
print("\n📊 Analyzing uplift score distributions...")

fig, axes = plt.subplots(2, 3, figsize=(18, 10))

# Row 1: Histograms
axes[0, 0].hist(df_eval_tlearner['uplift_score'], bins=50, alpha=0.7, color='blue', edgecolor='black')
axes[0, 0].axvline(0, color='red', linestyle='--', linewidth=2, label='Zero Uplift')
axes[0, 0].set_title(f'T-Learner Uplift Distribution\nMean: {avg_uplift_tlearner:.4f}', fontweight='bold')
axes[0, 0].set_xlabel('Uplift Score')
axes[0, 0].set_ylabel('Frequency')
axes[0, 0].legend()
axes[0, 0].grid(True, alpha=0.3)

axes[0, 1].hist(df_eval_xlearner['uplift_score'], bins=50, alpha=0.7, color='green', edgecolor='black')
axes[0, 1].axvline(0, color='red', linestyle='--', linewidth=2, label='Zero Uplift')
axes[0, 1].set_title(f'X-Learner (Full) Uplift Distribution\nMean: {avg_uplift_xlearner:.4f}', fontweight='bold')
axes[0, 1].set_xlabel('Uplift Score')
axes[0, 1].set_ylabel('Frequency')
axes[0, 1].legend()
axes[0, 1].grid(True, alpha=0.3)

axes[0, 2].hist(df_eval_xlearner_v2['uplift_score'], bins=50, alpha=0.7, color='orange', edgecolor='black')
axes[0, 2].axvline(0, color='red', linestyle='--', linewidth=2, label='Zero Uplift')
axes[0, 2].set_title(f'X-Learner V2 Uplift Distribution\nMean: {avg_uplift_xlearner_v2:.4f}', fontweight='bold')
axes[0, 2].set_xlabel('Uplift Score')
axes[0, 2].set_ylabel('Frequency')
axes[0, 2].legend()
axes[0, 2].grid(True, alpha=0.3)

# Row 2: Box plots
box_data = [
    df_eval_tlearner['uplift_score'].values,
    df_eval_xlearner['uplift_score'].values,
    df_eval_xlearner_v2['uplift_score'].values
]

bp = axes[1, 0].boxplot(box_data, labels=['T-Learner', 'X-Learner\n(Full)', 'X-Learner\nV2'],
                         patch_artist=True)
for patch, color in zip(bp['boxes'], ['blue', 'green', 'orange']):
    patch.set_facecolor(color)
    patch.set_alpha(0.7)
axes[1, 0].axhline(0, color='red', linestyle='--', linewidth=2, label='Zero Uplift')
axes[1, 0].set_title('Uplift Score Comparison (Box Plot)', fontweight='bold')
axes[1, 0].set_ylabel('Uplift Score')
axes[1, 0].legend()
axes[1, 0].grid(True, alpha=0.3, axis='y')

# Print percentile statistics
percentiles = [10, 25, 50, 75, 90]
stats_text = "Uplift Score Percentiles:\n\n"
stats_text += f"{'Model':<20} {' '.join([f'P{p:2d}' for p in percentiles])}\n"
stats_text += "-" * 60 + "\n"

for model_name, scores in [
    ('T-Learner', df_eval_tlearner['uplift_score'].values),
    ('X-Learner (Full)', df_eval_xlearner['uplift_score'].values),
    ('X-Learner V2', df_eval_xlearner_v2['uplift_score'].values)
]:
    percs = [np.percentile(scores, p) for p in percentiles]
    stats_text += f"{model_name:<20} {' '.join([f'{p:6.4f}' for p in percs])}\n"

axes[1, 1].text(0.05, 0.95, stats_text, 
                transform=axes[1, 1].transAxes,
                fontsize=10,
                verticalalignment='top',
                fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
axes[1, 1].axis('off')
axes[1, 1].set_title('Percentile Statistics', fontweight='bold')

# Summary statistics table
summary_stats = []
for model_name, scores in [
    ('T-Learner', df_eval_tlearner['uplift_score'].values),
    ('X-Learner (Full)', df_eval_xlearner['uplift_score'].values),
    ('X-Learner V2', df_eval_xlearner_v2['uplift_score'].values)
]:
    summary_stats.append({
        'Model': model_name,
        'Mean': f'{np.mean(scores):.4f}',
        'Std': f'{np.std(scores):.4f}',
        'Min': f'{np.min(scores):.4f}',
        'Max': f'{np.max(scores):.4f}',
        'Positive %': f'{(scores > 0).mean():.2%}'
    })

summary_df = pd.DataFrame(summary_stats)
table_text = summary_df.to_string(index=False)

axes[1, 2].text(0.05, 0.95, table_text, 
                transform=axes[1, 2].transAxes,
                fontsize=10,
                verticalalignment='top',
                fontfamily='monospace',
                bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))
axes[1, 2].axis('off')
axes[1, 2].set_title('Summary Statistics', fontweight='bold')

plt.tight_layout()
display(fig)

print("\n" + "="*80)
print("📊 KEY OBSERVATIONS:")
print("="*80)
print("\n🎯 Model Performance Metrics:")
print("-" * 80)
print(f"{'Model':<25} {'Qini AUC':<12} {'Uplift AUC':<12} {'Avg Uplift':<12} {'Positive %':<12}")
print("-" * 80)
print(f"{'T-Learner':<25} {qini_tlearner:<12.4f} {uplift_auc_tlearner:<12.4f} {avg_uplift_tlearner:<12.4f} {positive_uplift_pct_tlearner:<12.2%}")
print(f"{'X-Learner (Full)':<25} {qini_xlearner:<12.4f} {uplift_auc_xlearner:<12.4f} {avg_uplift_xlearner:<12.4f} {positive_uplift_pct_xlearner:<12.2%}")
print(f"{'X-Learner V2':<25} {qini_xlearner_v2:<12.4f} {uplift_auc_xlearner_v2:<12.4f} {avg_uplift_xlearner_v2:<12.4f} {positive_uplift_pct_xlearner_v2:<12.2%}")
print("-" * 80)

print("\n💡 Key Insights:")
print(f"1. All Qini AUC scores are LOW (~{qini_tlearner:.3f}-{qini_xlearner:.3f})")
print(f"   → Weak uplift signal - models barely better than random targeting")
print(f"")
print(f"2. All Uplift AUC scores are also LOW (~{uplift_auc_tlearner:.3f}-{uplift_auc_xlearner:.3f})")
print(f"   → Confirms weak treatment effect overall")
print(f"")
print(f"3. X-Learner (Full) has MUCH HIGHER positive uplift % ({positive_uplift_pct_xlearner:.1%})")
print(f"   vs T-Learner ({positive_uplift_pct_tlearner:.1%}) and X-Learner V2 ({positive_uplift_pct_xlearner_v2:.1%})")
print(f"   → But similar Qini/Uplift AUC scores → predictions are optimistic but poorly ranked")
print(f"")
print(f"4. All models have NEGATIVE average uplift ({avg_uplift_xlearner:.4f} to {avg_uplift_tlearner:.4f})")
print(f"   → Treatment may actually HURT conversions on average")
print(f"")
print(f"⚠️  RECOMMENDATION: DO NOT DEPLOY any model - uplift signal is too weak (<0.1)")
print(f"   → Consider: better features, different time period, or treatment may not work")
print("="*80)

# COMMAND ----------

# Note: To train models for all channels instead of just Direct, 
# uncomment and modify the code below to iterate through all_channels
# all_channels = list(channel_flags.keys()) + ["All", "Null"]

# COMMAND ----------
