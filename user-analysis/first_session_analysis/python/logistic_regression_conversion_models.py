"""
Logistic Regression Models for Session Conversion Prediction

This script builds two logistic regression models:
1. Pre-Funnel Model: Uses only browsing/engagement features
2. Funnel-Inclusive Model: Includes funnel progression features

Outputs:
- Model coefficients with odds ratios and percentage impacts
- Model performance metrics
- Predictions saved to Snowflake
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, confusion_matrix, classification_report,
    roc_curve
)
from sklearn.metrics.pairwise import cosine_similarity
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from sentence_transformers import SentenceTransformer
from utils.snowflake_connection import SnowflakeHook

# Set up plotting style
plt.style.use('default')
sns.set_palette("husl")

def load_data():
    """Load session features from Snowflake"""
    print("Loading data from Snowflake...")
    
    query = """
    SELECT *
    FROM proddb.fionafan.all_user_sessions_with_events_features_gen
    WHERE funnel_converted_bool IS NOT NULL
    """
    
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(query, method='pandas')
    
    print(f"Loaded {len(df):,} sessions")
    print(f"Conversion rate: {df['FUNNEL_CONVERTED_BOOL'].mean():.2%}")
    
    return df


def create_store_embeddings(df):
    """Create embeddings for store text features and calculate session similarity"""
    print("\nCreating store embeddings...")
    
    # Load sentence transformer model
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    # Combine store text features into a single text representation
    df['STORE_TEXT'] = (
        df['STORE_TAGS_CONCAT'].fillna('') + ' | ' +
        df['STORE_CATEGORIES_CONCAT'].fillna('') + ' | ' +
        df['STORE_NAMES_CONCAT'].fillna('')
    )
    
    # Replace empty strings with a placeholder
    df['STORE_TEXT'] = df['STORE_TEXT'].replace('', 'no_store_info')
    df['STORE_TEXT'] = df['STORE_TEXT'].replace(' |  | ', 'no_store_info')
    
    print(f"  Generating embeddings for {len(df)} sessions...")
    # Generate embeddings in batches
    batch_size = 1000
    embeddings_list = []
    
    for i in range(0, len(df), batch_size):
        batch = df['STORE_TEXT'].iloc[i:i+batch_size].tolist()
        batch_embeddings = model.encode(batch, show_progress_bar=False)
        embeddings_list.append(batch_embeddings)
    
    embeddings = np.vstack(embeddings_list)
    
    # Calculate similarity to previous session
    print("  Calculating session-to-session similarity...")
    
    # Sort by user, device, date, session_num to ensure temporal ordering
    df = df.sort_values(['USER_ID', 'DD_DEVICE_ID', 'EVENT_DATE', 'SESSION_NUM']).reset_index(drop=True)
    
    # Initialize similarity features
    df['STORE_EMBEDDING_SIMILARITY_PREV'] = 0.0
    df['HAS_PREVIOUS_SESSION'] = 0
    
    # Calculate similarity within each user-device combination
    for (user_id, device_id), group_df in df.groupby(['USER_ID', 'DD_DEVICE_ID']):
        indices = group_df.index.tolist()
        
        if len(indices) > 1:
            for i in range(1, len(indices)):
                curr_idx = indices[i]
                prev_idx = indices[i-1]
                
                # Calculate cosine similarity between current and previous embedding
                similarity = cosine_similarity(
                    embeddings[curr_idx].reshape(1, -1),
                    embeddings[prev_idx].reshape(1, -1)
                )[0, 0]
                
                df.loc[curr_idx, 'STORE_EMBEDDING_SIMILARITY_PREV'] = similarity
                df.loc[curr_idx, 'HAS_PREVIOUS_SESSION'] = 1
    
    print(f"  ✓ Created embeddings and similarity features")
    print(f"    - Sessions with previous session: {df['HAS_PREVIOUS_SESSION'].sum():,}")
    print(f"    - Mean similarity (when prev exists): {df[df['HAS_PREVIOUS_SESSION']==1]['STORE_EMBEDDING_SIMILARITY_PREV'].mean():.3f}")
    
    return df


def prepare_categorical_features(df):
    """One-hot encode categorical features"""
    # Normalize column names to uppercase (from Snowflake)
    df.columns = df.columns.str.upper()
    
    # Create store embeddings and similarity features
    df = create_store_embeddings(df)
    
    categorical_cols = ['SESSION_TYPE', 'COHORT_TYPE', 'PLATFORM', 'ATTRIBUTION_DOMINANT_FEATURE']
    
    # Handle missing values in categorical columns
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].fillna('UNKNOWN')
    
    # One-hot encode
    df_encoded = pd.get_dummies(df, columns=categorical_cols, prefix=categorical_cols, drop_first=True)
    
    return df_encoded


def define_feature_sets():
    """Define the two feature sets for modeling"""
    
    # EXCLUDED FEATURES (Data Leakage):
    # - ATTRIBUTION_CNT_DOUBLEDASH: DoubleDash orders are a proxy for conversion
    # - ACTION_HAD_PAYMENT: Payment actions occur during checkout (target event)
    # - ACTION_NUM_PAYMENT_ACTIONS: Payment actions occur during checkout (target event)
    # - NUM_UNIQUE_DISCOVERY_SURFACES: Highly correlated with checkout behavior
    # - NUM_UNIQUE_DISCOVERY_FEATURES: Highly correlated with checkout behavior
    #
    # EXCLUDED FEATURES (Too Generic):
    # - TOTAL_EVENTS: Too generic, not predictive
    # - EVENTS_PER_MINUTE: Too generic, not predictive
    
    # Feature Set 1: Pre-Funnel Features (Early Signal Model)
    pre_funnel_features = [
        # Temporal features
        'DAYS_SINCE_ONBOARDING',
        'DAYS_SINCE_ONBOARDING_MOD7',
        'SESSION_DAY_OF_WEEK',
        'ONBOARDING_DAY_OF_WEEK',
        
        # Overall session engagement (EXCLUDED: TOTAL_EVENTS, EVENTS_PER_MINUTE, NUM_UNIQUE_DISCOVERY_FEATURES, NUM_UNIQUE_DISCOVERY_SURFACES)
        'SESSION_DURATION_SECONDS',
        'NUM_UNIQUE_STORES',
        
        # Impression features
        'IMPRESSION_EVENT_COUNT',
        'IMPRESSION_UNIQUE_STORES',
        'IMPRESSION_UNIQUE_FEATURES',
        'IMPRESSION_HAD_ANY',
        
        # Action features (EXCLUDED: ACTION_HAD_PAYMENT, ACTION_NUM_PAYMENT_ACTIONS)
        'ACTION_EVENT_COUNT',
        'ACTION_NUM_BACKGROUNDS',
        'ACTION_NUM_FOREGROUNDS',
        'ACTION_NUM_TAB_SWITCHES',
        'ACTION_NUM_ADDRESS_ACTIONS',
        'ACTION_NUM_AUTH_ACTIONS',
        'ACTION_HAD_TAB_SWITCH',
        'ACTION_HAD_ADDRESS',
        'ACTION_HAD_AUTH',
        
        # Attribution features (EXCLUDED: ATTRIBUTION_CNT_DOUBLEDASH)
        'ATTRIBUTION_EVENT_COUNT',
        'ATTRIBUTION_NUM_CARD_CLICKS',
        'ATTRIBUTION_UNIQUE_STORES_CLICKED',
        'ATTRIBUTION_HAD_ANY',
        'ATTRIBUTION_CNT_TRADITIONAL_CAROUSEL',
        'ATTRIBUTION_CNT_CORE_SEARCH',
        'ATTRIBUTION_CNT_PILL_FILTER',
        'ATTRIBUTION_CNT_CUISINE_FILTER',
        'ATTRIBUTION_CNT_HOME_FEED',
        'ATTRIBUTION_CNT_BANNER',
        'ATTRIBUTION_CNT_OFFERS',
        'ATTRIBUTION_CNT_TAB_EXPLORE',
        'ATTRIBUTION_CNT_TAB_ME',
        'ATTRIBUTION_PCT_FROM_DOMINANT',
        'ATTRIBUTION_ENTROPY',
        'ATTRIBUTION_FIRST_CLICK_FROM_SEARCH',
        
        # Error indicators
        'ERROR_EVENT_COUNT',
        'ERROR_HAD_ANY',
        
        # Timing features
        'TIMING_MEAN_INTER_EVENT_SECONDS',
        'TIMING_MEDIAN_INTER_EVENT_SECONDS',
        'TIMING_STD_INTER_EVENT_SECONDS',
        'TIMING_EVENTS_FIRST_30S',
        'TIMING_EVENTS_FIRST_2MIN',
        'ATTRIBUTION_SECONDS_TO_FIRST_CARD_CLICK',
        'ACTION_SECONDS_TO_FIRST_TAB_SWITCH',
        'ACTION_SECONDS_TO_FIRST_FOREGROUND',
        
        # Behavioral sequences (pre-funnel)
        'SEQUENCE_IMPRESSION_TO_ATTRIBUTION',
        
        # Store context
        'STORE_HAD_NV',
        'STORE_NV_IMPRESSION_OCCURRED',
        'STORE_NV_IMPRESSION_POSITION',
        
        # NEW: Store embedding similarity features
        'STORE_EMBEDDING_SIMILARITY_PREV',
        'HAS_PREVIOUS_SESSION',
    ]
    
    # Feature Set 2: Funnel-Inclusive Features (Full Journey Model)
    funnel_features = [
        # Funnel progression indicators (exclude target-related)
        'FUNNEL_EVENT_COUNT',
        'FUNNEL_NUM_ADDS',
        'FUNNEL_NUM_STORE_PAGE',
        'FUNNEL_NUM_CART',
        'FUNNEL_NUM_CHECKOUT',
        'FUNNEL_HAD_ANY',
        'FUNNEL_HAD_ADD',
        'FUNNEL_REACHED_STORE_BOOL',
        'FUNNEL_REACHED_CART_BOOL',
        'FUNNEL_REACHED_CHECKOUT_BOOL',
        
        # Funnel timing (exclude target-related)
        'FUNNEL_SECONDS_TO_FIRST_ADD',
        'FUNNEL_SECONDS_TO_STORE_PAGE',
        'FUNNEL_SECONDS_TO_CART',
        'FUNNEL_SECONDS_TO_CHECKOUT',
        
        # Conversion rates
        'IMPRESSION_TO_ATTRIBUTION_RATE',
        'ATTRIBUTION_TO_ADD_RATE',
        'FUNNEL_ADD_PER_ATTRIBUTION_CLICK',
        
        # Additional sequences
        'SEQUENCE_ATTRIBUTION_TO_FUNNEL_STORE',
        'SEQUENCE_FUNNEL_STORE_TO_ACTION_ADD',
    ]
    
    full_funnel_features = pre_funnel_features + funnel_features
    
    return pre_funnel_features, full_funnel_features


def train_logistic_model(X_train, y_train, X_test, y_test, model_name, feature_names):
    """Train a logistic regression model and evaluate it"""
    print(f"\n{'='*80}")
    print(f"Training {model_name}")
    print(f"{'='*80}")
    
    # Standardize features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Train logistic regression with L2 regularization
    model = LogisticRegression(
        penalty='l2',
        C=1.0,
        max_iter=1000,
        random_state=42,
        solver='lbfgs',
        class_weight='balanced'  # Handle class imbalance
    )
    
    model.fit(X_train_scaled, y_train)
    
    # Make predictions
    y_train_pred = model.predict(X_train_scaled)
    y_test_pred = model.predict(X_test_scaled)
    y_train_proba = model.predict_proba(X_train_scaled)[:, 1]
    y_test_proba = model.predict_proba(X_test_scaled)[:, 1]
    
    # Calculate metrics
    metrics = {
        'model_name': model_name,
        'train_accuracy': accuracy_score(y_train, y_train_pred),
        'test_accuracy': accuracy_score(y_test, y_test_pred),
        'train_precision': precision_score(y_train, y_train_pred),
        'test_precision': precision_score(y_test, y_test_pred),
        'train_recall': recall_score(y_train, y_train_pred),
        'test_recall': recall_score(y_test, y_test_pred),
        'train_f1': f1_score(y_train, y_train_pred),
        'test_f1': f1_score(y_test, y_test_pred),
        'train_auc': roc_auc_score(y_train, y_train_proba),
        'test_auc': roc_auc_score(y_test, y_test_proba),
        'num_features': len(feature_names),
    }
    
    # Print metrics
    print(f"\nModel Performance:")
    print(f"  Train AUC-ROC: {metrics['train_auc']:.4f}")
    print(f"  Test AUC-ROC:  {metrics['test_auc']:.4f}")
    print(f"  Train Accuracy: {metrics['train_accuracy']:.4f}")
    print(f"  Test Accuracy:  {metrics['test_accuracy']:.4f}")
    print(f"  Test Precision: {metrics['test_precision']:.4f}")
    print(f"  Test Recall:    {metrics['test_recall']:.4f}")
    print(f"  Test F1-Score:  {metrics['test_f1']:.4f}")
    
    # Confusion matrix
    print(f"\nConfusion Matrix (Test Set):")
    cm = confusion_matrix(y_test, y_test_pred)
    print(cm)
    
    # Extract coefficients and calculate odds ratios
    coefficients = extract_coefficients(model, scaler, feature_names, model_name)
    
    return model, scaler, metrics, coefficients, y_test_proba


def extract_coefficients(model, scaler, feature_names, model_name):
    """Extract coefficients and calculate odds ratios and percentage impacts"""
    
    # Get coefficients
    coefs = model.coef_[0]
    intercept = model.intercept_[0]
    
    # Get feature scaling parameters
    feature_means = scaler.mean_
    feature_stds = scaler.scale_
    
    # Create coefficient dataframe
    coef_df = pd.DataFrame({
        'model_name': model_name,
        'feature': feature_names,
        'coefficient': coefs,
        'feature_mean': feature_means,
        'feature_std': feature_stds,
    })
    
    # Calculate odds ratios for 1 standard deviation change
    coef_df['odds_ratio_1sd'] = np.exp(coef_df['coefficient'])
    
    # Calculate percentage impact on odds for 1 SD increase
    # Formula: (OR - 1) * 100
    coef_df['pct_impact_1sd'] = (coef_df['odds_ratio_1sd'] - 1) * 100
    
    # Calculate percentage impact on probability (marginal effect at mean)
    # For logistic regression: β * p * (1-p) where p is baseline probability
    baseline_prob = 1 / (1 + np.exp(-intercept))
    coef_df['marginal_effect_1sd'] = coef_df['coefficient'] * baseline_prob * (1 - baseline_prob)
    coef_df['pct_point_impact_1sd'] = coef_df['marginal_effect_1sd'] * 100
    
    # Add absolute value for sorting
    coef_df['abs_coefficient'] = np.abs(coef_df['coefficient'])
    coef_df['abs_pct_impact'] = np.abs(coef_df['pct_impact_1sd'])
    
    # Sort by absolute impact
    coef_df = coef_df.sort_values('abs_pct_impact', ascending=False)
    
    # Add rank
    coef_df['rank'] = range(1, len(coef_df) + 1)
    
    print(f"\n{model_name} - Top 20 Features by Impact:")
    print(f"{'Rank':<6} {'Feature':<50} {'Coef':<10} {'Odds Ratio':<12} {'% Impact':<12} {'PP Impact':<12}")
    print("-" * 110)
    
    for idx, row in coef_df.head(20).iterrows():
        print(f"{row['rank']:<6} {row['feature']:<50} {row['coefficient']:>9.4f} "
              f"{row['odds_ratio_1sd']:>11.4f} {row['pct_impact_1sd']:>11.1f}% "
              f"{row['pct_point_impact_1sd']:>11.2f}pp")
    
    return coef_df


def save_coefficients_to_snowflake(coef_df, table_name='session_conversion_model_coefficients'):
    """Save model coefficients to Snowflake"""
    print(f"\nSaving coefficients to proddb.fionafan.{table_name}...")
    
    # Add metadata
    coef_df['created_at'] = datetime.now()
    
    # Select columns to save
    save_cols = [
        'model_name', 'feature', 'rank', 'coefficient', 
        'odds_ratio_1sd', 'pct_impact_1sd', 'pct_point_impact_1sd',
        'marginal_effect_1sd', 'feature_mean', 'feature_std',
        'abs_coefficient', 'abs_pct_impact', 'created_at'
    ]
    
    coef_save = coef_df[save_cols].copy()
    
    # Write to Snowflake
    with SnowflakeHook(database='PRODDB', schema='FIONAFAN') as hook:
        hook.create_and_populate_table(
            df=coef_save,
            table_name=table_name.upper(),
            method='pandas'
        )
    
    print(f"✓ Saved {len(coef_save)} coefficients to Snowflake")


def save_metrics_to_snowflake(all_metrics, table_name='session_conversion_model_metrics'):
    """Save model performance metrics to Snowflake"""
    print(f"\nSaving metrics to proddb.fionafan.{table_name}...")
    
    metrics_df = pd.DataFrame(all_metrics)
    metrics_df['created_at'] = datetime.now()
    
    with SnowflakeHook(database='PRODDB', schema='FIONAFAN') as hook:
        hook.create_and_populate_table(
            df=metrics_df,
            table_name=table_name.upper(),
            method='pandas'
        )
    
    print(f"✓ Saved metrics to Snowflake")


def plot_roc_curves(y_test, y_proba_dict, output_dir):
    """Plot ROC curves for both models"""
    print("\nGenerating ROC curve comparison...")
    
    plt.figure(figsize=(10, 8))
    
    colors = ['#1f77b4', '#ff7f0e']
    
    for i, (model_name, y_proba) in enumerate(y_proba_dict.items()):
        fpr, tpr, _ = roc_curve(y_test, y_proba)
        auc = roc_auc_score(y_test, y_proba)
        
        plt.plot(fpr, tpr, color=colors[i], lw=2, 
                label=f'{model_name} (AUC = {auc:.4f})')
    
    # Plot diagonal
    plt.plot([0, 1], [0, 1], 'k--', lw=1, label='Random Classifier')
    
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate', fontsize=12)
    plt.ylabel('True Positive Rate', fontsize=12)
    plt.title('ROC Curves: Session Conversion Prediction Models', fontsize=14, fontweight='bold')
    plt.legend(loc="lower right", fontsize=11)
    plt.grid(alpha=0.3)
    plt.tight_layout()
    
    output_path = os.path.join(output_dir, 'roc_curves_comparison.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Saved ROC curve to {output_path}")
    plt.close()


def plot_coefficient_comparison(coef_dfs, output_dir):
    """Plot top coefficients for both models side by side"""
    print("\nGenerating coefficient comparison plots...")
    
    # Get top 15 features from each model by absolute impact
    top_n = 15
    
    fig, axes = plt.subplots(1, 2, figsize=(18, 10))
    
    for idx, (model_name, coef_df) in enumerate(coef_dfs.items()):
        ax = axes[idx]
        
        top_features = coef_df.head(top_n).copy()
        
        # Create color based on sign
        colors = ['#d62728' if x < 0 else '#2ca02c' for x in top_features['pct_impact_1sd']]
        
        # Horizontal bar plot
        y_pos = np.arange(len(top_features))
        ax.barh(y_pos, top_features['pct_impact_1sd'], color=colors, alpha=0.7)
        
        # Customize
        ax.set_yticks(y_pos)
        ax.set_yticklabels(top_features['feature'], fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel('% Change in Odds (for 1 SD increase)', fontsize=11, fontweight='bold')
        ax.set_title(f'{model_name}\nTop {top_n} Features by Impact', 
                    fontsize=12, fontweight='bold')
        ax.axvline(x=0, color='black', linestyle='-', linewidth=0.8)
        ax.grid(axis='x', alpha=0.3)
        
        # Add value labels
        for i, (idx_val, row) in enumerate(top_features.iterrows()):
            value = row['pct_impact_1sd']
            x_pos = value + (5 if value > 0 else -5)
            ha = 'left' if value > 0 else 'right'
            ax.text(x_pos, i, f'{value:+.1f}%', va='center', ha=ha, fontsize=8)
    
    plt.tight_layout()
    output_path = os.path.join(output_dir, 'coefficient_comparison.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Saved coefficient comparison to {output_path}")
    plt.close()


def plot_feature_importance_heatmap(coef_dfs, output_dir):
    """Create a heatmap showing shared features across models"""
    print("\nGenerating feature importance heatmap...")
    
    # Find common features
    pre_funnel_df = coef_dfs['Pre-Funnel Model']
    full_funnel_df = coef_dfs['Funnel-Inclusive Model']
    
    common_features = set(pre_funnel_df['feature']) & set(full_funnel_df['feature'])
    
    # Get top 30 common features by average absolute impact
    pre_dict = dict(zip(pre_funnel_df['feature'], pre_funnel_df['pct_impact_1sd']))
    full_dict = dict(zip(full_funnel_df['feature'], full_funnel_df['pct_impact_1sd']))
    
    common_impacts = []
    for feat in common_features:
        avg_impact = (abs(pre_dict[feat]) + abs(full_dict[feat])) / 2
        common_impacts.append((feat, avg_impact, pre_dict[feat], full_dict[feat]))
    
    common_impacts.sort(key=lambda x: x[1], reverse=True)
    top_common = common_impacts[:30]
    
    # Create matrix
    features = [x[0] for x in top_common]
    data = np.array([[x[2], x[3]] for x in top_common])
    
    # Plot
    fig, ax = plt.subplots(figsize=(10, 14))
    
    im = ax.imshow(data, cmap='RdYlGn', aspect='auto', vmin=-100, vmax=100)
    
    # Set ticks
    ax.set_xticks([0, 1])
    ax.set_xticklabels(['Pre-Funnel\nModel', 'Funnel-Inclusive\nModel'], fontsize=11)
    ax.set_yticks(np.arange(len(features)))
    ax.set_yticklabels(features, fontsize=9)
    
    # Add colorbar
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label('% Change in Odds (1 SD)', rotation=270, labelpad=20, fontsize=11)
    
    # Add text annotations
    for i in range(len(features)):
        for j in range(2):
            text = ax.text(j, i, f'{data[i, j]:+.1f}%',
                          ha="center", va="center", color="black", fontsize=7)
    
    ax.set_title('Feature Importance Comparison Across Models\n(Top 30 Shared Features)', 
                fontsize=13, fontweight='bold', pad=15)
    
    plt.tight_layout()
    output_path = os.path.join(output_dir, 'feature_importance_heatmap.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Saved feature importance heatmap to {output_path}")
    plt.close()


def main():
    """Main execution function"""
    
    # Create output directory
    output_dir = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/first_session_analysis/plots'
    os.makedirs(output_dir, exist_ok=True)
    
    print("="*80)
    print("SESSION CONVERSION PREDICTION - LOGISTIC REGRESSION MODELS")
    print("="*80)
    
    # Load data
    df = load_data()
    
    # Prepare categorical features
    df_encoded = prepare_categorical_features(df)
    
    # Define feature sets
    pre_funnel_base, full_funnel_base = define_feature_sets()
    
    # Get all categorical dummy columns
    categorical_dummy_cols = [col for col in df_encoded.columns 
                              if any(col.startswith(prefix) for prefix in 
                                   ['SESSION_TYPE_', 'COHORT_TYPE_', 'PLATFORM_', 
                                    'ATTRIBUTION_DOMINANT_FEATURE_'])]
    
    # Combine with base features
    pre_funnel_features = pre_funnel_base + categorical_dummy_cols
    full_funnel_features = full_funnel_base + categorical_dummy_cols
    
    # Filter to available columns
    pre_funnel_features = [f for f in pre_funnel_features if f in df_encoded.columns]
    full_funnel_features = [f for f in full_funnel_features if f in df_encoded.columns]
    
    print(f"\nPre-Funnel Model: {len(pre_funnel_features)} features")
    print(f"Funnel-Inclusive Model: {len(full_funnel_features)} features")
    
    # Prepare target variable
    target = 'FUNNEL_CONVERTED_BOOL'
    
    # Handle missing values - fill numeric with median
    for feature in pre_funnel_features + full_funnel_features:
        if feature in df_encoded.columns:
            if df_encoded[feature].dtype in ['float64', 'int64']:
                df_encoded[feature].fillna(df_encoded[feature].median(), inplace=True)
    
    # Split data (stratified to maintain class balance)
    train_df, test_df = train_test_split(
        df_encoded, 
        test_size=0.2, 
        random_state=42, 
        stratify=df_encoded[target]
    )
    
    print(f"\nTrain set: {len(train_df):,} sessions")
    print(f"Test set:  {len(test_df):,} sessions")
    print(f"Train conversion rate: {train_df[target].mean():.2%}")
    print(f"Test conversion rate:  {test_df[target].mean():.2%}")
    
    # Store results
    all_metrics = []
    all_coefficients = {}
    y_proba_dict = {}
    
    # Model 1: Pre-Funnel Features
    X_train_pre = train_df[pre_funnel_features]
    y_train = train_df[target]
    X_test_pre = test_df[pre_funnel_features]
    y_test = test_df[target]
    
    model_pre, scaler_pre, metrics_pre, coef_pre, y_proba_pre = train_logistic_model(
        X_train_pre, y_train, X_test_pre, y_test,
        'Pre-Funnel Model',
        pre_funnel_features
    )
    
    all_metrics.append(metrics_pre)
    all_coefficients['Pre-Funnel Model'] = coef_pre
    y_proba_dict['Pre-Funnel Model'] = y_proba_pre
    
    # Model 2: Funnel-Inclusive Features
    X_train_full = train_df[full_funnel_features]
    X_test_full = test_df[full_funnel_features]
    
    model_full, scaler_full, metrics_full, coef_full, y_proba_full = train_logistic_model(
        X_train_full, y_train, X_test_full, y_test,
        'Funnel-Inclusive Model',
        full_funnel_features
    )
    
    all_metrics.append(metrics_full)
    all_coefficients['Funnel-Inclusive Model'] = coef_full
    y_proba_dict['Funnel-Inclusive Model'] = y_proba_full
    
    # Save results to Snowflake
    combined_coefs = pd.concat(all_coefficients.values(), ignore_index=True)
    save_coefficients_to_snowflake(combined_coefs)
    save_metrics_to_snowflake(all_metrics)
    
    # Generate visualizations
    plot_roc_curves(y_test, y_proba_dict, output_dir)
    plot_coefficient_comparison(all_coefficients, output_dir)
    plot_feature_importance_heatmap(all_coefficients, output_dir)
    
    # Print summary comparison
    print("\n" + "="*80)
    print("MODEL COMPARISON SUMMARY")
    print("="*80)
    print(f"\n{'Metric':<30} {'Pre-Funnel':<15} {'Funnel-Inclusive':<15} {'Difference':<15}")
    print("-"*75)
    
    comparison_metrics = [
        ('Test AUC-ROC', 'test_auc'),
        ('Test Accuracy', 'test_accuracy'),
        ('Test Precision', 'test_precision'),
        ('Test Recall', 'test_recall'),
        ('Test F1-Score', 'test_f1'),
        ('Number of Features', 'num_features'),
    ]
    
    for metric_name, metric_key in comparison_metrics:
        val_pre = metrics_pre[metric_key]
        val_full = metrics_full[metric_key]
        diff = val_full - val_pre
        
        if metric_key == 'num_features':
            print(f"{metric_name:<30} {val_pre:<15.0f} {val_full:<15.0f} {diff:<+15.0f}")
        else:
            print(f"{metric_name:<30} {val_pre:<15.4f} {val_full:<15.4f} {diff:<+15.4f}")
    
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE!")
    print("="*80)
    print(f"\nResults saved to:")
    print(f"  - Tables: proddb.fionafan.session_conversion_model_coefficients")
    print(f"  - Tables: proddb.fionafan.session_conversion_model_metrics")
    print(f"  - Plots:  {output_dir}/")


if __name__ == "__main__":
    main()

