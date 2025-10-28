"""
Propensity Score Matching for DP First Order Analysis

Finds matched control units (dp_first_order=0) for treated units (dp_first_order=1)
by controlling for NV and DP related covariates using propensity score matching.
"""

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from scipy.spatial.distance import cdist
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Import Snowflake hook
from utils.snowflake_connection import SnowflakeHook

# Covariates to control for
COVARIATES = [
    'had_nv_imp_1h',
    'had_nv_imp_4h',
    'had_nv_imp_12h',
    'had_nv_imp_24h',
    'had_nv_imp_7d',
    'had_nv_imp_30d',
    'had_nv_imp_first_session',
    'had_nv_imp_second_session',
    'nv_first_order',
    'nv_second_order',
    'has_nv_notif_30d',
    'has_nv_notif_60d',
    'minutes_to_first_non_nv_impression',
    'minutes_to_first_nv_impression',
    'had_non_nv_imp_1h',
    'had_non_nv_imp_4h',
    'had_non_nv_imp_12h',
    'had_non_nv_imp_24h',
    'had_non_nv_imp_7d',
    'had_non_nv_imp_30d',
    'had_non_nv_imp_first_session',
    'had_non_nv_imp_second_session',
    'dp_has_notif_30d',
    'dp_has_notif_any_day',
    'dp_second_order'
]


def load_data_from_snowflake():
    """Load data from Snowflake table"""
    print("Loading data from proddb.fionafan.nv_dp_features_targets...")
    
    query = """
    SELECT 
        device_id,
        consumer_id,
        join_day,
        
        -- Treatment variable
        dp_first_order,
        
        -- Covariates: NV impressions
        had_nv_imp_1h,
        had_nv_imp_4h,
        had_nv_imp_12h,
        had_nv_imp_24h,
        had_nv_imp_7d,
        had_nv_imp_30d,
        had_nv_imp_first_session,
        had_nv_imp_second_session,
        
        -- Covariates: NV orders
        nv_first_order,
        nv_second_order,
        
        -- Covariates: NV notifications
        has_nv_notif_30d,
        has_nv_notif_60d,
        
        -- Covariates: Time to impressions
        minutes_to_first_non_nv_impression,
        minutes_to_first_nv_impression,
        
        -- Covariates: Non-NV impressions
        had_non_nv_imp_1h,
        had_non_nv_imp_4h,
        had_non_nv_imp_12h,
        had_non_nv_imp_24h,
        had_non_nv_imp_7d,
        had_non_nv_imp_30d,
        had_non_nv_imp_first_session,
        had_non_nv_imp_second_session,
        
        -- Covariates: DP notifications and orders
        dp_has_notif_30d,
        dp_has_notif_any_day,
        dp_second_order,
        
        -- Additional target variables for downstream analysis
        target_ordered_24h,
        target_first_order_new_cx,
        target_ordered_month1,
        target_ordered_month2,
        target_ordered_month3,
        target_ordered_month4

    FROM proddb.fionafan.nv_dp_features_targets
    SAMPLE (10)  -- Sample 10% of data
    ORDER BY dp_first_order DESC, device_id
    """
    
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(query, method='pandas')
    
    print(f"Loaded {len(df):,} records")
    return df


def calculate_standardized_mean_difference(treated, control, variable):
    """
    Calculate standardized mean difference (SMD) for balance checking.
    SMD = (mean_treated - mean_control) / sqrt((var_treated + var_control) / 2)
    
    Rule of thumb: SMD < 0.1 indicates good balance
    """
    mean_t = treated[variable].mean()
    mean_c = control[variable].mean()
    var_t = treated[variable].var()
    var_c = control[variable].var()
    
    pooled_std = np.sqrt((var_t + var_c) / 2)
    
    if pooled_std == 0:
        return 0
    
    smd = (mean_t - mean_c) / pooled_std
    return smd


def check_balance(df, treatment_col, covariates, label="Before Matching"):
    """
    Check covariate balance between treated and control groups
    """
    treated = df[df[treatment_col] == 1]
    control = df[df[treatment_col] == 0]
    
    balance_stats = []
    
    for var in covariates:
        mean_t = treated[var].mean()
        mean_c = control[var].mean()
        smd = calculate_standardized_mean_difference(treated, control, var)
        
        balance_stats.append({
            'variable': var,
            'mean_treated': mean_t,
            'mean_control': mean_c,
            'smd': smd,
            'balanced': abs(smd) < 0.1
        })
    
    balance_df = pd.DataFrame(balance_stats)
    
    print(f"\n{'='*80}")
    print(f"{label}")
    print(f"{'='*80}")
    print(f"\nTreated group size: {len(treated):,}")
    print(f"Control group size: {len(control):,}")
    print(f"\nBalance Statistics:")
    print(balance_df.to_string(index=False))
    print(f"\nVariables balanced (|SMD| < 0.1): {balance_df['balanced'].sum()} / {len(balance_df)}")
    
    return balance_df


def propensity_score_matching(df, treatment_col, covariates, caliper=0.2, replace=False):
    """
    Perform 1:1 propensity score matching using nearest neighbor with caliper
    
    Parameters:
    -----------
    df: DataFrame with treatment indicator and covariates
    treatment_col: name of treatment column (dp_first_order)
    covariates: list of covariate column names
    caliper: maximum propensity score difference for matching (in std units)
    replace: whether to sample with replacement
    
    Returns:
    --------
    matched_df: DataFrame with matched pairs
    ps_model: fitted propensity score model
    """
    
    print("\n" + "="*80)
    print("PROPENSITY SCORE MATCHING")
    print("="*80)
    
    # Prepare data
    X = df[covariates].fillna(0)  # Handle missing values
    y = df[treatment_col]
    
    # Standardize covariates for better logistic regression performance
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    X_scaled_df = pd.DataFrame(X_scaled, columns=covariates, index=df.index)
    
    # Fit propensity score model
    print("\nFitting propensity score model (Logistic Regression)...")
    ps_model = LogisticRegression(max_iter=1000, random_state=42)
    ps_model.fit(X_scaled, y)
    
    # Calculate propensity scores
    propensity_scores = ps_model.predict_proba(X_scaled)[:, 1]
    df['propensity_score'] = propensity_scores
    
    print(f"Propensity score range: [{propensity_scores.min():.4f}, {propensity_scores.max():.4f}]")
    
    # Separate treated and control
    treated = df[df[treatment_col] == 1].copy()
    control = df[df[treatment_col] == 0].copy()
    
    print(f"\nTreated units: {len(treated):,}")
    print(f"Control units: {len(control):,}")
    
    # Calculate caliper in propensity score units
    ps_std = propensity_scores.std()
    caliper_distance = caliper * ps_std
    print(f"Caliper: {caliper} std = {caliper_distance:.4f} propensity score units")
    
    # Perform nearest neighbor matching
    matched_pairs = []
    used_control_indices = set()
    
    for _, treated_row in treated.iterrows():
        treated_ps = treated_row['propensity_score']
        
        # Find available controls
        if replace:
            available_controls = control
        else:
            available_controls = control[~control.index.isin(used_control_indices)]
        
        if len(available_controls) == 0:
            continue
        
        # Calculate distances
        distances = np.abs(available_controls['propensity_score'] - treated_ps)
        
        # Find nearest neighbor within caliper
        min_distance = distances.min()
        
        if min_distance <= caliper_distance:
            matched_control_idx = distances.idxmin()
            
            matched_pairs.append({
                'treated_idx': treated_row.name,
                'control_idx': matched_control_idx,
                'treated_ps': treated_ps,
                'control_ps': available_controls.loc[matched_control_idx, 'propensity_score'],
                'ps_distance': min_distance
            })
            
            if not replace:
                used_control_indices.add(matched_control_idx)
    
    print(f"\nMatched pairs: {len(matched_pairs):,} / {len(treated):,} "
          f"({100 * len(matched_pairs) / len(treated):.1f}% of treated units matched)")
    
    # Create matched dataset
    matched_treated_indices = [p['treated_idx'] for p in matched_pairs]
    matched_control_indices = [p['control_idx'] for p in matched_pairs]
    
    matched_df = pd.concat([
        df.loc[matched_treated_indices],
        df.loc[matched_control_indices]
    ]).copy()
    
    # Add pair ID for tracking
    pair_ids = []
    for i, pair in enumerate(matched_pairs):
        pair_ids.extend([i, i])  # Same pair ID for treated and control
    
    matched_df['pair_id'] = pair_ids
    matched_df['is_treated'] = matched_df[treatment_col]
    
    # Store matching info
    matching_info = pd.DataFrame(matched_pairs)
    
    return matched_df, ps_model, matching_info, scaler


def plot_propensity_score_distributions(df, matched_df, treatment_col, output_dir):
    """Plot propensity score distributions before and after matching"""
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Before matching
    ax = axes[0]
    treated_before = df[df[treatment_col] == 1]['propensity_score']
    control_before = df[df[treatment_col] == 0]['propensity_score']
    
    ax.hist(control_before, bins=50, alpha=0.6, label='Control', density=True)
    ax.hist(treated_before, bins=50, alpha=0.6, label='Treated', density=True)
    ax.set_xlabel('Propensity Score')
    ax.set_ylabel('Density')
    ax.set_title('Before Matching')
    ax.legend()
    ax.grid(alpha=0.3)
    
    # After matching
    ax = axes[1]
    treated_after = matched_df[matched_df[treatment_col] == 1]['propensity_score']
    control_after = matched_df[matched_df[treatment_col] == 0]['propensity_score']
    
    ax.hist(control_after, bins=50, alpha=0.6, label='Control', density=True)
    ax.hist(treated_after, bins=50, alpha=0.6, label='Treated', density=True)
    ax.set_xlabel('Propensity Score')
    ax.set_ylabel('Density')
    ax.set_title('After Matching')
    ax.legend()
    ax.grid(alpha=0.3)
    
    plt.tight_layout()
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = os.path.join(output_dir, f'propensity_score_distributions_{timestamp}.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\nSaved propensity score distribution plot to: {output_path}")
    plt.close()


def plot_covariate_balance(balance_before, balance_after, output_dir):
    """Plot covariate balance (SMD) before and after matching"""
    
    fig, ax = plt.subplots(figsize=(10, max(8, len(balance_before) * 0.3)))
    
    # Prepare data
    variables = balance_before['variable']
    smd_before = balance_before['smd']
    smd_after = balance_after['smd']
    
    y_pos = np.arange(len(variables))
    
    # Plot SMD values
    ax.scatter(smd_before, y_pos, label='Before Matching', alpha=0.7, s=100, marker='o')
    ax.scatter(smd_after, y_pos, label='After Matching', alpha=0.7, s=100, marker='s')
    
    # Add balance threshold lines
    ax.axvline(x=0.1, color='red', linestyle='--', alpha=0.5, label='Balance Threshold (±0.1)')
    ax.axvline(x=-0.1, color='red', linestyle='--', alpha=0.5)
    ax.axvline(x=0, color='black', linestyle='-', alpha=0.3, linewidth=0.5)
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels(variables)
    ax.set_xlabel('Standardized Mean Difference')
    ax.set_title('Covariate Balance: Before vs After Matching')
    ax.legend()
    ax.grid(alpha=0.3, axis='x')
    
    plt.tight_layout()
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = os.path.join(output_dir, f'covariate_balance_{timestamp}.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Saved covariate balance plot to: {output_path}")
    plt.close()


def main():
    """Main execution function"""
    
    # Setup paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir = os.path.join(base_dir, 'outputs')
    plots_dir = os.path.join(base_dir, 'plots')
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(plots_dir, exist_ok=True)
    
    print("="*80)
    print("PROPENSITY SCORE MATCHING FOR DP FIRST ORDER")
    print("="*80)
    print(f"\nTreatment: dp_first_order = 1")
    print(f"Control: dp_first_order = 0")
    print(f"\nCovariates to balance ({len(COVARIATES)}):")
    for cov in COVARIATES:
        print(f"  - {cov}")
    
    # Load data directly from Snowflake
    print("\n" + "="*80)
    print("DATA LOADING")
    print("="*80)
    
    df = load_data_from_snowflake()
    
    print(f"\nColumns: {df.columns.tolist()}")
    
    # Basic stats
    print(f"\nTreatment distribution:")
    print(df['dp_first_order'].value_counts())
    print(f"\nTreatment rate: {df['dp_first_order'].mean():.4f}")
    
    # Check for missing values in covariates
    missing_counts = df[COVARIATES].isnull().sum()
    if missing_counts.sum() > 0:
        print(f"\nMissing values in covariates:")
        print(missing_counts[missing_counts > 0])
        print("\nFilling missing values with 0...")
        df[COVARIATES] = df[COVARIATES].fillna(0)
    
    # Check balance before matching
    balance_before = check_balance(df, 'dp_first_order', COVARIATES, 
                                   label="COVARIATE BALANCE - BEFORE MATCHING")
    
    # Perform propensity score matching
    matched_df, ps_model, matching_info, scaler = propensity_score_matching(
        df, 
        treatment_col='dp_first_order',
        covariates=COVARIATES,
        caliper=0.2,  # 0.2 standard deviations
        replace=False  # 1:1 matching without replacement
    )
    
    # Check balance after matching
    balance_after = check_balance(matched_df, 'dp_first_order', COVARIATES,
                                 label="COVARIATE BALANCE - AFTER MATCHING")
    
    # Save matched dataset to Snowflake
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    print("\n" + "="*80)
    print("WRITING RESULTS TO SNOWFLAKE")
    print("="*80)
    
    with SnowflakeHook(schema='fionafan', database='proddb') as hook:
        # Save matched dataset
        matched_table_name = 'psm_matched_data_dp_first_order'
        print(f"\nWriting matched dataset to: proddb.fionafan.{matched_table_name}")
        hook.create_and_populate_table(
            df=matched_df,
            table_name=matched_table_name,
            schema='fionafan',
            database='proddb',
            method='pandas'
        )
        print(f"✓ Successfully wrote {len(matched_df):,} records to {matched_table_name}")
        
        # Save matching info
        matching_info_table_name = 'psm_matching_info_dp_first_order'
        print(f"\nWriting matching info to: proddb.fionafan.{matching_info_table_name}")
        hook.create_and_populate_table(
            df=matching_info,
            table_name=matching_info_table_name,
            schema='fionafan',
            database='proddb',
            method='pandas'
        )
        print(f"✓ Successfully wrote {len(matching_info):,} records to {matching_info_table_name}")
        
        # Save balance statistics
        balance_before['stage'] = 'before'
        balance_after['stage'] = 'after'
        balance_combined = pd.concat([balance_before, balance_after])
        balance_table_name = 'psm_balance_stats_dp_first_order'
        print(f"\nWriting balance statistics to: proddb.fionafan.{balance_table_name}")
        hook.create_and_populate_table(
            df=balance_combined,
            table_name=balance_table_name,
            schema='fionafan',
            database='proddb',
            method='pandas'
        )
        print(f"✓ Successfully wrote {len(balance_combined):,} records to {balance_table_name}")
    
    # Also save local CSV backups
    matched_output_file = os.path.join(output_dir, f'psm_matched_data_{timestamp}.csv')
    matched_df.to_csv(matched_output_file, index=False)
    print(f"\n✓ Local backup saved to: {matched_output_file}")
    
    # Generate plots
    plot_propensity_score_distributions(df, matched_df, 'dp_first_order', plots_dir)
    plot_covariate_balance(balance_before, balance_after, plots_dir)
    
    # Summary statistics
    print("\n" + "="*80)
    print("MATCHING SUMMARY")
    print("="*80)
    print(f"\nOriginal dataset size: {len(df):,}")
    print(f"Matched dataset size: {len(matched_df):,}")
    print(f"Matched pairs: {len(matched_df) // 2:,}")
    print(f"\nMatch rate: {100 * (len(matched_df) // 2) / df['dp_first_order'].sum():.1f}%")
    
    print(f"\nVariables balanced before matching: "
          f"{balance_before['balanced'].sum()} / {len(COVARIATES)}")
    print(f"Variables balanced after matching: "
          f"{balance_after['balanced'].sum()} / {len(COVARIATES)}")
    
    print("\n" + "="*80)
    print("OUTPUT TABLES")
    print("="*80)
    print("\nSnowflake tables created:")
    print(f"  1. proddb.fionafan.psm_matched_data_dp_first_order")
    print(f"     - Matched treated and control units")
    print(f"     - {len(matched_df):,} records ({len(matched_df) // 2:,} matched pairs)")
    print(f"  2. proddb.fionafan.psm_matching_info_dp_first_order")
    print(f"     - Matching details (propensity scores, distances)")
    print(f"     - {len(matching_info):,} records")
    print(f"  3. proddb.fionafan.psm_balance_stats_dp_first_order")
    print(f"     - Covariate balance statistics (before/after)")
    print(f"     - {len(balance_combined):,} records")
    
    print("\n" + "="*80)
    print("PSM ANALYSIS COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()

