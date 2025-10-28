"""
Session Clustering Analysis
Clusters sessions based on behavioral features and analyzes cluster characteristics.
Excludes outcome variables (checkout, order placement) from clustering features.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score, davies_bouldin_score
import sys
import os

# Add utils to path
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')
from utils.snowflake_connection import SnowflakeHook

# Set plotting style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

# Create output directories
os.makedirs('/Users/fiona.fan/Documents/fiona_analyses/user-analysis/first_session_analysis/plots', exist_ok=True)
os.makedirs('/Users/fiona.fan/Documents/fiona_analyses/user-analysis/first_session_analysis/outputs', exist_ok=True)


def load_session_data():
    """Load session features from Snowflake"""
    print("Loading session data from Snowflake...")
    
    query = """
    SELECT *
    FROM proddb.fionafan.all_user_sessions_with_events_features_gen
    WHERE total_events > 0  -- Exclude empty sessions
    """
    
    # Use SnowflakeHook to query
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(query, method='pandas')
    
    print(f"Loaded {len(df):,} sessions")
    print(f"Columns: {len(df.columns)}")
    
    return df


def select_clustering_features(df):
    """
    Select features for clustering, excluding outcome variables.
    
    Excluded outcome variables:
    - funnel_had_order, funnel_converted_bool, funnel_num_orders
    - funnel_num_success, funnel_reached_success
    - funnel_seconds_to_success, funnel_seconds_to_checkout
    """
    
    # Define feature categories to include
    clustering_features = []
    
    # Temporal features (keep basic ones)
    clustering_features.extend([
        'days_since_onboarding',
        'days_since_onboarding_mod7',
        'session_day_of_week',
    ])
    
    # Overall session metrics
    clustering_features.extend([
        'total_events',
        'session_duration_seconds',
        'events_per_minute',
        'num_unique_discovery_features',
        'num_unique_discovery_surfaces',
        'num_unique_stores',
    ])
    
    # Impression features
    clustering_features.extend([
        'impression_event_count',
        'impression_unique_stores',
        'impression_unique_features',
        'impression_had_any',
    ])
    
    # Action features
    clustering_features.extend([
        'action_event_count',
        'action_num_backgrounds',
        'action_num_foregrounds',
        'action_num_tab_switches',
        'action_num_address_actions',
        'action_num_payment_actions',
        'action_num_auth_actions',
        'action_had_tab_switch',
        'action_had_foreground',
    ])
    
    # Attribution features
    clustering_features.extend([
        'attribution_event_count',
        'attribution_num_card_clicks',
        'attribution_unique_stores_clicked',
        'attribution_had_any',
        'attribution_cnt_traditional_carousel',
        'attribution_cnt_core_search',
        'attribution_cnt_pill_filter',
        'attribution_cnt_home_feed',
    ])
    
    # Error features
    clustering_features.extend([
        'error_event_count',
    ])
    
    # Funnel features (EXCLUDING ORDER/CONVERSION OUTCOMES)
    clustering_features.extend([
        'funnel_num_adds',  # Keep adds - it's a behavior not final outcome
        'funnel_had_add',
        'funnel_num_store_page',
        'funnel_num_cart',
        'funnel_reached_store_bool',
        'funnel_reached_cart_bool',
    ])
    
    # Timing features (EXCLUDING order/success timing)
    clustering_features.extend([
        'timing_mean_inter_event_seconds',
        'timing_median_inter_event_seconds',
        'timing_events_first_30s',
        'timing_events_first_2min',
        'funnel_seconds_to_first_add',
        'funnel_seconds_to_store_page',
        'funnel_seconds_to_cart',
        'attribution_seconds_to_first_card_click',
        'action_seconds_to_first_tab_switch',
    ])
    
    # Store features
    clustering_features.extend([
        'store_had_nv',
        'store_nv_impression_occurred',
    ])
    
    # Filter to features that exist in the dataframe
    available_features = [f for f in clustering_features if f in df.columns]
    missing_features = [f for f in clustering_features if f not in df.columns]
    
    if missing_features:
        print(f"\nWarning: {len(missing_features)} features not found in data:")
        print(missing_features[:10])
    
    print(f"\nUsing {len(available_features)} features for clustering")
    
    return available_features


def preprocess_features(df, features):
    """Preprocess features for clustering"""
    print("\nPreprocessing features...")
    
    # Select features
    X = df[features].copy()
    
    # Handle missing values
    print(f"Missing values before imputation:")
    missing_pct = (X.isnull().sum() / len(X) * 100).sort_values(ascending=False)
    print(missing_pct[missing_pct > 0].head(10))
    
    # Fill missing values with median for numerical features
    X = X.fillna(X.median())
    
    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    X_scaled = pd.DataFrame(X_scaled, columns=features, index=X.index)
    
    print(f"Preprocessed shape: {X_scaled.shape}")
    
    return X_scaled, scaler


def determine_optimal_clusters(X_scaled, max_k=10):
    """Determine optimal number of clusters using elbow method and silhouette score"""
    print("\nDetermining optimal number of clusters...")
    
    # Subsample for faster computation if dataset is large
    if len(X_scaled) > 50000:
        print(f"Subsampling {50000:,} sessions for cluster optimization...")
        X_sample = X_scaled.sample(n=50000, random_state=42)
    else:
        X_sample = X_scaled
    
    inertias = []
    silhouette_scores = []
    davies_bouldin_scores = []
    k_range = range(2, max_k + 1)
    
    for k in k_range:
        print(f"Testing k={k}...")
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10, max_iter=300)
        labels = kmeans.fit_predict(X_sample)
        
        inertias.append(kmeans.inertia_)
        silhouette_scores.append(silhouette_score(X_sample, labels))
        davies_bouldin_scores.append(davies_bouldin_score(X_sample, labels))
    
    # Plot metrics
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    
    # Elbow plot
    axes[0].plot(k_range, inertias, 'bo-', linewidth=2, markersize=8)
    axes[0].set_xlabel('Number of Clusters (k)', fontsize=12)
    axes[0].set_ylabel('Inertia', fontsize=12)
    axes[0].set_title('Elbow Method', fontsize=14, fontweight='bold')
    axes[0].grid(True, alpha=0.3)
    
    # Silhouette score
    axes[1].plot(k_range, silhouette_scores, 'go-', linewidth=2, markersize=8)
    axes[1].set_xlabel('Number of Clusters (k)', fontsize=12)
    axes[1].set_ylabel('Silhouette Score', fontsize=12)
    axes[1].set_title('Silhouette Score (higher is better)', fontsize=14, fontweight='bold')
    axes[1].grid(True, alpha=0.3)
    
    # Davies-Bouldin score
    axes[2].plot(k_range, davies_bouldin_scores, 'ro-', linewidth=2, markersize=8)
    axes[2].set_xlabel('Number of Clusters (k)', fontsize=12)
    axes[2].set_ylabel('Davies-Bouldin Score', fontsize=12)
    axes[2].set_title('Davies-Bouldin Score (lower is better)', fontsize=14, fontweight='bold')
    axes[2].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('/Users/fiona.fan/Documents/fiona_analyses/user-analysis/first_session_analysis/plots/clustering_optimization.png', 
                dpi=300, bbox_inches='tight')
    print("Saved clustering optimization plot")
    
    # Print recommendations
    best_silhouette_k = k_range[np.argmax(silhouette_scores)]
    best_db_k = k_range[np.argmin(davies_bouldin_scores)]
    
    print(f"\nRecommendations:")
    print(f"Best k by Silhouette Score: {best_silhouette_k} (score: {max(silhouette_scores):.3f})")
    print(f"Best k by Davies-Bouldin: {best_db_k} (score: {min(davies_bouldin_scores):.3f})")
    
    return best_silhouette_k


def fit_kmeans_clustering(X_scaled, n_clusters):
    """Fit K-Means clustering"""
    print(f"\nFitting K-Means with {n_clusters} clusters...")
    
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=20, max_iter=500)
    cluster_labels = kmeans.fit_predict(X_scaled)
    
    # Calculate metrics
    silhouette = silhouette_score(X_scaled, cluster_labels)
    davies_bouldin = davies_bouldin_score(X_scaled, cluster_labels)
    
    print(f"Silhouette Score: {silhouette:.3f}")
    print(f"Davies-Bouldin Score: {davies_bouldin:.3f}")
    
    return kmeans, cluster_labels


def visualize_clusters_pca(X_scaled, cluster_labels, n_clusters):
    """Visualize clusters using PCA"""
    print("\nCreating PCA visualization...")
    
    # Apply PCA
    pca = PCA(n_components=2, random_state=42)
    X_pca = pca.fit_transform(X_scaled)
    
    # Create DataFrame for plotting
    pca_df = pd.DataFrame({
        'PC1': X_pca[:, 0],
        'PC2': X_pca[:, 1],
        'Cluster': cluster_labels
    })
    
    # Subsample for visualization if too many points
    if len(pca_df) > 20000:
        pca_df = pca_df.sample(n=20000, random_state=42)
    
    # Plot
    fig, ax = plt.subplots(figsize=(14, 10))
    
    for cluster in range(n_clusters):
        cluster_data = pca_df[pca_df['Cluster'] == cluster]
        ax.scatter(cluster_data['PC1'], cluster_data['PC2'], 
                  label=f'Cluster {cluster}', alpha=0.5, s=20)
    
    ax.set_xlabel(f'PC1 ({pca.explained_variance_ratio_[0]:.1%} variance)', fontsize=12)
    ax.set_ylabel(f'PC2 ({pca.explained_variance_ratio_[1]:.1%} variance)', fontsize=12)
    ax.set_title(f'Session Clusters (PCA Visualization)', fontsize=14, fontweight='bold')
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('/Users/fiona.fan/Documents/fiona_analyses/user-analysis/first_session_analysis/plots/clusters_pca_visualization.png', 
                dpi=300, bbox_inches='tight')
    print("Saved PCA visualization")
    
    return pca


def analyze_cluster_characteristics(df, cluster_labels, features):
    """Analyze and describe cluster characteristics"""
    print("\nAnalyzing cluster characteristics...")
    
    # Add cluster labels to dataframe
    df_analysis = df.copy()
    df_analysis['cluster'] = cluster_labels
    
    # Get cluster sizes
    cluster_sizes = df_analysis['cluster'].value_counts().sort_index()
    print("\nCluster Sizes:")
    for cluster, size in cluster_sizes.items():
        print(f"Cluster {cluster}: {size:,} sessions ({size/len(df)*100:.1f}%)")
    
    # Analyze key characteristics for each cluster
    key_metrics = [
        # Session basics
        'total_events',
        'session_duration_seconds',
        'events_per_minute',
        
        # User journey
        'impression_event_count',
        'attribution_num_card_clicks',
        'funnel_num_adds',
        'funnel_num_cart',
        
        # Outcome variables (for understanding, not clustering)
        'funnel_had_order',
        'funnel_converted_bool',
        
        # Timing
        'timing_median_inter_event_seconds',
        'funnel_seconds_to_first_add',
        'attribution_seconds_to_first_card_click',
        
        # Behavior flags
        'action_had_tab_switch',
        'action_num_payment_actions',
        'error_event_count',
        
        # Store
        'num_unique_stores',
        'store_had_nv',
    ]
    
    # Filter to available metrics
    key_metrics = [m for m in key_metrics if m in df_analysis.columns]
    
    # Calculate statistics by cluster
    cluster_stats = df_analysis.groupby('cluster')[key_metrics].agg(['mean', 'median', 'std']).round(2)
    
    # Save to file
    cluster_stats.to_csv('/Users/fiona.fan/Documents/fiona_analyses/user-analysis/first_session_analysis/outputs/cluster_statistics.csv')
    print("Saved cluster statistics to CSV")
    
    # Create cluster profiles
    print("\n" + "="*80)
    print("CLUSTER PROFILES")
    print("="*80)
    
    for cluster in sorted(df_analysis['cluster'].unique()):
        cluster_data = df_analysis[df_analysis['cluster'] == cluster]
        
        print(f"\n{'='*80}")
        print(f"CLUSTER {cluster} - {len(cluster_data):,} sessions ({len(cluster_data)/len(df)*100:.1f}%)")
        print(f"{'='*80}")
        
        # Session duration and events
        print(f"\n📊 Session Intensity:")
        print(f"  - Avg Duration: {cluster_data['session_duration_seconds'].mean():.0f} seconds ({cluster_data['session_duration_seconds'].mean()/60:.1f} min)")
        print(f"  - Median Duration: {cluster_data['session_duration_seconds'].median():.0f} seconds")
        print(f"  - Avg Total Events: {cluster_data['total_events'].mean():.1f}")
        print(f"  - Avg Events/Minute: {cluster_data['events_per_minute'].mean():.1f}")
        
        # User journey
        print(f"\n🛒 User Journey:")
        print(f"  - Avg Impressions: {cluster_data['impression_event_count'].mean():.1f}")
        print(f"  - Avg Card Clicks: {cluster_data['attribution_num_card_clicks'].mean():.1f}")
        print(f"  - Avg Adds to Cart: {cluster_data['funnel_num_adds'].mean():.1f}")
        print(f"  - % Reached Cart: {cluster_data['funnel_reached_cart_bool'].mean()*100:.1f}%")
        
        # Conversion outcomes
        print(f"\n🎯 Outcomes (not used for clustering):")
        print(f"  - % Had Order: {cluster_data['funnel_had_order'].mean()*100:.1f}%")
        print(f"  - % Converted: {cluster_data['funnel_converted_bool'].mean()*100:.1f}%")
        
        # Timing behavior
        print(f"\n⏱️  Timing Behavior:")
        print(f"  - Median Inter-Event Time: {cluster_data['timing_median_inter_event_seconds'].median():.1f}s")
        if cluster_data['funnel_seconds_to_first_add'].notna().any():
            print(f"  - Avg Time to First Add: {cluster_data['funnel_seconds_to_first_add'].mean():.0f}s")
        if cluster_data['attribution_seconds_to_first_card_click'].notna().any():
            print(f"  - Avg Time to First Click: {cluster_data['attribution_seconds_to_first_card_click'].mean():.0f}s")
        
        # Behavioral flags
        print(f"\n🎬 User Actions:")
        print(f"  - % Had Tab Switch: {cluster_data['action_had_tab_switch'].mean()*100:.1f}%")
        print(f"  - Avg Payment Actions: {cluster_data['action_num_payment_actions'].mean():.2f}")
        print(f"  - Avg Errors: {cluster_data['error_event_count'].mean():.2f}")
        
        # Store interaction
        print(f"\n🏪 Store Interaction:")
        print(f"  - Avg Unique Stores: {cluster_data['num_unique_stores'].mean():.1f}")
        print(f"  - % Had NV Store: {cluster_data['store_had_nv'].mean()*100:.1f}%")
        
        # Cohort distribution
        if 'cohort_type' in cluster_data.columns:
            print(f"\n👥 Cohort Distribution:")
            cohort_dist = cluster_data['cohort_type'].value_counts(normalize=True) * 100
            for cohort, pct in cohort_dist.items():
                print(f"  - {cohort}: {pct:.1f}%")
    
    return df_analysis


def create_cluster_comparison_plots(df_analysis):
    """Create visualizations comparing clusters"""
    print("\nCreating cluster comparison plots...")
    
    n_clusters = df_analysis['cluster'].nunique()
    
    # Plot 1: Session Duration and Event Count by Cluster
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # Duration distribution
    for cluster in range(n_clusters):
        data = df_analysis[df_analysis['cluster'] == cluster]['session_duration_seconds'] / 60
        axes[0, 0].hist(data.clip(upper=30), bins=50, alpha=0.5, label=f'Cluster {cluster}')
    axes[0, 0].set_xlabel('Session Duration (minutes)', fontsize=11)
    axes[0, 0].set_ylabel('Frequency', fontsize=11)
    axes[0, 0].set_title('Session Duration Distribution by Cluster', fontsize=12, fontweight='bold')
    axes[0, 0].legend()
    axes[0, 0].set_xlim(0, 30)
    
    # Event count distribution
    for cluster in range(n_clusters):
        data = df_analysis[df_analysis['cluster'] == cluster]['total_events']
        axes[0, 1].hist(data.clip(upper=200), bins=50, alpha=0.5, label=f'Cluster {cluster}')
    axes[0, 1].set_xlabel('Total Events', fontsize=11)
    axes[0, 1].set_ylabel('Frequency', fontsize=11)
    axes[0, 1].set_title('Total Events Distribution by Cluster', fontsize=12, fontweight='bold')
    axes[0, 1].legend()
    axes[0, 1].set_xlim(0, 200)
    
    # Funnel progression by cluster
    funnel_metrics = ['funnel_reached_store_bool', 'funnel_had_add', 
                      'funnel_reached_cart_bool', 'funnel_converted_bool']
    funnel_rates = df_analysis.groupby('cluster')[funnel_metrics].mean() * 100
    funnel_rates.plot(kind='bar', ax=axes[1, 0])
    axes[1, 0].set_xlabel('Cluster', fontsize=11)
    axes[1, 0].set_ylabel('% of Sessions', fontsize=11)
    axes[1, 0].set_title('Funnel Progression by Cluster', fontsize=12, fontweight='bold')
    axes[1, 0].legend(['Store Page', 'Add to Cart', 'Cart', 'Converted'], fontsize=9)
    axes[1, 0].set_xticklabels(axes[1, 0].get_xticklabels(), rotation=0)
    
    # Average adds and clicks by cluster
    journey_metrics = df_analysis.groupby('cluster')[['attribution_num_card_clicks', 'funnel_num_adds']].mean()
    journey_metrics.plot(kind='bar', ax=axes[1, 1])
    axes[1, 1].set_xlabel('Cluster', fontsize=11)
    axes[1, 1].set_ylabel('Average Count', fontsize=11)
    axes[1, 1].set_title('Card Clicks & Cart Adds by Cluster', fontsize=12, fontweight='bold')
    axes[1, 1].legend(['Card Clicks', 'Cart Adds'], fontsize=9)
    axes[1, 1].set_xticklabels(axes[1, 1].get_xticklabels(), rotation=0)
    
    plt.tight_layout()
    plt.savefig('/Users/fiona.fan/Documents/fiona_analyses/user-analysis/first_session_analysis/plots/cluster_comparison_overview.png', 
                dpi=300, bbox_inches='tight')
    print("Saved cluster comparison overview")
    
    # Plot 2: Timing Behavior by Cluster
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    # Box plot of inter-event time
    timing_data = []
    labels = []
    for cluster in range(n_clusters):
        data = df_analysis[df_analysis['cluster'] == cluster]['timing_median_inter_event_seconds'].dropna()
        timing_data.append(data.clip(upper=60))
        labels.append(f'Cluster {cluster}')
    
    axes[0].boxplot(timing_data, labels=labels)
    axes[0].set_ylabel('Median Inter-Event Time (seconds)', fontsize=11)
    axes[0].set_title('Inter-Event Timing by Cluster', fontsize=12, fontweight='bold')
    axes[0].grid(True, alpha=0.3, axis='y')
    
    # Box plot of time to first add
    add_timing_data = []
    add_labels = []
    for cluster in range(n_clusters):
        data = df_analysis[df_analysis['cluster'] == cluster]['funnel_seconds_to_first_add'].dropna()
        if len(data) > 0:
            add_timing_data.append(data.clip(upper=300))
            add_labels.append(f'Cluster {cluster}')
    
    if add_timing_data:
        axes[1].boxplot(add_timing_data, labels=add_labels)
        axes[1].set_ylabel('Seconds to First Add', fontsize=11)
        axes[1].set_title('Time to First Cart Add by Cluster', fontsize=12, fontweight='bold')
        axes[1].grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig('/Users/fiona.fan/Documents/fiona_analyses/user-analysis/first_session_analysis/plots/cluster_timing_behavior.png', 
                dpi=300, bbox_inches='tight')
    print("Saved cluster timing behavior plot")


def save_cluster_assignments(df, cluster_labels):
    """Save cluster assignments for each session"""
    print("\nSaving cluster assignments...")
    
    # Create output dataframe
    output_df = df[['user_id', 'dd_device_id', 'event_date', 'session_num', 
                     'cohort_type', 'session_type']].copy()
    output_df['cluster'] = cluster_labels
    
    # Save to CSV
    output_path = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/first_session_analysis/outputs/session_cluster_assignments.csv'
    output_df.to_csv(output_path, index=False)
    print(f"Saved cluster assignments to {output_path}")
    
    return output_df


def main():
    """Main execution function"""
    print("="*80)
    print("SESSION CLUSTERING ANALYSIS")
    print("="*80)
    
    # Load data
    df = load_session_data()
    
    # Select features for clustering
    features = select_clustering_features(df)
    
    # Preprocess features
    X_scaled, scaler = preprocess_features(df, features)
    
    # Determine optimal number of clusters
    optimal_k = determine_optimal_clusters(X_scaled, max_k=10)
    
    # Allow user to override if needed
    print(f"\nUsing k={optimal_k} clusters (based on silhouette score)")
    print("If you want to use a different k, modify the script and set n_clusters manually")
    n_clusters = optimal_k
    
    # Fit clustering
    kmeans, cluster_labels = fit_kmeans_clustering(X_scaled, n_clusters)
    
    # Visualize clusters
    pca = visualize_clusters_pca(X_scaled, cluster_labels, n_clusters)
    
    # Analyze cluster characteristics
    df_analysis = analyze_cluster_characteristics(df, cluster_labels, features)
    
    # Create comparison plots
    create_cluster_comparison_plots(df_analysis)
    
    # Save cluster assignments
    output_df = save_cluster_assignments(df, cluster_labels)
    
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)
    print("\nOutput files:")
    print("  - plots/clustering_optimization.png")
    print("  - plots/clusters_pca_visualization.png")
    print("  - plots/cluster_comparison_overview.png")
    print("  - plots/cluster_timing_behavior.png")
    print("  - outputs/cluster_statistics.csv")
    print("  - outputs/session_cluster_assignments.csv")


if __name__ == "__main__":
    main()

