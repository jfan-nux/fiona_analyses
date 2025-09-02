"""
Simulated Power Analysis for New User MAU Experiment
Author: Fiona Fan
Date: 2025-01-05

This script simulates power analysis using typical new user conversion patterns
to determine optimal experiment duration for detecting a 3k MAU effect size.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import os
import warnings
warnings.filterwarnings('ignore')

# Set style for plots
plt.style.use('default')
sns.set_palette("husl")

def simulate_baseline_metrics():
    """
    Simulate baseline metrics based on typical new user onboarding patterns.
    Using industry benchmarks and DoorDash-like patterns.
    """
    
    # Typical patterns for food delivery onboarding
    simulated_metrics = {
        # Daily enrollment (users starting onboarding flow)
        'avg_daily_enrollments': 12000,  # Typical for large food delivery app
        'std_daily_enrollments': 2400,   # ~20% CV
        
        # MAU conversion within 28 days of enrollment
        'baseline_mau_rate': 0.35,  # 35% of new users become MAU within 28 days
        'total_enrolled_cohort': 350000,  # 28-day cohort size for baseline
        'mau_count_baseline': 122500,     # 35% of enrolled cohort
        
        # Experiment parameters
        'target_effect_absolute': 3000,  # 3k MAU increase
        'alpha': 0.05,                   # 5% significance
        'power': 0.80,                   # 80% power
        'z_alpha': 1.96,                 # Critical value
        'z_beta': 0.84                   # Power critical value
    }
    
    return simulated_metrics

def calculate_power_analysis(metrics):
    """Calculate power analysis based on simulated metrics."""
    
    # Extract key metrics
    baseline_rate = metrics['baseline_mau_rate']
    daily_enrollments = metrics['avg_daily_enrollments']
    target_effect = metrics['target_effect_absolute']
    z_alpha = metrics['z_alpha']
    z_beta = metrics['z_beta']
    
    # Calculate relative effect size
    baseline_mau_absolute = metrics['mau_count_baseline']
    relative_effect = target_effect / baseline_mau_absolute
    
    # Variance for binomial distribution
    variance = baseline_rate * (1 - baseline_rate)
    
    # Required sample size per arm for proportion test
    # n = 2 * (z_α + z_β)² * p(1-p) / (effect_size)²
    required_sample_per_arm = (2 * (z_alpha + z_beta)**2 * variance) / (relative_effect**2)
    required_sample_per_arm = int(np.ceil(required_sample_per_arm))
    
    # Duration calculations
    enrollment_days = int(np.ceil(required_sample_per_arm / daily_enrollments))
    total_duration_days = enrollment_days + 28  # +28 for MAU measurement
    
    # Scenario analysis
    scenarios = {
        '50% Higher Enrollment': {
            'daily_rate': daily_enrollments * 1.5,
            'duration': int(np.ceil(required_sample_per_arm / (daily_enrollments * 1.5)) + 28)
        },
        '20% Lower Enrollment': {
            'daily_rate': daily_enrollments * 0.8,
            'duration': int(np.ceil(required_sample_per_arm / (daily_enrollments * 0.8)) + 28)
        },
        'Current Rate': {
            'daily_rate': daily_enrollments,
            'duration': total_duration_days
        }
    }
    
    results = {
        **metrics,
        'relative_effect_size': relative_effect,
        'variance': variance,
        'required_sample_per_arm': required_sample_per_arm,
        'total_required_sample': required_sample_per_arm * 2,
        'enrollment_days': enrollment_days,
        'total_duration_days': total_duration_days,
        'estimated_end_date': datetime.now() + timedelta(days=total_duration_days),
        'scenarios': scenarios
    }
    
    return results

def print_power_analysis_results(results):
    """Print comprehensive power analysis results."""
    
    print("=" * 80)
    print("🔍 POWER ANALYSIS RESULTS - NEW USER MAU EXPERIMENT")
    print("=" * 80)
    
    print(f"\n📊 ENROLLMENT METRICS:")
    print(f"   • Average daily enrollments: {results['avg_daily_enrollments']:,} users/day")
    print(f"   • Standard deviation: {results['std_daily_enrollments']:,} users/day")
    
    print(f"\n🎯 BASELINE MAU METRICS:")
    print(f"   • Historical MAU count (28-day cohort): {results['mau_count_baseline']:,} MAU")
    print(f"   • Baseline MAU rate: {results['baseline_mau_rate']*100:.1f}%")
    print(f"   • Total enrolled in baseline cohort: {results['total_enrolled_cohort']:,} users")
    
    print(f"\n⚡ POWER ANALYSIS PARAMETERS:")
    print(f"   • Target effect size (absolute): {results['target_effect_absolute']:,} MAU increase")
    print(f"   • Target effect size (relative): {results['relative_effect_size']*100:.2f}% increase")
    print(f"   • Significance level (α): {results['alpha']*100:.0f}%")
    print(f"   • Statistical power: {results['power']*100:.0f}%")
    
    print(f"\n📏 SAMPLE SIZE REQUIREMENTS:")
    print(f"   • Required sample size per arm: {results['required_sample_per_arm']:,} users")
    print(f"   • Total required sample size: {results['total_required_sample']:,} users")
    
    print(f"\n⏱️  DURATION RECOMMENDATIONS:")
    print(f"   • Days to reach sample size (enrollment): {results['enrollment_days']} days")
    print(f"   • Total experiment duration (w/ MAU measurement): {results['total_duration_days']} days")
    print(f"   • Estimated end date: {results['estimated_end_date'].strftime('%Y-%m-%d')}")
    
    # Interpretation
    print(f"\n💡 INTERPRETATION:")
    if results['total_duration_days'] <= 45:
        print(f"   ✅ MANAGEABLE: {results['total_duration_days']} days is reasonable for your team")
    elif results['total_duration_days'] <= 75:
        print(f"   ⚠️  MODERATE: {results['total_duration_days']} days requires planning and commitment")
    else:
        print(f"   🚨 CHALLENGING: {results['total_duration_days']} days is a long experiment")
    
    weeks = results['total_duration_days'] / 7
    print(f"   • Duration in weeks: ~{weeks:.1f} weeks")
    print(f"   • Total users needed: {results['total_required_sample']:,} (both arms)")
    
    print(f"\n📈 SCENARIO ANALYSIS:")
    for scenario, data in results['scenarios'].items():
        print(f"   • {scenario}: {data['duration']} days total "
              f"({data['daily_rate']:,.0f} enrollments/day)")

def create_power_analysis_visualizations(results):
    """Create comprehensive visualizations for power analysis."""
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Power Analysis Results - New User MAU Experiment Duration', 
                 fontsize=16, fontweight='bold')
    
    # 1. Sensitivity Analysis: Duration vs Enrollment Rate
    enrollment_multipliers = np.arange(0.5, 2.1, 0.1)
    base_enrollments = results['avg_daily_enrollments']
    required_sample = results['required_sample_per_arm']
    
    durations = []
    for multiplier in enrollment_multipliers:
        adjusted_enrollment = base_enrollments * multiplier
        duration = (required_sample / adjusted_enrollment) + 28
        durations.append(duration)
    
    ax1.plot(enrollment_multipliers * 100, durations, 'b-', linewidth=3, marker='o', markersize=5)
    ax1.axhline(y=results['total_duration_days'], color='r', linestyle='--', alpha=0.8, 
                linewidth=2, label=f'Current: {results["total_duration_days"]:.0f} days')
    ax1.axvline(x=100, color='gray', linestyle=':', alpha=0.7)
    ax1.set_xlabel('Enrollment Rate (% of current)', fontsize=12)
    ax1.set_ylabel('Total Experiment Duration (days)', fontsize=12)
    ax1.set_title('Duration Sensitivity to Enrollment Rate', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    
    # 2. Sample Size Breakdown
    categories = ['Per Arm', 'Total\n(Both Arms)']
    values = [results['required_sample_per_arm'], results['total_required_sample']]
    colors = ['skyblue', 'lightcoral']
    
    bars = ax2.bar(categories, values, color=colors, alpha=0.8, width=0.6)
    ax2.set_ylabel('Required Sample Size', fontsize=12)
    ax2.set_title('Sample Size Requirements', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Add value labels on bars
    for bar, value in zip(bars, values):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + height*0.02,
                f'{int(value):,}', ha='center', va='bottom', fontweight='bold', fontsize=11)
    
    # 3. Timeline Breakdown
    phases = ['Enrollment\nPeriod', 'MAU Measurement\nPeriod', 'Analysis &\nReporting']
    phase_durations = [results['enrollment_days'], 28, 7]
    colors_timeline = ['lightgreen', 'orange', 'lightblue']
    
    bottom = 0
    for i, (phase, duration, color) in enumerate(zip(phases, phase_durations, colors_timeline)):
        ax3.barh(0, duration, left=bottom, height=0.6, color=color, alpha=0.8, label=phase)
        
        # Add duration labels
        ax3.text(bottom + duration/2, 0, f'{int(duration)}d', 
                ha='center', va='center', fontweight='bold', fontsize=11)
        bottom += duration
    
    ax3.set_xlim(0, sum(phase_durations) + 2)
    ax3.set_ylim(-0.5, 0.5)
    ax3.set_xlabel('Days from Start', fontsize=12)
    ax3.set_title('Experiment Timeline Breakdown', fontsize=14, fontweight='bold')
    ax3.set_yticks([])
    ax3.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax3.grid(True, alpha=0.3, axis='x')
    
    # 4. Effect Size Scenarios
    effect_sizes = [1000, 2000, 3000, 4000, 5000, 6000]
    baseline_mau = results['mau_count_baseline']
    baseline_rate = results['baseline_mau_rate']
    variance = baseline_rate * (1 - baseline_rate)
    z_sum = results['z_alpha'] + results['z_beta']
    
    durations_by_effect = []
    for effect in effect_sizes:
        relative_effect = effect / baseline_mau
        sample_needed = 2 * (z_sum**2) * variance / (relative_effect**2)
        duration_needed = (sample_needed / base_enrollments) + 28
        durations_by_effect.append(duration_needed)
    
    ax4.plot(effect_sizes, durations_by_effect, 'g-', linewidth=3, marker='s', markersize=6)
    ax4.axvline(x=3000, color='r', linestyle='--', alpha=0.8, linewidth=2, label='Target: 3k MAU')
    ax4.axhline(y=results['total_duration_days'], color='r', linestyle='--', alpha=0.5, linewidth=1)
    ax4.set_xlabel('Effect Size (MAU increase)', fontsize=12)
    ax4.set_ylabel('Required Duration (days)', fontsize=12)
    ax4.set_title('Duration vs Effect Size', fontsize=14, fontweight='bold')
    ax4.grid(True, alpha=0.3)
    ax4.legend()
    
    plt.tight_layout()
    
    # Save the plot
    plots_dir = os.path.join(os.path.dirname(__file__), '..', 'plots')
    os.makedirs(plots_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"simulated_power_analysis_{timestamp}.png"
    filepath = os.path.join(plots_dir, filename)
    
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    print(f"\n📊 Visualization saved: {filepath}")
    
    plt.show()

def create_scenario_comparison_table(results):
    """Create a detailed scenario comparison table."""
    
    scenarios_data = []
    for scenario, data in results['scenarios'].items():
        scenarios_data.append({
            'Scenario': scenario,
            'Daily Enrollments': f"{data['daily_rate']:,.0f}",
            'Total Duration': f"{data['duration']} days",
            'Duration (Weeks)': f"{data['duration']/7:.1f}",
            'End Date': (datetime.now() + timedelta(days=data['duration'])).strftime('%Y-%m-%d')
        })
    
    df_scenarios = pd.DataFrame(scenarios_data)
    
    print(f"\n📋 DETAILED SCENARIO COMPARISON:")
    print("=" * 80)
    print(df_scenarios.to_string(index=False))
    
    return df_scenarios

def main():
    """Main execution function."""
    
    print("🔍 Simulated Power Analysis for New User MAU Experiment")
    print("=" * 60)
    print("Note: Using industry-typical conversion rates for new user onboarding")
    print()
    
    # Generate simulated baseline metrics
    print("📊 Generating baseline metrics simulation...")
    metrics = simulate_baseline_metrics()
    
    # Calculate power analysis
    print("⚡ Calculating power analysis...")
    results = calculate_power_analysis(metrics)
    
    # Display results
    print_power_analysis_results(results)
    
    # Create scenario comparison
    scenario_df = create_scenario_comparison_table(results)
    
    # Create visualizations
    print("\n📊 Creating visualizations...")
    create_power_analysis_visualizations(results)
    
    # Recommendations
    print("\n" + "=" * 80)
    print("🎯 STRATEGIC RECOMMENDATIONS:")
    print("=" * 80)
    
    duration = results['total_duration_days']
    
    if duration <= 45:
        print("✅ PROCEED: Experiment duration is manageable")
        print("   • Timeline allows for quick iteration and learning")
        print("   • Low opportunity cost for experiment resources")
    elif duration <= 75:
        print("⚠️  PLAN CAREFULLY: Moderate-length experiment")
        print("   • Ensure strong stakeholder alignment before starting")
        print("   • Consider interim analysis checkpoints")
        print("   • Plan for potential external factors over duration")
    else:
        print("🚨 REASSESS: Long experiment duration")
        print("   • Consider reducing target effect size")
        print("   • Explore ways to increase enrollment rate")
        print("   • Evaluate if effect is worth the time investment")
    
    print(f"\n📋 IMMEDIATE NEXT STEPS:")
    print("1. 📊 Run the actual SQL query when Snowflake access is available")
    print("2. 🔍 Validate baseline MAU rate assumptions with real data")
    print("3. 📈 Confirm daily enrollment rate projections with growth team")
    print("4. 🎯 Validate that 3k MAU increase aligns with business impact goals")
    print("5. 📅 Plan experiment timeline and resource allocation")
    print("6. 🔔 Set up monitoring dashboard for enrollment tracking")
    
    print(f"\n💡 KEY INSIGHTS:")
    relative_lift = results['relative_effect_size'] * 100
    print(f"   • Target represents a {relative_lift:.1f}% relative increase in MAU")
    print(f"   • Experiment requires {results['total_required_sample']:,} total users")
    print(f"   • Each day of delay costs ~{results['avg_daily_enrollments']:,} potential enrollments")
    
    sample_efficiency = results['required_sample_per_arm'] / results['mau_count_baseline']
    print(f"   • Sample efficiency: {sample_efficiency:.1f}x baseline cohort size per arm")

if __name__ == "__main__":
    main()
