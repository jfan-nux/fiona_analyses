"""
Power Analysis for Experiment Duration Planning
Author: Fiona Fan
Date: 2025-01-05

This script executes the power analysis to determine optimal experiment duration
for detecting a 3k MAU effect size.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import os
import sys

# Add utils to path for Snowflake connection
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..', 'utils'))
from snowflake_connection import SnowflakeHook

def load_power_analysis_results():
    """Execute the power analysis SQL and return results."""
    
    # Read the SQL file
    sql_file_path = os.path.join(os.path.dirname(__file__), '..', 'sql', 'power_analysis.sql')
    
    with open(sql_file_path, 'r') as f:
        query = f.read()
    
    # Execute query using SnowflakeHook
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(query, method='pandas')
        return df

def interpret_power_analysis_results(df):
    """Extract key metrics from power analysis results and provide interpretation."""
    
    # Convert results to a more usable format
    results_dict = {}
    
    for _, row in df.iterrows():
        if row['METRIC'] is not None:
            results_dict[row['METRIC']] = {
                'value': row['VALUE'],
                'unit': row['UNIT']
            }
    
    print("=" * 80)
    print("POWER ANALYSIS RESULTS SUMMARY")
    print("=" * 80)
    
    # Extract key metrics
    daily_enrollments = results_dict.get('Average Daily Enrollments (Past 30 Days)', {}).get('value', 'N/A')
    baseline_mau_rate = results_dict.get('Baseline MAU Rate', {}).get('value', 'N/A')
    required_sample_size = results_dict.get('Required Sample Size Per Arm', {}).get('value', 'N/A')
    total_duration = results_dict.get('Total Experiment Duration (w/ MAU measurement)', {}).get('value', 'N/A')
    
    print(f"\n📊 ENROLLMENT METRICS:")
    print(f"   • Daily enrollments: {daily_enrollments} users/day")
    print(f"   • Baseline MAU rate: {baseline_mau_rate}%")
    
    print(f"\n🎯 EXPERIMENT REQUIREMENTS:")
    print(f"   • Target effect: 3,000 MAU increase")
    print(f"   • Required sample size per arm: {required_sample_size:,} users")
    print(f"   • Recommended duration: {total_duration} days")
    
    if isinstance(total_duration, (int, float)) and total_duration > 0:
        end_date = datetime.now() + timedelta(days=int(total_duration))
        print(f"   • Estimated end date: {end_date.strftime('%Y-%m-%d')}")
        
        # Provide interpretation
        print(f"\n💡 INTERPRETATION:")
        if total_duration <= 30:
            print(f"   ✅ SHORT EXPERIMENT: {total_duration} days is manageable")
        elif total_duration <= 60:
            print(f"   ⚠️  MEDIUM EXPERIMENT: {total_duration} days requires commitment")
        else:
            print(f"   🚨 LONG EXPERIMENT: {total_duration} days may be challenging")
            
        weeks = total_duration / 7
        print(f"   • Duration in weeks: ~{weeks:.1f} weeks")
        
        if isinstance(required_sample_size, (int, float)):
            print(f"   • Total users needed: {int(required_sample_size * 2):,} (both arms)")
    
    # Scenario analysis
    scenario_50_higher = results_dict.get('Days if 50% Higher Enrollment', {}).get('value', 'N/A')
    scenario_20_lower = results_dict.get('Days if 20% Lower Enrollment', {}).get('value', 'N/A')
    
    print(f"\n📈 SCENARIO ANALYSIS:")
    print(f"   • If enrollment +50%: {scenario_50_higher} days")
    print(f"   • If enrollment -20%: {scenario_20_lower} days")
    
    return results_dict

def create_power_analysis_visualizations(results_dict):
    """Create visualizations for the power analysis results."""
    
    # Extract numeric values for plotting
    try:
        daily_enrollments = float(results_dict.get('Average Daily Enrollments (Past 30 Days)', {}).get('value', 0))
        required_sample_size = float(results_dict.get('Required Sample Size Per Arm', {}).get('value', 0))
        base_duration = float(results_dict.get('Total Experiment Duration (w/ MAU measurement)', {}).get('value', 0))
        
        if daily_enrollments <= 0 or required_sample_size <= 0:
            print("⚠️  Cannot create visualizations - missing key metrics")
            return
            
    except (ValueError, TypeError):
        print("⚠️  Cannot create visualizations - invalid numeric values")
        return
    
    # Create visualizations
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('Power Analysis Results - Experiment Duration Planning', fontsize=16, fontweight='bold')
    
    # 1. Duration vs Enrollment Rate Sensitivity
    enrollment_multipliers = np.arange(0.5, 2.1, 0.1)
    durations = []
    
    for multiplier in enrollment_multipliers:
        adjusted_enrollment = daily_enrollments * multiplier
        duration = (required_sample_size / adjusted_enrollment) + 28  # +28 for MAU measurement
        durations.append(duration)
    
    ax1.plot(enrollment_multipliers * 100, durations, 'b-', linewidth=2, marker='o', markersize=4)
    ax1.axhline(y=base_duration, color='r', linestyle='--', alpha=0.7, label=f'Current: {base_duration:.0f} days')
    ax1.axvline(x=100, color='gray', linestyle=':', alpha=0.5)
    ax1.set_xlabel('Enrollment Rate (% of current)')
    ax1.set_ylabel('Total Experiment Duration (days)')
    ax1.set_title('Duration Sensitivity to Enrollment Rate')
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    
    # 2. Sample Size Requirements Breakdown
    categories = ['Per Arm', 'Total (Both Arms)']
    values = [required_sample_size, required_sample_size * 2]
    colors = ['skyblue', 'lightcoral']
    
    bars = ax2.bar(categories, values, color=colors, alpha=0.8)
    ax2.set_ylabel('Required Sample Size')
    ax2.set_title('Sample Size Requirements')
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Add value labels on bars
    for bar, value in zip(bars, values):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                f'{int(value):,}', ha='center', va='bottom', fontweight='bold')
    
    # 3. Timeline Visualization
    phases = ['Enrollment\nPeriod', 'MAU Measurement\nPeriod', 'Analysis &\nReporting']
    phase_durations = [base_duration - 28, 28, 7]  # Assuming 7 days for analysis
    colors_timeline = ['lightgreen', 'orange', 'lightblue']
    
    bottom = 0
    for i, (phase, duration, color) in enumerate(zip(phases, phase_durations, colors_timeline)):
        ax3.barh(0, duration, left=bottom, height=0.5, color=color, alpha=0.8, label=phase)
        
        # Add duration labels
        ax3.text(bottom + duration/2, 0, f'{int(duration)}d', 
                ha='center', va='center', fontweight='bold', fontsize=10)
        bottom += duration
    
    ax3.set_xlim(0, sum(phase_durations))
    ax3.set_ylim(-0.5, 0.5)
    ax3.set_xlabel('Days from Start')
    ax3.set_title('Experiment Timeline')
    ax3.set_yticks([])
    ax3.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax3.grid(True, alpha=0.3, axis='x')
    
    # 4. Effect Size Context
    effect_sizes = [1000, 2000, 3000, 4000, 5000]
    durations_by_effect = []
    
    baseline_mau_rate = float(results_dict.get('Baseline MAU Rate', {}).get('value', 10)) / 100
    baseline_variance = baseline_mau_rate * (1 - baseline_mau_rate)
    
    for effect in effect_sizes:
        # Rough approximation of required sample size for different effect sizes
        effect_rate = effect / required_sample_size  # Approximate relative effect
        sample_needed = 2 * (1.96 + 0.84)**2 * baseline_variance / (effect_rate**2)
        duration_needed = (sample_needed / daily_enrollments) + 28
        durations_by_effect.append(duration_needed)
    
    ax4.plot(effect_sizes, durations_by_effect, 'g-', linewidth=2, marker='s', markersize=6)
    ax4.axvline(x=3000, color='r', linestyle='--', alpha=0.7, label='Target: 3k MAU')
    ax4.axhline(y=base_duration, color='r', linestyle='--', alpha=0.5)
    ax4.set_xlabel('Effect Size (MAU increase)')
    ax4.set_ylabel('Required Duration (days)')
    ax4.set_title('Duration vs Effect Size')
    ax4.grid(True, alpha=0.3)
    ax4.legend()
    
    plt.tight_layout()
    
    # Save the plot
    plots_dir = os.path.join(os.path.dirname(__file__), '..', 'plots')
    os.makedirs(plots_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"power_analysis_results_{timestamp}.png"
    filepath = os.path.join(plots_dir, filename)
    
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    print(f"\n📊 Visualization saved: {filepath}")
    
    plt.show()

def main():
    """Main execution function."""
    
    print("🔍 Executing Power Analysis for Experiment Duration...")
    print("=" * 60)
    
    try:
        # Load and display results
        df = load_power_analysis_results()
        print("✅ Power analysis query executed successfully")
        
        # Interpret results
        results_dict = interpret_power_analysis_results(df)
        
        # Create visualizations
        print("\n📊 Creating visualizations...")
        create_power_analysis_visualizations(results_dict)
        
        print("\n" + "=" * 80)
        print("🎯 RECOMMENDATIONS:")
        print("=" * 80)
        
        total_duration = results_dict.get('Total Experiment Duration (w/ MAU measurement)', {}).get('value')
        
        if isinstance(total_duration, (int, float)):
            if total_duration <= 45:
                print("✅ PROCEED: Experiment duration is reasonable for your team's capacity")
            elif total_duration <= 90:
                print("⚠️  CONSIDER: Long experiment - ensure stakeholder alignment")
            else:
                print("🚨 REASSESS: Very long experiment - consider reducing effect size or improving enrollment")
        
        print("\n📋 NEXT STEPS:")
        print("1. Review enrollment rate projections with growth team")
        print("2. Confirm 3k MAU effect size aligns with business goals") 
        print("3. Plan experiment timeline with stakeholders")
        print("4. Set up monitoring for enrollment rates during experiment")
        
    except Exception as e:
        print(f"❌ Error executing power analysis: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
