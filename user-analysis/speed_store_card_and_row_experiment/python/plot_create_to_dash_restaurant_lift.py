"""
Plot lift analysis for create_to_dash and create_to_restaurant metrics.
These metrics represent:
- create_to_dash: Time from order creation to dasher confirmation
- create_to_restaurant: Time from order creation to restaurant pickup

Usage: 
    python plot_create_to_dash_restaurant_lift.py
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from utils.snowflake_connection import SnowflakeHook


def fetch_data():
    """Fetch create_to_dash and create_to_restaurant metrics from Snowflake."""
    
    # Read the SQL query from the eta_analysis.sql file
    sql_file_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "sql",
        "eta_analysis.sql"
    )
    
    with open(sql_file_path, "r") as f:
        query = f.read()
    
    print("Executing ETA analysis query...")
    
    # Use SnowflakeHook to execute the query
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(query, method="pandas")
    
    return df


def plot_lift_analysis(df, output_dir):
    """Create visualization for create_to_dash, create_to_restaurant, r2c, and d2c lift."""
    
    # Set style
    sns.set_style("whitegrid")
    plt.rcParams["figure.figsize"] = (20, 16)
    
    fig, axes = plt.subplots(4, 2, figsize=(18, 20))
    fig.suptitle(
        "ETA Component Lift Analysis\n"
        "Treatment vs Control",
        fontsize=18,
        fontweight="bold",
        y=0.995
    )
    
    # Convert order_date to datetime if it's not already
    df["order_date"] = pd.to_datetime(df["order_date"])
    
    # Sort by date
    df = df.sort_values("order_date")
    
    # Filter to only daily metrics (exclude overall)
    df_daily = df[df["metric_type"] == "daily"].copy()
    
    if df_daily.empty:
        print("❌ No daily data found in the results")
        return
    
    # Plot 1: Create to Dash - Absolute Values
    ax1 = axes[0, 0]
    ax1.plot(
        df_daily["order_date"],
        df_daily["treatment_create_to_dash_minutes"],
        marker="o",
        label="Treatment",
        color="#1f77b4",
        linewidth=2
    )
    ax1.plot(
        df_daily["order_date"],
        df_daily["control_create_to_dash_minutes"],
        marker="s",
        label="Control",
        color="#ff7f0e",
        linewidth=2
    )
    ax1.set_title(
        "Create to Dash: Absolute Values (Minutes)",
        fontsize=12,
        fontweight="bold"
    )
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Minutes")
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis="x", rotation=45)
    
    # Plot 2: Create to Dash - Lift %
    ax2 = axes[0, 1]
    colors = [
        "green" if x < 0 else "red"
        for x in df_daily["create_to_dash_minutes_lift_pct"]
    ]
    ax2.bar(
        df_daily["order_date"],
        df_daily["create_to_dash_minutes_lift_pct"],
        color=colors,
        alpha=0.6
    )
    ax2.axhline(y=0, color="black", linestyle="--", linewidth=1)
    ax2.set_title("Create to Dash: Lift %", fontsize=12, fontweight="bold")
    ax2.set_xlabel("Date")
    ax2.set_ylabel("Lift %")
    ax2.grid(True, alpha=0.3, axis="y")
    ax2.tick_params(axis="x", rotation=45)
    
    # Add average line
    avg_lift = df_daily["create_to_dash_minutes_lift_pct"].mean()
    ax2.axhline(
        y=avg_lift,
        color="blue",
        linestyle=":",
        linewidth=2,
        label=f"Avg: {avg_lift:.2f}%"
    )
    ax2.legend()
    
    # Plot 3: Create to Restaurant - Absolute Values
    ax3 = axes[1, 0]
    ax3.plot(
        df_daily["order_date"],
        df_daily["treatment_create_to_restaurant_minutes"],
        marker="o",
        label="Treatment",
        color="#1f77b4",
        linewidth=2
    )
    ax3.plot(
        df_daily["order_date"],
        df_daily["control_create_to_restaurant_minutes"],
        marker="s",
        label="Control",
        color="#ff7f0e",
        linewidth=2
    )
    ax3.set_title(
        "Create to Restaurant: Absolute Values (Minutes)",
        fontsize=12,
        fontweight="bold"
    )
    ax3.set_xlabel("Date")
    ax3.set_ylabel("Minutes")
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    ax3.tick_params(axis="x", rotation=45)
    
    # Plot 4: Create to Restaurant - Lift %
    ax4 = axes[1, 1]
    colors = [
        "green" if x < 0 else "red"
        for x in df_daily["create_to_restaurant_minutes_lift_pct"]
    ]
    ax4.bar(
        df_daily["order_date"],
        df_daily["create_to_restaurant_minutes_lift_pct"],
        color=colors,
        alpha=0.6
    )
    ax4.axhline(y=0, color="black", linestyle="--", linewidth=1)
    ax4.set_title("Create to Restaurant: Lift %", fontsize=12, fontweight="bold")
    ax4.set_xlabel("Date")
    ax4.set_ylabel("Lift %")
    ax4.grid(True, alpha=0.3, axis="y")
    ax4.tick_params(axis="x", rotation=45)
    
    # Add average line
    avg_lift_restaurant = df_daily["create_to_restaurant_minutes_lift_pct"].mean()
    ax4.axhline(
        y=avg_lift_restaurant,
        color="blue",
        linestyle=":",
        linewidth=2,
        label=f"Avg: {avg_lift_restaurant:.2f}%"
    )
    ax4.legend()
    
    # Plot 5: R2C (Restaurant to Customer) - Absolute Values
    ax5 = axes[2, 0]
    ax5.plot(
        df_daily["order_date"],
        df_daily["treatment_r2c_minutes"],
        marker="o",
        label="Treatment",
        color="#1f77b4",
        linewidth=2
    )
    ax5.plot(
        df_daily["order_date"],
        df_daily["control_r2c_minutes"],
        marker="s",
        label="Control",
        color="#ff7f0e",
        linewidth=2
    )
    ax5.set_title(
        "R2C (Restaurant → Customer): Absolute Values (Minutes)",
        fontsize=12,
        fontweight="bold"
    )
    ax5.set_xlabel("Date")
    ax5.set_ylabel("Minutes")
    ax5.legend()
    ax5.grid(True, alpha=0.3)
    ax5.tick_params(axis="x", rotation=45)
    
    # Plot 6: R2C - Lift %
    ax6 = axes[2, 1]
    colors = [
        "green" if x < 0 else "red"
        for x in df_daily["r2c_minutes_lift_pct"]
    ]
    ax6.bar(
        df_daily["order_date"],
        df_daily["r2c_minutes_lift_pct"],
        color=colors,
        alpha=0.6
    )
    ax6.axhline(y=0, color="black", linestyle="--", linewidth=1)
    ax6.set_title("R2C (Restaurant → Customer): Lift %", fontsize=12, fontweight="bold")
    ax6.set_xlabel("Date")
    ax6.set_ylabel("Lift %")
    ax6.grid(True, alpha=0.3, axis="y")
    ax6.tick_params(axis="x", rotation=45)
    
    # Add average line
    avg_lift_r2c = df_daily["r2c_minutes_lift_pct"].mean()
    ax6.axhline(
        y=avg_lift_r2c,
        color="blue",
        linestyle=":",
        linewidth=2,
        label=f"Avg: {avg_lift_r2c:.2f}%"
    )
    ax6.legend()
    
    # Plot 7: D2C (Dasher confirmed to Customer) - Absolute Values
    ax7 = axes[3, 0]
    ax7.plot(
        df_daily["order_date"],
        df_daily["treatment_d2c_minutes"],
        marker="o",
        label="Treatment",
        color="#1f77b4",
        linewidth=2
    )
    ax7.plot(
        df_daily["order_date"],
        df_daily["control_d2c_minutes"],
        marker="s",
        label="Control",
        color="#ff7f0e",
        linewidth=2
    )
    ax7.set_title(
        "D2C (Dasher Confirmed → Customer): Absolute Values (Minutes)",
        fontsize=12,
        fontweight="bold"
    )
    ax7.set_xlabel("Date")
    ax7.set_ylabel("Minutes")
    ax7.legend()
    ax7.grid(True, alpha=0.3)
    ax7.tick_params(axis="x", rotation=45)
    
    # Plot 8: D2C - Lift %
    ax8 = axes[3, 1]
    colors = [
        "green" if x < 0 else "red"
        for x in df_daily["d2c_minutes_lift_pct"]
    ]
    ax8.bar(
        df_daily["order_date"],
        df_daily["d2c_minutes_lift_pct"],
        color=colors,
        alpha=0.6
    )
    ax8.axhline(y=0, color="black", linestyle="--", linewidth=1)
    ax8.set_title("D2C (Dasher Confirmed → Customer): Lift %", fontsize=12, fontweight="bold")
    ax8.set_xlabel("Date")
    ax8.set_ylabel("Lift %")
    ax8.grid(True, alpha=0.3, axis="y")
    ax8.tick_params(axis="x", rotation=45)
    
    # Add average line
    avg_lift_d2c = df_daily["d2c_minutes_lift_pct"].mean()
    ax8.axhline(
        y=avg_lift_d2c,
        color="blue",
        linestyle=":",
        linewidth=2,
        label=f"Avg: {avg_lift_d2c:.2f}%"
    )
    ax8.legend()
    
    plt.tight_layout()
    
    # Save plot
    output_path = os.path.join(output_dir, "create_to_dash_restaurant_lift_plot.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"✓ Plot saved to: {output_path}")
    
    plt.close()
    
    # Print summary statistics
    print("\n" + "=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)
    
    # Get overall row if it exists
    df_overall = df[df["metric_type"] == "overall"]
    
    if not df_overall.empty:
        print("\n📊 Create to Dash (Order Creation → Dasher Confirmation)")
        print("-" * 80)
        print(
            f"Overall Treatment: "
            f"{df_overall['treatment_create_to_dash_minutes'].iloc[0]:.2f} minutes"
        )
        print(
            f"Overall Control: "
            f"{df_overall['control_create_to_dash_minutes'].iloc[0]:.2f} minutes"
        )
        print(
            f"Overall Lift: "
            f"{df_overall['create_to_dash_minutes_lift_pct'].iloc[0]:.2f}%"
        )
        print(
            f"Interpretation: "
            f"{'🟢 Treatment is FASTER' if df_overall['create_to_dash_minutes_lift_pct'].iloc[0] < 0 else '🔴 Treatment is SLOWER'}"
        )
        
        print("\n📊 Create to Restaurant (Order Creation → Restaurant Pickup)")
        print("-" * 80)
        print(
            f"Overall Treatment: "
            f"{df_overall['treatment_create_to_restaurant_minutes'].iloc[0]:.2f} minutes"
        )
        print(
            f"Overall Control: "
            f"{df_overall['control_create_to_restaurant_minutes'].iloc[0]:.2f} minutes"
        )
        print(
            f"Overall Lift: "
            f"{df_overall['create_to_restaurant_minutes_lift_pct'].iloc[0]:.2f}%"
        )
        print(
            f"Interpretation: "
            f"{'🟢 Treatment is FASTER' if df_overall['create_to_restaurant_minutes_lift_pct'].iloc[0] < 0 else '🔴 Treatment is SLOWER'}"
        )
        
        print("\n📊 R2C (Restaurant → Customer)")
        print("-" * 80)
        print(
            f"Overall Treatment: "
            f"{df_overall['treatment_r2c_minutes'].iloc[0]:.2f} minutes"
        )
        print(
            f"Overall Control: "
            f"{df_overall['control_r2c_minutes'].iloc[0]:.2f} minutes"
        )
        print(
            f"Overall Lift: "
            f"{df_overall['r2c_minutes_lift_pct'].iloc[0]:.2f}%"
        )
        print(
            f"Interpretation: "
            f"{'🟢 Treatment is FASTER' if df_overall['r2c_minutes_lift_pct'].iloc[0] < 0 else '🔴 Treatment is SLOWER'}"
        )
        
        print("\n📊 D2C (Dasher Confirmed → Customer)")
        print("-" * 80)
        print(
            f"Overall Treatment: "
            f"{df_overall['treatment_d2c_minutes'].iloc[0]:.2f} minutes"
        )
        print(
            f"Overall Control: "
            f"{df_overall['control_d2c_minutes'].iloc[0]:.2f} minutes"
        )
        print(
            f"Overall Lift: "
            f"{df_overall['d2c_minutes_lift_pct'].iloc[0]:.2f}%"
        )
        print(
            f"Interpretation: "
            f"{'🟢 Treatment is FASTER' if df_overall['d2c_minutes_lift_pct'].iloc[0] < 0 else '🔴 Treatment is SLOWER'}"
        )
    else:
        print("\n📊 Create to Dash (Order Creation → Dasher Confirmation)")
        print("-" * 80)
        print(
            f"Average Treatment: "
            f"{df_daily['treatment_create_to_dash_minutes'].mean():.2f} minutes"
        )
        print(
            f"Average Control: "
            f"{df_daily['control_create_to_dash_minutes'].mean():.2f} minutes"
        )
        print(f"Average Lift: {avg_lift:.2f}%")
        print(
            f"Interpretation: "
            f"{'🟢 Treatment is FASTER' if avg_lift < 0 else '🔴 Treatment is SLOWER'}"
        )
        
        print("\n📊 Create to Restaurant (Order Creation → Restaurant Pickup)")
        print("-" * 80)
        print(
            f"Average Treatment: "
            f"{df_daily['treatment_create_to_restaurant_minutes'].mean():.2f} minutes"
        )
        print(
            f"Average Control: "
            f"{df_daily['control_create_to_restaurant_minutes'].mean():.2f} minutes"
        )
        print(f"Average Lift: {avg_lift_restaurant:.2f}%")
        print(
            f"Interpretation: "
            f"{'🟢 Treatment is FASTER' if avg_lift_restaurant < 0 else '🔴 Treatment is SLOWER'}"
        )
        
        print("\n📊 R2C (Restaurant → Customer)")
        print("-" * 80)
        print(
            f"Average Treatment: "
            f"{df_daily['treatment_r2c_minutes'].mean():.2f} minutes"
        )
        print(
            f"Average Control: "
            f"{df_daily['control_r2c_minutes'].mean():.2f} minutes"
        )
        avg_lift_r2c = df_daily["r2c_minutes_lift_pct"].mean()
        print(f"Average Lift: {avg_lift_r2c:.2f}%")
        print(
            f"Interpretation: "
            f"{'🟢 Treatment is FASTER' if avg_lift_r2c < 0 else '🔴 Treatment is SLOWER'}"
        )
        
        print("\n📊 D2C (Dasher Confirmed → Customer)")
        print("-" * 80)
        print(
            f"Average Treatment: "
            f"{df_daily['treatment_d2c_minutes'].mean():.2f} minutes"
        )
        print(
            f"Average Control: "
            f"{df_daily['control_d2c_minutes'].mean():.2f} minutes"
        )
        avg_lift_d2c = df_daily["d2c_minutes_lift_pct"].mean()
        print(f"Average Lift: {avg_lift_d2c:.2f}%")
        print(
            f"Interpretation: "
            f"{'🟢 Treatment is FASTER' if avg_lift_d2c < 0 else '🔴 Treatment is SLOWER'}"
        )
    
    print("\n" + "=" * 80)


def main():
    """Main execution function."""
    print("Fetching create_to_dash and create_to_restaurant metrics from Snowflake...")
    
    try:
        df = fetch_data()
    except Exception as e:
        print(f"❌ Error fetching data: {str(e)}")
        sys.exit(1)
    
    if df.empty:
        print("❌ No data returned from query")
        sys.exit(1)
    
    print(f"✓ Fetched {len(df)} rows")
    
    # Check for required columns
    required_cols = [
        "order_date",
        "metric_type",
        "treatment_create_to_dash_minutes",
        "control_create_to_dash_minutes",
        "create_to_dash_minutes_lift_pct",
        "treatment_create_to_restaurant_minutes",
        "control_create_to_restaurant_minutes",
        "create_to_restaurant_minutes_lift_pct",
        "treatment_r2c_minutes",
        "control_r2c_minutes",
        "r2c_minutes_lift_pct",
        "treatment_d2c_minutes",
        "control_d2c_minutes",
        "d2c_minutes_lift_pct"
    ]
    
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"❌ Missing required columns: {missing_cols}")
        print(f"Available columns: {df.columns.tolist()}")
        sys.exit(1)
    
    # Create output directory
    output_dir = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "output"
    )
    os.makedirs(output_dir, exist_ok=True)
    
    # Create visualization
    print("\nCreating visualizations...")
    plot_lift_analysis(df, output_dir)
    
    print("\n✅ Analysis complete!")


if __name__ == "__main__":
    main()
