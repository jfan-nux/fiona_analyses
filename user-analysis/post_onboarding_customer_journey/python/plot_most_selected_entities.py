import os
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Add utils path for SnowflakeHook
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'utils'))
from snowflake_connection import SnowflakeHook


MOST_SELECTED_SQL = """
with latest_adds as (
  select consumer_id, entity_id
  from (
    select *, row_number() over(partition by consumer_id, entity_id order by iguazu_event_time desc) as rn
    from IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE
    where page = 'onboarding_preference_page'
  ) where rn = 1 and toggle_type = 'add'
)
select entity_id, count(distinct consumer_id) as consumers_selected,
       count(1) as selections
from latest_adds
group by all
order by consumers_selected desc, selections desc
limit 50;
"""


def fetch_most_selected(limit: int = 30) -> pd.DataFrame:
    """Fetch most selected entities for onboarding page.

    Args:
        limit: Top N entities to return (applied after query order)
    """
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(MOST_SELECTED_SQL, method='pandas')
    # enforce top N client-side if needed
    if limit is not None and limit > 0:
        df = df.head(limit)
    return df


def plot_most_selected(df: pd.DataFrame, title_suffix: str = "Top Selected Entities") -> plt.Figure:
    """Create bar plot for most selected entities.

    Expects columns: entity_id, consumers_selected, selections
    """
    plt.style.use('default')
    sns.set_palette("deep")

    # Sort for plotting
    df_plot = df.sort_values('consumers_selected', ascending=True).copy()

    fig, ax = plt.subplots(figsize=(12, max(6, 0.3 * len(df_plot))))

    bars = ax.barh(df_plot['entity_id'].astype(str), df_plot['consumers_selected'], alpha=0.85)

    # Labels
    ax.set_xlabel('Consumers Selected (Distinct)')
    ax.set_ylabel('Entity')
    ax.set_title(f"{title_suffix} on Onboarding Preference Page")

    # Annotate with percent of total distinct consumers in parentheses
    total_consumers = float(df_plot['consumers_selected'].sum()) if len(df_plot) else 0.0
    for bar, sel in zip(bars, df_plot['selections']):
        width = bar.get_width()
        pct = (width / total_consumers * 100.0) if total_consumers > 0 else 0.0
        ax.annotate(f"{int(width):,} ({pct:.1f}%)",
                    xy=(width, bar.get_y() + bar.get_height()/2),
                    xytext=(5, 0), textcoords='offset points',
                    va='center', ha='left', fontsize=9)

    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    return fig


def main():
    print("Querying Snowflake for most selected entities...")
    df = fetch_most_selected(limit=30)
    if df.empty:
        print("No data returned.")
        return

    print(df.head())

    print("Creating plot...")
    fig = plot_most_selected(df)

    # Save plot
    plots_dir = Path(__file__).parent.parent / 'plots'
    plots_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = plots_dir / f"most_selected_entities_{ts}.png"
    fig.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Saved plot to: {out_path}")

    # Show plot
    plt.show()


if __name__ == '__main__':
    # Ensure working dir is analysis root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    analysis_dir = os.path.dirname(script_dir)
    os.chdir(analysis_dir)
    main()
