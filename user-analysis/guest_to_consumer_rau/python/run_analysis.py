"""
Run the comprehensive guest-to-consumer analysis
"""

import sys
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')

import pandas as pd
from pathlib import Path

# Try to import snowflake connection
try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
except ImportError:
    print("Snowflake connector not available. Please install: pip install snowflake-connector-python")
    sys.exit(1)

# Set up paths
BASE_DIR = Path(__file__).parent.parent
SQL_DIR = BASE_DIR / "sql"
OUTPUT_DIR = BASE_DIR / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

# Snowflake connection parameters
import os
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT', 'doordash')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE', 'ANALYST')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'ANALYST_WH')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'PRODDB')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')

def get_snowflake_connection():
    """Create Snowflake connection"""
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

def run_query(query, conn):
    """Run a query and return results as DataFrame"""
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

def main():
    """Main analysis function"""
    
    # Read the comprehensive analysis SQL
    with open(SQL_DIR / "05_comprehensive_analysis.sql", "r") as f:
        full_query = f.read()
    
    # Replace SET statements with actual values
    full_query = full_query.replace("SET start_date = '2024-09-01';", "")
    full_query = full_query.replace("SET end_date = '2024-09-30';", "")
    full_query = full_query.replace("SET metric_end_date = '2024-10-09';", "")
    full_query = full_query.replace("$start_date", "'2024-09-01'")
    full_query = full_query.replace("$end_date", "'2024-09-30'")
    full_query = full_query.replace("$metric_end_date", "'2024-10-09'")
    
    # Connect to Snowflake
    print("Connecting to Snowflake...")
    conn = get_snowflake_connection()
    
    try:
        print("Running comprehensive analysis...")
        results = run_query(full_query, conn)
        
        if results is not None and not results.empty:
            print(f"\nAnalysis complete! Found {len(results)} result rows")
            print("\nResults:")
            print(results.to_string())
            
            # Save to CSV
            output_file = OUTPUT_DIR / "comprehensive_analysis_results.csv"
            results.to_csv(output_file, index=False)
            print(f"\nResults saved to: {output_file}")
        else:
            print("No results returned from query")
            
    except Exception as e:
        print(f"Error running analysis: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()
        print("\nConnection closed")

if __name__ == "__main__":
    main()

