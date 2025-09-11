#!/usr/bin/env python3

import sys
from pathlib import Path

# Add utils directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "utils"))
from snowflake_connection import SnowflakeHook


def execute_sql_file(sql_path: Path):
    sql_text = sql_path.read_text()
    # Simple split on semicolons; ignore empty chunks
    statements = [s.strip() for s in sql_text.split(';') if s.strip()]
    with SnowflakeHook() as hook:
        for stmt in statements:
            hook.query_without_result(stmt)


def main():
    if len(sys.argv) < 2:
        print("Usage: execute_sql_file.py <path_to_sql>")
        sys.exit(1)
    sql_path = Path(sys.argv[1])
    if not sql_path.exists():
        print(f"SQL file not found: {sql_path}")
        sys.exit(1)
    execute_sql_file(sql_path)
    print(f"Executed SQL file: {sql_path}")


if __name__ == "__main__":
    main()




