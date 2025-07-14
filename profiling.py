import mysql.connector
import pandas as pd
import os
from datetime import datetime
from ydata_profiling import ProfileReport

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '21102004',
    'database': 'sakila'
}
OUTPUT_DIR = './profiling_reports_json'
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_metadata(conn):
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = %s", (DB_CONFIG['database'],))
    num_columns = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = %s", (DB_CONFIG['database'],))
    num_indexes = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema = %s", (DB_CONFIG['database'],))
    num_constraints = cursor.fetchone()[0]

    print("\nDatabase Metadata:")
    print(f"Columns: {num_columns}")
    print(f"Indexes: {num_indexes}")
    print(f"Constraints: {num_constraints}")
    print("Row counts per table:")

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s", (DB_CONFIG['database'],))
    for (table_name,) in cursor.fetchall():
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
        except Exception:
            count = None
        print(f"  {table_name}: {count}")

    cursor.close()

def check_foreign_keys(conn):
    cursor = conn.cursor()
    print("\nForeign Key Integrity Checks:")

    cursor.execute("""
        SELECT table_name, column_name, referenced_table_name, referenced_column_name
        FROM information_schema.key_column_usage
        WHERE referenced_table_schema = %s AND referenced_table_name IS NOT NULL
    """, (DB_CONFIG['database'],))
    
    fks = cursor.fetchall()
    if not fks:
        print("  No foreign keys found.")
    
    for (table, col, ref_table, ref_col) in fks:
        query = f"""
            SELECT COUNT(*)
            FROM {table} t LEFT JOIN {ref_table} r
            ON t.{col} = r.{ref_col}
            WHERE r.{ref_col} IS NULL AND t.{col} IS NOT NULL
        """
        cursor.execute(query)
        invalid_refs = cursor.fetchone()[0]
        if invalid_refs > 0:
            print(f"  FK violation: {table}.{col} has {invalid_refs} invalid references to {ref_table}.{ref_col}")
        else:
            print(f"  FK OK: {table}.{col} references {ref_table}.{ref_col} correctly")

    cursor.close()

def profile_table(conn, table_name):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"\nProfiling table: {table_name}")

    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, set)).any():
            df[col] = df[col].apply(lambda x: tuple(sorted(x)) if isinstance(x, set) else x)

    print("\nNull values per column:")
    print(df.isnull().sum())

    print(f"\nDuplicate rows: {df.duplicated().sum()}")
    print("\nColumn data types:")
    print(df.dtypes)

    numeric_df = df.select_dtypes(include=['number'])
    outliers = ((numeric_df - numeric_df.mean()) / numeric_df.std()).abs() > 3
    print("\nOutliers (z-score > 3):")
    print(outliers.sum())

    profile = ProfileReport(df, title=f"{table_name} Profiling Report", explorative=True)
    json_file = os.path.join(OUTPUT_DIR, f"{table_name}_profile_{timestamp}.json")
    
    with open(json_file, "w") as f:
        f.write(profile.to_json())

    print(f"Saved JSON profile to {json_file}")

if __name__ == "__main__":
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        get_metadata(conn)
        check_foreign_keys(conn)

        cursor = conn.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s", (DB_CONFIG['database'],))
        tables_to_profile = [row[0] for row in cursor.fetchall()]
        cursor.close()

        for table in tables_to_profile:
            profile_table(conn, table)

    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")

    finally:
        if conn.is_connected():
            conn.close()
            print("\nDatabase connection closed.")
