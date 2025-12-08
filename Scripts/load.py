import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

# -----------------------------------------
# Initialize Supabase Client
# -----------------------------------------
def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("Missing Supabase URL or KEY")

    return create_client(url, key)


# -----------------------------------------
# Load CSV into Supabase (table must already exist)
# -----------------------------------------
def load_to_supabase(staged_path: str, table_name: str = "iris_data"):

    if not os.path.exists(staged_path):
        print(f"Error: File not found at {staged_path}")
        return

    supabase = get_supabase_client()
    df = pd.read_csv(staged_path)

    total_rows = len(df)
    batch_size = 50

    print(f"Loading {total_rows} rows into `{table_name}`...")

    for i in range(0, total_rows, batch_size):
        batch = df.iloc[i: i + batch_size].copy()
        batch = batch.where(pd.notnull(batch), None)

        try:
            supabase.table(table_name).insert(batch.to_dict("records")).execute()
            print(f"Inserted rows {i+1} to {min(i+batch_size, total_rows)}")
        except Exception as e:
            print(f"Insert batch error: {e}")

    print("Data load complete.")


# -----------------------------------------
# MAIN
# -----------------------------------------
if __name__ == "__main__":
    csv_path = os.path.join("..", "data", "staged", "iris_transformed.csv")
    load_to_supabase(csv_path)
