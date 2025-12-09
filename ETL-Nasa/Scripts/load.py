import os
import time
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

# Initialize Supabase
load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

def load_to_supabase():
    # Load cleaned CSV
    csv_path = "../data/staged/NasaData_cleaned.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing file: {csv_path}")
    df = pd.read_csv(csv_path)

    # Convert timestamps to strings
    df["inserted_at"] = pd.to_datetime(df["inserted_at"]).dt.strftime("%Y-%m-%dT%H:%M:%S")

    batch_size = 20

    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size].where(pd.notnull(df), None).to_dict("records")

        values = [
            f"('{r['date']}', '{r.get('title','')}', '{r.get('explanation','')}', "
            f"'{r.get('media_type','')}', '{r.get('image_url','')}', '{r['inserted_at']}')"
            for r in batch
        ]

        insert_sql = f"""
        INSERT INTO nasa_apod(date, title, explanation, media_type, image_url, inserted_at)
        VALUES {",".join(values)}
        """

        supabase.rpc("execute_sql", {"query": insert_sql}).execute()

        print(f"Inserted rows {i+1} --- {min(i+batch_size, len(df))}")
        time.sleep(0.5)

    print("Finished Loading NASA APOD data.")

if __name__ == "__main__":
    load_to_supabase()
