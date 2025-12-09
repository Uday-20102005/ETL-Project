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
    csv_path = "../data/staged/weather_cleaned.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing file: {csv_path}")
    df = pd.read_csv(csv_path)

    # Convert timestamps to strings
    df["time"] = pd.to_datetime(df["time"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    df["extracted_at"] = pd.to_datetime(df["extracted_at"]).dt.strftime("%Y-%m-%dT%H:%M:%S")

    batch_size = 20

    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size].where(pd.notnull(df), None).to_dict("records")

        values = [
            f"('{r['time']}', {r.get('temperature_c','NULL')}, {r.get('humidity_percent','NULL')}, "
            f"{r.get('wind_speed_kmph','NULL')}, '{r.get('city','Hyderabad')}', '{r['extracted_at']}')"
            for r in batch
        ]

        insert_sql = f"""
        INSERT INTO weather_data(time, temperature_c, humidity_percent, wind_speed_kmph, city, extracted_at)
        VALUES {",".join(values)}
        """

        # Remote procedure call
        supabase.rpc("execute_sql", {"query": insert_sql}).execute()

        print(f"Inserted rows {i+1} --- {min(i+batch_size, len(df))}")
        time.sleep(0.5)

    print("Finished Loading Weather data.")

if __name__ == "__main__":
    load_to_supabase()

