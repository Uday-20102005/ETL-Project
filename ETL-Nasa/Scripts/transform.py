import pandas as pd
import json
import glob
import os
def transform_nasa_data():
    os.makedirs("../data/staged",exist_ok=True)
    latest_file=sorted(glob.glob("../data/raw/NasaData.json"))[-1]
    with open(latest_file,"r") as f:
        data=json.load(f)
    image_data={
        "title": data.get("title", "No Title"),
        "description": data.get("explanation", "No Description"),
        "image_url": data.get("url", "No Image URL"),
        "date": data.get("date", "No Date"),
        "media_type": data.get("media_type", "No Media Type"),
    }
    df=pd.DataFrame([image_data])
    df["inserted_at"]=pd.Timestamp.now()
    output_path="../data/staged/NasaData_cleaned.csv"
    df.to_csv(output_path,index=False)
    print(f"Transformed {len(df)} Nasa records saved to {output_path}")
    return df
if __name__=="__main__":
    transform_nasa_data()