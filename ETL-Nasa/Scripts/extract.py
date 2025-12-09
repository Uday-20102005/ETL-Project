import json
from pathlib import Path
import requests

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

def extract_nasa_data():
    api_key="oj49ByXfgb8Ie0vaF9E0jqhpx2uloU9Tq2eUdq0k"
    url="https://api.nasa.gov/planetary/apod?api_key=oj49ByXfgb8Ie0vaF9E0jqhpx2uloU9Tq2eUdq0k"
    params = {"api_key": api_key}

    resp = requests.get(url, params=params)
    if resp.status_code != 200:
        print("Error:", resp.status_code, resp.text)
        resp.raise_for_status()

    data = resp.json()
    filename = DATA_DIR / "NasaData.json"
    filename.write_text(json.dumps(data, indent=2))

    print(f"Extracted NASA data saved to: {filename}")
    return data

if __name__ == "__main__":
    extract_nasa_data()
