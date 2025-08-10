import requests
import time
import pandas as pd
from datetime import datetime, timezone, timedelta
import os
import argparse

# --- Configuration ---
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

def fetch_full_day_data(asset: str, interval: str, day: datetime.date):
    """
    Fetches all 1-minute candle data for a single day from Binance.
    The API limit is 1000 records, so two calls are needed for a full day (1440 minutes).
    """
    print(f"--- Fetching full day data for {asset} on {day} ---")
    
    all_day_data = []
    
    # Define the start of the target day in UTC
    start_of_day = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    
    # Batch 1: First 1000 minutes of the day
    params1 = {
        'symbol': asset.replace('-', ''), 
        'interval': interval, 
        'startTime': int(start_of_day.timestamp() * 1000), 
        'limit': 1000
    }
    
    # Batch 2: Starts 1000 minutes after the beginning of the day
    start_of_batch2 = start_of_day + timedelta(minutes=1000)
    params2 = {
        'symbol': asset.replace('-', ''), 
        'interval': interval, 
        'startTime': int(start_of_batch2.timestamp() * 1000), 
        'limit': 440 # Remaining minutes
    }

    try:
        print(f"⬇️  Fetching first batch of 1000 records...")
        response1 = requests.get(BINANCE_API_URL, params=params1)
        response1.raise_for_status()
        data1 = response1.json()
        all_day_data.extend(data1)
        print(f"    ✅ Fetched {len(data1)} records.")
        
        time.sleep(1) # Pause between requests

        print(f"⬇️  Fetching second batch of records...")
        response2 = requests.get(BINANCE_API_URL, params=params2)
        response2.raise_for_status()
        data2 = response2.json()
        all_day_data.extend(data2)
        print(f"    ✅ Fetched {len(data2)} records.")
        
        return all_day_data

    except requests.exceptions.RequestException as e:
        print(f"Error fetching full day data for {day}: {e}")
        return None

def save_to_csv(data: list, asset: str, day: datetime.date):
    """Saves the fetched data to a CSV file in the output folder."""
    if not data:
        print("No data to save.")
        return

    headers = [
        "open_time", "open_price", "high_price", "low_price", "close_price", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
    ]
    
    # Convert timestamps from milliseconds to human-readable format for the CSV
    for row in data:
        row[0] = datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        row[6] = datetime.fromtimestamp(row[6] / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    df = pd.DataFrame(data, columns=headers)

    output_dir = os.path.join(PROJECT_ROOT, 'output')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")

    filename = f"{asset.replace('-', '')}_{day.strftime('%Y-%m-%d')}_fullday.csv"
    filepath = os.path.join(output_dir, filename)
    
    df.to_csv(filepath, index=False)
    print(f"✅ Data successfully saved to '{filepath}'")
    print(f"Total records saved: {len(df)}")


def main():
    """Main function to run the diagnostic data fetch."""
    parser = argparse.ArgumentParser(description="Fetch full-day candle data from Binance and save to CSV.")
    parser.add_argument('--asset', required=True, help="The asset to fetch (e.g., BTC-USDT).")
    parser.add_argument('--date', required=True, help="The specific date to fetch in YYYY-MM-DD format.")
    args = parser.parse_args()

    try:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        
        full_day_data = fetch_full_day_data(args.asset, '1m', target_date)
        
        save_to_csv(full_day_data, args.asset, target_date)

    except ValueError:
        print("❌ Invalid date format. Please use YYYY-MM-DD.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()