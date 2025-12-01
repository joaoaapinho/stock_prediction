"""
Scraping OHLCV stock data for model training
"""

import yfinance as yf 
import pandas as pd 
import os 
import time
import json 
from datetime import datetime, timedelta

def scrape_yahoo_finance(tickers, save_folder = "data/raw/candles", delay_sec=0.1):
    """
    Download daily stock data from Yahoo Finance.

    Args:
        tickers: List of ticker symbols
        save_folder: Directory to save CSV files
        delay_sec: Delay between requests
    """
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)
    
    counter = 0
    start_time = time.time()

    for ticker in tickers:
        counter += 1
        
        try:
            filepath = os.path.join(save_folder, f"{ticker}_daily.csv")
            file_exists = os.path.exists(filepath)
            should_download = True
            last_date = None
        
            if file_exists:
                    existing_data = pd.read_csv(filepath, parse_dates=['date'])
                    last_date = existing_data['date'].max()

                    # If the latest date is today or yesterday (markets might not be open), check if update is needed
                    today = pd.Timestamp.now().date()

                    if pd.to_datetime(last_date).date() >= today - timedelta(days=1):
                        print(f"🫸 {ticker} is up to date (latest date: {last_date.date()})")
                        should_download = False
            
            if should_download:
                # Download data from Yahoo Finance (period='max' gets all available historical data).
                stock = yf.Ticker(ticker)
                data = stock.history(period='max', interval='1d', auto_adjust=False)

                if data.empty:
                    print(f"⚠️ No data returned for {ticker}")
                    continue

                # Reset index to make Date a column.
                data.reset_index(inplace=True)
        
                # Rename columns.
                data.rename(columns={
                    'Date': 'date',
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Volume': 'volume'
                }, inplace=True)

                # Keep the necessary columns only.
                data = data[['date', 'open', 'high', 'low', 'close', 'volume']]

                # Convert date to date only.
                data['date'] = pd.to_datetime(data['date']).dt.date
                data['date'] = pd.to_datetime(data['date'])

                # Sort by date
                data.sort_values('date', inplace=True)
                data.reset_index(drop=True, inplace=True)

                if file_exists:
                    # Append only new data.
                    new_data = data[data['date'] > last_date]
                    if not new_data.empty:
                        updated_data = pd.concat([existing_data, new_data], ignore_index=True)
                        updated_data.sort_values('date', inplace=True)
                        # Remove any potential duplicates.
                        updated_data = updated_data.drop_duplicates(subset=['date'], keep='last')
                        updated_data.to_csv(filepath, index=False)
                        print(f"📁 Appended {len(new_data)} new rows to {filepath}")
                    else:
                        print(f"🫸 {ticker} has no new data (latest date: {last_date.date()})")
                else:
                    data.to_csv(filepath, index=False)
                    print(f"✅ Saved {ticker} data to {filepath} ({len(data)} rows)")
                
                # Progress & ETA.
                elapsed = time.time() - start_time
                percent = (counter / len(tickers)) * 100
                avg_time = elapsed / counter
                eta_seconds = avg_time * (len(tickers) - counter)
                eta_str = time.strftime('%H:%M:%S', time.gmtime(eta_seconds))
                print(f'Completion: {percent:.2f}% | ETA: {eta_str}')

                # Add delay to avoid rate limiting.
                if should_download:
                    time.sleep(delay_sec)
                
        except Exception as e:
            print(f"⛔ Error downloading {ticker}: {e}")
        
        print(f"\n✅ Download complete! Processed {counter} tickers in {time.strftime('%H:%M:%S', time.gmtime(time.time() - start_time))}")


if __name__ == "__main__":
    stock_list_path = 'src/scraping/company_tickers.json'

    with open(stock_list_path, 'r') as file:
        data = json.load(file)
        tickers = [item['ticker'] for item in data.values()]

    print(f"📊 Downloading data for {len(tickers)} tickers from Yahoo Finance...")
    print(f"Tickers: {tickers}\n")
    scrape_yahoo_finance(tickers)