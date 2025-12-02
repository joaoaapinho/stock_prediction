"""
Scraping insider trading data from SEC Form 4 filings for model training
"""

import os
import time
import json
import xml.etree.ElementTree as ET
import pandas as pd
from sec_edgar_downloader import Downloader
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def extract_xml(filepath):
    """Extract XML content from SEC filing submission text file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    start = content.find('<?xml')
    end = content.rfind('</ownershipDocument>') + len('</ownershipDocument>')
    return content[start:end] if start != -1 and end != -1 else None


def parse_form4(xml_str):
    """
    Parse Form 4 XML to extract insider transaction details.

    Returns:
        List of tuples: (date, shares, amount, buy_flag)
    """
    try:
        root = ET.fromstring(xml_str)
    except Exception as e:
        print(f"⚠️ XML parsing failed: {e}")
        return []

    ns = {'ns': root.tag.split('}')[0].strip('{')} if '}' in root.tag else {}
    transactions = []

    for txn in root.findall('.//nonDerivativeTransaction', ns):
        try:
            code_elem = txn.find('.//transactionCoding/transactionCode', ns)
            if code_elem is None:
                continue

            code = code_elem.text.upper()
            if code not in ['P', 'S']:
                continue

            buy_flag = 1 if code == 'P' else 0
            date = txn.find('.//transactionDate/value', ns).text
            shares = float(txn.find('.//transactionShares/value', ns).text)
            price_el = txn.find('.//transactionPricePerShare/value', ns)
            price = float(price_el.text) if price_el is not None else 0.0
            amount = shares * price

            transactions.append((date, shares, amount, buy_flag))
        except Exception as e:
            print(f"⛔ Error extracting transaction: {e}")

    return transactions


def aggregate_by_day(transactions):
    """Aggregate multiple transactions on the same day."""
    daily = {}
    for date, shares, amount, flag in transactions:
        key = (date, flag)
        if key not in daily:
            daily[key] = {"shares": 0.0, "amount": 0.0}
        daily[key]["shares"] += shares
        daily[key]["amount"] += amount

    return sorted([(d, s["shares"], s["amount"], b) for (d, b), s in daily.items()])


def scrape_insider_trades(tickers, save_folder="data/raw/inside_trades",
                          candles_folder="data/raw/candles",
                          filings_dir="./sec-edgar-filings",
                          company_name=None,
                          email=None,
                          delay_sec=1.0):
    """
    Download insider trading data from SEC Form 4 filings.

    Args:
        tickers: List of ticker symbols
        save_folder: Directory to save CSV files
        candles_folder: Directory containing stock candles data (used to determine date range)
        filings_dir: Temporary directory for SEC filings
        company_name: Company name for SEC user agent (required by SEC)
        email: Email address for SEC user agent (required by SEC)
        delay_sec: Delay between requests to respect SEC rate limits
    """
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    counter = 0
    start_time = time.time()
    downloader = Downloader(company_name, email, filings_dir)

    for ticker in tickers:
        counter += 1

        try:
            # Read the candles CSV to get the date range
            candles_path = os.path.join(candles_folder, f"{ticker}_daily.csv")
            if not os.path.exists(candles_path):
                print(f"⚠️  Candles file not found for {ticker}, skipping...")
                continue

            candles_df = pd.read_csv(candles_path, parse_dates=['date'])
            if candles_df.empty:
                print(f"⚠️  Candles file empty for {ticker}, skipping...")
                continue

            candles_df = candles_df.sort_values('date')
            earliest_date = candles_df['date'].iloc[0]
            latest_date = candles_df['date'].iloc[-1]

            # Check if we already have insider trades data
            filepath = os.path.join(save_folder, f"{ticker}_insider_trades_daily.csv")
            file_exists = os.path.exists(filepath)
            last_insider_date = None

            if file_exists:
                existing_data = pd.read_csv(filepath, parse_dates=['date'])
                if not existing_data.empty:
                    last_insider_date = existing_data['date'].max()

                    # Check if we're already up to date with the latest candles date
                    if pd.to_datetime(last_insider_date).date() >= pd.to_datetime(latest_date).date():
                        print(f"🫸 {ticker} is up to date (latest insider date: {last_insider_date.date()})")
                        continue

            # Determine date range for download
            after_date = earliest_date.strftime('%Y-%m-%d')
            before_date = latest_date.strftime('%Y-%m-%d')

            print(f"📊 Downloading Form 4s for {ticker} (from {after_date} to {before_date})")

            # Download Form 4 filings.
            downloader.get("4", ticker, after=after_date, before=before_date)

            # Note: Downloader creates a nested structure: filings_dir/sec-edgar-filings/TICKER/4/
            form4_dir = os.path.join(filings_dir, "sec-edgar-filings", ticker.upper(), "4")

            if not os.path.exists(form4_dir):
                print(f"⚠️ SEC returned no Form 4 filings for {ticker} (company may not have insider trades in this period)")
                continue

            # Count number of filings downloaded
            num_filings = len([d for d in os.listdir(form4_dir) if os.path.isdir(os.path.join(form4_dir, d))])
            if last_insider_date:
                print(f"📥 Found {num_filings} Form 4 filing(s) for {ticker}, processing...")
            else:
                print(f"📥 Downloaded {num_filings} Form 4 filing(s) for {ticker}, processing...")

            # Parse all Form 4 filings.
            all_transactions = []
            for submission_dir in os.listdir(form4_dir):
                submission_path = os.path.join(form4_dir, submission_dir, "full-submission.txt")
                if not os.path.exists(submission_path):
                    continue

                xml_content = extract_xml(submission_path)
                if xml_content:
                    all_transactions.extend(parse_form4(xml_content))

            if not all_transactions:
                print(f"⚠️ No insider transactions found for {ticker}")
                continue

            # Aggregate transactions by day.
            daily_data = aggregate_by_day(all_transactions)
            new_df = pd.DataFrame(daily_data, columns=["date", "shares", "amount", "buy_flag"])
            new_df['date'] = pd.to_datetime(new_df['date'])
            new_df.sort_values('date', inplace=True)
            new_df.reset_index(drop=True, inplace=True)

            if file_exists and last_insider_date is not None:
                # Append only new data.
                new_data = new_df[new_df['date'] > last_insider_date]
                if not new_data.empty:
                    updated_data = pd.concat([existing_data, new_data], ignore_index=True)
                    updated_data.sort_values('date', inplace=True)
                    updated_data = updated_data.drop_duplicates(subset=['date', 'buy_flag'], keep='last')
                    updated_data.to_csv(filepath, index=False)
                    print(f"📁 Appended {len(new_data)} new rows to {filepath}")
                else:
                    print(f"🫸 {ticker} has no new data")
            else:
                new_df.to_csv(filepath, index=False)
                print(f"✅ Saved {ticker} data to {filepath} ({len(new_df)} rows)")

            # Progress & ETA.
            elapsed = time.time() - start_time
            percent = (counter / len(tickers)) * 100
            avg_time = elapsed / counter
            eta_seconds = avg_time * (len(tickers) - counter)
            eta_str = time.strftime('%H:%M:%S', time.gmtime(eta_seconds))
            print(f'Completion: {percent:.2f}% | ETA: {eta_str}')

            # Add delay to respect SEC rate limits.
            time.sleep(delay_sec)

        except Exception as e:
            print(f"⛔ Error processing {ticker}: {e}")

    print(f"\n ✅ Download complete! Processed {counter} tickers in {time.strftime('%H:%M:%S', time.gmtime(time.time() - start_time))}")


if __name__ == "__main__":
    stock_list_path = 'src/scraping/company_tickers.json'

    with open(stock_list_path, 'r') as file:
        data = json.load(file)
        tickers = [item['ticker'] for item in data.values()]

    print(f"📊 Downloading insider trading data for {len(tickers)} tickers from SEC EDGAR...")
    print(f"Tickers: {tickers}\n")

    # Get credentials from environment variables.
    company_name = os.getenv("SEC_COMPANY_NAME")
    email = os.getenv("SEC_EMAIL")

    if not company_name or not email:
        raise ValueError(
            "SEC_COMPANY_NAME and SEC_EMAIL environment variables must be set.\n"
            "Add them to your .env file:\n"
            "SEC_COMPANY_NAME=Company Name\n"
            "SEC_EMAIL=your.email@example.com"
        )

    scrape_insider_trades(tickers, company_name=company_name, email=email)