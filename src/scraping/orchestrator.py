"""
Scraping orchestration of stock and market data for model training
"""

import subprocess

# Execute candles.py to scrape OHLCV stock data
subprocess.run(["python", "src/scraping/scrapers/candles.py"])

# For other sources...