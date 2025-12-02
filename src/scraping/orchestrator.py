"""
Scraping orchestration of stock and market data for model training
"""

import subprocess

# Execute candles.py to scrape OHLCV stock data.
subprocess.run(["python", "src/scraping/scrapers/candles.py"])

# Execute inside_trades.py to scrape insider trading data.
subprocess.run(["python", "src/scraping/scrapers/inside_trades.py"])

# Execute market_indicators.py to scrape market indicator data.
subprocess.run(["python", "src/scraping/scrapers/market_indicators.py"])