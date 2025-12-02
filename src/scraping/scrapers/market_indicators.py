"""
Scraping market indicators for model training
"""

from candles import scrape_yahoo_finance

# Declare the market indicators that provide context for stock predictions.
market_indicator_tickers = [
    
    # Major Index ETFs
    'SPY',  # S&P 500 - Broad US market
    'QQQ',  # Nasdaq 100 - Tech-heavy
    'DIA',  # Dow Jones - Industrial stocks
    'IWM',  # Russell 2000 - Small cap stocks

    # Volatility
    '^VIX', # CBOE Volatility Index - Fear gauge
    'VXX',  # VIX Short-Term Futures - Volatility exposure
    'VIXY', # VIX Mid-Term Futures - Volatility exposure

    # Treasury/Interest Rates
    'TLT',  # 20+ Year Treasury Bonds - Long-term interest rates
    'SHY',  # 1-3 Year Treasury Bonds - Short-term rates
    '^TNX', # 10-Year Treasury Yield - Key benchmark rate
    '^IRX', # 13 Week Treasury Bill - Risk-free rate

    # Currency
    'UUP',  # US Dollar Index - Dollar strength

    # Commodities
    'GLD',  # Gold ETF - Safe haven asset
    'USO',  # Oil ETF - Energy sector indicator
    'DBC',  # Commodity Index - Broad commodities

    # Sector ETFs
    'XLF',  # Financials
    'XLE',  # Energy
    'XLK',  # Technology
    'XLV',  # Healthcare
]

if __name__ == "__main__":
    print(f"📊 Downloading market indicators data for {len(market_indicator_tickers)} tickers from Yahoo Finance...")
    print(f"Indicators: {market_indicator_tickers}\n")

    scrape_yahoo_finance(
        market_indicator_tickers,
        save_folder="data/raw/market_indicators",
    )