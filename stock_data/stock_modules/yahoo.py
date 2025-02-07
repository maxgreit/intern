import yfinance as yf

def get_stock_data(ticker):

    # Haal historische data op
    stock = yf.Ticker(ticker)
    data = stock.history(period="1y")
    return data
