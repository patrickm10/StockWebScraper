from bs4 import BeautifulSoup
import csv
import pandas as pd
import requests
import time


def getStockInfo():
    """
    Get the stock information for the day.

    Returns:
    str: The stock information for the day.
    """
    print("---------------------Stock Information---------------------\n")
    # Read the CSV file into a DataFrame
    stock_info = pd.read_csv('StockWebScraper/CEO_Stocks.csv')

    # Remove the dollar sign and comma from the 'Price' and 'Qty' columns and convert them to float
    stock_info['Price'] = stock_info['Price'].replace({'\$': '', ',': ''}, regex=True).astype(float)
    stock_info['Qty'] = stock_info['Qty'].replace({'\+': '', ',': ''}, regex=True).astype(float)

    # Calculate the total stock value for each person
    stock_info['Total_Value'] = stock_info['Price'] * stock_info['Qty']
    
    
    # TODO: Fix the current_price so that it checks the api for the current price of the stock.
    # and updates the current price in the csv file.
    
    # current_price = getCurrentPrice(stock_info['Ticker'])
    # print(current_price)
    # Get the current price for each ticker
    # stock_info['Current_Price'] = stock_info['Ticker'].apply(getCurrentPrice)
    
    
    # Group by 'Insider Name' and aggregate 'Total Value', 'Ticker', and 'Price'
    grouped = stock_info.groupby('Insider_Name').agg({
    'Total_Value': 'sum', 
    'Ticker': lambda x: list(x.unique()), 
    'Price': 'mean'
    # 'Current_Price': 'mean'
    })
    
    # Create a new DataFrame from the grouped data
    ind_stocks = pd.DataFrame(grouped)

    # Write the new DataFrame to a CSV file
    ind_stocks.to_csv('StockWebScraper/Individual_Stocks.csv')
    print(ind_stocks)
    print()
    print("**********************************************************************************************\n")
    
def getCurrentPrice(ticker):
    '''
    Get the current price of a stock.
    Parameters:
    ticker (str): The ticker symbol of the stock.
    Returns:
    float: The current price of the stock.
    '''
    # Replace 'demo' with your API key
    url = f'https://financialmodelingprep.com/api/v3/quote/{ticker}?apikey=demo'
    response = requests.get(url)
    data = response.json()

    if data:
        return data[0]['price']
    else:
        print(f"No data found for ticker {ticker}")
        return 0


def scrapeWebsite(url):
    '''
    Scrape the website for the stock information.
    Parameters:
    url (str): The url of the website to scrape.
    '''
    if url is None:
        print("Invalid URL \n")
        return None
    else:
        print("URL is valid \n")
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        for row in soup.find_all("tr"):
            cells = [cell.text for cell in row.find_all("td")]
            # print(', '.join(cells))
            print
            writer.writerow(cells)

url = "http://openinsider.com/screener?s=&o=&pl=&ph=&ll=&lh=&fd=730&fdr=&td=0&tdr=&fdlyl=&fdlyh=&daysago=&xp=1&vl=&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&isceo=1&ispres=1&isvp=1&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=100&page=1"
# writer = csv.writer(open("output.csv", "w"))
# scrapeWebsite(url)
getStockInfo()
