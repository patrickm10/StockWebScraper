from bs4 import BeautifulSoup
import csv
import pandas as pd
import requests
import logging

# Initialize Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler('job_scraper.log')

def scrapeOpenInsider():
    """
    Scrapes the OpenInsider website for the latest insider trading data.
        Args:
            None
        Returns:
            data: A dataframe containing the scraped data
    """
    url = "http://openinsider.com/screener?s=&o=&pl=&ph=&ll=&lh=&fd=730&fdr=&td=0&tdr=&fdlyl=&fdlyh=&daysago=&xp=1&vl=&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&isceo=1&ispres=1&isvp=1&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=100&page=1"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    table = soup.find("table", {"class": "tinytable"})
    rows = table.find_all("tr")

    data = []

    # Extract the table headers
    headers = [header.text for header in rows[0].find_all("th")]
    print(headers) # Debugging

    # Extract the data from the table
    for row in rows[1:]:
        cols = row.find_all("td")
        cols = [ele.text.strip() for ele in cols]
        data.append(cols)

    # Create a DataFrame
    df = pd.DataFrame(data, columns=headers)
    
    return df


def scrapeNvda():
    """
    Scrapes open insider for the latest insider trading data for NVDA.
        Args:
            None
        Returns:
            data: A dataframe containing the scraped data
    """
    url = "http://openinsider.com/screener?s=NVDA&o=&pl=&ph=&ll=&lh=&fd=730&fdr=&td=0&tdr=&fdlyl=&fdlyh=&daysago=&xp=1&xs=1&vl=&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&isofficer=1&iscob=1&isceo=1&ispres=1&iscoo=1&iscfo=1&isgc=1&isvp=1&isdirector=1&istenpercent=1&isother=1&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=100&page=1"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    table = soup.find("table", {"class": "tinytable"})
    rows = table.find_all("tr")

    data = []

    # Extract the table headers
    headers = [header.text for header in rows[0].find_all("th")]
    
    
    # Extract the data from the table
    for row in rows[1:]:
        cols = row.find_all("td")
        cols = [ele.text.strip() for ele in cols]
        data.append(cols)

    # Create a DataFrame
    df = pd.DataFrame(data, columns=headers)
    
    return df

def get_top_insider_trades(time):
    """
    Get the top insider trades from OpenInsider
        Args:
            time: The time frame to get the trades from (Day, Week, Month)
        Returns:
            data: A dataframe containing the scraped data
    """
    url = "http://openinsider.com/top-insider-purchases-of-the-month"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    table = soup.find("table", {"class": "tinytable"})
    rows = table.find_all("tr")

    data = []

    # Extract the table headers
    headers = [header.text for header in rows[0].find_all("th")]
    # print(headers) # Debugging

    # Extract the data from the table
    for row in rows[1:]:
        cols = row.find_all("td")
        cols = [ele.text.strip() for ele in cols]
        data.append(cols)

    # Create a DataFrame
    df = pd.DataFrame(data, columns=headers)

    return df


def sortData(table):
    """
    Sorts the data by the "Trade Date" and "Value" columns.
        Args:
            table: A data frame of the scraped data
        Returns:
            table: A sorted data frame
    """
    table = pd.DataFrame(table[1:], columns=table.columns)
    table.iloc[:, 2] = pd.to_datetime(table.iloc[:, 2])  # Trade Date is the 3rd column (index 2)
    table.iloc[:, 12] = table.iloc[:, 12].str.replace("$", "").str.replace(",", "").astype(float)  # Value is the 13th column (index 12)
    table = table.sort_values(by=["Trade\xa0Date", "Value"], ascending=[False, False])
    return table

def main():
    data = scrapeOpenInsider()

    # Sort the data
    df = pd.DataFrame(data[1:], columns=data.columns)
    sorted_df = sortData(data)
    sorted_df.to_csv("insider_trading.csv", index=False)
    logger.info("Data saved to insider_trading.csv")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    print(df)

    data.to_csv("insider_trading_sorted.csv", index=False)
    logger.info("Data saved to insider_trading_sorted.csv\n")

    # scrape NVDA data
    # nvda_data = scrapeNvda()
    # nvda_df = pd.DataFrame(nvda_data[1:], columns=nvda_data[0])
    # nvda_df.to_csv("nvda_insider_trading.csv", index=False)
    # logger.info("NVDA data saved to nvda_insider_trading.csv")
    # print(nvda_df)

    # Get the top insider trades
    top_trades = get_top_insider_trades("month")
    top_trades.to_csv("top_insider_trades_feb.csv", index=False)






if __name__ == "__main__":
    main()
