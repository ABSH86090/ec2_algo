"""
Fetch Chartink Scanner results from Chartink.com website using python

Input:
1. Chartink scan URL
2. Scan Clause (Inspect webpage and find this under 'Network' / 'process' / 'Payload' section)

-- Dependencies to be installed --
pip install requests
pip install pandas
pip install beautifulsoup4

Disclaimer:
The information provided is for educational and informational purposes only and
should not be construed as financial, investment, or legal advice. The content is based on publicly available
information and personal opinions and may not be suitable for all investors. Investing involves risks,
including the loss of principal.

"""

# Import all dependencies
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import time


def chartink_scraper(url, scan_clause):
    # Retrieves the results of Chartink scanner. Input is the url and the scan clause
    # Example of scan clause : {'scan_clause': '( {33619} ( latest close > latest sma( latest close , 200 )
    # and latest close > latest sma( latest close , 100 ) and latest close > latest sma( latest close , 50 ) )'}
    # Scan clause can be obtained by doing an 'inspect' on the page and under 'Network' / 'process' / 'Payload'

    df = pd.DataFrame()

    try:
        with requests.Session() as s:
            r = s.get(url)
            soup = bs(r.text, "html.parser")
            csrf = soup.select_one("[name='csrf-token']")['content']
            s.headers['x-csrf-token'] = csrf
            s.headers['Content-Type'] = 'application/x-www-form-urlencoded'
            r = s.post('https://chartink.com/screener/process', data=scan_clause)
            df = pd.DataFrame().from_dict(r.json()['data'])
        return df
    except requests.exceptions.HTTPError as e:
        print("Error in network connection")
    except requests.exceptions.RequestException as e:
        print("Error extracting data from website: ", e)

if __name__ == "__main__":

    # Extract Chartink scan results into a dataframe
    # Sometimes extract fails temporarily due to network congestion. The loop below will try thrice before giving up
    try_again = 0
    smart_money_stocks = []
    while try_again <= 3:  # If the chartink extract fails, try for a couple of times
        scan_clause = {
            'scan_clause': '( {57960} ( ( {57960} ( latest sma( latest close , 5 ) '
                           '< latest close * 1.03 and latest sma( latest close , 20 ) '
                           '< latest close * 1.03 and latest sma( latest close , 50 ) '
                           '< latest close * 1.03 and latest sma( latest close , 100 ) '
                           '< latest close * 1.03 and latest sma( latest close , 200 ) '
                           '< latest close * 1.03 and latest sma( latest close , 5 ) '
                           '> latest close * 0.97 and latest sma( latest close , 20 ) '
                           '> latest close * 0.97 and latest sma( latest close , 50 ) '
                           '> latest close * 0.97 and latest sma( latest close , 100 ) '
                           '> latest close * 0.97 and latest sma( latest close , 200 ) '
                           '> latest close * 0.97 ) ) ) )'}
        url = 'https://chartink.com/screener/consolidatedbo'
        df = chartink_scraper(url, scan_clause)
        print(df)
        stocks_list = df['nsecode'].tolist()
        print(stocks_list)
        print("Extract Successful!")
        if df.empty:
            print("Extract failed. Going to try again...")
            time.sleep(10)
            try_again += 1
        else:
            break