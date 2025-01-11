from datetime import timedelta
from datetime import datetime
from typing import Optional, Tuple, Union
from bs4 import BeautifulSoup
import pandas as pd
from os.path import exists
import requests
from __init__ import headers


def search_in_web(symbol: str) -> Union[str, None]:
    """
    Searches for the company description (About) of a symbol from Yahoo Finance.

    Args:
        symbol (str): The stock symbol (ticker) to search for.

    Returns:
        str or None: The company description if found, otherwise None.
    """
    url = f"https://finance.yahoo.com/quote/{symbol}/"
    response = download_web(url)
    soup = load_html_page(response)
    if soup.find("span", attrs={"class": "ellipsis"}) is None:
        return None
    else:
        about = soup.find("h1", attrs={"class": "yf-xxbei9"}).text
    return about


def download_web(url: str) -> str:
    """
    Downloads the content of a webpage from the specified URL.

    Args:
        url (str): The URL of the webpage to download.

    Returns:
        str: The HTML content of the webpage.
    """
    response = requests.get(url, headers=headers)
    return response.text


def load_html_page(text: str) -> BeautifulSoup:
    """
    Loads an HTML page either from a file or raw HTML text.

    Args:
        text (str): The raw HTML content or a file path to load the HTML content.

    Returns:
        BeautifulSoup: The parsed HTML content as a BeautifulSoup object.
    """
    if exists(text):
        with open(text, "r", encoding="utf-8") as f:
            html_doc = f.read()
    else:
        html_doc = text
    soup = BeautifulSoup(html_doc, 'html.parser')
    return soup


def check_in_file(df: pd.DataFrame, symbol: str) -> Tuple[bool, Optional[str]]:
    """
    Checks if the symbol exists in the provided dataframe.

    Args:
        df (pd.DataFrame): The dataframe containing symbol data.
        symbol (str): The stock symbol to check.

    Returns:
        tuple: A tuple containing a boolean indicating if the symbol was found,
               and the corresponding 'About' field if found, otherwise None.
    """
    try:
        index = df.loc[symbol]
        return True, index["About"]
    except KeyError:
        return False, None


def check_symbol(symbol: str) -> bool:
    """
    Checks if a stock symbol exists either in the local data file or on the web.
    If the symbol is found on the web, it is added to the local data file.

    Args:
        symbol (str): The stock symbol to check.

    Returns:
        bool: True if the symbol exists, otherwise False.
    """
    symbol = symbol.lower()
    symbol_path = ".data/symbols.csv"
    df = pd.read_csv(symbol_path, index_col="symbol")

    in_file, about = check_in_file(df, symbol)
    if in_file:
        return True
    else:
        in_web = search_in_web(symbol)
        if in_web is not None:
            df.loc[symbol] = in_web
            df.to_csv(symbol_path)
            return True
        else:
            return False


def process_data(soup: BeautifulSoup) -> pd.DataFrame:
    """
    Processes the HTML soup object to extract and organize historical stock data into a DataFrame.

    This function processes a BeautifulSoup object representing the HTML content of a stock's historical data page.
    It extracts the relevant stock data from the table, cleans and formats it, and returns it as a pandas DataFrame.

    Args:
        soup (BeautifulSoup): A BeautifulSoup object containing the parsed HTML content of the page.

    Returns:
        pd.DataFrame: A DataFrame containing the historical stock data with columns for Date, Open, High, Low, Close,
                      Adj Close, and Volume. The 'Date' column is set as the index, and numerical columns are cast to
                      appropriate data types (float for stock data and int for volume).
    """

    def preprocess(cell: str) -> str:
        """ 
        Cleans and formats the content of a table cell by stripping whitespace and removing commas.

        This function processes a single table cell's content by removing unwanted spaces and commas, 
        ensuring that the data is clean and ready for further processing or conversion to a numerical type.

        Args:
            cell (Tag): A BeautifulSoup Tag object representing a table cell (i.e., <td> element).

        Returns:
            str: The cleaned text content of the table cell, with leading/trailing whitespace removed and commas removed.
        """
        cell = cell.text.strip()
        cell = str.replace(cell, ",", "")
        return cell

    rows = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all("td")
        row = [preprocess(cell) for cell in cells]
        if len(row) < 3:
            continue
        else:
            rows.append(row)

    table = soup.find(attrs={"class": "table"})
    headers = "Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"
    df = pd.DataFrame(rows, columns=headers)
    df['Date'] = pd.to_datetime(df['Date'])
    df[["Open", "High", "Low", "Close", "Adj Close"]] = df[[
        "Open", "High", "Low", "Close", "Adj Close"]].astype(float)
    df[["Volume"]] = df[["Volume"]].astype(int)
    df.set_index("Date", inplace=True)
    df.sort_index(inplace=True)
    return df


def fetch_data(symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """
    Fetches historical stock data for a given symbol within a specified date range.

    Args:
        symbol (str): The stock symbol.
        start_date (str or None): The start date in "YYYY-MM-DD" format or None for default.
        end_date (str or None): The end date in "YYYY-MM-DD" format or None for the current date.

    Returns:
        pd.DataFrame: A dataframe containing the historical stock data.
    """

    if start_date is None:
        start_date = "2000-01-01"
    if end_date is None:
        end_date = datetime.now().date()
    end_date = last_trading_day(end_date)

    start_timestamp = date_to_strptime(start_date)
    end_timestamp = date_to_strptime(end_date)
    if start_timestamp >= end_timestamp:
        return None
    url = f"https://finance.yahoo.com/quote/{symbol}/history/?period1={start_timestamp}&period2={end_timestamp}"

    response = download_web(url)
    soup = load_html_page(response)
    df = process_data(soup)

    return df


def date_to_strptime(date: Union[str, int]) -> int:
    """
    Converts a date (string or numeric) to a Unix timestamp.

    Args:
        date (str or int): The date to convert.

    Returns:
        int: The Unix timestamp of the provided date.
    """
    if str.isnumeric(str(date)):
        return int(date)

    if not isinstance(date, str):
        date = str(date)
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    return int(date_obj.timestamp())


def save_html_page(content: str, file_path: str = ".tmp/index.html") -> None:
    """
    Saves HTML content to a file.

    Args:
        content (str): The HTML content to save.
        file_path (str): The path where the HTML file should be saved (default is ".tmp/index.html").
    """
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)


def download_data(symbol: str) -> pd.DataFrame:
    """
    Downloads historical data for a stock symbol and saves it to a CSV file.

    Args:
        symbol (str): The stock symbol to download data for.

    Returns:
        pd.DataFrame: The historical stock data.

    Raises:
        KeyError: If the symbol is not found.
    """
    if check_symbol(symbol):
        start_date = "2000-01-01"
        end_date = datetime.now().date()
        df = fetch_data(symbol, start_date, end_date)
        df.to_csv(f".data/{symbol}.csv")
        return df
    else:
        raise KeyError


def update_data(symbol: str) -> pd.DataFrame:
    """
    Updates the historical stock data for a given symbol if new data is available.

    Args:
        symbol (str): The stock symbol to update data for.

    Returns:
        pd.DataFrame: The updated historical stock data.
    """
    file_path = f".data/{symbol}.csv"
    df = pd.read_csv(file_path, index_col="Date",
                     parse_dates=True, date_format="%Y-%m-%d")
    now = datetime.now().date()
    last_date = df.index.max().date()
    if now == last_date:
        return df
    else:
        df2 = fetch_data(symbol, last_date, now)

        df = pd.concat([df, df2], axis=0)
        df.sort_index(inplace=True)
        df.drop_duplicates(inplace=True)
        df.to_csv(f".data/{symbol}.csv")
    return df


def last_trading_day(date: datetime.date) -> datetime.date:
    """
    Returns the last trading day before the given date, excluding weekends.

    Args:
        date (datetime.date): The date to check.

    Returns:
        datetime.date: The last trading day before the given date.
    """
    def is_weekend(date: datetime.date) -> bool:
        return date.weekday() >= 5

    current_date = date

    while is_weekend(current_date):
        current_date -= timedelta(days=1)
    return current_date


def load_data(symbol: str) -> pd.DataFrame:
    """
    Loads historical stock data for a given symbol. If the data is not available locally,
    it will be downloaded.

    Args:
        symbol (str): The stock symbol to load data for.

    Returns:
        pd.DataFrame: The historical stock data for the symbol.
    """
    symbol = symbol.lower()
    file_path = f".data/{symbol}.csv"
    if exists(file_path):
        df = update_data(symbol)
        return df
    else:
        df = download_data(symbol)
        return df
