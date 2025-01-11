from os.path import exists
from os import makedirs 
import pandas as pd
    
makedirs(".tmp", exist_ok=True)
makedirs(".data", exist_ok=True)

html_path = ".tmp/index.html"
symbols_path = ".data/symbols.csv"


if not exists(symbols_path):
    df = pd.DataFrame(columns=["symbol", "about"])
    df.set_index("symbol", inplace=True)
    df.to_csv(symbols_path)


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'max-age=0',
    'Pragma': 'no-cache'
}
