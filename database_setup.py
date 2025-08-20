import sqlite3
import pandas as pd
import os 

def get_snp500_tickers():
    ''' 
    this funtion is scraing data from wiki link(url link)
    '''
    print("Fetching S&P 500 tickers from Wikipedia...")
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables=pd.read_html(url)
        sp500_df=tables[0]
        #the df is used for data frame , like snp500 dataframe or nifty50 dataframe
        sp500_df=sp500_df[['Symbol','Security','GICS Sector']].copy()

        sp500_df.rename(columns={'Symbol': 'ticker', 'Security': 'company_name', 'GICS Sector': 'sector'}, inplace=True)
        print("Successfully fetched S&P 500 tickers.")
        
        sp500_df['ticker'] = sp500_df['ticker'].str.replace('.', '-',regex=False)
        #Some tickers on Wikipedia might have a '.' (e.g., BRK.B). yfinance uses '-' (BRK-B).

        return sp500_df
    
    except Exception as e:
        print(f"Error fetching S&P500 : {e}")
        return None

def get_nifty50_tickers_from_csv():
    """
    Reads the NIFTY 50 tickers from a local CSV file.
    This version is more robust and cleans column names automatically.
    """
    csv_filename = 'ind_nifty50list.csv'
    print(f"Reading NIFTY 50 tickers from local file: '{csv_filename}'...")
    
    if not os.path.exists(csv_filename):
        print(f"[ERROR] The file '{csv_filename}' was not found.")
        return None

    try:
        nifty50_df = pd.read_csv(csv_filename)
        
        # --- THE BULLETPROOF FIX IS HERE ---
        # 1. Clean up all column names: remove leading/trailing spaces
        original_columns = nifty50_df.columns.tolist()
        nifty50_df.columns = [col.strip() for col in nifty50_df.columns]
        cleaned_columns = nifty50_df.columns.tolist()
        
        print("\nOriginal columns read from CSV:", original_columns)
        print("Cleaned columns for processing:", cleaned_columns)
        
        # 2. Check if required columns exist after cleaning
        required_cols = ['Company Name', 'Industry', 'Symbol']
        if not all(col in nifty50_df.columns for col in required_cols):
            print("\n[CRITICAL ERROR] Even after cleaning, one of the required columns is missing.")
            print(f"The script needs: {required_cols}")
            return None

        # 3. Proceed with the reliable, cleaned column names
        nifty50_df = nifty50_df[['Company Name', 'Industry', 'Symbol']].copy()
        nifty50_df.rename(columns={
            'Symbol': 'ticker', 
            'Company Name': 'company_name', 
            'Industry': 'sector'
        }, inplace=True)
        
        nifty50_df['ticker'] = nifty50_df['ticker'] + '.NS'
        
        print("\nSuccessfully processed NIFTY 50 tickers from CSV file.")
        return nifty50_df
        
    except Exception as e:
        print(f"\nAn error occurred while processing the CSV file: {e}")
        return None
    
def create_database_tables(conn):
    print("Creating database tables...")
    try:
        cur=conn.cursor()

        cur.executescript('''
            DROP TABLE IF EXISTS stocks;
            DROP TABLE IF EXISTS fundamental_data;
            DROP TABLE IF EXISTS daily_prices;
                        
        CREATE TABLE stocks(
            id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            ticker TEXT NOT NULL UNIQUE,
            company_name TEXT,
            sector TEXT
        );
                    
        CREATE TABLE fundamental_data(
            id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            stock_id INTEGER NOT NULL,
            date_recorded DATE NOT NULL,
            pe_ratio REAL,
            pb_ratio REAL,
            roe REAL,
            debt_equity REAL,
            FOREIGN KEY(stock_id) REFERENCES stocks(id)
        );
                    
        CREATE TABLE daily_prices(
            id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            stock_id INTEGER NOT NULL,
            date DATE NOT NULL,
            close_price REAL NOT NULL,
            volume INTEGER,
            FOREIGN KEY(stock_id) REFERENCES stocks(id),
            UNIQUE (stock_id,date)
        );
                    ''')
    
        conn.commit()
        print("Tables created successfully.")
    except Exception as e:
        print(f"Error creating tables: {e}")


def filling_stocks_table(conn,tickers_df):
    """ this function takes the sql and any ticker data frame as input. This is really useful as we can give any index as input.
    the fuction fills the stocks table in db with all the ticker info"""
    print("populating 'stocks' table")
    try:
        cur=conn.cursor()
        for index,row in tickers_df.iterrows():
            cur.execute('''
        INSERT OR IGNORE INTO stocks(ticker,company_name,sector) VALUES (?, ?, ?)
        ''',(row['ticker'],row['company_name'],row['sector']))
            
        conn.commit()
        print(f"stocks table has now been populated with {len(tickers_df)} tickers")
    except Exception as e:
        print(f"there is an error: {e}") 


def main():
    db_name='quant_portfolio.db'
   # sp500_tickers = get_snp500_tickers() # getting the list of snp500 stocks
    nifty50_tickers = get_nifty50_tickers_from_csv()
    if nifty50_tickers is not None:
        try:
            conn = sqlite3.connect(db_name)
            print("database successsfully connected")

            create_database_tables(conn)
            filling_stocks_table(conn,nifty50_tickers)

        except sqlite3.Error as e:
            print(f"DATABASE error: {e}")
        finally:
            if conn:
                conn.close()
                print("Database connection closed.")

if __name__ == '__main__':
    main()