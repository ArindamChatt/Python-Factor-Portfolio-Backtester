#in this code we will be retrieving the ticker details from the
#database and then retrieving it trough yahoo finance api 
import sqlite3
import yfinance as yf
import pandas as pd
from datetime import date, timedelta
from tqdm import tqdm

DB_name='quant_portfolio.db'

def get_stock(conn):
    cur=conn.cursor()
    cur.execute("SELECT id,ticker FROM stocks")
    return cur.fetchall()

def get_last_price(conn,stock_id):
    cur=conn.cursor()
    cur.execute("SELECT MAX(date) FROM daily_prices WHERE stock_id=?",(stock_id,))
    result = cur.fetchone()[0]

    if result:
        return date.fromisoformat(result)
    return None

def fetch_n_save_price_data(conn):
    tickers_info= get_stock(conn)
    default_start_date = date(2010, 1, 1)
    print(f"Fetching price data for {len(tickers_info)} tickers...")


    # Using tqdm for a progress bar
    for stock_id, ticker in tqdm(tickers_info, desc="Updating Prices"):
        try:
            last_date = get_last_price(conn,stock_id)
            
            # Determine the start date for the download
            if last_date:
                start_date = last_date + timedelta(days=1)
            else:
                start_date = default_start_date
            '''the above tells me that if last date exists in daily prices then 
                 the start date is last date + 1 day
                 else its the default start date
            '''
            end_date = date.today()

            #fetching only if the start date is before end date
            if start_date< end_date:
                data = yf.download(ticker,start=start_date,end=end_date,progress=False,auto_adjust=True)
                #Returns a Pandas DataFrame with columns like:
                #Open, High, Low, Close, Adj Close, Volume
                if not data.empty:
                    #if the data returned isnt empty we should
                    data.rename(columns={'Close':'close_price','Volume':'volume'},inplace=True)
        
                data['stock_id'] = stock_id

                data_to_save = data[['stock_id', 'close_price', 'volume']].reset_index()
                data_to_save.columns = ['date', 'stock_id', 'close_price', 'volume']
                data_to_save['date'] = data_to_save['date'].dt.strftime('%Y-%m-%d')

                data_to_save.to_sql('daily_prices', conn, if_exists='append', index=False)
        
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")

###############################################################################################################################
###############################################################################################################################
###############################################################################################################################

'''Now our goal to fetch and save our fundamental data'''

def fetch_and_save_fundamental_data(conn):
    tickers_info=get_stock(conn)
    cur=conn.cursor()

    today_str = date.today().strftime('%Y-%m-%d')

    print("\nFetching fundamental data...")

    for stock_id,ticker in tqdm(tickers_info,desc="UPDATING FUNDAMENTALS"):
        try:
            cur.execute("""
            SELECT id FROM fundamental_data 
            WHERE stock_id = ? AND date_recorded = ?
            """,(stock_id, today_str))

            if cur.fetchone() is not None:
                continue

            stock_info = yf.Ticker(ticker)
            info = stock_info.info

            pe_ratio=info.get('trailingPE',None)
            pb_ratio = info.get('priceToBook', None)
            roe = info.get('returnOnEquity', None)
            debt_equity = info.get('debtToEquity', None)

            cur.execute('''
                INSERT INTO fundamental_data 
                (stock_id, date_recorded, pe_ratio, pb_ratio, roe, debt_equity)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (stock_id, today_str, pe_ratio, pb_ratio, roe, debt_equity))

        except Exception as e:
            print(f"Error fetching fundamental data for {ticker}: {e}")

    conn.commit()

def main():
    try:
        conn = sqlite3.connect(DB_name)
        print(f"Connected to database '{DB_name}'.")
         
        fetch_n_save_price_data(conn)
        fetch_and_save_fundamental_data(conn)

    except sqlite3.Error as e:
        print(f"Database error: {e}")

    finally:
        if conn:
            conn.close()
            print("Database connection closed. Price data is up-to-date.")

if __name__ == '__main__':
    main()