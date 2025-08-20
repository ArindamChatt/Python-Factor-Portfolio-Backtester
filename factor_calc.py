import sqlite3
import pandas as pd
from datetime import date

DB_NAME = 'quant_portfolio.db'

def create_factor_scores_table(conn):
    try:
        cur = conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS factor_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_id INTEGER NOT NULL,
                date_calculated DATE NOT NULL,
                value_score INTEGER,
                quality_score INTEGER,
                momentum_score INTEGER,
                low_volatility_score INTEGER,
                FOREIGN KEY (stock_id) REFERENCES stocks (id),
                UNIQUE (stock_id, date_calculated)
            )
        ''')
        conn.commit()
    except Exception as e:
        print(f"Error creating factor_scores table: {e}")

def get_data(conn):
    """
    Retrieves all necessary raw data from the database.
    """
    print("Reading data from database...")
    stocks_query = "SELECT id, ticker FROM stocks"
    
    # --- FIX IS HERE: Widen the data window to ~18 months (550 days) ---
    # This provides a large buffer and makes the momentum calculation robust.
    prices_query = "SELECT stock_id, date, close_price FROM daily_prices WHERE date >= date('now', '-550 days')"
    
    fundamentals_query = """
    SELECT f.*
    FROM fundamental_data f
    INNER JOIN (
        SELECT stock_id, MAX(date_recorded) as max_date
        FROM fundamental_data
        GROUP BY stock_id
    ) fm ON f.stock_id = fm.stock_id AND f.date_recorded = fm.max_date
    """
    
    stocks_df = pd.read_sql_query(stocks_query, conn)
    prices_df = pd.read_sql_query(prices_query, conn, parse_dates=['date'])
    fundamentals_df = pd.read_sql_query(fundamentals_query, conn)
    
    return stocks_df, prices_df, fundamentals_df

def calculate_factors(stocks_df, prices_df, fundamentals_df):
    print("Calculating factors...")

    #---Momentum factor---
    prices_df = prices_df.sort_values(by=['stock_id', 'date'])
   
    trading_day_periods = {
        '1m': 21,
        '3m': 63,
        '6m': 126,
        '12m': 252
    }
    for name, days in trading_day_periods.items():
        prices_df[f'price_{name}_ago'] = prices_df.groupby('stock_id')['close_price'].shift(days)
   

    latest_prices = prices_df.groupby('stock_id').last().reset_index()

    for name, days in trading_day_periods.items():
        latest_prices[f'return_{name}'] = (latest_prices['close_price'] - latest_prices[f'price_{name}_ago']) / latest_prices[f'price_{name}_ago']

    #the above is just a simple calculation of %return = current price-old price/old price

    latest_prices['momentum_raw'] =(
        latest_prices['return_12m'] * 0.4 + 
        latest_prices['return_6m'] * 0.3 + 
        latest_prices['return_3m'] * 0.2 + 
        latest_prices['return_1m'] * 0.1
    )

    """ copied from notion notes A common weighting scheme is:
    
        - **12-month return:** 40% weight
        - **6-month return:** 30% weight
        - **3-month return:** 20% weight
        - **1-month return:** 10% weight

        **Composite Momentum Score = (12m_return * 0.4) + (6m_return * 0.3) + (3m_return * 0.2) + (1m_return * 0.1)**
        """
    
    #---Low Volatility Score---
    # 1-year standard deviation of daily returns
    prices_df['daily_return'] = prices_df.groupby('stock_id')['close_price'].pct_change()
    volatility = prices_df.groupby('stock_id')['daily_return'].std().reset_index()
    volatility.rename(columns={'daily_return': 'volatility_raw'}, inplace=True)
    
    # Merge factors into a single DataFrame
    factors_df = pd.merge(stocks_df, fundamentals_df, left_on='id', right_on='stock_id', how='left')
    factors_df = pd.merge(factors_df, latest_prices[['stock_id', 'momentum_raw']], on='stock_id', how='left')
    factors_df = pd.merge(factors_df, volatility, on='stock_id', how='left')

    # --- 3. Ranking and Scaling ---
    print("Ranking and scaling factors...")
    
    # Create ranks for each factor. `na_option='bottom'` is crucial for handling NULLs.
    # Value Ranks
    factors_df['pe_rank'] = factors_df['pe_ratio'].rank(ascending=True, na_option='bottom')
    factors_df['pb_rank'] = factors_df['pb_ratio'].rank(ascending=True, na_option='bottom')
    
    # Quality Ranks
    factors_df['roe_rank'] = factors_df['roe'].rank(ascending=False, na_option='bottom')
    factors_df['de_rank'] = factors_df['debt_equity'].rank(ascending=True, na_option='bottom')

    # Momentum and Volatility Ranks
    factors_df['momentum_rank'] = factors_df['momentum_raw'].rank(ascending=False, na_option='bottom')
    factors_df['volatility_rank'] = factors_df['volatility_raw'].rank(ascending=True, na_option='bottom')

      # Average the sub-factor ranks to get the final factor ranks
    factors_df['value_rank_final'] = factors_df[['pe_rank', 'pb_rank']].mean(axis=1)
    factors_df['quality_rank_final'] = factors_df[['roe_rank', 'de_rank']].mean(axis=1)

    # Convert final ranks to Hexile Scores (1-6)
    # `pd.qcut` divides the data into N equal-sized groups (quantiles)
    factors_df['value_score'] = pd.qcut(factors_df['value_rank_final'], 6, labels=False, duplicates='drop') + 1
    factors_df['quality_score'] = pd.qcut(factors_df['quality_rank_final'], 6, labels=False, duplicates='drop') + 1
    factors_df['momentum_score'] = pd.qcut(factors_df['momentum_rank'], 6, labels=False, duplicates='drop') + 1
    factors_df['low_volatility_score'] = pd.qcut(factors_df['volatility_rank'], 6, labels=False, duplicates='drop') + 1
    
    # Prepare final DataFrame for saving
    final_scores = factors_df[['stock_id', 'value_score', 'quality_score', 'momentum_score', 'low_volatility_score']].copy()
    final_scores['date_calculated'] = date.today().strftime('%Y-%m-%d')
    
    return final_scores

def save_scores_to_db(conn, scores_df):
    """
    Saves the calculated factor scores to the database, ensuring no duplicates for the day.
    """
    print("Saving scores to database...")
    today_str = date.today().strftime('%Y-%m-%d')
    cursor = conn.cursor()

    # --- FIX IS HERE: Delete any scores from today before inserting new ones ---
    print(f"Deleting any existing scores for {today_str} to prevent duplicates...")
    cursor.execute("DELETE FROM factor_scores WHERE date_calculated = ?", (today_str,))

    # Now, insert the new scores
    scores_df.to_sql('factor_scores', conn, if_exists='append', index=False)
    
    conn.commit()
    print(f"Successfully saved {len(scores_df)} factor scores for {today_str}.")


def main():
    """
    Main function to run the factor calculation and save the results.
    """
    try:
        conn = sqlite3.connect(DB_NAME)
        print(f"Connected to database '{DB_NAME}'.")
        
        create_factor_scores_table(conn)
        
        stocks, prices, fundamentals = get_data(conn)
        
        final_scores_df = calculate_factors(stocks, prices, fundamentals)
        
        save_scores_to_db(conn, final_scores_df)
        
        # Optional: Print the top 10 stocks by quality score to see the results
        print("\n--- Top 10 by Quality Score ---")
        quality_query = """
        SELECT s.ticker, fs.* FROM factor_scores fs
        JOIN stocks s ON s.id = fs.stock_id
        ORDER BY fs.quality_score DESC, s.ticker
        LIMIT 10
        """
        top_quality = pd.read_sql_query(quality_query, conn)
        print(top_quality)
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")

if __name__ == '__main__':
    main()