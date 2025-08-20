import sqlite3
import pandas as pd
from datetime import date
from tqdm import tqdm

DB_NAME = 'quant_portfolio.db'

def calculate_factors_for_date(target_date_str, conn):
    """
    Calculates all factor scores from raw data available up to a specific historical date.
    MODIFIED: It now uses the LATEST available fundamental data for all periods.
    """
    target_date = pd.to_datetime(target_date_str)
    
    # Prices query remains point-in-time
    prices_query = f"SELECT stock_id, date, close_price FROM daily_prices WHERE date <= '{target_date_str}' AND date >= '{ (target_date - pd.DateOffset(days=550)).strftime('%Y-%m-%d') }'"
    
    # --- FIX IS HERE: This query now gets the single most recent fundamental snapshot ---
    fundamentals_query = """
    SELECT f.* FROM fundamental_data f
    INNER JOIN (SELECT stock_id, MAX(date_recorded) as max_date FROM fundamental_data GROUP BY stock_id) fm 
    ON f.stock_id = fm.stock_id AND f.date_recorded = fm.max_date
    """
    stocks_df = pd.read_sql_query("SELECT id as stock_id, ticker FROM stocks", conn)
    prices_df = pd.read_sql_query(prices_query, conn, parse_dates=['date'])
    fundamentals_df = pd.read_sql_query(fundamentals_query, conn)

    if prices_df.empty or fundamentals_df.empty:
        print(f"Warning: Missing price or fundamental data for {target_date_str}. Cannot generate scores.")
        return None

    # ... (The rest of the calculation logic remains exactly the same) ...
    prices_df = prices_df.sort_values(by=['stock_id', 'date'])
    trading_day_periods = {'1m': 21, '3m': 63, '6m': 126, '12m': 252}
    for name, days in trading_day_periods.items():
        prices_df[f'price_{name}_ago'] = prices_df.groupby('stock_id')['close_price'].shift(days)
    
    latest_prices = prices_df.groupby('stock_id').last().reset_index()
    for name in trading_day_periods:
        latest_prices[f'return_{name}'] = (latest_prices['close_price'] - latest_prices[f'price_{name}_ago']) / latest_prices[f'price_{name}_ago']
    
    latest_prices['momentum_raw'] = (latest_prices['return_12m']*0.4 + latest_prices['return_6m']*0.3 + latest_prices['return_3m']*0.2 + latest_prices['return_1m']*0.1)
    
    prices_df['daily_return'] = prices_df.groupby('stock_id')['close_price'].pct_change()
    volatility = prices_df.groupby('stock_id')['daily_return'].std().reset_index().rename(columns={'daily_return': 'volatility_raw'})
    
    factors_df = pd.merge(stocks_df, fundamentals_df, on='stock_id', how='left')
    factors_df = pd.merge(factors_df, latest_prices[['stock_id', 'momentum_raw']], on='stock_id', how='left')
    factors_df = pd.merge(factors_df, volatility, on='stock_id', how='left')
    
    factors_df['value_rank'] = factors_df[['pe_ratio', 'pb_ratio']].mean(axis=1).rank(ascending=True, na_option='bottom')
    factors_df['quality_rank'] = factors_df[['roe', 'debt_equity']].mean(axis=1).rank(ascending=False, na_option='bottom')
    factors_df['momentum_rank'] = factors_df['momentum_raw'].rank(ascending=False, na_option='bottom')
    factors_df['volatility_rank'] = factors_df['volatility_raw'].rank(ascending=True, na_option='bottom')
    
    factors_df['value_score'] = pd.qcut(factors_df['value_rank'], 6, labels=False, duplicates='drop') + 1
    factors_df['quality_score'] = pd.qcut(factors_df['quality_rank'], 6, labels=False, duplicates='drop') + 1
    factors_df['momentum_score'] = pd.qcut(factors_df['momentum_rank'], 6, labels=False, duplicates='drop') + 1
    factors_df['low_volatility_score'] = pd.qcut(factors_df['volatility_rank'], 6, labels=False, duplicates='drop') + 1

    final_scores = factors_df[['stock_id', 'value_score', 'quality_score', 'momentum_score', 'low_volatility_score']].copy()
    final_scores['date_calculated'] = target_date_str
    
    return final_scores

def save_scores_to_db(conn, scores_df):
    scores_df.to_sql('factor_scores', conn, if_exists='append', index=False)

def main():
    """
    Main function to generate and save historical factor scores.
    """
    # Define the historical period for which to generate scores
    start_date = date(2020, 1, 1)
    end_date = date.today()
    
    # --- THE CHANGE IS HERE ---
    # Generate a list of dates for the first business day of each QUARTER
    dates_to_process = pd.date_range(start=start_date, end=end_date, freq='BQS') # Business Quarter Start

    conn = sqlite3.connect(DB_NAME)
    
    # Clean the slate before starting
    print("Deleting all existing factor scores to start fresh...")
    conn.cursor().execute("DELETE FROM factor_scores")
    conn.commit()

    print(f"Starting QUARTERLY historical factor generation for {len(dates_to_process)} periods...")
    for target_date in tqdm(dates_to_process, desc="Generating Quarterly Scores"):
        target_date_str = target_date.strftime('%Y-%m-%d')
        
        scores_df = calculate_factors_for_date(target_date_str, conn)
        
        if scores_df is not None and not scores_df.empty:
            save_scores_to_db(conn, scores_df)
    
    conn.close()
    print("Quarterly historical factor generation complete.")

if __name__ == '__main__':
    main()