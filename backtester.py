import sqlite3
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import date, timedelta
from tqdm import tqdm

DB_NAME = 'quant_portfolio.db'

# --- Replicated Logic from portfolio_constructor.py for consistency ---
FACTOR_WEIGHTS = {
    'conservative': {'value_score': 0.15, 'quality_score': 0.40, 'momentum_score': 0.05, 'low_volatility_score': 0.40},
    'balanced': {'value_score': 0.25, 'quality_score': 0.25, 'momentum_score': 0.25, 'low_volatility_score': 0.25},
    'aggressive': {'value_score': 0.40, 'quality_score': 0.15, 'momentum_score': 0.40, 'low_volatility_score': 0.05}
}

def calculate_composite_score(scores_df, risk_profile):
    weights = FACTOR_WEIGHTS[risk_profile]
    scores_df['composite_score'] = (
        scores_df['value_score'].fillna(0) * weights['value_score'] +
        scores_df['quality_score'].fillna(0) * weights['quality_score'] +
        scores_df['momentum_score'].fillna(0) * weights['momentum_score'] +
        scores_df['low_volatility_score'].fillna(0) * weights['low_volatility_score']
    )
    return scores_df

def build_portfolio_for_date(target_date, risk_profile, num_stocks):
    conn = None
    try:
        conn = sqlite3.connect(DB_NAME)
        query = f"SELECT s.ticker, fs.* FROM factor_scores fs JOIN stocks s ON s.id = fs.stock_id WHERE fs.date_calculated = '{target_date}'"
        scores_for_date = pd.read_sql_query(query, conn)
        
        if scores_for_date.empty: return None

        valid_scores = scores_for_date.dropna(subset=['momentum_score']).copy()
        portfolio_df = valid_scores[valid_scores['momentum_score'] > 3].copy()

        portfolio_df = calculate_composite_score(portfolio_df, risk_profile)
        portfolio_df = portfolio_df.sort_values(by='composite_score', ascending=False)
        
        return portfolio_df.head(num_stocks)
    finally:
        if conn: conn.close()

def get_stock_prices(tickers, start_date, end_date):
    """Gets historical prices and cleans out any stocks that fail to download."""
    prices = yf.download(tickers, start=start_date, end=end_date, progress=False, auto_adjust=True)['Close']
    if isinstance(prices, pd.Series):
        prices = prices.to_frame(name=tickers[0] if tickers else 'data')
    return prices.dropna(axis=1, how='all')

def calculate_weights(prices, scheme='equal'):
    """Calculates portfolio weights based on the chosen scheme."""
    if scheme == 'equal':
        return pd.Series(1 / len(prices.columns), index=prices.columns)
    elif scheme == 'inverse_volatility':
        returns = prices.pct_change(fill_method=None).dropna()
        volatility = returns.std()
        volatility = volatility.replace(0, np.nan) # Avoid division by zero
        inverse_vol = 1 / volatility
        return inverse_vol / inverse_vol.sum()

def run_backtest(risk_profile, weighting_scheme, start_date_str, end_date_str, num_stocks=20):
    start_date = pd.to_datetime(start_date_str)
    end_date = pd.to_datetime(end_date_str)
    rebalance_dates = pd.date_range(start=start_date, end=end_date, freq='BQS')
    all_returns = []

    print(f"\n--- Starting Backtest (Profile: '{risk_profile}', Weighting: {weighting_scheme}) ---")
    
    for i in tqdm(range(len(rebalance_dates) - 1), desc="Backtesting Quarters"):
        rebalance_date = rebalance_dates[i]
        period_end = rebalance_dates[i+1]
        
        portfolio_df = build_portfolio_for_date(rebalance_date.strftime('%Y-%m-%d'), risk_profile, num_stocks)
        if portfolio_df is None or portfolio_df.empty: continue

        tickers = portfolio_df['ticker'].tolist()
        
        # Get historical prices to calculate weights
        prices_for_weights = get_stock_prices(tickers, rebalance_date - timedelta(days=365), rebalance_date)
        if prices_for_weights.empty: continue
        
        weights = calculate_weights(prices_for_weights, weighting_scheme)
        
        # Get prices for the actual holding period
        prices_for_returns = get_stock_prices(weights.index.tolist(), rebalance_date, period_end)
        if prices_for_returns.empty: continue
            
        daily_returns = prices_for_returns.pct_change(fill_method=None).dropna()
        period_portfolio_returns = (daily_returns * weights).sum(axis=1)
        all_returns.append(period_portfolio_returns)

    if not all_returns: return pd.Series(dtype=float)
    return pd.concat(all_returns)

def calculate_performance_metrics(returns):
    # ... (This is the final, robust version from before)
    if returns.empty or len(returns) < 2: return {metric: "N/A" for metric in ["Total Return", "Annualized Return (CAGR)", "Annualized Volatility", "Sharpe Ratio"]}
    if isinstance(returns, pd.DataFrame): returns = returns.iloc[:, 0]
    total_return = (1 + returns).prod() - 1
    annualized_return = ((1 + total_return) ** (252 / len(returns))) - 1
    annualized_volatility = returns.std() * np.sqrt(252)
    sharpe_ratio = annualized_return / annualized_volatility if annualized_volatility != 0 else 0.0
    return {"Total Return": f"{total_return:.2%}", "Annualized Return (CAGR)": f"{annualized_return:.2%}", "Annualized Volatility": f"{annualized_volatility:.2%}", "Sharpe Ratio": f"{sharpe_ratio:.2f}"}

def main():
    print("\n--- Backtesting Engine ---")
    
    profile_map = {'1': 'conservative', '2': 'balanced', '3': 'aggressive'}
    profile_choice = input("Select risk profile (1: Conservative, 2: Balanced, 3: Aggressive): ")
    risk_profile = profile_map.get(profile_choice, 'balanced')

    # --- THE WEIGHTING CHOICE IS BACK ---
    weight_map = {'1': 'equal', '2': 'inverse_volatility'}
    weight_choice = input("Select weighting scheme (1: Equal Weight, 2: Inverse Volatility): ")
    weighting_scheme = weight_map.get(weight_choice, 'equal')

    START_DATE = '2020-01-01'
    END_DATE = date.today().strftime('%Y-%m-%d')
    BENCHMARK_TICKER = '^NSEI'

    strategy_returns = run_backtest(risk_profile, weighting_scheme, START_DATE, END_DATE)
    
    benchmark_data = yf.download(BENCHMARK_TICKER, start=START_DATE, end=END_DATE, progress=False, auto_adjust=True)
    benchmark_returns = benchmark_data['Close'].pct_change().dropna()
    
    print("\n--- Strategy Performance ---")
    print(calculate_performance_metrics(strategy_returns))

    print(f"\n--- Benchmark ({BENCHMARK_TICKER}) Performance ---")
    print(calculate_performance_metrics(benchmark_returns))

if __name__ == '__main__':
    main()