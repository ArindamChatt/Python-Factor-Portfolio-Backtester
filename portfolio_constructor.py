import sqlite3
import pandas as pd

DB_NAME = 'quant_portfolio.db'

# The "Factor Recipes" for each risk profile
FACTOR_WEIGHTS = {
    'conservative': {
        'value_score': 0.15,
        'quality_score': 0.40,
        'momentum_score': 0.05,
        'low_volatility_score': 0.40
    },
    'balanced': {
        'value_score': 0.25,
        'quality_score': 0.25,
        'momentum_score': 0.25,
        'low_volatility_score': 0.25
    },
    'aggressive': {
        'value_score': 0.40,
        'quality_score': 0.15,
        'momentum_score': 0.40,
        'low_volatility_score': 0.05
    }
}

def get_latest_factor_scores(conn):
    """
    Retrieves the most recent set of factor scores for all stocks.
    """
    query = """
    SELECT s.ticker, fs.*
    FROM factor_scores fs
    JOIN stocks s ON s.id = fs.stock_id
    WHERE fs.date_calculated = (SELECT MAX(date_calculated) FROM factor_scores)
    """
    return pd.read_sql_query(query, conn)

def calculate_composite_score(scores_df, risk_profile):
    """
    Calculates the composite score based on the selected risk profile.
    """
    if risk_profile not in FACTOR_WEIGHTS:
        raise ValueError("Invalid risk profile specified.")
        
    weights = FACTOR_WEIGHTS[risk_profile]
    
    # Calculate the weighted average score
    scores_df['composite_score'] = (
        scores_df['value_score'] * weights['value_score'] +
        scores_df['quality_score'] * weights['quality_score'] +
        scores_df['momentum_score'] * weights['momentum_score'] +
        scores_df['low_volatility_score'] * weights['low_volatility_score']
    )
    return scores_df

def build_portfolio(risk_profile='balanced', num_stocks=30):
    """
    Constructs a portfolio based on risk profile and number of stocks.

    Args:
        risk_profile (str): 'conservative', 'balanced', or 'aggressive'.
        num_stocks (int): The number of stocks to include in the portfolio.

    Returns:
        pd.DataFrame: A sorted DataFrame of the top N stocks for the portfolio.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_NAME)
        
        # 1. Get the latest scores
        latest_scores = get_latest_factor_scores(conn)
        
        # 2. Calculate composite scores based on risk profile
        portfolio_df = calculate_composite_score(latest_scores, risk_profile)
        
        #This is a crucial rule to avoid buying stocks that are in a strong downtrend.
        
        print(f"\nOriginal number of stocks considered: {len(portfolio_df)}")
        portfolio_df = portfolio_df[portfolio_df['momentum_score'] > 3]
        print(f"Number of stocks after applying momentum filter: {len(portfolio_df)}")
        
       
        portfolio_df = portfolio_df.sort_values(by='composite_score', ascending=False)
        
        return portfolio_df.head(num_stocks)
        
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        if conn:
            conn.close()

def main():
    """
    Main function to interactively build a portfolio based on user input.
    """
    while True:
        print("\n--- Quantitative Portfolio Builder ---")
        print("Select your risk profile:")
        print("1. Conservative")
        print("2. Balanced")
        print("3. Aggressive")
        
        choice = input("Enter your choice (1, 2, or 3) or 'q' to quit: ")

        if choice.lower() == 'q':
            print("Exiting program.")
            break
        
        if choice not in ['1', '2', '3']:
            print("\n*** Invalid choice. Please enter 1, 2, or 3. ***")
            continue

        # Map choice to profile name
        profile_map = {'1': 'conservative', '2': 'balanced', '3': 'aggressive'}
        selected_profile = profile_map[choice]

        # Get number of stocks from user
        while True:
            try:
                num_stocks_str = input("Enter the number of stocks for the portfolio (e.g., 30): ")
                num_stocks = int(num_stocks_str)
                if num_stocks > 0:
                    break
                else:
                    print("*** Please enter a positive number. ***")
            except ValueError:
                print("*** Invalid input. Please enter a whole number. ***")

        print(f"\nBuilding a '{selected_profile}' portfolio with {num_stocks} stocks...")
        
        portfolio = build_portfolio(risk_profile=selected_profile, num_stocks=num_stocks)
        
        if portfolio is not None:
            print("--- Here is your generated portfolio ---")
            # Select and reorder columns for a clean display
            display_cols = [
                'ticker', 'composite_score', 'value_score', 
                'quality_score', 'momentum_score', 'low_volatility_score'
            ]
            print(portfolio[display_cols].reset_index(drop=True))

if __name__ == '__main__':
    main()