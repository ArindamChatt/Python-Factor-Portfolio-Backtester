## Getting Started: How to Operate

Follow these steps to set up and run the entire quantitative system locally.

### Prerequisites

You must have Python (3.7+) installed. Open your terminal in the project directory and install the necessary libraries:

```bash
pip install pandas numpy yfinance streamlit tqdm matplotlib scipy
```

### Setup Files

1.  Ensure all `.py` files listed above are in your project folder.
2.  **CRITICAL STEP:** Save the provided NIFTY 50 list as `ind_nifty50list.csv` in the project root directory.

### Execution Workflow for the Streamlit App

This is the primary workflow to get the interactive dashboard running. The system must be run in this sequence:

#### 1. Database Setup
This creates the tables and populates the initial list of stocks from the CSV.
```bash
python database_setup.py
```

#### 2. Data Acquisition
This downloads the historical prices and latest fundamentals for all stocks (can take 10-20 minutes).
```bash
python updater.py
```

#### 3. Historical Intelligence Generation
This runs the analytical model historically (quarterly) to generate the rich data needed for backtesting inside the app (can take 15-30 minutes).
```bash
python historical_factor_generator.py
```

#### 4. Run the Interactive Dashboard (Streamlit)
Once all data is processed, launch the final application.
```bash
# Use this command if 'streamlit' is in your system's PATH
streamlit run app.py

# Use this command if the one above fails
python -m streamlit run app.py
```

---

### Running Standalone Tools (Optional)

The backend scripts can also be used as command-line tools for quick analysis without launching the web app. **Run these only after completing steps 1 and 2 above.**

*   **To get the latest factor scores for today:**
    ```bash
    python factor_calc.py
    ```

*   **To build a portfolio directly in your terminal:**
    ```bash
    python portfolio_constructor.py
    ```

*   **To run a backtest directly in your terminal:**
    ```bash
    # You must run the historical_factor_generator.py script at least once before this will work.
    python backtester.py
    ```

### How to Use the Streamlit Application

1.  In the web application, select a **Risk Profile** (Conservative, Balanced, or Aggressive).
2.  Click **"Build My Portfolio"** to see the current list of top-ranked stocks based on your chosen factor recipe.
3.  Click **"Run Backtest"** to initiate the historical simulation and view the performance chart and key metrics (CAGR, Sharpe Ratio) compared to the NIFTY 50 benchmark.

---

## Customization: Changing the Index

The design of this project is index-agnostic. You can easily switch the investment universe (e.g., from NIFTY 50 to S&P 500) with minimal changes:

1.  **Obtain New CSV:** Find the current list of stocks for your chosen index (e.g., S&P 500) and save it as a new CSV file (e.g., `sp500_list.csv`). Ensure the column names for Ticker, Company Name, and Sector/Industry are consistent.
2.  **Update `database_setup.py`:**
    *   Change the filename variable in `get_nifty50_tickers_from_csv()` to point to your new CSV file (e.g., `csv_filename = 'sp500_list.csv'`).
    *   **Remove the suffix line:** If you are moving to a US index (like S&P 500), you must comment out or delete the line that adds the `.NS` suffix:
        ```python
        # nifty50_df['ticker'] = nifty50_df['ticker'] + '.NS'
        ```
3.  **Run the entire primary workflow again (Setup, Data Acquisition, Historical Generation, and Streamlit App).**

By making these minor adjustments, the entire analytical engine automatically adapts to the new market.
