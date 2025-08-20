# Quantitative Multi-Factor Equity Portfolio Strategy with SQL Database Designing
## Project Structure

1. database_setup.py
2. updater.py
3. factor_calc.py
4. portfolio_constructor.py
5. historical_factor_generator.py
6. backtester.py
7. ind_nifty50list.csv
8. app.py
## Project Overview

This project implements a complete, systematic quantitative investment framework designed to identify and select stocks from the **NIFTY 50 Index** or any index. It establishes a full **ETL (Extract, Transform, Load)** pipeline, builds a custom multi-factor scoring engine, and rigorously tests the strategy's historical performance using a quarterly rebalancing backtester.

The entire system is powered by Python and utilizes SQLite for efficient, local data persistence.

## Project Motivation and Rationale

This project was built to tackle several critical real-world challenges in investment and data science, highlighting a blend of finance theory and technical implementation:

### 1. Solving the Problem of Emotional Investing
Traditional investing often falls victim to human biases like fear and greed. This project replaces emotion with a **rules-based, data-driven system**. The motivation was to demonstrate the effectiveness of **Factor Investing**, targeting drivers of long-term returns:
*   **Value (P/E, P/B):** Identifying cheap stocks.
*   **Quality (ROE, D/E):** Targeting robust, high-quality businesses.
*   **Momentum (Price Return):** Following winning trends.
*   **Low Volatility:** Minimizing risk and market noise.

### 2. Building a Robust Data Pipeline
The project demonstrates mastery of the full data workflow:
*   **Data Acquisition:** Using `yfinance` to extract thousands of historical price and fundamental data points.
*   **Data Persistence (SQLite):** Implementing a local SQL database to manage millions of price records, solving the crucial problem of slow API calls and rate limits.

### 3. Handling Real-World Data Flaws
A key motivation was to address problems that break typical beginner scripts:
*   **The "Falling Knife" Paradox:** We solved the problem of a high-quality stock being in a catastrophic downtrend by implementing a **hard momentum filter** (`momentum_score > 2`), ensuring we avoid buying stocks in a freefall.
*   **Survivorship Bias:** The backtesting engine was hardened to ignore "ghost stocks" (like `JIOFIN.NS`) that did not exist during the historical simulation period, ensuring the performance results are truthful.
*   **Missing Data:** We implemented an effective `na_option='bottom'` strategy for ranking, correctly penalizing stocks that have missing data for a given factor.

### 4. Demonstrating Portfolio Customization
The system features **Factor Tilting**, allowing a user to adjust the weight given to each factor based on their risk profile (Conservative, Balanced, Aggressive), creating a personalized investment recipe.

---

## Project Structure and Components

The project is structured into distinct, modular Python files:

| File Name                        | Role                   | Function                                                                                                 |
| -------------------------------- | ---------------------- | -------------------------------------------------------------------------------------------------------- |
| `database_setup.py`              | **ETL/Setup**            | Creates the SQLite database and populates the `stocks` table from the local CSV list.                    |
| `updater.py`                | **ETL/Maintenance**      | Fetches the latest daily prices and fundamental data from `yfinance` and dumps it into the database.     |
| `historical_factor_generator.py` | **Analysis Engine**      | Runs the factor calculation logic historically (quarterly) and populates the `factor_scores` table.        |
| `factor_calc.py`           | **Analysis Core**        | Contains the core functions for calculating raw factors and scaling them into 1-6 Hexile scores.         |
| `portfolio_constructor.py`       | **Decision Engine**      | Calculates the composite score based on the risk profile and selects the final stock portfolio.         |
| `backtester.py`                  | **Performance Engine**   | Simulates the quarterly rebalanced portfolio and compares its performance against the NIFTY 50 benchmark. |
| `app.py`                         | **Frontend/UI**          | The Streamlit application that provides the interactive dashboard and visualization.                     |
| `ind_nifty50list.csv`            | **Data Source**          | The local source file containing the list of current NIFTY 50 constituents.                              |

---

