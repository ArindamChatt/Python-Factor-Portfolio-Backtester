import streamlit as st
import pandas as pd
import yfinance as yf
from datetime import date

# Import the functions from your existing backend scripts
from portfolio_constructor import build_portfolio
from backtester import run_backtest, calculate_performance_metrics

# --- Page Configuration ---
st.set_page_config(
    page_title="Quantitative Factor Investing Dashboard",
    page_icon="📈",
    layout="wide"
)

# --- Caching ---
@st.cache_data
def cached_run_backtest(risk_profile, start_date, end_date, num_stocks):
    """A cached version of the backtest function."""
    # Note: We will use 'equal' weighting for the Streamlit app for speed and simplicity.
    return run_backtest(risk_profile, 'equal', start_date, end_date, num_stocks)

# --- Main Application ---
st.title("📈 Quantitative Multi-Factor Portfolio Strategy")
st.write("""
This application builds and backtests a portfolio of NIFTY 50 stocks based on quantitative factors 
(Value, Quality, Momentum, and Low Volatility). Use the sidebar to configure your portfolio.
""")

# --- Sidebar for User Inputs ---
st.sidebar.header("Portfolio Configuration")

BENCHMARK_TICKER = '^NSEI' # NIFTY 50 Index

profile_map = {'Conservative (Safe)': 'conservative', 'Balanced (Standard)': 'balanced', 'Aggressive (High Growth)': 'aggressive'}
profile_choice = st.sidebar.selectbox(
    "Select Your Risk Profile",
    options=list(profile_map.keys()),
    index=1 # Default to 'Balanced'
)
selected_profile = profile_map[profile_choice]

num_stocks = st.sidebar.slider(
    "Number of Stocks in Portfolio",
    min_value=10,
    max_value=50,
    value=20,
    step=5
)

# --- Portfolio Generation ---
if st.sidebar.button("Build My Portfolio"):
    with st.spinner("Building your custom portfolio..."):
        portfolio_df = build_portfolio(risk_profile=selected_profile, num_stocks=num_stocks)
        st.session_state.portfolio_df = portfolio_df

# --- Display Portfolio ---
if 'portfolio_df' in st.session_state and st.session_state.portfolio_df is not None:
    st.header("Your Custom-Built Portfolio")
    st.info(f"Here are the top **{len(st.session_state.portfolio_df)}** stocks selected for the **'{selected_profile}'** profile.")
    
    display_df = st.session_state.portfolio_df[[
        'ticker', 'composite_score', 'value_score', 
        'quality_score', 'momentum_score', 'low_volatility_score'
    ]].copy()
    display_df['composite_score'] = display_df['composite_score'].map('{:.2f}'.format)
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    # --- Backtesting Section ---
    st.header("Historical Performance Simulation (Backtest)")
    
    if st.button("Run Backtest (may take a few minutes)"):
        with st.spinner("Running historical simulation from 2020 to today..."):
            
            START_DATE = '2020-01-01'
            END_DATE = date.today().strftime('%Y-%m-%d')
            
            strategy_returns = cached_run_backtest(selected_profile, START_DATE, END_DATE, num_stocks)
            
            benchmark_data = yf.download(BENCHMARK_TICKER, start=START_DATE, end=END_DATE, progress=False, auto_adjust=True)
            benchmark_returns = benchmark_data['Close'].pct_change().dropna()
            
            st.subheader("Performance Metrics")
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"**Your '{selected_profile}' Strategy**")
                strategy_metrics = calculate_performance_metrics(strategy_returns)
                for metric, value in strategy_metrics.items():
                    st.metric(label=metric, value=str(value))

            with col2:
                st.markdown(f"**Benchmark ({BENCHMARK_TICKER})**")
                benchmark_metrics = calculate_performance_metrics(benchmark_returns)
                for metric, value in benchmark_metrics.items():
                    st.metric(label=metric, value=str(value))
            
            st.subheader("Portfolio Growth (Cumulative Returns)")
            
            # --- THE FIX IS HERE ---
            # Ensure both return series are 1-dimensional before creating the DataFrame
            if isinstance(strategy_returns, pd.DataFrame):
                strategy_returns = strategy_returns.iloc[:, 0]
            if isinstance(benchmark_returns, pd.DataFrame):
                benchmark_returns = benchmark_returns.iloc[:, 0]
            
            # Now, create the DataFrame for charting
            chart_df = pd.DataFrame({
                'Strategy': (1 + strategy_returns).cumprod(),
                'Benchmark': (1 + benchmark_returns).cumprod()
            })
            
            st.line_chart(chart_df)