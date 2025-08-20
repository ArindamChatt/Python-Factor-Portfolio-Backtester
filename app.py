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
# The cache now includes the weighting_scheme to store different results
@st.cache_data
def cached_run_backtest(risk_profile, weighting_scheme, start_date, end_date, num_stocks):
    """A cached version of the backtest function."""
    return run_backtest(risk_profile, weighting_scheme, start_date, end_date, num_stocks)

# --- Main Application ---
st.title("📈 Quantitative Multi-Factor Portfolio Strategy")
st.write("""
This application builds and backtests a portfolio of NIFTY 50 stocks based on quantitative factors. 
Use the sidebar to configure and test your custom strategy.
""")

# --- Sidebar for User Inputs ---
st.sidebar.header("Strategy Configuration")

BENCHMARK_TICKER = '^NSEI'

profile_map = {'Conservative': 'conservative', 'Balanced': 'balanced', 'Aggressive': 'aggressive'}
profile_choice = st.sidebar.selectbox(
    "1. Select Your Risk Profile",
    options=list(profile_map.keys()),
    index=1 # Default to 'Balanced'
)
selected_profile = profile_map[profile_choice]

# --- CHANGE IS HERE: Added the weighting scheme selection ---
weight_map = {'Equal Weight': 'equal', 'Inverse Volatility': 'inverse_volatility'}
weight_choice = st.sidebar.selectbox(
    "2. Select Your Portfolio Weighting Scheme",
    options=list(weight_map.keys()),
    index=0 # Default to 'Equal Weight'
)
selected_weighting = weight_map[weight_choice]

num_stocks = st.sidebar.slider(
    "3. Number of Stocks in Portfolio",
    min_value=10,
    max_value=50,
    value=20,
    step=5
)

# --- Portfolio Generation ---
st.sidebar.markdown("---")
if st.sidebar.button("Build My Current Portfolio", type="primary"):
    with st.spinner("Building your custom portfolio..."):
        # This function correctly uses the profile and number of stocks
        portfolio_df = build_portfolio(risk_profile=selected_profile, num_stocks=num_stocks)
        st.session_state.portfolio_df = portfolio_df

# --- Display Portfolio ---
if 'portfolio_df' in st.session_state and st.session_state.portfolio_df is not None:
    st.header("Your Custom-Built Portfolio")
    st.info(f"Here are the top **{len(st.session_state.portfolio_df)}** stocks selected for the **'{selected_profile}'** profile, based on the most recent data.")
    
    display_df = st.session_state.portfolio_df[[
        'ticker', 'composite_score', 'value_score', 
        'quality_score', 'momentum_score', 'low_volatility_score'
    ]].copy()
    display_df['composite_score'] = display_df['composite_score'].map('{:.2f}'.format)
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    # --- Backtesting Section ---
    st.header("Historical Performance Simulation (Backtest)")
    st.write(f"This will simulate your chosen strategy: **{profile_choice}** profile, **{selected_weighting.replace('_', ' ').title()}**, and top **{num_stocks}** stocks.")
    
    if st.button("Run Backtest (may take a few minutes)"):
        with st.spinner("Running historical simulation from 2020 to today... This may take a moment."):
            
            START_DATE = '2020-01-01'
            END_DATE = date.today().strftime('%Y-%m-%d')
            
            # --- CHANGE IS HERE: Passing all three user parameters to the backtester ---
            strategy_returns = cached_run_backtest(selected_profile, selected_weighting, START_DATE, END_DATE, num_stocks)
            
            benchmark_data = yf.download(BENCHMARK_TICKER, start=START_DATE, end=END_DATE, progress=False, auto_adjust=True)
            benchmark_returns = benchmark_data['Close'].pct_change().dropna()
            
            st.subheader("Performance Metrics")
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"**Your Strategy**")
                strategy_metrics = calculate_performance_metrics(strategy_returns)
                for metric, value in strategy_metrics.items():
                    st.metric(label=metric, value=str(value))

            with col2:
                st.markdown(f"**Benchmark ({BENCHMARK_TICKER})**")
                benchmark_metrics = calculate_performance_metrics(benchmark_returns)
                for metric, value in benchmark_metrics.items():
                    st.metric(label=metric, value=str(value))
            
            st.subheader("Portfolio Growth (Cumulative Returns)")
            
            if isinstance(strategy_returns, pd.DataFrame): strategy_returns = strategy_returns.iloc[:, 0]
            if isinstance(benchmark_returns, pd.DataFrame): benchmark_returns = benchmark_returns.iloc[:, 0]
            
            chart_df = pd.DataFrame({
                'Strategy': (1 + strategy_returns).cumprod(),
                'Benchmark': (1 + benchmark_returns).cumprod()
            })
            
            st.line_chart(chart_df)
