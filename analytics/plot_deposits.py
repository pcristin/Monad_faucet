import os
import pandas as pd # type: ignore
import matplotlib.pyplot as plt # type: ignore
import matplotlib.dates as mdates # type: ignore 
import numpy as np # type: ignore
import seaborn as sns # type: ignore
from sqlalchemy import create_engine # type: ignore
from datetime import datetime, timedelta
from dotenv import load_dotenv # type: ignore
import matplotlib.ticker as ticker # type: ignore
import matplotlib.patches as mpatches # type: ignore
from sklearn.linear_model import LinearRegression # type: ignore
from statsmodels.tsa.arima.model import ARIMA # type: ignore
import warnings
warnings.filterwarnings("ignore")
load_dotenv()

# Set plot style
plt.style.use('ggplot')
plt.rcParams['font.family'] = 'DejaVu Sans'

# Currency mapping
CURRENCY_MAP = {
    0: 'ETH',
    1: 'USDC',
    2: 'USDT'
}

# Currency colors
CURRENCY_COLORS = {
    'ETH': '#3498db',    # Blue
    'USDT': '#f39c12',   # Amber
    'USDC': '#2ecc71'    # Green
}

# Status colors and labels for the transaction count plot
STATUS_COLORS = {
    'processed': '#2ecc71',  # Green - Fix status names to match actual DB values
    'refunded': '#e74c3c',   # Red
    'pending': '#f1c40f'     # Yellow
}

STATUS_LABELS = {
    'processed': 'Processed',
    'refunded': 'Refunded',
    'pending': 'Pending'
}

# Denomination divisors for each currency
DENOMINATION_DIVISORS = {
    'ETH': 10**18,    # ETH is stored in wei (10^18 wei = 1 ETH)
    'USDT': 10**6,    # USDT is stored in smallest units (10^6 = 1 USDT)
    'USDC': 10**6,    # USDC is stored in smallest units (10^6 = 1 USDC)
}

# Get database URL from environment
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set")

print(f"Connecting to database...")
# Create database engine
engine = create_engine(DATABASE_URL)

# Query to get deposits data with status
query = """
    SELECT 
        DATE(created_at) as date,
        currency,
        status,
        SUM(CAST(amount AS NUMERIC)) as total_amount,
        COUNT(*) as transaction_count
    FROM deposits
    GROUP BY DATE(created_at), currency, status
    ORDER BY date
"""

print(f"Executing query...")
# Read data into pandas DataFrame
df = pd.read_sql_query(query, engine)
print(f"Retrieved {len(df)} rows of data")

if len(df) == 0:
    print("No data found. Please check your database connection and query.")
    exit(1)

# Debug info
print("Found currencies:", df['currency'].unique())
print("Found statuses:", df['status'].unique())

# Map currency codes to currency names
df['currency'] = df['currency'].map(lambda x: CURRENCY_MAP.get(x, f'Unknown-{x}'))

# Debug info
print("Available currencies after mapping:", df['currency'].unique())

# Apply denomination conversion based on currency type
print("Converting denominations...")
for idx, row in df.iterrows():
    currency = row['currency']
    if currency in DENOMINATION_DIVISORS:
        divisor = DENOMINATION_DIVISORS[currency]
        df.loc[idx, 'total_amount'] = row['total_amount'] / divisor

# ---- PLOT 1: Deposit Amounts (Only Processed) ----

# Filter for processed deposits only (note the status name from the debug output)
df_processed = df[df['status'] == 'processed']

# Pivot the processed data for amounts
df_pivot_amounts = df_processed.pivot_table(
    index='date', 
    columns='currency', 
    values='total_amount',
    aggfunc='sum'
).fillna(0)

# Convert index to datetime if it's not already
df_pivot_amounts.index = pd.to_datetime(df_pivot_amounts.index)

# Sort by date
df_pivot_amounts = df_pivot_amounts.sort_index()

print(f"Plotting amounts data from {df_pivot_amounts.index.min().date() if not df_pivot_amounts.empty else 'N/A'} to {df_pivot_amounts.index.max().date() if not df_pivot_amounts.empty else 'N/A'}")

# Create the plot with dual Y-axes - adjust figure size to accommodate legend
fig, ax1 = plt.subplots(figsize=(16, 9), dpi=100)
fig.patch.set_facecolor('#f8f9fa')  # Light background for the figure

# Set background color
ax1.set_facecolor('#f8f9fa')

# Check if we have any data to plot
if df_pivot_amounts.empty:
    print("No data to plot after pivoting. Check currency mapping or status filtering.")
    plt.text(0.5, 0.5, "No processed deposit data available", horizontalalignment='center', 
             verticalalignment='center', transform=plt.gca().transAxes, fontsize=14)
else:
    # Create a secondary Y-axis for ETH
    ax2 = ax1.twinx()
    
    # Plot stablecoins with fill
    stablecoins = ['USDT', 'USDC']
    for currency in stablecoins:
        if currency in df_pivot_amounts.columns and df_pivot_amounts[currency].sum() > 0:
            color = CURRENCY_COLORS[currency]
            line = ax1.plot(df_pivot_amounts.index, df_pivot_amounts[currency], 
                          label=f"{currency}", 
                          marker='o', 
                          markersize=8,
                          linewidth=3,
                          color=color)
            
            # Add light fill below the line
            ax1.fill_between(df_pivot_amounts.index, df_pivot_amounts[currency], alpha=0.2, color=color)
            
            # Annotate max points
            max_idx = df_pivot_amounts[currency].idxmax()
            max_val = df_pivot_amounts[currency].max()
            if max_val > 0.5:  # Only annotate significant peaks
                ax1.annotate(f'{max_val:.1f}',
                          xy=(max_idx, max_val),
                          xytext=(0, 10),
                          textcoords='offset points',
                          ha='center',
                          va='bottom',
                          fontsize=10,
                          fontweight='bold',
                          bbox=dict(boxstyle='round,pad=0.3', fc='white', alpha=0.7))
    
    # Plot ETH on the secondary Y-axis with fill
    if 'ETH' in df_pivot_amounts.columns and df_pivot_amounts['ETH'].sum() > 0:
        color = CURRENCY_COLORS['ETH']
        line = ax2.plot(df_pivot_amounts.index, df_pivot_amounts['ETH'], 
                      label='ETH', 
                      marker='s', 
                      markersize=8,
                      linewidth=3,
                      color=color,
                      linestyle='-')
        
        # Add light fill below the line
        ax2.fill_between(df_pivot_amounts.index, df_pivot_amounts['ETH'], alpha=0.2, color=color)
        
        # Annotate max points
        max_idx = df_pivot_amounts['ETH'].idxmax()
        max_val = df_pivot_amounts['ETH'].max()
        if max_val > 0:  # Only annotate significant peaks
            ax2.annotate(f'{max_val:.3f}',
                      xy=(max_idx, max_val),
                      xytext=(0, 10),
                      textcoords='offset points',
                      ha='center',
                      va='bottom',
                      fontsize=10,
                      fontweight='bold',
                      bbox=dict(boxstyle='round,pad=0.3', fc='white', alpha=0.7))

    # Set labels and title
    ax1.set_xlabel('Date', fontsize=13, fontweight='bold', labelpad=10)
    ax1.set_ylabel('Stablecoin Amount (USDT/USDC)', fontsize=13, fontweight='bold', labelpad=10)
    ax2.set_ylabel('ETH Amount', fontsize=13, fontweight='bold', labelpad=10, color=CURRENCY_COLORS['ETH'])
    
    # Create custom legend elements
    from matplotlib.lines import Line2D # type: ignore
    legend_elements = []
    
    for currency in df_pivot_amounts.columns:
        if df_pivot_amounts[currency].sum() > 0:
            color = CURRENCY_COLORS.get(currency, 'gray')
            if currency == 'ETH':
                legend_elements.append(Line2D([0], [0], color=color, lw=3, marker='s', markersize=8,
                                            label=f"{currency} (right axis)"))
            else:
                legend_elements.append(Line2D([0], [0], color=color, lw=3, marker='o', markersize=8,
                                            label=f"{currency} (left axis)"))
    
    # Add custom legend - place it at the top of the figure to avoid overlapping with data
    ax1.legend(handles=legend_elements, 
              loc='upper center', 
              fontsize=11,
              frameon=True, 
              framealpha=0.9, 
              edgecolor='lightgray',
              bbox_to_anchor=(0.5, 1.05),  # Position above the plot area
              ncol=3)  # Arrange items horizontally
    
    # Format x-axis date ticks
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=45)
    
    # Add grid to the primary axis with light color
    ax1.grid(True, linestyle='--', alpha=0.4, color='gray')
    
    # Remove top and right spines for clean look
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax2.spines['top'].set_visible(False)
    
    # Add statistics info
    stablecoin_sum = sum(df_pivot_amounts[c].sum() for c in stablecoins if c in df_pivot_amounts.columns)
    eth_sum = df_pivot_amounts['ETH'].sum() if 'ETH' in df_pivot_amounts.columns else 0
    
    stats_text = (
        f"Total Processed Deposits:\n"
        f"Stablecoins: {stablecoin_sum:.2f}\n"
        f"ETH: {eth_sum:.4f}"
    )
    
    # Add text box with stats
    plt.figtext(0.02, 0.02, stats_text, fontsize=11, 
              bbox=dict(facecolor='white', alpha=0.8, boxstyle='round,pad=0.5'))

# Add title with styling
plt.suptitle('Processed Cryptocurrency Deposits Over Time', fontsize=18, fontweight='bold', y=0.98)
plt.title('Daily processed deposit volumes with dual scale (ETH vs Stablecoins)', 
        fontsize=14, pad=20, loc='center', style='italic')

# Adjust layout to prevent label cutoff - increased top margin to accommodate legend
plt.tight_layout(rect=[0, 0.03, 1, 0.90])

# Save the plot with high DPI for better quality
output_path = 'deposits_by_day.png'  # Save in current directory
plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor=fig.get_facecolor())
print(f"Plot has been saved as '{output_path}'")

# ---- PLOT 2: ENHANCED Transaction Counts by Status with Forecast ----

# Prepare data for transaction counts plot
# Pivot transaction counts by date and status
df_pivot_counts = df.pivot_table(
    index='date',
    columns='status',
    values='transaction_count',
    aggfunc='sum'
).fillna(0)

# Convert index to datetime
df_pivot_counts.index = pd.to_datetime(df_pivot_counts.index)
df_pivot_counts = df_pivot_counts.sort_index()

# Calculate additional metrics - using 'processed' status instead of 'completed'
total_daily_counts = df_pivot_counts.sum(axis=1)
if 'processed' in df_pivot_counts.columns:
    success_rate = df_pivot_counts['processed'] / total_daily_counts
    df_pivot_counts['success_rate'] = success_rate

# Get all dates from min to max with 1-day intervals (for proper x-axis)
if not df_pivot_counts.empty:
    date_range = pd.date_range(start=df_pivot_counts.index.min(), end=df_pivot_counts.index.max(), freq='D')
    # Reindex to ensure all dates are represented (fill missing with 0)
    df_pivot_counts = df_pivot_counts.reindex(date_range, fill_value=0)

# Create the transaction count plot with enhanced features
fig2 = plt.figure(figsize=(16, 10), dpi=100)
# Create a layout with two subplots: main bar chart and success rate line chart
gs = fig2.add_gridspec(3, 1, height_ratios=[4, 1.5, 0.5])
ax_bars = fig2.add_subplot(gs[0])  # Main bar chart
ax_rate = fig2.add_subplot(gs[1], sharex=ax_bars)  # Success rate chart
ax_dist = fig2.add_subplot(gs[2])  # Status distribution chart

# Set background colors
fig2.patch.set_facecolor('#f8f9fa')
ax_bars.set_facecolor('#f8f9fa')
ax_rate.set_facecolor('#f8f9fa')
ax_dist.set_facecolor('#f8f9fa')

# Check if we have any data
if df_pivot_counts.empty:
    ax_bars.text(0.5, 0.5, "No transaction data available", horizontalalignment='center',
             verticalalignment='center', transform=ax_bars.transAxes, fontsize=14)
else:
    # Prioritize statuses in a meaningful order
    all_statuses = list(df_pivot_counts.columns)
    if 'success_rate' in all_statuses:
        all_statuses.remove('success_rate')  # This is not a status
        
    prioritized_statuses = []
    if 'processed' in all_statuses:
        prioritized_statuses.append('processed')
        all_statuses.remove('processed')
    
    if 'refunded' in all_statuses:
        prioritized_statuses.append('refunded')
        all_statuses.remove('refunded')
    
    if 'pending' in all_statuses:
        prioritized_statuses.append('pending')
        all_statuses.remove('pending')
    
    # Add any remaining statuses
    prioritized_statuses.extend(sorted(all_statuses))
    
    # --- Main Bar Chart ---
    # Create stacked bar chart for transaction counts
    bottoms = np.zeros(len(df_pivot_counts))
    
    # Plot each status as a bar segment
    for status in prioritized_statuses:
        if status in df_pivot_counts.columns:
            color = STATUS_COLORS.get(status, 'gray')
            label = STATUS_LABELS.get(status, status.capitalize())
            counts = df_pivot_counts[status].values  # Get values as array
            
            bars = ax_bars.bar(df_pivot_counts.index, counts, bottom=bottoms, 
                    label=label, color=color, alpha=0.8, width=0.8)
            
            # Add count labels on the bars (only for significant segments)
            for i, count in enumerate(counts):
                if count >= 3:  # Only label segments with at least 3 transactions
                    # Calculate position (center of segment)
                    height = bottoms[i] + (count / 2)
                    ax_bars.text(df_pivot_counts.index[i], height, f'{int(count)}', 
                           ha='center', va='center', fontweight='bold', 
                           color='white', fontsize=9)
            
            # Update bottoms for next series
            bottoms = bottoms + counts
    
    # Add total count at the top of each stacked bar
    for i, idx in enumerate(df_pivot_counts.index):
        total = sum(df_pivot_counts.loc[idx, status] for status in prioritized_statuses)
        if total > 0:
            ax_bars.text(idx, bottoms[i] + 2, f'Total: {int(total)}', 
                   ha='center', va='bottom', fontweight='bold', 
                   color='black', fontsize=10)
    
    # Find the day with the most transactions
    busiest_day_idx = total_daily_counts.idxmax() if not total_daily_counts.empty else None
    if busiest_day_idx is not None:
        busiest_day_count = int(total_daily_counts[busiest_day_idx])
        # Add annotation for the busiest day
        ax_bars.annotate(f'Busiest Day: {busiest_day_count} txns',
                    xy=(busiest_day_idx, busiest_day_count + 5),
                    xytext=(busiest_day_idx, busiest_day_count + 20),
                    arrowprops=dict(facecolor='black', shrink=0.05, width=1.5),
                    ha='center', fontsize=11, fontweight='bold',
                    bbox=dict(boxstyle='round,pad=0.4', fc='white', alpha=0.9))
    
    # Add 7-day moving average line for total transactions - FIX FOR DIMENSION MISMATCH
    if len(total_daily_counts) >= 3:
        window_size = min(7, len(total_daily_counts))
        # Calculate rolling average with min_periods=1
        rolling_avg = total_daily_counts.rolling(window=window_size, min_periods=1).mean()
        
        # Make sure to align the dimensions before plotting
        # Explicitly specify x and y data with same dimensions
        valid_indices = rolling_avg.index  # These are the same as df_pivot_counts.index
        valid_values = rolling_avg.values  # These might be shorter if there are NaNs
        
        # Plot only the valid data points
        ax_bars.plot(valid_indices, valid_values, color='navy', 
               linestyle='-', linewidth=2, label=f'{window_size}-Day Avg')
        
        # Add trend indicator
        if len(rolling_avg) >= 2:  # At least 2 points for trend
            first_week = rolling_avg.iloc[:min(7, len(rolling_avg))].mean()
            last_week = rolling_avg.iloc[-min(7, len(rolling_avg)):].mean()
            
            if first_week > 0:
                trend_pct = ((last_week - first_week) / first_week * 100)
                trend_text = (f"Trend: {'↑' if trend_pct > 0 else '↓'} {abs(trend_pct):.1f}% "
                             f"({first_week:.1f} → {last_week:.1f} txns/day)")
                
                if not np.isnan(trend_pct):
                    ax_bars.text(0.98, 0.05, trend_text, 
                           transform=ax_bars.transAxes, ha='right', va='bottom',
                           fontsize=11, fontweight='bold',
                           bbox=dict(boxstyle='round,pad=0.4', fc='white', ec='gray', alpha=0.9))
        
        # ---- Add forecasting ----
        # Only add forecast if we have enough data points
        if len(rolling_avg) >= 5:  # Need at least 5 data points for a reasonable forecast
            # Number of days to forecast
            forecast_days = 14  # Two weeks forecast
            
            # Create a date range for future dates
            last_date = df_pivot_counts.index[-1]
            future_dates = pd.date_range(start=last_date + timedelta(days=1), 
                                        periods=forecast_days, freq='D')
            
            # Prepare data for forecasting model - use numerical index for fitting
            df_for_forecast = pd.DataFrame(rolling_avg).reset_index()
            df_for_forecast['numeric_date'] = range(len(df_for_forecast))
            
            # Try ARIMA forecasting
            try:
                # Fit ARIMA model
                model = ARIMA(rolling_avg, order=(1, 0, 0))
                model_fit = model.fit()
                
                # Generate forecast with confidence intervals
                forecast_result = model_fit.get_forecast(steps=forecast_days)
                forecast_mean = forecast_result.predicted_mean
                forecast_ci = forecast_result.conf_int(alpha=0.2)  # 80% confidence interval
                
                # Plot forecasted line
                ax_bars.plot(future_dates, forecast_mean, color='#9b59b6', linestyle='-', 
                       linewidth=2.5, label='Forecast')
                
                # Plot confidence interval as shaded area
                ax_bars.fill_between(future_dates, 
                               forecast_ci.iloc[:, 0], 
                               forecast_ci.iloc[:, 1], 
                               color='#9b59b6', alpha=0.2)
                
                # Add forecast annotation
                forecast_end_value = forecast_mean.iloc[-1]
                ax_bars.annotate(f'Forecast: {forecast_end_value:.1f} txns/day',
                           xy=(future_dates[-1], forecast_end_value),
                           xytext=(future_dates[-1] - timedelta(days=2), forecast_end_value + 10),
                           arrowprops=dict(facecolor='#9b59b6', shrink=0.05, width=1.5),
                           ha='right', fontsize=11, fontweight='bold',
                           color='#9b59b6',
                           bbox=dict(boxstyle='round,pad=0.4', fc='white', alpha=0.9))
            except Exception as e:
                print(f"ARIMA forecasting failed: {e}. Falling back to linear regression.")
                try:
                    # Fallback to linear regression if ARIMA fails
                    X = df_for_forecast['numeric_date'].values.reshape(-1, 1)
                    y = df_for_forecast[0].values
                    
                    model = LinearRegression()
                    model.fit(X, y)
                    
                    # Generate future numeric dates
                    future_numeric_dates = np.array(range(len(X), len(X) + forecast_days)).reshape(-1, 1)
                    
                    # Predict future values
                    future_values = model.predict(future_numeric_dates)
                    
                    # Calculate simple confidence interval
                    errors = y - model.predict(X)
                    std_error = np.std(errors)
                    
                    # Plot forecast line
                    ax_bars.plot(future_dates, future_values, color='#9b59b6', linestyle='-', 
                           linewidth=2.5, label='Forecast (Linear)')
                    
                    # Plot confidence interval
                    ax_bars.fill_between(future_dates, 
                                   future_values - 1.96 * std_error, 
                                   future_values + 1.96 * std_error, 
                                   color='#9b59b6', alpha=0.2)
                    
                    # Add forecast annotation
                    forecast_end_value = future_values[-1]
                    ax_bars.annotate(f'Forecast: {forecast_end_value:.1f} txns/day',
                               xy=(future_dates[-1], forecast_end_value),
                               xytext=(future_dates[-1] - timedelta(days=2), forecast_end_value + 10),
                               arrowprops=dict(facecolor='#9b59b6', shrink=0.05, width=1.5),
                               ha='right', fontsize=11, fontweight='bold',
                               color='#9b59b6',
                               bbox=dict(boxstyle='round,pad=0.4', fc='white', alpha=0.9))
                except Exception as e:
                    print(f"Linear regression forecasting failed too: {e}")
                    
            # Adjust x-axis limits to include forecast
            ax_bars.set_xlim(df_pivot_counts.index[0] - timedelta(days=1), 
                      future_dates[-1] + timedelta(days=1))
    
    # Set labels and formatting for main bar chart
    ax_bars.set_ylabel('Number of Transactions', fontsize=13, fontweight='bold', labelpad=10)
    ax_bars.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))  # Use integer ticks
    ax_bars.grid(True, linestyle='--', alpha=0.3, color='gray')
    ax_bars.spines['top'].set_visible(False)
    ax_bars.spines['right'].set_visible(False)
    
    # --- Success Rate Chart ---
    if 'success_rate' in df_pivot_counts.columns:
        # Plot success rate as a line
        success_line = ax_rate.plot(df_pivot_counts.index, df_pivot_counts['success_rate'] * 100, 
                 color='#27ae60', linewidth=2.5, marker='o', markersize=5)
        
        # Add shaded region for success rate
        ax_rate.fill_between(df_pivot_counts.index, 0, df_pivot_counts['success_rate'] * 100, 
                      color='#27ae60', alpha=0.15)
        
        # Format y-axis as percentage
        ax_rate.yaxis.set_major_formatter(ticker.PercentFormatter())
        
        # Set labels
        ax_rate.set_ylabel('Success Rate', fontsize=12, fontweight='bold', color='#27ae60')
        ax_rate.tick_params(axis='y', colors='#27ae60')
        
        # Handle empty or NaN data for average calculation
        if not df_pivot_counts['success_rate'].isna().all() and len(df_pivot_counts['success_rate']) > 0:
            # Add success rate average line
            avg_success = df_pivot_counts['success_rate'].mean() * 100
            if not np.isnan(avg_success):
                ax_rate.axhline(y=avg_success, color='#27ae60', linestyle='--', alpha=0.7)
                ax_rate.text(df_pivot_counts.index[-1], avg_success + 2, 
                        f'Avg: {avg_success:.1f}%', color='#27ae60', fontsize=10)
                
        # Adjust x-axis to match the main plot (include forecast period)
        if 'future_dates' in locals():
            ax_rate.set_xlim(df_pivot_counts.index[0] - timedelta(days=1), 
                      future_dates[-1] + timedelta(days=1))
        
        # Set y-axis range
        ax_rate.set_ylim(0, 105)  # 0-105% to leave room for the average label
    
    # Format x-axis date ticks (only on the bottom subplot)
    ax_rate.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax_rate.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax_rate.tick_params(axis='x', rotation=45)
    ax_rate.grid(True, linestyle='--', alpha=0.3, color='gray')
    ax_rate.spines['top'].set_visible(False)
    ax_rate.spines['right'].set_visible(False)
    
    # --- Status Distribution Pie Chart ---
    # Calculate total transactions by status
    status_totals = {status: df_pivot_counts[status].sum() for status in prioritized_statuses}
    total_txns = sum(status_totals.values())
    
    # Skip pie chart if no transactions
    if total_txns > 0:
        # Create horizontal bar for status distribution
        status_names = []
        status_percentages = []
        status_colors = []
        
        for status in prioritized_statuses:
            if status in status_totals and status_totals[status] > 0:
                count = status_totals[status]
                pct = (count / total_txns) * 100
                label = STATUS_LABELS.get(status, status.capitalize())
                status_names.append(f"{label}\n{int(count)} ({pct:.1f}%)")
                status_percentages.append(pct)
                status_colors.append(STATUS_COLORS.get(status, 'gray'))
        
        # Create distribution bar - handle empty data
        if status_percentages:  # Only if we have data
            # Fix the cumsum calculation for left parameter
            lefts = np.zeros(len(status_percentages))
            for i in range(1, len(status_percentages)):
                lefts[i] = lefts[i-1] + status_percentages[i-1]
            
            # Create the bars
            ax_dist.barh(0, status_percentages, left=lefts, 
                    height=0.5, color=status_colors)
            
            # Add text labels in center of each segment
            for i, (pct, name, left) in enumerate(zip(status_percentages, status_names, lefts)):
                ax_dist.text(left + pct/2, 0, name, ha='center', va='center', fontweight='bold',
                        color='white' if pct > 10 else 'black', fontsize=10)
        
        # Remove ticks and spines
        ax_dist.set_yticks([])
        ax_dist.set_xticks([])
        for spine in ax_dist.spines.values():
            spine.set_visible(False)
    
    # Add custom legend to main plot with a bit more sophistication
    handles = []
    for status in prioritized_statuses:
        if status in df_pivot_counts.columns and df_pivot_counts[status].sum() > 0:
            color = STATUS_COLORS.get(status, 'gray')
            label = STATUS_LABELS.get(status, status.capitalize())
            handles.append(mpatches.Patch(color=color, label=label, alpha=0.8))
    
    # Add moving average to legend
    if len(total_daily_counts) >= 3:
        handles.append(Line2D([0], [0], color='navy', linewidth=2, label=f'{window_size}-Day Avg'))
    
    # Add forecast to legend
    if 'forecast_mean' in locals() or 'future_values' in locals():
        handles.append(Line2D([0], [0], color='#9b59b6', linewidth=2.5, 
                           label='14-Day Forecast'))
    
    # Place legend at the top
    if handles:  # Only add legend if we have handles
        ax_bars.legend(handles=handles, loc='upper center', 
                    bbox_to_anchor=(0.5, 1.10), ncol=len(handles), 
                    fontsize=11, frameon=True, framealpha=0.9, edgecolor='lightgray')

# Add titles
fig2.suptitle('Cryptocurrency Deposit Transaction Activity and Forecast', fontsize=18, fontweight='bold', y=0.98)
fig2.text(0.5, 0.94, 'Daily transaction counts with success rate trends and 14-day forecast', 
         fontsize=14, ha='center', style='italic')

# Add summary stats
if not df_pivot_counts.empty:
    try:
        total_transactions = sum(status_totals.values()) if 'status_totals' in locals() else df['transaction_count'].sum()
        
        stats_text = f"Total Deposit Transactions: {int(total_transactions)}\n"
        for status in prioritized_statuses:
            if status in status_totals:
                count = status_totals[status]
                percent = (count / total_transactions) * 100 if total_transactions > 0 else 0
                label = STATUS_LABELS.get(status, status.capitalize())
                stats_text += f"{label}: {int(count)} ({percent:.1f}%)\n"
        
        # Add completion rate
        if 'processed' in status_totals and total_transactions > 0:
            completion_rate = (status_totals['processed'] / total_transactions) * 100
            stats_text += f"\nOverall Success Rate: {completion_rate:.1f}%"
            
            # Calculate daily average transactions
            days_with_activity = len(df_pivot_counts[df_pivot_counts.sum(axis=1) > 0])
            if days_with_activity > 0:
                avg_daily_txns = total_transactions / days_with_activity
                stats_text += f"\nAvg Daily Transactions: {avg_daily_txns:.1f}"
                
            # Add forecast summary if available
            if 'forecast_mean' in locals():
                last_value = rolling_avg.iloc[-1] if not rolling_avg.empty else 0
                forecast_end = forecast_mean.iloc[-1]
                forecast_change = ((forecast_end - last_value) / last_value * 100) if last_value > 0 else 0
                forecast_direction = "↑" if forecast_change >= 0 else "↓"
                stats_text += f"\nForecast (14d): {forecast_direction} {abs(forecast_change):.1f}%"
            elif 'future_values' in locals():
                last_value = rolling_avg.iloc[-1] if not rolling_avg.empty else 0
                forecast_end = future_values[-1]
                forecast_change = ((forecast_end - last_value) / last_value * 100) if last_value > 0 else 0
                forecast_direction = "↑" if forecast_change >= 0 else "↓"
                stats_text += f"\nForecast (14d): {forecast_direction} {abs(forecast_change):.1f}%"
        
        # Add text box with stats
        fig2.text(0.02, 0.02, stats_text, fontsize=11, 
               bbox=dict(facecolor='white', alpha=0.9, boxstyle='round,pad=0.5'))
    except Exception as e:
        print(f"Error generating stats summary: {e}")

# Adjust layout
fig2.tight_layout(rect=[0, 0.03, 1, 0.93])

# Save the transaction count plot with high quality
tx_output_path = 'deposit_transactions_by_status.png'
fig2.savefig(tx_output_path, dpi=300, bbox_inches='tight', facecolor=fig2.get_facecolor())
print(f"Enhanced transaction count plot with forecast has been saved as '{tx_output_path}'")

# Print a summary of the data
if not df_pivot_amounts.empty:
    print("\nSummary of processed deposits by currency (denominated):")
    for currency in df_pivot_amounts.columns:
        total = df_pivot_amounts[currency].sum()
        if total > 0:
            print(f"{currency}: {total:,.4f}") 