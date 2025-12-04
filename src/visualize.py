# src/visualize.py
import matplotlib.pyplot as plt
from pathlib import Path
import pandas as pd

def plot_dashboard(daily_df: pd.DataFrame, weekly_df: pd.DataFrame, combined_df: pd.DataFrame, out_path="outputs/dashboard.png"):
    # daily_df and weekly_df are pivot tables: index timestamps, columns buildings
    fig, axes = plt.subplots(3,1, figsize=(12,14))
    # 1. Trend Line – daily consumption over time for all buildings
    daily_df.plot(ax=axes[0], legend=True)
    axes[0].set_title("Daily Consumption per Building (kWh)")
    axes[0].set_ylabel("kWh")
    # 2. Bar Chart – average weekly usage across buildings
    avg_weekly = weekly_df.mean().sort_values(ascending=False)
    avg_weekly.plot(kind="bar", ax=axes[1])
    axes[1].set_title("Average Weekly Usage by Building (kWh)")
    axes[1].set_ylabel("kWh")
    # 3. Scatter of peak-hour consumption: get hourly peaks from combined_df
    # combined_df: timestamp, kwh, building
    if "timestamp" in combined_df.columns:
        combined_df["timestamp"] = pd.to_datetime(combined_df["timestamp"])
        combined_df = combined_df.set_index("timestamp")
    # peak per day per building
    peaks = combined_df.groupby(["building", pd.Grouper(freq="D")])["kwh"].max().reset_index()
    # scatter: x = date, y = kwh, color by building
    for b in peaks["building"].unique():
        subset = peaks[peaks["building"]==b]
        axes[2].scatter(subset["timestamp"], subset["kwh"], label=b, s=8)
    axes[2].set_title("Daily Peak Hour Consumption per Building (kWh)")
    axes[2].set_ylabel("kWh")
    axes[2].legend()
    plt.tight_layout()
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=150)
    plt.close()
    return out_path
