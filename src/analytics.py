# src/analytics.py
import pandas as pd
import numpy as np

def calculate_daily_totals(df: pd.DataFrame):
    # df: must have timestamp index or timestamp column
    d = df.copy()
    if "timestamp" in d.columns:
        d = d.set_index("timestamp")
    daily = d.groupby("building").resample("D", on=d.index).sum().reset_index()
    # simpler: pivot table
    daily_pivot = daily.pivot(index="timestamp", columns="building", values="kwh").fillna(0)
    return daily_pivot

def calculate_weekly_aggregates(df: pd.DataFrame):
    d = df.copy()
    if "timestamp" in d.columns:
        d = d.set_index("timestamp")
    weekly = d.groupby("building").resample("W", on=d.index).sum().reset_index()
    weekly_pivot = weekly.pivot(index="timestamp", columns="building", values="kwh").fillna(0)
    return weekly_pivot

def building_wise_summary(df_combined: pd.DataFrame):
    # returns dataframe with aggregate stats per building
    d = df_combined.copy()
    if "timestamp" in d.columns:
        d = d.set_index("timestamp")
    agg = d.groupby("building")["kwh"].agg(["sum","mean","min","max","std"]).reset_index()
    agg = agg.rename(columns={"sum":"total_kwh","mean":"avg_kwh","min":"min_kwh","max":"max_kwh","std":"std_kwh"})
    return agg
