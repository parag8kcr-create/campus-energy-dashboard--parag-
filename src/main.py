# src/main.py
import logging
from pathlib import Path
from ingestion import load_all
from models import BuildingManager
from analytics import calculate_daily_totals, calculate_weekly_aggregates, building_wise_summary
from visualize import plot_dashboard
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = Path("../data")  # adjust if running from project root
OUT_DIR = Path("../outputs")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    # 1. Ingest
    df = load_all(DATA_DIR)
    if df.empty:
        logger.error("No data loaded. Exiting.")
        return
    # save cleaned combined
    cleaned_path = OUT_DIR / "cleaned_energy_data.csv"
    df.to_csv(cleaned_path, index=False)
    logger.info(f"Saved cleaned data to {cleaned_path}")

    # 2. Build objects
    mgr = BuildingManager()
    mgr.ingest_df(df)
    # 3. Aggregations
    combined_df = mgr.combined_dataframe()
    daily = calculate_daily_totals(combined_df)
    weekly = calculate_weekly_aggregates(combined_df)
    summary = building_wise_summary(combined_df)
    summary_path = OUT_DIR / "building_summary.csv"
    summary.to_csv(summary_path, index=False)
    logger.info(f"Saved building summary to {summary_path}")

    # 4. Visualize dashboard
    dashboard_path = plot_dashboard(daily, weekly, combined_df, out_path=OUT_DIR/"dashboard.png")
    logger.info(f"Saved dashboard to {dashboard_path}")

    # 5. Executive summary (text)
    total_consumption = summary["total_kwh"].sum()
    top = summary.sort_values("total_kwh", ascending=False).iloc[0]
    peak_time = combined_df.loc[combined_df["kwh"].idxmax(),"timestamp"] if not combined_df.empty else None
    with open(OUT_DIR/"summary.txt","w", encoding="utf-8") as f:
        f.write(f"Total campus consumption (kWh): {total_consumption:.2f}\n")
        f.write(f"Highest consuming building: {top['building']} ({top['total_kwh']:.2f} kWh)\n")
        f.write(f"Peak recorded kWh: {combined_df['kwh'].max():.2f} at {peak_time}\n")
    logger.info("Wrote textual summary.")

if __name__ == "__main__":
    main()
