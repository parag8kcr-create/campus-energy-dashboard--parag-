# src/ingestion.py
import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def discover_csvs(data_dir: str | Path):
    p = Path(data_dir)
    return sorted(p.glob("*.csv"))

def read_building_file(path: Path, building_name: str = None):
    try:
        df = pd.read_csv(path)
    except FileNotFoundError:
        logger.error(f"File not found: {path}")
        return None
    except Exception as e:
        logger.error(f"Error reading {path}: {e}")
        return None

    # Normalize columns if necessary
    if "timestamp" not in df.columns:
        # try common alternatives
        for alt in ("time","date","datetime"):
            if alt in df.columns:
                df = df.rename(columns={alt: "timestamp"})
                break
    if "kwh" not in df.columns:
        for alt in ("kWh","consumption","energy"):
            if alt in df.columns:
                df = df.rename(columns={alt: "kwh"})
                break

    if "timestamp" not in df.columns or "kwh" not in df.columns:
        logger.error(f"Required columns missing in {path.name}")
        return None

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df["kwh"] = pd.to_numeric(df["kwh"], errors="coerce").fillna(0.0)
    if building_name is None:
        building_name = path.stem
    df["building"] = building_name
    return df

def load_all(data_dir: str | Path):
    files = discover_csvs(data_dir)
    dfs = []
    for f in files:
        df = read_building_file(f)
        if df is None:
            logger.warning(f"Skipping file: {f}")
            continue
        dfs.append(df)
    if not dfs:
        return pd.DataFrame(columns=["timestamp","kwh","building"])
    combined = pd.concat(dfs, ignore_index=True)
    # unify timezone and sort
    combined = combined.sort_values("timestamp").reset_index(drop=True)
    return combined
