# src/models.py
from dataclasses import dataclass, field
from datetime import datetime
from typing import List
import pandas as pd

@dataclass
class MeterReading:
    timestamp: pd.Timestamp
    kwh: float

@dataclass
class Building:
    name: str
    readings: List[MeterReading] = field(default_factory=list)

    def add_reading(self, ts, kwh):
        self.readings.append(MeterReading(pd.to_datetime(ts), float(kwh)))

    def total_consumption(self):
        return sum(r.kwh for r in self.readings)

    def to_dataframe(self):
        data = {"timestamp":[r.timestamp for r in self.readings],
                "kwh":[r.kwh for r in self.readings]}
        df = pd.DataFrame(data).set_index("timestamp").sort_index()
        return df

    def generate_report(self):
        df = self.to_dataframe()
        if df.empty:
            return {}
        return {
            "building": self.name,
            "total_kwh": df["kwh"].sum(),
            "mean_kwh": df["kwh"].mean(),
            "min_kwh": df["kwh"].min(),
            "max_kwh": df["kwh"].max()
        }

class BuildingManager:
    def __init__(self):
        self.buildings = {}

    def ingest_df(self, df: pd.DataFrame):
        # df must have columns: timestamp, kwh, building
        for _, row in df.iterrows():
            bname = row["building"]
            if bname not in self.buildings:
                self.buildings[bname] = Building(bname)
            self.buildings[bname].add_reading(row["timestamp"], row["kwh"])

    def summaries(self):
        return [b.generate_report() for b in self.buildings.values()]

    def combined_dataframe(self):
        # returns DataFrame with columns: timestamp, kwh, building
        parts = []
        for b in self.buildings.values():
            df = b.to_dataframe().reset_index()
            df["building"] = b.name
            parts.append(df)
        if parts:
            return pd.concat(parts, ignore_index=True)
        return pd.DataFrame(columns=["timestamp","kwh","building"])
