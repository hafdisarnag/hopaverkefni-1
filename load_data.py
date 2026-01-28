import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine

import os
from dotenv import load_dotenv
load_dotenv()

SERVER = os.getenv("AZ_SERVER")
DATABASE = os.getenv("AZ_DATABASE")
USERNAME = os.getenv("AZ_USERNAME")
PASSWORD = os.getenv("AZ_PASSWORD")
DRIVER = os.getenv("AZ_DRIVER", "ODBC+Driver+18+for+SQL+Server")

# ==========================

BASE = Path(__file__).resolve().parent
DATA = BASE / "data"

def load_inflation():
    df = pd.read_csv(DATA / "inflation.csv", sep=";", engine="python")

    month_col = df.columns[0]
    value_col = [c for c in df.columns if "vísit" in c.lower()][0]

    out = pd.DataFrame()
    out["year_month"] = (
        df[month_col].astype(str)
        .str.replace(r"^(\d{4})M(\d{2})$", r"\1-\2", regex=True)
    )

    out["inflation_index"] = (
    df[value_col].astype(str)
    .str.replace(".", "", regex=False)
    .str.replace(",", ".", regex=False)
    )

    out["inflation_index"] = pd.to_numeric(
        out["inflation_index"], errors="coerce"
    )

    return out.dropna()

def load_interest_rates():
    df = pd.read_csv(DATA / "interest_rate.csv")

    date_col = df.columns[0]
    rate_col = df.columns[-1]

    df["date"] = pd.to_datetime(df[date_col])
    df["interest_rate"] = pd.to_numeric(df[rate_col], errors="coerce")

    df = df.dropna().sort_values("date")
    df["year_month"] = df["date"].dt.to_period("M").astype(str)

    monthly = df.groupby("year_month").tail(1)
    return monthly[["year_month", "interest_rate"]]

def main():
    inflation = load_inflation()
    rates = load_interest_rates()

    conn_str = (
        f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}:1433/{DATABASE}"
        f"?driver={DRIVER}&Encrypt=yes&TrustServerCertificate=no"
    )
    engine = create_engine(conn_str, fast_executemany=True)

    inflation.to_sql("inflation", engine, if_exists="replace", index=False)
    rates.to_sql("interest_rates", engine, if_exists="replace", index=False)

    print("✅ Gögn komin í Azure SQL")
    print("Inflation rows:", len(inflation))
    print("Interest rate rows:", len(rates))

if __name__ == "__main__":
    main()
