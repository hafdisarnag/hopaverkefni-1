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
    # Lesum án header því skráin er með titill og haus "inni í gögnum"
    df = pd.read_csv(DATA / "inflation.csv", sep=";", engine="python", header=None)

    # Finna línuna þar sem "Mánuður" kemur (hauslína)
    header_row_idx = None
    for i in range(min(len(df), 50)):
        row = " ".join([str(x) for x in df.iloc[i].tolist()])
        if "Mánuður" in row:
            header_row_idx = i
            break
    if header_row_idx is None:
        raise ValueError("Fann ekki línu með 'Mánuður' í inflation.csv")

    # Gögnin byrja línuna á eftir hausnum
    data = df.iloc[header_row_idx + 1 :].copy()

    # Í þinni skrá virðist:
    # - mánuður vera í fyrri dálki (col0) EÐA seinni (col1)
    # - vísitalan vera í næsta dálki
    # Við veljum mánuðadálk sem passar YYYY M MM
    col0 = data.iloc[:, 0].astype(str).str.strip()
    col1 = data.iloc[:, 1].astype(str).str.strip() if data.shape[1] > 1 else pd.Series([], dtype=str)

    if col0.str.match(r"^\d{4}M\d{2}$", na=False).any():
        month = col0
        # gildi í dálk 1
        value_raw = data.iloc[:, 1] if data.shape[1] > 1 else None
    elif col1.str.match(r"^\d{4}M\d{2}$", na=False).any():
        month = col1
        # gildi í dálk 0
        value_raw = data.iloc[:, 0]
    else:
        raise ValueError("Fann ekki mánuðadálk (YYYYMmm) í inflation.csv")

    out = pd.DataFrame()
    out["year_month"] = month.str.replace(r"^(\d{4})M(\d{2})$", r"\1-\2", regex=True)

    out["inflation_index"] = (
        value_raw.astype(str).str.strip()
        #.str.replace(".", "", regex=False)
        
        .str.replace(",", ".", regex=False)
    )
    out["inflation_index"] = pd.to_numeric(out["inflation_index"], errors="coerce")

    out = out.dropna(subset=["year_month", "inflation_index"]).sort_values("year_month")
    out = out.drop_duplicates(subset=["year_month"])
    return out[["year_month", "inflation_index"]]


def load_interest_rates():
    # Seðlabanki: þetta er semíkommu CSV (þess vegna varð allt í einum dálki)
    df = pd.read_csv(DATA / "interest_rate.csv", sep=";", engine="python")

    # Dálkar eiga að heita: Dagsetning, Meginvextir
    date_col = df.columns[0]
    rate_col = df.columns[1]

    df["date"] = pd.to_datetime(df[date_col], errors="coerce")
    df["interest_rate"] = pd.to_numeric(
        df[rate_col].astype(str).str.replace(",", ".", regex=False),
        errors="coerce"
    )

    df = df.dropna(subset=["date", "interest_rate"]).sort_values("date")
    df["year_month"] = df["date"].dt.to_period("M").astype(str)

    # End-of-month gildi
    monthly = df.groupby("year_month", as_index=False).tail(1)
    return monthly[["year_month", "interest_rate"]].sort_values("year_month")


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

    print("Inflation columns:", list(inflation.columns))
    print(inflation.head(5).to_string(index=False))

    print("Rates columns:", list(rates.columns))
    print(rates.head(5).to_string(index=False))

if __name__ == "__main__":
    main()
