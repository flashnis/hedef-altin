import os
import requests
import pandas as pd
from datetime import date

EVDS_KEY = os.environ["EVDS_KEY"].strip()

INF_SERIES = "TP.FG.TUFE.AYLIK"
START_DATE = "01-01-2005"
END_DATE = date.today().strftime("%d-%m-%Y")
OUT_PATH = "data/enflasyon_aylik.csv"
EVDS_BASE = "https://evds2.tcmb.gov.tr/service/evds/"

def main():
    url = f"{EVDS_BASE}series={INF_SERIES}&startDate={START_DATE}&endDate={END_DATE}&type=json"
    headers = {"key": EVDS_KEY}  # <-- kritik değişiklik

    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()

    items = r.json().get("items", [])
    if not items:
        raise RuntimeError("EVDS items boş döndü. Seri kodlarını kontrol et.")

    df = pd.DataFrame(items)
    if "Tarih" not in df.columns:
        raise RuntimeError(f"Tarih kolonu yok. Kolonlar: {list(df.columns)}")

    df["date"] = pd.to_datetime(df["Tarih"], dayfirst=True, errors="coerce")
    v = pd.to_numeric(df.get(INF_SERIES), errors="coerce")

    # yüzde geldiyse (2.5 gibi) -> 0.025
    monthly = v / 100.0
    monthly = monthly.where(v > 1, v)

    out = pd.DataFrame({"date": df["date"], "monthlyInflation": monthly}).dropna().sort_values("date")
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    out["monthlyInflation"] = out["monthlyInflation"].astype(float).round(8)

    os.makedirs("data", exist_ok=True)
    out.to_csv(OUT_PATH, index=False)
    print(f"OK -> {OUT_PATH} rows={len(out)} last={out.iloc[-1].to_dict()}")

if __name__ == "__main__":
    main()
