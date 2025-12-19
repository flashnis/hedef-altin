import os
import requests
import pandas as pd
from datetime import date

EVDS_KEY = os.environ["EVDS_KEY"].strip()

# Senin verdiğin seriler
XAU_SERIES = "TP.DK.XAU.S"        # Ons altın (USD/ONS)
USDTRY_SERIES = "TP.DK.USD.S.YTL" # USD/TRY satış

START_DATE = "01-01-2005"
END_DATE = date.today().strftime("%d-%m-%Y")
OUT_PATH = "data/gram_altin_2005.csv"
EVDS_URL = "https://evds2.tcmb.gov.tr/service/evds/"

def main():
    params = {
        "series": f"{XAU_SERIES}-{USDTRY_SERIES}",
        "startDate": START_DATE,
        "endDate": END_DATE,
        "type": "json",
        "key": EVDS_KEY,
    }
    r = requests.get(EVDS_URL, params=params, timeout=60)
    r.raise_for_status()

    items = r.json().get("items", [])
    if not items:
        raise RuntimeError("EVDS items boş döndü. Seri kodlarını kontrol et.")

    df = pd.DataFrame(items)

    if "Tarih" not in df.columns:
        raise RuntimeError(f"Tarih kolonu yok. Kolonlar: {list(df.columns)}")

    df["date"] = pd.to_datetime(df["Tarih"], dayfirst=True, errors="coerce")
    xau = pd.to_numeric(df.get(XAU_SERIES), errors="coerce")
    usdtry = pd.to_numeric(df.get(USDTRY_SERIES), errors="coerce")

    gram_try = (xau * usdtry) / 31.1034768  # TL/gram

    out = pd.DataFrame({"date": df["date"], "price": gram_try}).dropna().sort_values("date")
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    out["price"] = out["price"].astype(float).round(6)

    os.makedirs("data", exist_ok=True)
    out.to_csv(OUT_PATH, index=False)
    print(f"OK -> {OUT_PATH} rows={len(out)} last={out.iloc[-1].to_dict()}")

if __name__ == "__main__":
    main()
