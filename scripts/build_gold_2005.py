import os
import time
import requests
import pandas as pd
from datetime import date

EVDS_KEY = os.environ["EVDS_KEY"].strip()

XAU_SERIES = "TP.DK.XAU.S"
USDTRY_SERIES = "TP.DK.USD.S.YTL"

START_YEAR = 2005
OUT_PATH = "data/gram_altin_2005.csv"
EVDS_BASE = "https://evds2.tcmb.gov.tr/service/evds/"

CONNECT_TIMEOUT = 20
READ_TIMEOUT = 180

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "key": EVDS_KEY,
        "User-Agent": "hedef-altin-github-actions/1.0"
    })
    return s

def fetch_range(session: requests.Session, series: str, start_date: str, end_date: str) -> pd.DataFrame:
    url = f"{EVDS_BASE}series={series}&startDate={start_date}&endDate={end_date}&type=json"

    # retry with backoff
    last_err = None
    for attempt in range(1, 6):
        try:
            r = session.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            r.raise_for_status()
            items = r.json().get("items", [])
            if not items:
                raise RuntimeError("EVDS items boş döndü.")
            return pd.DataFrame(items)
        except Exception as e:
            last_err = e
            sleep_s = min(5 * attempt, 25)
            print(f"[WARN] fetch failed attempt={attempt} range={start_date}->{end_date}: {e}")
            time.sleep(sleep_s)

    raise last_err

def main():
    session = make_session()
    series = f"{XAU_SERIES}-{USDTRY_SERIES}"

    dfs = []
    this_year = date.today().year
    for y in range(START_YEAR, this_year + 1):
        start = f"01-01-{y}"
        end = f"31-12-{y}" if y < this_year else date.today().strftime("%d-%m-%Y")
        df = fetch_range(session, series, start, end)
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)

    if "Tarih" not in df.columns:
        raise RuntimeError(f"Tarih kolonu yok. Kolonlar: {list(df.columns)}")

    df["date"] = pd.to_datetime(df["Tarih"], dayfirst=True, errors="coerce")
    xau = pd.to_numeric(df.get(XAU_SERIES), errors="coerce")
    usdtry = pd.to_numeric(df.get(USDTRY_SERIES), errors="coerce")

    gram_try = (xau * usdtry) / 31.1034768

    out = pd.DataFrame({"date": df["date"], "price": gram_try}).dropna().sort_values("date")
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    out["price"] = out["price"].astype(float).round(6)

    os.makedirs("data", exist_ok=True)
    out.to_csv(OUT_PATH, index=False)
    print(f"OK -> {OUT_PATH} rows={len(out)} last={out.iloc[-1].to_dict()}")

if __name__ == "__main__":
    main()
