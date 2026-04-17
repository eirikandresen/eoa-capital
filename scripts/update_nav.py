"""
update_nav.py
Fetches latest NAV for Pareto Aksje Norge A.
Priority: 1) Morningstar NO  2) Yahoo Finance  3) Keep existing

Runs automatically via GitHub Actions every weekday at 20:00 Oslo time.
Also imports PAN_A_-_daglig_nav.xlsx if present in repo root.
"""
import json
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

ROOT = Path(__file__).parent.parent
OUT  = ROOT / 'nav_data.json'
XLSX = ROOT / 'PAN_A_-_daglig_nav.xlsx'

# Morningstar ID for Pareto Aksje Norge A
MS_ID      = 'F0GBR04OMP'
MS_UNIVERSE = 'FONOR$$ALL'

# Yahoo Finance fallback tickers
YAHOO_TICKERS = ['0P00001F9P.IR', '0P0001BNTE.F', 'POAKTNY.OL']

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

# ─── helpers ────────────────────────────────────────────────────────────────

def load_existing():
    if OUT.exists():
        with open(OUT) as f:
            return json.load(f)
    return []

def save(data):
    data.sort(key=lambda r: r['d'])
    with open(OUT, 'w') as f:
        json.dump(data, f, separators=(',', ':'))

def get_last_date(data):
    return data[-1]['d'] if data else '2021-12-19'

# ─── Morningstar ─────────────────────────────────────────────────────────────

def fetch_morningstar(start_date):
    """
    Use Morningstar NO timeseries API.
    Returns list of {date, nav} or None on failure.
    """
    try:
        # Step 1: get auth token from Morningstar homepage
        r0 = requests.get('https://www.morningstar.no', headers=HEADERS, timeout=10)
        token = None
        for part in r0.text.split('"'):
            if len(part) == 16 and part.isalnum():
                token = part
                break
        if not token:
            # try known token pattern from URL
            token = 'dr6pz9spfi'
        print(f'Morningstar token: {token}')

        url = (
            f'https://tools.morningstar.no/api/rest.svc/timeseries_price/{token}'
            f'?currencyId=NOK&idtype=Morningstar&frequency=daily&outputType=JSON'
            f'&startDate={start_date}'
            f'&id={MS_ID}]2]1]{MS_UNIVERSE}'
        )
        r = requests.get(url, headers=HEADERS, timeout=15)
        if not r.ok:
            print(f'Morningstar HTTP {r.status_code}')
            return None

        d = r.json()
        series = (d.get('TimeSeries', {})
                   .get('Security', [{}])[0]
                   .get('HistoryDetail', []))
        if not series:
            print('Morningstar: empty series')
            return None

        rows = []
        for point in series:
            date_str = point.get('EndDate', '')[:10]
            val = point.get('Value')
            if date_str and val is not None:
                nav = round(float(val), 4)
                if nav > 1000:
                    rows.append({'date': date_str, 'nav': nav})

        print(f'Morningstar: got {len(rows)} rows')
        return rows if rows else None

    except Exception as e:
        print(f'Morningstar failed: {e}')
        return None

# ─── Yahoo Finance fallback ───────────────────────────────────────────────────

def fetch_yahoo(ticker, start_date):
    try:
        start = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end   = int((datetime.now() + timedelta(days=2)).timestamp())
        url   = (f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}'
                 f'?interval=1d&period1={start}&period2={end}')
        r = requests.get(url, headers=HEADERS, timeout=10)
        if not r.ok:
            return None
        d = r.json()
        result = d.get('chart', {}).get('result', [])
        if not result:
            return None
        timestamps = result[0].get('timestamp', [])
        closes = result[0].get('indicators', {}).get('quote', [{}])[0].get('close', [])
        rows = []
        for t, c in zip(timestamps, closes):
            if c is None:
                continue
            nav = round(float(c), 4)
            if nav > 1000:
                rows.append({'date': datetime.utcfromtimestamp(t).strftime('%Y-%m-%d'), 'nav': nav})
        return rows if rows else None
    except Exception as e:
        print(f'Yahoo {ticker} failed: {e}')
        return None

# ─── OSEFX ───────────────────────────────────────────────────────────────────

def fetch_osefx(start_date):
    try:
        start = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end   = int((datetime.now() + timedelta(days=2)).timestamp())
        url   = (f'https://query1.finance.yahoo.com/v8/finance/chart/%5EOSEAX'
                 f'?interval=1d&period1={start}&period2={end}')
        r = requests.get(url, headers=HEADERS, timeout=10)
        if not r.ok:
            return {}
        d = r.json()
        result = d.get('chart', {}).get('result', [])
        if not result:
            return {}
        timestamps = result[0].get('timestamp', [])
        closes = result[0].get('indicators', {}).get('quote', [{}])[0].get('close', [])
        return {
            datetime.utcfromtimestamp(t).strftime('%Y-%m-%d'): round(float(c), 4)
            for t, c in zip(timestamps, closes) if c is not None
        }
    except Exception as e:
        print(f'OSEFX failed: {e}')
        return {}

# ─── main ────────────────────────────────────────────────────────────────────

def main():
    existing = load_existing()
    existing_dates = {r['d'] for r in existing}

    # Bulk import from xlsx if present
    if XLSX.exists():
        print(f'Found {XLSX.name}, importing...')
        df = pd.read_excel(XLSX, sheet_name='Sheet3', header=None)
        data = df.iloc[6:].copy()
        data.columns = ['date', 'pan_nav', 'osefx']
        data = data.dropna(subset=['date', 'pan_nav'])
        data['date'] = pd.to_datetime(data['date'])
        data = data[data['pan_nav'].apply(lambda x: isinstance(x, (int, float)))]
        data = data[data['pan_nav'] > 0]
        data = data.sort_values('date')
        since = data[data['date'] >= '2021-12-20'].copy()
        existing = []
        existing_dates = set()
        for _, row in since.iterrows():
            d = row['date'].strftime('%Y-%m-%d')
            existing.append({
                'd': d,
                'n': round(float(row['pan_nav']), 4),
                'o': round(float(row['osefx']), 4) if pd.notna(row['osefx']) else None
            })
            existing_dates.add(d)
        print(f'xlsx import: {len(existing)} rows, last: {existing[-1]["d"]}')

    fetch_from = get_last_date(existing)
    print(f'Fetching NAV from {fetch_from}...')

    # Try Morningstar first
    new_rows = fetch_morningstar(fetch_from)

    # Fallback to Yahoo Finance
    if not new_rows:
        print('Trying Yahoo Finance fallback...')
        for ticker in YAHOO_TICKERS:
            print(f'  Trying {ticker}...')
            new_rows = fetch_yahoo(ticker, fetch_from)
            if new_rows:
                print(f'  Success with {ticker}: {len(new_rows)} rows')
                break

    if not new_rows:
        print('No new NAV data found from any source. Saving existing.')
        save(existing)
        return

    # Fetch OSEFX for same period
    osefx_map = fetch_osefx(fetch_from)

    # Append new rows
    added = 0
    for row in new_rows:
        if row['date'] not in existing_dates:
            existing.append({
                'd': row['date'],
                'n': row['nav'],
                'o': osefx_map.get(row['date'])
            })
            existing_dates.add(row['date'])
            added += 1

    save(existing)
    last = existing[-1]
    print(f'Added {added} new rows. Total: {len(existing)}. Latest: {last["d"]}, NAV: {last["n"]}')

if __name__ == '__main__':
    main()
