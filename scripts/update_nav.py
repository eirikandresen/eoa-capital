"""
update_nav.py
Fetches latest NAV for Pareto Aksje Norge A from Yahoo Finance
and appends new data points to nav_data.json.

Runs automatically via GitHub Actions every weekday at 20:00 Oslo time.
Also converts PAN_A_-_daglig_nav.xlsx if present (for bulk historical import).
"""
import json
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

ROOT = Path(__file__).parent.parent
OUT = ROOT / 'nav_data.json'
XLSX = ROOT / 'PAN_A_-_daglig_nav.xlsx'

# Yahoo Finance tickers to try for Pareto Aksje Norge A
TICKERS = [
    '0P0001BNTE.F',   # Morningstar/Yahoo fund ticker
    'POAKTNY.OL',
    '0P00001F9P.IR',
]

def load_existing():
    if OUT.exists():
        with open(OUT) as f:
            data = json.load(f)
        return data
    return []

def get_last_date(data):
    if not data:
        return None
    return data[-1]['d']

def fetch_yahoo(ticker, start_date):
    """Fetch daily prices from Yahoo Finance."""
    try:
        start = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end = int((datetime.now() + timedelta(days=1)).timestamp())
        url = f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&period1={start}&period2={end}'
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=10)
        if not r.ok:
            return None
        d = r.json()
        result = d.get('chart', {}).get('result', [])
        if not result:
            return None
        timestamps = result[0].get('timestamp', [])
        closes = result[0].get('indicators', {}).get('quote', [{}])[0].get('close', [])
        if not timestamps or not closes:
            return None
        rows = []
        for t, c in zip(timestamps, closes):
            if c is None:
                continue
            date_str = datetime.utcfromtimestamp(t).strftime('%Y-%m-%d')
            rows.append({'date': date_str, 'nav': round(float(c), 4)})
        return rows
    except Exception as e:
        print(f'Yahoo fetch failed for {ticker}: {e}')
        return None

def fetch_osefx(start_date):
    """Fetch OSEFX from Yahoo Finance."""
    try:
        start = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end = int((datetime.now() + timedelta(days=1)).timestamp())
        url = f'https://query1.finance.yahoo.com/v8/finance/chart/^OSEAX?interval=1d&period1={start}&period2={end}'
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=10)
        if not r.ok:
            return {}
        d = r.json()
        result = d.get('chart', {}).get('result', [])
        if not result:
            return {}
        timestamps = result[0].get('timestamp', [])
        closes = result[0].get('indicators', {}).get('quote', [{}])[0].get('close', [])
        osefx = {}
        for t, c in zip(timestamps, closes):
            if c is None:
                continue
            date_str = datetime.utcfromtimestamp(t).strftime('%Y-%m-%d')
            osefx[date_str] = round(float(c), 4)
        return osefx
    except Exception as e:
        print(f'OSEFX fetch failed: {e}')
        return {}

def main():
    # Load existing data
    existing = load_existing()
    existing_dates = {r['d'] for r in existing}
    last_date = get_last_date(existing)

    # If xlsx exists, use it as base (bulk historical)
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
        last_date = existing[-1]['d'] if existing else '2021-12-20'
        print(f'Loaded {len(existing)} rows from xlsx, last date: {last_date}')

    # Fetch new NAV data from Yahoo Finance
    fetch_from = last_date if last_date else '2021-12-20'
    print(f'Fetching new NAV data from {fetch_from}...')

    new_nav_rows = None
    for ticker in TICKERS:
        print(f'Trying ticker {ticker}...')
        rows = fetch_yahoo(ticker, fetch_from)
        if rows and len(rows) > 0:
            # Validate: NAV should be in reasonable range for this fund (>1000)
            valid = [r for r in rows if r['nav'] > 1000]
            if valid:
                new_nav_rows = valid
                print(f'Success with {ticker}: {len(valid)} rows')
                break

    if not new_nav_rows:
        print('Could not fetch new NAV data from Yahoo Finance. Keeping existing data.')
        # Still save existing (e.g. after xlsx import)
        with open(OUT, 'w') as f:
            json.dump(existing, f, separators=(',', ':'))
        print(f'Saved {len(existing)} rows to {OUT.name}')
        return

    # Fetch OSEFX for same period
    osefx_map = fetch_osefx(fetch_from)

    # Append new rows not already in existing
    added = 0
    for row in new_nav_rows:
        if row['date'] not in existing_dates:
            existing.append({
                'd': row['date'],
                'n': row['nav'],
                'o': osefx_map.get(row['date'])
            })
            existing_dates.add(row['date'])
            added += 1

    # Sort by date
    existing.sort(key=lambda r: r['d'])

    # Save
    with open(OUT, 'w') as f:
        json.dump(existing, f, separators=(',', ':'))

    print(f'Added {added} new rows. Total: {len(existing)}. Latest: {existing[-1]["d"]}, NAV: {existing[-1]["n"]}')

if __name__ == '__main__':
    main()
