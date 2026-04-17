"""
update_nav.py
Fetches latest NAV for Pareto Aksje Norge A + OSEFX benchmark.
Priority NAV:   1) Morningstar NO  2) Yahoo Finance
Priority OSEFX: 1) Yahoo Finance OSEFX.OL  2) Morningstar OSEFX index

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
MS_ID       = 'F0GBR04OMP'
MS_UNIVERSE = 'FONOR$$ALL'

# Morningstar ID for OSEFX
MS_OSEFX_ID       = 'F00000YQEP'
MS_OSEFX_UNIVERSE = 'IXNOR$$ALL'

# Yahoo Finance tickers
YAHOO_NAV_TICKERS = ['0P00001F9P.IR', '0P0001BNTE.F', 'POAKTNY.OL']
YAHOO_OSEFX       = "OSEFX.OL"

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

def get_ms_token():
    try:
        r = requests.get('https://www.morningstar.no', headers=HEADERS, timeout=10)
        for part in r.text.split('"'):
            if len(part) == 16 and part.isalnum():
                return part
    except:
        pass
    return 'dr6pz9spfi'

# ─── Morningstar NAV ─────────────────────────────────────────────────────────

def fetch_morningstar_nav(start_date, token):
    try:
        url = (
            f'https://tools.morningstar.no/api/rest.svc/timeseries_price/{token}'
            f'?currencyId=NOK&idtype=Morningstar&frequency=daily&outputType=JSON'
            f'&startDate={start_date}'
            f'&id={MS_ID}]2]1]{MS_UNIVERSE}'
        )
        r = requests.get(url, headers=HEADERS, timeout=15)
        if not r.ok:
            print(f'Morningstar NAV HTTP {r.status_code}')
            return None
        d = r.json()
        series = (d.get('TimeSeries', {})
                   .get('Security', [{}])[0]
                   .get('HistoryDetail', []))
        if not series:
            print('Morningstar NAV: empty series')
            return None
        rows = []
        for point in series:
            date_str = point.get('EndDate', '')[:10]
            val = point.get('Value')
            if date_str and val is not None:
                nav = round(float(val), 4)
                if nav > 1000:
                    rows.append({'date': date_str, 'nav': nav})
        print(f'Morningstar NAV: got {len(rows)} rows')
        return rows if rows else None
    except Exception as e:
        print(f'Morningstar NAV failed: {e}')
        return None

# ─── Morningstar OSEFX ───────────────────────────────────────────────────────

def fetch_morningstar_osefx(start_date, token):
    try:
        url = (
            f'https://tools.morningstar.no/api/rest.svc/timeseries_price/{token}'
            f'?currencyId=NOK&idtype=Morningstar&frequency=daily&outputType=JSON'
            f'&startDate={start_date}'
            f'&id={MS_OSEFX_ID}]2]1]{MS_OSEFX_UNIVERSE}'
        )
        r = requests.get(url, headers=HEADERS, timeout=15)
        if not r.ok:
            return {}
        d = r.json()
        series = (d.get('TimeSeries', {})
                   .get('Security', [{}])[0]
                   .get('HistoryDetail', []))
        result = {}
        for point in series:
            date_str = point.get('EndDate', '')[:10]
            val = point.get('Value')
            if date_str and val is not None:
                v = round(float(val), 4)
                if 100 < v < 2500:
                    result[date_str] = v
        print(f'Morningstar OSEFX: got {len(result)} rows')
        return result
    except Exception as e:
        print(f'Morningstar OSEFX failed: {e}')
        return {}

# ─── Yahoo Finance NAV fallback ───────────────────────────────────────────────

def fetch_yahoo_nav(ticker, start_date):
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
        print(f'Yahoo NAV {ticker} failed: {e}')
        return None

# ─── Yahoo Finance OSEFX ─────────────────────────────────────────────────────

def fetch_yahoo_osefx(start_date, last_known=None):
    try:
        start = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end   = int((datetime.now() + timedelta(days=2)).timestamp())
        url   = (f'https://query1.finance.yahoo.com/v8/finance/chart/OSEFX.OL'
                 f'?interval=1d&period1={start}&period2={end}')
        r = requests.get(url, headers=HEADERS, timeout=10)
        if not r.ok:
            print(f'Yahoo OSEFX HTTP {r.status_code}')
            return {}
        d = r.json()
        result = d.get('chart', {}).get('result', [])
        if not result:
            return {}
        timestamps = result[0].get('timestamp', [])
        closes = result[0].get('indicators', {}).get('quote', [{}])[0].get('close', [])
        adjcloses = result[0].get('indicators', {}).get('adjclose', [{}])[0].get('adjclose', [])
        if not closes or all(c is None for c in closes):
            closes = adjcloses

        # Dynamic validation: max 30% move from last known value
        # Fallback to absolute range if no last known
        def is_valid(val):
            if val < 100:
                return False
            if last_known and last_known > 0:
                return val < last_known * 1.30 and val > last_known * 0.70
            return val < 5000  # very wide fallback

        osefx = {}
        for t, c in zip(timestamps, closes):
            if c is None:
                continue
            val = round(float(c), 4)
            if is_valid(val):
                date_utc = datetime.utcfromtimestamp(t)
                date_oslo = date_utc + timedelta(hours=1)
                osefx[date_oslo.strftime('%Y-%m-%d')] = val
        print(f'Yahoo OSEFX: got {len(osefx)} rows, dates: {list(osefx.keys())[-3:] if osefx else []}')
        return osefx
    except Exception as e:
        print(f'Yahoo OSEFX failed: {e}')
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
            # Keep old OSEFX from xlsx but rename to 'o' field — will be
            # replaced with OSEFX going forward for new rows
            existing.append({
                'd': d,
                'n': round(float(row['pan_nav']), 4),
                'o': round(float(row['osefx']), 4) if pd.notna(row['osefx']) else None
            })
            existing_dates.add(d)
        print(f'xlsx import: {len(existing)} rows, last: {existing[-1]["d"]}')

    fetch_from = get_last_date(existing)

    # Clean up bad index values using dynamic 30% threshold
    valid_osefx = [r['o'] for r in existing if r.get('o') and r['o'] > 100]
    ref_osefx = valid_osefx[-1] if valid_osefx else 1500
    fixed = 0
    for row in existing:
        o = row.get('o')
        if o is not None and (o < 100 or o > ref_osefx * 1.30 or o < ref_osefx * 0.70):
            print(f'Fixing bad index value {o} on {row["d"]} (ref: {ref_osefx})')
            row['o'] = None
            fixed += 1
            ref_osefx = o if o > 100 else ref_osefx  # don't update ref with bad value
    if fixed:
        print(f'Fixed {fixed} bad index values')

    print(f'Fetching NAV from {fetch_from}...')

    # Get Morningstar token once
    token = get_ms_token()
    print(f'Morningstar token: {token}')

    # ── Fetch NAV ──────────────────────────────────────
    new_rows = fetch_morningstar_nav(fetch_from, token)
    if not new_rows:
        print('Trying Yahoo Finance fallback for NAV...')
        for ticker in YAHOO_NAV_TICKERS:
            print(f'  Trying {ticker}...')
            new_rows = fetch_yahoo_nav(ticker, fetch_from)
            if new_rows:
                print(f'  Success: {len(new_rows)} rows')
                break

    if not new_rows:
        print('No new NAV data. Saving existing.')
        save(existing)
        return

    # ── Fetch OSEFX ────────────────────────────────────
    print(f'Fetching OSEFX from {fetch_from}...')
    # Find last known valid OSEFX value for dynamic validation
    last_osefx = next((r['o'] for r in reversed(existing) if r.get('o') and r['o'] > 100), None)
    print(f'Last known OSEFX: {last_osefx}')
    osefx_map = fetch_yahoo_osefx(fetch_from, last_known=last_osefx)
    if not osefx_map:
        print('Yahoo OSEFX failed, trying Morningstar...')
        osefx_map = fetch_morningstar_osefx(fetch_from, token)

    # ── Append new rows ────────────────────────────────
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

    # Also fill missing OSEFX for existing rows where o is None
    filled = 0
    for row in existing:
        if row.get('o') is None and row['d'] in osefx_map:
            row['o'] = osefx_map[row['d']]
            filled += 1
    if filled:
        print(f'Filled {filled} missing OSEFX values')

    save(existing)
    last = existing[-1]
    print(f'Added {added} new rows. Total: {len(existing)}. Latest: {last["d"]}, NAV: {last["n"]}, OSEBX: {last.get("o")}')

if __name__ == '__main__':
    main()
