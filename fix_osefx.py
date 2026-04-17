"""
fix_osefx.py
One-time script to fix bad OSEFX values in nav_data.json.
Run via GitHub Actions manually once, then delete.
"""
import json
from pathlib import Path

ROOT = Path(__file__).parent
OUT  = ROOT / 'nav_data.json'

with open(OUT) as f:
    data = json.load(f)

fixed = 0
for row in data:
    o = row.get('o')
    if o is not None and (o < 100 or o > 2500):
        print(f'Fixing {row["d"]}: {o} -> None')
        row['o'] = None
        fixed += 1

print(f'Fixed {fixed} bad values out of {len(data)} rows')

data.sort(key=lambda r: r['d'])
with open(OUT, 'w') as f:
    json.dump(data, f, separators=(',', ':'))

print('Saved.')
