"""
fix_osefx.py — v2
Fixes bad OSEFX values. OSEFX.OL max ever ~1800, so anything > 2000 is bad.
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
    if o is not None and (o < 100 or o > 2000):
        print(f'Fixing {row["d"]}: {o} -> None')
        row['o'] = None
        fixed += 1

print(f'Fixed {fixed} bad values out of {len(data)} rows')

data.sort(key=lambda r: r['d'])
with open(OUT, 'w') as f:
    json.dump(data, f, separators=(',', ':'))

print('Saved.')
