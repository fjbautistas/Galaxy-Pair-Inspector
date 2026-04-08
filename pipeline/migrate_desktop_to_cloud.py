"""
migrate_desktop_to_cloud.py — Migrates existing desktop classifications to Supabase.

Reads outputs/catalogs/progress.json and uploads all classifications
with device_id = "DESKTOP". Safe to run multiple times (upsert).

Usage:
    python pipeline/migrate_desktop_to_cloud.py
"""

import json
import sys
import urllib.request as urlreq
from datetime import datetime
from pathlib import Path

# ── Read .env ──────────────────────────────────────────────────────────────────
def _load_env(path='.env'):
    env = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    env[k.strip()] = v.strip()
    except FileNotFoundError:
        pass
    return env

_env            = _load_env()
SUPABASE_URL    = _env.get('SUPABASE_URL', '').rstrip('/')
SUPABASE_ANON   = _env.get('SUPABASE_ANON_KEY', '')
PROGRESS_FILE   = Path('outputs/catalogs/progress.json')
DEVICE_ID       = 'DESKTOP'

LABEL_MAP = {
    'false_positives':   'FP',
    'confirmed_pairs':   'Pair',
    'possible_mergers':  'PM',
}
# ──────────────────────────────────────────────────────────────────────────────


def load_classifications() -> list:
    """Reads progress.json and returns a flat list of {id_par, classification}."""
    if not PROGRESS_FILE.exists():
        print(f'ERROR: {PROGRESS_FILE} not found.')
        sys.exit(1)

    with open(PROGRESS_FILE) as f:
        progress = json.load(f)

    rows = []
    for key, label in LABEL_MAP.items():
        for entry in progress.get(key, []):
            id_par = entry.get('id_par')
            if id_par is None:
                # Fallback: build from id1/id2 if id_par missing
                id_par = int(str(entry.get('id1', 0)) + str(entry.get('id2', 0)))
            rows.append({
                'device_id':      DEVICE_ID,
                'id_par':         int(id_par),
                'classification': label,
                'exported_at':    datetime.now().isoformat(),
            })
    return rows


def _post(url, headers, rows):
    data = json.dumps(rows).encode('utf-8')
    req  = urlreq.Request(url, data=data, headers=headers, method='POST')
    try:
        with urlreq.urlopen(req, timeout=30) as resp:
            return resp.status, None
    except urlreq.HTTPError as e:
        return e.code, e.read().decode('utf-8')


def upsert_rows(rows: list, batch_size: int = 50):
    """Sends rows to Supabase in batches. Falls back to row-by-row on error."""
    url = (f'{SUPABASE_URL}/rest/v1/clasificaciones'
           f'?on_conflict=device_id,id_par')
    headers = {
        'apikey':        SUPABASE_ANON,
        'Authorization': f'Bearer {SUPABASE_ANON}',
        'Content-Type':  'application/json',
        'Prefer':        'resolution=merge-duplicates,return=minimal',
    }
    skipped = []
    for i in range(0, len(rows), batch_size):
        batch  = rows[i:i + batch_size]
        status, err = _post(url, headers, batch)
        if err:
            print(f'  Batch {i // batch_size + 1}: ERROR {status} — retrying row by row…')
            for row in batch:
                s, e = _post(url, headers, [row])
                if e:
                    print(f'    ✗ id_par={row["id_par"]} ({row["classification"]}) → {s}: {e[:80]}')
                    skipped.append(row)
                else:
                    print(f'    ✓ id_par={row["id_par"]}')
        else:
            print(f'  Batch {i // batch_size + 1}: {len(batch)} rows → HTTP {status}')
    if skipped:
        print(f'\n{len(skipped)} rows could not be uploaded:')
        for r in skipped:
            print(f'  {r}')
    return skipped


def main():
    if not SUPABASE_URL or not SUPABASE_ANON:
        print('ERROR: SUPABASE_URL or SUPABASE_ANON_KEY missing in .env')
        sys.exit(1)

    rows = load_classifications()
    if not rows:
        print('No classifications found in progress.json.')
        sys.exit(0)

    counts = {label: sum(1 for r in rows if r['classification'] == label)
              for label in ('FP', 'Pair', 'PM')}
    print(f'Found {len(rows)} desktop classifications:')
    print(f'  FP: {counts["FP"]}  |  Pair: {counts["Pair"]}  |  PM: {counts["PM"]}')
    print(f'Uploading to Supabase as device_id="{DEVICE_ID}"...')

    skipped = upsert_rows(rows)
    if skipped:
        print(f'\n⚠  Completed with {len(skipped)} skipped rows.')
    else:
        print('\n✓  All rows uploaded successfully.')


if __name__ == '__main__':
    main()
