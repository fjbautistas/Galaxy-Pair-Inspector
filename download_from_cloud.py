"""
download_from_cloud.py — Descarga todas las clasificaciones de Supabase
y las consolida en un único JSON compatible con import_from_mobile.py.

Uso:
    python download_from_cloud.py

Genera: outputs/catalogs/cloud_classifications_YYYY-MM-DD.json

El archivo resultante puede importarse directamente:
    python import_from_mobile.py outputs/catalogs/cloud_classifications_YYYY-MM-DD.json

Requiere: .env en el mismo directorio con SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY
"""

import json
import sys
import urllib.request as urlreq
from datetime import datetime
from pathlib import Path

# ── Leer .env ─────────────────────────────────────────────────────────────────
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

_env = _load_env()
SUPABASE_URL      = _env.get('SUPABASE_URL', '').rstrip('/')
SERVICE_ROLE_KEY  = _env.get('SUPABASE_SERVICE_ROLE_KEY', '')
OUTPUT_DIR        = Path('outputs/catalogs')
# ──────────────────────────────────────────────────────────────────────────────


def fetch_all_rows() -> list:
    """Descarga todas las filas de la tabla 'clasificaciones' vía REST."""
    url = f'{SUPABASE_URL}/rest/v1/clasificaciones?select=*&order=created_at.asc'
    req = urlreq.Request(url, headers={
        'apikey':        SERVICE_ROLE_KEY,
        'Authorization': f'Bearer {SERVICE_ROLE_KEY}',
        'Accept':        'application/json',
    })
    with urlreq.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode('utf-8'))


def consolidate(rows: list) -> dict:
    """
    Convierte las filas de Supabase al formato de import_from_mobile.py.
    Por diseño ya hay un único registro por (device_id, id_par) en la tabla
    (UNIQUE constraint + upsert), pero en caso de duplicados nos quedamos
    con el más reciente.
    """
    latest: dict = {}
    for row in rows:
        key = (str(row['device_id']), int(row['id_par']))
        ts  = str(row.get('created_at', ''))
        if key not in latest or ts > latest[key]['created_at']:
            latest[key] = row

    entries = [
        {'id_par': int(r['id_par']), 'classification': r['classification']}
        for r in latest.values()
    ]
    devices = sorted({r['device_id'] for r in latest.values()})

    return {
        'exported_at':      datetime.now().isoformat(),
        'source':           'supabase_download',
        'device_ids':       devices,
        'total_classified': len(entries),
        'classifications':  entries,
    }


def main():
    if not SUPABASE_URL or not SERVICE_ROLE_KEY:
        print('ERROR: falta SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY en .env')
        sys.exit(1)

    print('Descargando clasificaciones de Supabase...')
    try:
        rows = fetch_all_rows()
    except Exception as exc:
        print(f'ERROR al conectar:\n  {exc}')
        sys.exit(1)

    print(f'  {len(rows):,} filas recibidas')
    payload = consolidate(rows)
    print(f'  {payload["total_classified"]:,} clasificaciones únicas')
    print(f'  Dispositivos: {", ".join(payload["device_ids"]) or "—"}')

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_file = OUTPUT_DIR / f'cloud_classifications_{datetime.now().strftime("%Y-%m-%d")}.json'
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f'\n✓  Guardado: {out_file}')
    print(f'\nPara importar al sistema principal:')
    print(f'  python import_from_mobile.py {out_file}')


if __name__ == '__main__':
    main()
