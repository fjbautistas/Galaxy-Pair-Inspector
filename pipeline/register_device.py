"""
register_device.py — Registra un nuevo dispositivo clasificador en Supabase
y le asigna automáticamente un bloque de trabajo sin solapamiento.

Uso:
    python pipeline/register_device.py --device NOMBRE_DISPOSITIVO

Ejemplos:
    python pipeline/register_device.py --device IPHONE_FRANK
    python pipeline/register_device.py --device LAPTOP_USUARIO2

Salida:
    Imprime la configuración asignada (calib_seed, work_start, work_end).
    Si el dispositivo ya existe, muestra su configuración actual sin modificarla.

Requiere: .env en la raíz del proyecto con SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY.

Constantes del catálogo:
    CALIB_PAIRS      = 120   primeros N pares del catálogo, pool de calibración compartido
    CALIB_GROUPS     = 80    primeros N grupos del catálogo, pool de calibración compartido
    BLOCK_SIZE       = 3000  pares de trabajo asignados por dispositivo
    GROUP_BLOCK_SIZE = 500   grupos de trabajo asignados por dispositivo
    catalog_len      = leído dinámicamente desde la ruta PAIRS_CATALOG en .env
"""

import argparse
import json
import random
import sys
import urllib.request as urlreq
from pathlib import Path
import pyarrow.parquet as pq

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

_env             = _load_env()
SUPABASE_URL     = _env.get('SUPABASE_URL', '').rstrip('/')
SERVICE_ROLE_KEY = _env.get('SUPABASE_SERVICE_ROLE_KEY', '')
CATALOG_PATH     = _env.get('PAIRS_CATALOG', '')

# ── Constantes ────────────────────────────────────────────────────────────────
CALIB_PAIRS        = 120     # pares de calibración compartidos por todos los usuarios
CALIB_GROUPS       = 80      # grupos de calibración compartidos por todos los usuarios
CALIB_SIZE         = CALIB_PAIRS   # alias: work_start del primer dispositivo
BLOCK_SIZE         = 3_000
GROUP_BLOCK_SIZE   = 500     # grupos de trabajo por dispositivo

# ── Helpers REST ──────────────────────────────────────────────────────────────
def _headers():
    return {
        'apikey':        SERVICE_ROLE_KEY,
        'Authorization': f'Bearer {SERVICE_ROLE_KEY}',
        'Content-Type':  'application/json',
        'Accept':        'application/json',
    }


def _get_all_partitions() -> list:
    url = f'{SUPABASE_URL}/rest/v1/partitions?select=*&order=work_end.desc'
    req = urlreq.Request(url, headers=_headers())
    with urlreq.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode('utf-8'))


def _insert_partition(device_id: str, calib_seed: int,
                      work_start: int, work_end: int,
                      group_work_start: int, group_work_end: int) -> None:
    url  = f'{SUPABASE_URL}/rest/v1/partitions'
    data = json.dumps([{
        'device_id':        device_id,
        'calib_seed':       calib_seed,
        'work_start':       work_start,
        'work_end':         work_end,
        'group_work_start': group_work_start,
        'group_work_end':   group_work_end,
    }]).encode('utf-8')
    req = urlreq.Request(url, data=data, headers={
        **_headers(),
        'Prefer': 'return=minimal',
    }, method='POST')
    with urlreq.urlopen(req, timeout=15):
        pass


# ── Lógica principal ──────────────────────────────────────────────────────────
def register(device_id: str) -> dict:
    """
    Devuelve la configuración del dispositivo (existente o recién creada).
    """
    catalog_len = pq.read_metadata(CATALOG_PATH).num_rows
    partitions = _get_all_partitions()

    # ¿Ya está registrado?
    existing = next((p for p in partitions if p['device_id'] == device_id), None)
    if existing:
        return {'status': 'existing', 'partition': existing}

    # Calcular siguiente bloque de pares disponible
    if partitions:
        max_end = max(p['work_end'] for p in partitions)
    else:
        max_end = CALIB_SIZE

    work_start = max_end
    work_end   = work_start + BLOCK_SIZE

    if work_start >= catalog_len:
        raise RuntimeError(
            f'Catálogo agotado: todos los {catalog_len:,} pares ya están asignados.'
        )
    if work_end > catalog_len:
        work_end = catalog_len

    # Calcular siguiente bloque de grupos disponible
    if partitions:
        max_group_end = max(p.get('group_work_end', CALIB_GROUPS) for p in partitions)
    else:
        max_group_end = CALIB_GROUPS

    group_work_start = max_group_end
    group_work_end   = group_work_start + GROUP_BLOCK_SIZE

    calib_seed = random.randint(0, 999_999)
    _insert_partition(device_id, calib_seed, work_start, work_end,
                      group_work_start, group_work_end)

    partition = {
        'device_id':        device_id,
        'calib_seed':       calib_seed,
        'work_start':       work_start,
        'work_end':         work_end,
        'group_work_start': group_work_start,
        'group_work_end':   group_work_end,
    }
    return {'status': 'created', 'partition': partition}


def print_summary(result: dict) -> None:
    p        = result['partition']
    status   = 'YA EXISTÍA' if result['status'] == 'existing' else 'REGISTRADO'
    n_pairs  = p['work_end']         - p['work_start']
    n_groups = p.get('group_work_end', '?') if isinstance(p.get('group_work_end'), int) else '?'
    gs       = p.get('group_work_start', '?')
    ge_val   = p.get('group_work_end')
    n_groups = (ge_val - gs) if isinstance(ge_val, int) and isinstance(gs, int) else '?'
    print(f'\n── Dispositivo: {p["device_id"]}  [{status}] ──────────────────')
    print(f'  Pool calibración  : {CALIB_PAIRS} pares (0–{CALIB_PAIRS-1}) '
          f'+ {CALIB_GROUPS} grupos (0–{CALIB_GROUPS-1}) '
          f'= 200 ítems, seed={p["calib_seed"]}')
    print(f'  Bloque pares      : índices {p["work_start"]}–{p["work_end"] - 1} '
          f'({n_pairs:,} pares)')
    print(f'  Bloque grupos     : índices {gs}–{ge_val - 1 if isinstance(ge_val, int) else "?"} '
          f'({n_groups} grupos)')
    if result['status'] == 'existing':
        print(f'  Registrado el     : {p.get("registered_at", "—")}')
    print()


def main():
    parser = argparse.ArgumentParser(description='Registra un dispositivo clasificador.')
    parser.add_argument('--device', required=True,
                        help='Identificador del dispositivo (ej. IPHONE_FRANK)')
    args = parser.parse_args()

    device_id = args.device.strip().upper()

    if not SUPABASE_URL or not SERVICE_ROLE_KEY:
        print('ERROR: falta SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY en .env')
        sys.exit(1)
    if not CATALOG_PATH:
        print('ERROR: falta PAIRS_CATALOG en .env')
        sys.exit(1)
    if not Path(CATALOG_PATH).exists():
        print(f'ERROR: no se encontró el catálogo en {CATALOG_PATH}')
        sys.exit(1)

    try:
        result = register(device_id)
    except Exception as exc:
        print(f'ERROR: {exc}')
        sys.exit(1)

    print_summary(result)


if __name__ == '__main__':
    main()
