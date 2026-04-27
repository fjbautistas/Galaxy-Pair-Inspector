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
    BLOCK_SIZE       = 1000  pares de trabajo asignados por dispositivo
    GROUP_BLOCK_SIZE = 100   grupos de trabajo asignados por dispositivo
    catalog_len      = leído dinámicamente desde la ruta PAIRS_CATALOG en .env
"""

import argparse
import json
import random
import sys
import urllib.request as urlreq
from pathlib import Path
import pandas as pd
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
BLOCK_SIZE         = 1_000
GROUP_BLOCK_SIZE   = 100     # grupos de trabajo por dispositivo
RP_V1_KPC          = 20.0
WORK_V1_FRACTION   = 0.50

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
                      group_work_start: int, group_work_end: int,
                      work_start_v2=None,
                      work_end_v2=None) -> None:
    url  = f'{SUPABASE_URL}/rest/v1/partitions'
    row = {
        'device_id':        device_id,
        'calib_seed':       calib_seed,
        'work_start':       work_start,
        'work_end':         work_end,
        'group_work_start': group_work_start,
        'group_work_end':   group_work_end,
        'calib_v':          1,
    }
    if work_start_v2 is not None and work_end_v2 is not None:
        row['work_start_v2'] = work_start_v2
        row['work_end_v2']   = work_end_v2
    data = json.dumps([row]).encode('utf-8')
    req = urlreq.Request(url, data=data, headers={
        **_headers(),
        'Prefer': 'return=minimal',
    }, method='POST')
    with urlreq.urlopen(req, timeout=15):
        pass


def _first_free_interval(occupied: list[tuple[int, int]], start: int, stop: int, size: int) -> int:
    """Return the first aligned free interval start in [start, stop)."""
    cur = start
    intervals = sorted((max(start, a), min(stop, b)) for a, b in occupied if b > start and a < stop)
    while cur + size <= stop:
        end = cur + size
        overlap = next(((a, b) for a, b in intervals if cur < b and end > a), None)
        if overlap is None:
            return cur
        cur = overlap[1]
    raise RuntimeError(f'No queda un intervalo libre de {size} items entre {start} y {stop}.')


# ── Lógica principal ──────────────────────────────────────────────────────────
def register(device_id: str) -> dict:
    """
    Devuelve la configuración del dispositivo (existente o recién creada).
    """
    catalog_len = pq.read_metadata(CATALOG_PATH).num_rows
    rp = pd.read_parquet(CATALOG_PATH, columns=['rp_kpc'])['rp_kpc']
    n_v1 = int((rp < RP_V1_KPC).sum())
    q_v1 = round(BLOCK_SIZE * WORK_V1_FRACTION)
    q_v2 = BLOCK_SIZE - q_v1
    partitions = _get_all_partitions()

    # ¿Ya está registrado?
    existing = next((p for p in partitions if p['device_id'] == device_id), None)
    if existing:
        return {'status': 'existing', 'partition': existing}

    # Calcular primeros huecos disponibles por zona, reutilizando espacios de
    # particiones inactivas eliminadas en Supabase.
    occupied_v1 = [
        (int(p.get('work_start', CALIB_SIZE)), min(int(p.get('work_end', CALIB_SIZE)), n_v1))
        for p in partitions
        if int(p.get('work_start', 0)) < n_v1
    ]
    occupied_v2 = [
        (int(p.get('work_start_v2')), int(p.get('work_end_v2')))
        for p in partitions
        if p.get('work_start_v2') is not None and p.get('work_end_v2') is not None
    ] + [
        (int(p.get('work_start')), int(p.get('work_end')))
        for p in partitions
        if int(p.get('work_start', 0)) >= n_v1
    ]

    work_start    = _first_free_interval(occupied_v1, CALIB_SIZE, n_v1, q_v1)
    work_end      = work_start + q_v1
    work_start_v2 = _first_free_interval(occupied_v2, n_v1, catalog_len, q_v2)
    work_end_v2   = work_start_v2 + q_v2

    # Calcular siguiente bloque de grupos disponible
    if partitions:
        max_group_end = max(
            [int(p.get('group_work_end')) for p in partitions if p.get('group_work_end') is not None]
            or [CALIB_GROUPS]
        )
    else:
        max_group_end = CALIB_GROUPS

    group_work_start = max_group_end
    group_work_end   = group_work_start + GROUP_BLOCK_SIZE

    calib_seed = random.randint(0, 999_999)
    _insert_partition(device_id, calib_seed, work_start, work_end,
                      group_work_start, group_work_end,
                      work_start_v2, work_end_v2)

    partition = {
        'device_id':        device_id,
        'calib_seed':       calib_seed,
        'work_start':       work_start,
        'work_end':         work_end,
        'group_work_start': group_work_start,
        'group_work_end':   group_work_end,
        'calib_v':          1,
        'work_start_v2':    work_start_v2,
        'work_end_v2':      work_end_v2,
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
          f'= 200 ítems base (+150 pares suplementarios en mobile), seed={p["calib_seed"]}')
    print(f'  Bloque pares      : índices {p["work_start"]}–{p["work_end"] - 1} '
          f'({n_pairs:,} pares rp<20)')
    if p.get('work_start_v2') is not None and p.get('work_end_v2') is not None:
        print(f'  Bloque pares v2   : índices {p["work_start_v2"]}–{p["work_end_v2"] - 1} '
              f'({p["work_end_v2"] - p["work_start_v2"]:,} pares rp≥20)')
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
