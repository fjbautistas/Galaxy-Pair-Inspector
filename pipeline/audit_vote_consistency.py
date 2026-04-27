"""
audit_vote_consistency.py — Audita votos Supabase contra el set visible actual.

Cruza:
  - clasificaciones
  - partitions
  - PAIRS_CATALOG / GROUPS_CATALOG
  - data/supplementary_calib_ids.json

Genera:
  outputs/audit/vote_consistency_summary.csv
  outputs/audit/vote_consistency_rows.csv
  outputs/audit/visible_assignments.csv

Notas:
  - id_par es el identificador operativo actual.
  - pair_uid se deriva de (id1, id2) ordenados y sirve como llave estable si
    cambia el orden del archivo origen o se regenera id_par.
"""

from __future__ import annotations

import json
import sys
import urllib.parse
import urllib.request as urlreq
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / 'outputs' / 'audit'
GROUP_OFFSET = 10_000_000
RP_MAX_KPC = 50.0
RP_V1_KPC = 20.0
CALIB_PAIRS = 120
CALIB_GROUPS = 80
BLOCK_SIZE = 1000
GROUP_BLOCK_SIZE = 100
MAX_GROUP_MEMBERS_MOBILE = 10
GROUP_Z_MIN = 0.01


def _load_env(path: Path = ROOT / '.env') -> dict[str, str]:
    env: dict[str, str] = {}
    try:
      with open(path, encoding='utf-8') as f:
          for line in f:
              line = line.strip()
              if line and not line.startswith('#') and '=' in line:
                  k, v = line.split('=', 1)
                  env[k.strip()] = v.strip()
    except FileNotFoundError:
      pass
    return env


_ENV = _load_env()
SUPABASE_URL = _ENV.get('SUPABASE_URL', '').rstrip('/')
SERVICE_ROLE_KEY = _ENV.get('SUPABASE_SERVICE_ROLE_KEY', '')
PAIRS_CATALOG = Path(_ENV.get('PAIRS_CATALOG', ROOT / 'data' / 'DESI_v3_pairs.parquet'))
GROUPS_CATALOG = Path(_ENV.get('GROUPS_CATALOG', ROOT / 'data' / 'DESI_v3_groups.parquet'))
SUPP_CALIB_PATH = ROOT / 'data' / 'supplementary_calib_ids.json'


@dataclass
class VisibleSet:
    visible_ids: set[int]
    pair_ids: set[int]
    group_ids_supabase: set[int]


def _headers() -> dict[str, str]:
    return {
        'apikey': SERVICE_ROLE_KEY,
        'Authorization': f'Bearer {SERVICE_ROLE_KEY}',
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    }


def _fetch_table(table: str, order: str | None = None) -> list[dict]:
    rows: list[dict] = []
    limit = 1000
    offset = 0
    while True:
        params = {'select': '*', 'limit': str(limit), 'offset': str(offset)}
        if order:
            params['order'] = order
        url = f'{SUPABASE_URL}/rest/v1/{table}?{urllib.parse.urlencode(params)}'
        req = urlreq.Request(url, headers=_headers())
        with urlreq.urlopen(req, timeout=30) as resp:
            chunk = json.loads(resp.read().decode('utf-8'))
        rows.extend(chunk)
        if len(chunk) < limit:
            break
        offset += limit
    return rows


def _pair_uid(id1: int, id2: int) -> str:
    a, b = sorted((int(id1), int(id2)))
    return f'{a}:{b}'


def load_pairs() -> tuple[pd.DataFrame, int]:
    df = pd.read_parquet(PAIRS_CATALOG)
    rp_col = next((c for c in ('rp_kpc', 'rp_phys_kpc', 'rp') if c in df.columns), None)
    if rp_col is None:
        raise RuntimeError('El catálogo de pares no tiene columna rp_kpc/rp_phys_kpc/rp.')

    df = df[df[rp_col] < RP_MAX_KPC].reset_index(drop=True)
    mask_v1 = df[rp_col] < RP_V1_KPC
    df = pd.concat([df[mask_v1], df[~mask_v1]], ignore_index=True)
    df['pair_uid'] = [_pair_uid(a, b) for a, b in zip(df['id1'], df['id2'])]
    return df, int(mask_v1.sum())


def load_groups() -> list[int]:
    if not GROUPS_CATALOG.exists():
        return []
    df = pd.read_parquet(GROUPS_CATALOG)
    groups: list[int] = []
    for gid, edges in df.groupby('fof_component_id'):
        half1 = edges[['id1', 'ra1', 'dec1', 'z1']].rename(
            columns={'id1': 'id', 'ra1': 'ra', 'dec1': 'dec', 'z1': 'z'})
        half2 = edges[['id2', 'ra2', 'dec2', 'z2']].rename(
            columns={'id2': 'id', 'ra2': 'ra', 'dec2': 'dec', 'z2': 'z'})
        members = pd.concat([half1, half2]).drop_duplicates('id')
        z_c = float(members['z'].mean())
        if z_c <= GROUP_Z_MIN:
            continue
        groups.append(int(gid))
    return groups


def load_supp_calib_ids() -> list[int]:
    if not SUPP_CALIB_PATH.exists():
        return []
    with open(SUPP_CALIB_PATH, encoding='utf-8') as f:
        data = json.load(f)
    return [int(x) for x in data.get('id_par', [])]


def _int_or_none(value) -> int | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    return int(value)


def build_visible_set(partition: dict, pairs: pd.DataFrame, n_v1: int,
                      group_ids: list[int], supp_ids: list[int]) -> VisibleSet:
    pair_ids: set[int] = set(int(x) for x in pairs.iloc[:CALIB_PAIRS]['id_par'])
    pair_ids.update(supp_ids)

    ws = _int_or_none(partition.get('work_start')) or CALIB_PAIRS
    we = _int_or_none(partition.get('work_end')) or ws
    ws2 = _int_or_none(partition.get('work_start_v2'))
    we2 = _int_or_none(partition.get('work_end_v2'))

    if ws2 is None and we2 is None and (we - ws) <= BLOCK_SIZE:
        pair_ids.update(int(x) for x in pairs.iloc[ws:we]['id_par'])
    else:
        q_v1 = BLOCK_SIZE // 2
        q_v2 = BLOCK_SIZE - q_v1
        v1_start = max(CALIB_PAIRS, min(ws, n_v1))
        v1_end = max(v1_start, min(we, n_v1))
        pair_ids.update(int(x) for x in pairs.iloc[v1_start:v1_end].head(q_v1)['id_par'])
        if ws2 is not None and we2 is not None:
            pair_ids.update(int(x) for x in pairs.iloc[ws2:we2].head(q_v2)['id_par'])
        elif ws >= n_v1:
            pair_ids.update(int(x) for x in pairs.iloc[ws:we].head(q_v2)['id_par'])

    visible_group_ids = set(group_ids[:CALIB_GROUPS])
    gs = _int_or_none(partition.get('group_work_start'))
    ge = _int_or_none(partition.get('group_work_end'))
    if gs is not None and ge is not None:
        visible_group_ids.update(group_ids[gs:ge])

    group_supabase_ids = {gid + GROUP_OFFSET for gid in visible_group_ids}
    visible_ids = set(pair_ids) | group_supabase_ids
    return VisibleSet(visible_ids=visible_ids, pair_ids=pair_ids, group_ids_supabase=group_supabase_ids)


def main() -> None:
    if not SUPABASE_URL or not SERVICE_ROLE_KEY:
        sys.exit('ERROR: falta SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY en .env')
    if not PAIRS_CATALOG.exists():
        sys.exit(f'ERROR: no existe PAIRS_CATALOG: {PAIRS_CATALOG}')

    print('Cargando catálogos locales...')
    pairs, n_v1 = load_pairs()
    group_ids = load_groups()
    supp_ids = load_supp_calib_ids()
    pair_id_to_uid = dict(zip(pairs['id_par'].astype(int), pairs['pair_uid']))
    valid_pair_ids = set(pair_id_to_uid)
    valid_group_supabase_ids = {gid + GROUP_OFFSET for gid in group_ids}

    print('Descargando Supabase...')
    partitions = _fetch_table('partitions', order='device_id.asc')
    votes = _fetch_table('clasificaciones', order='device_id.asc')
    partitions_by_device = {str(p['device_id']): p for p in partitions}

    visible_by_device = {
        dev: build_visible_set(p, pairs, n_v1, group_ids, supp_ids)
        for dev, p in partitions_by_device.items()
    }

    visible_rows = []
    for dev, vs in visible_by_device.items():
        for id_par in sorted(vs.pair_ids):
            visible_rows.append({
                'device_id': dev,
                'id_par': id_par,
                'item_type': 'pair',
                'pair_uid': pair_id_to_uid.get(id_par, ''),
            })
        for id_par in sorted(vs.group_ids_supabase):
            visible_rows.append({
                'device_id': dev,
                'id_par': id_par,
                'item_type': 'group',
                'pair_uid': '',
            })

    valid_classes = {'FP', 'Pair', 'PM', 'GROUP', 'PP'}
    audit_rows = []
    for row in votes:
        dev = str(row.get('device_id', ''))
        id_par = int(row.get('id_par'))
        classification = str(row.get('classification', ''))
        item_type = 'group' if id_par >= GROUP_OFFSET else 'pair'
        in_catalog = id_par in (valid_group_supabase_ids if item_type == 'group' else valid_pair_ids)
        has_partition = dev in partitions_by_device
        visible = has_partition and id_par in visible_by_device[dev].visible_ids
        invalid_class = classification not in valid_classes
        if invalid_class:
            status = 'invalid_class'
        elif not has_partition:
            status = 'unknown_device'
        elif not in_catalog:
            status = 'missing_from_current_catalog'
        elif visible:
            status = 'visible_current_set'
        else:
            status = 'historical_outside_current_set'

        audit_rows.append({
            'device_id': dev,
            'id_par': id_par,
            'item_type': item_type,
            'pair_uid': pair_id_to_uid.get(id_par, ''),
            'classification': classification,
            'status': status,
            'visible_current_set': visible,
            'in_current_catalog': in_catalog,
            'exported_at': row.get('exported_at'),
            'created_at': row.get('created_at'),
        })

    audit = pd.DataFrame(audit_rows)
    if audit.empty:
        summary = pd.DataFrame()
    else:
        summary = (
            audit.groupby('device_id')
            .agg(
                total_votes=('id_par', 'count'),
                visible_votes=('visible_current_set', 'sum'),
                historical_or_invalid_votes=('status', lambda s: int((s != 'visible_current_set').sum())),
                pair_votes=('item_type', lambda s: int((s == 'pair').sum())),
                group_votes=('item_type', lambda s: int((s == 'group').sum())),
                invalid_class_votes=('status', lambda s: int((s == 'invalid_class').sum())),
                unknown_device_votes=('status', lambda s: int((s == 'unknown_device').sum())),
                missing_catalog_votes=('status', lambda s: int((s == 'missing_from_current_catalog').sum())),
            )
            .reset_index()
            .sort_values(['visible_votes', 'total_votes'], ascending=False)
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    audit_file = OUTPUT_DIR / 'vote_consistency_rows.csv'
    summary_file = OUTPUT_DIR / 'vote_consistency_summary.csv'
    visible_file = OUTPUT_DIR / 'visible_assignments.csv'
    audit.to_csv(audit_file, index=False)
    summary.to_csv(summary_file, index=False)
    pd.DataFrame(visible_rows).to_csv(visible_file, index=False)

    print('\nResumen:')
    if summary.empty:
        print('  No hay votos para auditar.')
    else:
        print(summary.to_string(index=False))

    print('\nArchivos:')
    print(f'  {summary_file}')
    print(f'  {audit_file}')
    print(f'  {visible_file}')
    print('\nNota id_total:')
    print('  Este catálogo no trae id_total. El reporte incluye pair_uid = min(id1,id2):max(id1,id2)')
    print('  como llave estable para migrar/reconocer pares si cambia el archivo origen.')
    print(f'  Ejecutado: {datetime.now().isoformat(timespec="seconds")}')


if __name__ == '__main__':
    main()
