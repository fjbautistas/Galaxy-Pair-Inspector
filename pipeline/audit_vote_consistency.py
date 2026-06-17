"""
Audit Supabase votes against the active pair/group catalogs.

Outputs:
    outputs/audit/vote_consistency_summary.csv
    outputs/audit/vote_consistency_rows.csv
    outputs/audit/visible_assignments.csv
"""

from __future__ import annotations

import json
import sys
import urllib.parse
import urllib.request as urlreq
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / 'outputs' / 'audit'
RP_MAX_KPC = 50.0
RP_SPLIT_KPC = 20.0
CALIB_PAIRS = 120
CALIB_GROUPS = 80
BLOCK_SIZE = 1000
GROUP_BLOCK_SIZE = 100
GROUP_Z_MIN = 0.01
SUPP_CALIB_PATH = ROOT / 'data' / 'supplementary_calib_ids_v5_3.json'


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
PAIRS_CATALOG = Path(_ENV.get('PAIRS_CATALOG', ROOT / 'data' / 'DESI_v5_3_pairs.parquet'))
GROUPS_CATALOG = Path(_ENV.get('GROUPS_CATALOG', ROOT / 'data' / 'DESI_v5_3_groups.parquet'))


@dataclass
class VisibleSet:
    item_uids: set[str]
    pair_uids: set[str]
    group_uids: set[str]


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


def _int_or_none(value) -> int | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    return int(value)


def load_pairs() -> tuple[pd.DataFrame, int]:
    df = pd.read_parquet(PAIRS_CATALOG)
    rp_col = next((c for c in ('rp_kpc', 'rp_phys_kpc', 'rp') if c in df.columns), None)
    if rp_col is None:
        raise RuntimeError('El catálogo de pares no tiene columna rp_kpc/rp_phys_kpc/rp.')

    df = df[df[rp_col] < RP_MAX_KPC].reset_index(drop=True)
    if 'pair_uid' not in df.columns:
        df['pair_uid'] = [_pair_uid(a, b) for a, b in zip(df['id1'], df['id2'])]
    mask_inner = df[rp_col] < RP_SPLIT_KPC
    df = pd.concat([df[mask_inner], df[~mask_inner]], ignore_index=True)
    return df, int(mask_inner.sum())


def load_groups() -> list[dict]:
    if not GROUPS_CATALOG.exists():
        return []
    df = pd.read_parquet(GROUPS_CATALOG)
    groups: list[dict] = []
    for gid, edges in df.groupby('fof_component_id'):
        half1 = edges[['id1', 'ra1', 'dec1', 'z1']].rename(
            columns={'id1': 'id', 'ra1': 'ra', 'dec1': 'dec', 'z1': 'z'})
        half2 = edges[['id2', 'ra2', 'dec2', 'z2']].rename(
            columns={'id2': 'id', 'ra2': 'ra', 'dec2': 'dec', 'z2': 'z'})
        members = pd.concat([half1, half2]).drop_duplicates('id')
        if float(members['z'].mean()) <= GROUP_Z_MIN:
            continue
        stable_system_id = None
        if 'stable_system_id' in edges.columns:
            values = edges['stable_system_id'].dropna().astype(str).unique()
            if len(values):
                stable_system_id = values[0]
        groups.append({
            'group_id': int(gid),
            'group_uid': stable_system_id or str(int(gid)),
            'stable_system_id': stable_system_id,
        })
    return groups


def load_supp_calib_ids() -> set[str]:
    if not SUPP_CALIB_PATH.exists():
        return set()
    with open(SUPP_CALIB_PATH, encoding='utf-8') as f:
        data = json.load(f)
    return {str(x) for x in data.get('pair_uid', [])}


def build_visible_set(partition: dict, pairs: pd.DataFrame, n_inner: int,
                      groups: list[dict], supp_pair_uids: set[str]) -> VisibleSet:
    pair_uids: set[str] = set(pairs.iloc[:CALIB_PAIRS]['pair_uid'].astype(str))
    pair_uids.update(supp_pair_uids)

    ws = _int_or_none(partition.get('work_start')) or CALIB_PAIRS
    we = _int_or_none(partition.get('work_end')) or ws
    ws_outer = _int_or_none(partition.get('work_start_v2'))
    we_outer = _int_or_none(partition.get('work_end_v2'))

    if ws_outer is None and we_outer is None and (we - ws) <= BLOCK_SIZE:
        pair_uids.update(pairs.iloc[ws:we]['pair_uid'].astype(str))
    else:
        q_inner = round(BLOCK_SIZE * 0.50)
        q_outer = BLOCK_SIZE - q_inner
        inner_start = max(CALIB_PAIRS, min(ws, n_inner))
        inner_end = max(inner_start, min(we, n_inner))
        pair_uids.update(pairs.iloc[inner_start:inner_end].head(q_inner)['pair_uid'].astype(str))
        if ws_outer is not None and we_outer is not None:
            pair_uids.update(pairs.iloc[ws_outer:we_outer].head(q_outer)['pair_uid'].astype(str))
        elif ws >= n_inner:
            pair_uids.update(pairs.iloc[ws:we].head(q_outer)['pair_uid'].astype(str))

    group_uids = {str(g['group_uid']) for g in groups[:CALIB_GROUPS]}
    gs = _int_or_none(partition.get('group_work_start'))
    ge = _int_or_none(partition.get('group_work_end'))
    if gs is not None and ge is not None:
        group_uids.update(str(g['group_uid']) for g in groups[gs:ge])

    return VisibleSet(
        item_uids=pair_uids | group_uids,
        pair_uids=pair_uids,
        group_uids=group_uids,
    )


def main() -> None:
    if not SUPABASE_URL or not SERVICE_ROLE_KEY:
        sys.exit('ERROR: falta SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY en .env')
    if not PAIRS_CATALOG.exists():
        sys.exit(f'ERROR: no existe PAIRS_CATALOG: {PAIRS_CATALOG}')

    print('Cargando catálogos locales...')
    pairs, n_inner = load_pairs()
    groups = load_groups()
    supp_pair_uids = load_supp_calib_ids()
    valid_pair_uids = set(pairs['pair_uid'].astype(str))
    valid_group_uids = {str(g['group_uid']) for g in groups}

    print('Descargando Supabase...')
    partitions = _fetch_table('partitions', order='device_id.asc')
    votes = _fetch_table('clasificaciones', order='device_id.asc')
    partitions_by_device = {str(p['device_id']): p for p in partitions}

    visible_by_device = {
        dev: build_visible_set(p, pairs, n_inner, groups, supp_pair_uids)
        for dev, p in partitions_by_device.items()
    }

    visible_rows = []
    for dev, visible in visible_by_device.items():
        for uid in sorted(visible.pair_uids):
            visible_rows.append({'device_id': dev, 'item_type': 'pair', 'item_uid': uid})
        for uid in sorted(visible.group_uids):
            visible_rows.append({'device_id': dev, 'item_type': 'group', 'item_uid': uid})

    valid_classes = {'FP', 'Pair', 'PM', 'GROUP', 'PP'}
    audit_rows = []
    for row in votes:
        dev = str(row.get('device_id', ''))
        item_type = str(row.get('item_type') or '')
        item_uid = str(row.get('item_uid') or '')
        classification = str(row.get('classification', ''))
        has_partition = dev in partitions_by_device
        visible = has_partition and item_uid in visible_by_device[dev].item_uids
        if item_type == 'pair':
            in_catalog = item_uid in valid_pair_uids
        elif item_type == 'group':
            in_catalog = item_uid in valid_group_uids
        else:
            in_catalog = False

        if classification not in valid_classes:
            status = 'invalid_class'
        elif item_type not in {'pair', 'group'}:
            status = 'invalid_item_type'
        elif not has_partition:
            status = 'unknown_device'
        elif not in_catalog:
            status = 'missing_from_current_catalog'
        elif visible:
            status = 'visible_current_set'
        else:
            status = 'outside_current_assignment'

        audit_rows.append({
            'device_id': dev,
            'item_type': item_type,
            'item_uid': item_uid,
            'pair_uid': row.get('pair_uid'),
            'stable_system_id': row.get('stable_system_id'),
            'display_id': row.get('id_par_v5'),
            'classification': classification,
            'status': status,
            'visible_current_set': visible,
            'in_current_catalog': in_catalog,
            'source': row.get('source'),
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
                total_votes=('item_uid', 'count'),
                visible_votes=('visible_current_set', 'sum'),
                outside_or_invalid_votes=('status', lambda s: int((s != 'visible_current_set').sum())),
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
    print(f'\nEjecutado: {datetime.now().isoformat(timespec="seconds")}')


if __name__ == '__main__':
    main()
