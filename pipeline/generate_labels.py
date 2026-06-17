"""
Download Supabase classifications and export majority-vote labels.

Operational keys:
    - pairs: item_type='pair', item_uid=pair_uid
    - groups: item_type='group', item_uid=stable_system_id/group_uid

Salidas:
    outputs/catalogs/labels.csv
    outputs/catalogs/labels_calib.csv
    outputs/catalogs/labels_groups.csv
    outputs/catalogs/labels_groups_calib.csv

Uso:
    python pipeline/generate_labels.py

Requiere: .env en la raíz con SUPABASE_URL y SUPABASE_ANON_KEY.
"""

import csv
import json
import sys
import urllib.request as urlreq
from collections import Counter
from pathlib import Path

import pandas as pd


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
SUPABASE_URL = _env.get('SUPABASE_URL', '').rstrip('/')
ANON_KEY = _env.get('SUPABASE_SERVICE_ROLE_KEY', '') or _env.get('SUPABASE_ANON_KEY', '')
PAIRS_CATALOG = _env.get('PAIRS_CATALOG', 'data/DESI_v5_3_pairs.parquet')
GROUPS_CATALOG = _env.get('GROUPS_CATALOG', 'data/DESI_v5_3_groups.parquet')
CALIB_PAIRS = 120
CALIB_GROUPS = 80
RP_SPLIT_KPC = 20.0
SUPP_CALIB_JSON = 'data/supplementary_calib_ids_v5_3.json'
OUTPUT_DIR = Path('outputs/catalogs')


def _load_supp_calib_ids() -> set:
    """Devuelve el set de pair_uid de calibración suplementaria (rp ∈ [20,50) kpc)."""
    try:
        with open(SUPP_CALIB_JSON) as f:
            data = json.load(f)
        ids = set(data.get('pair_uid', []))
        print(f'  Calibración suplementaria: {len(ids)} pair_uids cargados')
        return ids
    except FileNotFoundError:
        print(f'  Aviso: {SUPP_CALIB_JSON} no encontrado — supp calib tratada como work')
        return set()


def _pair_uid(id1, id2) -> str:
    a, b = sorted((int(id1), int(id2)))
    return f'{a}:{b}'


def _load_pair_lookup(path: str) -> dict[str, dict]:
    if not path or not Path(path).exists():
        return {}

    all_cols = pd.read_parquet(path, columns=None).columns
    required = ['id1', 'id2']
    optional = ['display_id', 'id_par_v5', 'id_par', 'pair_uid']
    rp_col = next((c for c in ('rp_kpc', 'rp_phys_kpc', 'rp') if c in all_cols), None)
    cols = required + [c for c in optional if c in all_cols]
    if rp_col:
        cols.append(rp_col)
    pairs = pd.read_parquet(path, columns=cols)

    if 'pair_uid' not in pairs.columns:
        pairs['pair_uid'] = [_pair_uid(a, b) for a, b in zip(pairs['id1'], pairs['id2'])]
    if 'display_id' not in pairs.columns and 'id_par_v5' in pairs.columns:
        pairs['display_id'] = pairs['id_par_v5']
    elif 'display_id' not in pairs.columns and 'id_par' in pairs.columns:
        pairs['display_id'] = pairs['id_par']

    if rp_col:
        mask_inner = pairs[rp_col] < RP_SPLIT_KPC
        pairs = pd.concat([pairs[mask_inner], pairs[~mask_inner]], ignore_index=True)

    lookup = {}
    for idx, row in enumerate(pairs.itertuples(index=False)):
        row_dict = row._asdict()
        pair_uid = str(row_dict['pair_uid'])
        display_id = row_dict.get('display_id')
        lookup[pair_uid] = {
            'order': idx,
            'display_id': int(display_id) if display_id is not None and not pd.isna(display_id) else None,
        }
    return lookup


def _load_group_lookup(path: str) -> dict[str, dict]:
    if not path or not Path(path).exists():
        return {}

    cols = pd.read_parquet(path, columns=None).columns
    read_cols = ['fof_component_id']
    if 'stable_system_id' in cols:
        read_cols.append('stable_system_id')
    groups = pd.read_parquet(path, columns=read_cols)

    lookup = {}
    for order, (gid, edges) in enumerate(groups.groupby('fof_component_id')):
        stable_system_id = None
        if 'stable_system_id' in edges.columns:
            values = edges['stable_system_id'].dropna().astype(str).unique()
            if len(values):
                stable_system_id = values[0]
        group_uid = stable_system_id or str(int(gid))
        lookup[group_uid] = {
            'order': order,
            'group_id': int(gid),
            'stable_system_id': stable_system_id,
        }
    return lookup


def _fetch_all_classifications() -> list[dict]:
    rows = []
    limit = 1000
    offset = 0
    select = (
        'device_id,item_type,item_uid,pair_uid,stable_system_id,'
        'id_par_v5,classification'
    )

    while True:
        url = (
            f'{SUPABASE_URL}/rest/v1/clasificaciones'
            f'?select={select}'
            f'&limit={limit}&offset={offset}'
        )
        req = urlreq.Request(url, headers={
            'apikey': ANON_KEY,
            'Authorization': f'Bearer {ANON_KEY}',
        })
        with urlreq.urlopen(req) as resp:
            page = json.loads(resp.read())

        rows.extend(page)
        if len(page) < limit:
            break
        offset += limit

    return rows


def _majority_vote(rows: list[dict], key_field: str) -> list[dict]:
    votes: dict[str, list[str]] = {}
    meta: dict[str, dict] = {}

    for row in rows:
        key = row.get(key_field) or row.get('item_uid')
        if not key:
            continue
        key = str(key)
        votes.setdefault(key, []).append(row['classification'])
        meta.setdefault(key, row)

    results = []
    skipped_ties = 0

    for key, labels in sorted(votes.items()):
        counts = Counter(labels)
        n_votes = len(labels)
        top_two = counts.most_common(2)
        if len(top_two) == 2 and top_two[0][1] == top_two[1][1]:
            skipped_ties += 1
            continue

        winner = top_two[0][0]
        agreement = round(top_two[0][1] / n_votes, 4)
        results.append({
            key_field: key,
            'classification': winner,
            'n_votes': n_votes,
            'agreement': agreement,
            '_meta': meta[key],
        })

    if skipped_ties:
        print(f'  Empates omitidos: {skipped_ties} elemento(s)')

    return results


def _write_csv(rows: list[dict], path: Path, fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    clean_rows = [{k: row.get(k) for k in fieldnames} for row in rows]
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clean_rows)
    print(f'  {len(rows)} filas -> {path}')


def main():
    if not SUPABASE_URL or not ANON_KEY:
        print('ERROR: falta SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY en .env')
        sys.exit(1)

    supp_calib_ids = _load_supp_calib_ids()
    pair_lookup = _load_pair_lookup(PAIRS_CATALOG)
    group_lookup = _load_group_lookup(GROUPS_CATALOG)
    print(f'Catálogo pares: {PAIRS_CATALOG or "no configurado"}')
    print(f'  Pares en lookup: {len(pair_lookup):,}')
    print(f'  Grupos en lookup: {len(group_lookup):,}')

    print('Descargando clasificaciones desde Supabase...')
    raw = _fetch_all_classifications()
    print(f'  {len(raw)} filas descargadas')

    raw_pairs = [r for r in raw if r.get('item_type') == 'pair']
    raw_groups = [r for r in raw if r.get('item_type') == 'group']
    print(f'  Pares: {len(raw_pairs)} votos  |  Grupos: {len(raw_groups)} votos')

    print('Aplicando mayoría de votos (pares)...')
    labeled_pairs = []
    for row in _majority_vote(raw_pairs, 'pair_uid'):
        pair_uid = row['pair_uid']
        catalog_meta = pair_lookup.get(pair_uid, {})
        supabase_meta = row.pop('_meta', {})
        display_id = catalog_meta.get('display_id')
        if display_id is None:
            display_id = supabase_meta.get('id_par_v5')
        labeled_pairs.append({
            'pair_uid': pair_uid,
            'display_id': int(display_id) if display_id is not None and not pd.isna(display_id) else None,
            'classification': row['classification'],
            'n_votes': row['n_votes'],
            'agreement': row['agreement'],
            '_order': catalog_meta.get('order'),
        })

    pair_fields = ['pair_uid', 'display_id', 'classification', 'n_votes', 'agreement']
    calib_pairs = [r for r in labeled_pairs
                   if (r.get('_order') is not None and r['_order'] < CALIB_PAIRS)
                   or r['pair_uid'] in supp_calib_ids]
    work_pairs  = [r for r in labeled_pairs
                   if (r.get('_order') is None or r['_order'] >= CALIB_PAIRS)
                   and r['pair_uid'] not in supp_calib_ids]
    print('Escribiendo archivos de pares...')
    _write_csv(work_pairs, OUTPUT_DIR / 'labels.csv', pair_fields)
    _write_csv(calib_pairs, OUTPUT_DIR / 'labels_calib.csv', pair_fields)

    print('Aplicando mayoría de votos (grupos)...')
    labeled_groups = []
    for row in _majority_vote(raw_groups, 'stable_system_id'):
        group_uid = row['stable_system_id']
        catalog_meta = group_lookup.get(group_uid, {})
        row.pop('_meta', None)
        stable_system_id = catalog_meta.get('stable_system_id') or group_uid
        labeled_groups.append({
            'group_uid': group_uid,
            'stable_system_id': stable_system_id,
            'group_id': catalog_meta.get('group_id'),
            'classification': row['classification'],
            'n_votes': row['n_votes'],
            'agreement': row['agreement'],
            '_order': catalog_meta.get('order'),
        })

    group_fields = ['group_uid', 'stable_system_id', 'group_id', 'classification', 'n_votes', 'agreement']
    calib_groups = [r for r in labeled_groups if r.get('_order') is not None and r['_order'] < CALIB_GROUPS]
    work_groups = [r for r in labeled_groups if r.get('_order') is None or r['_order'] >= CALIB_GROUPS]
    print('Escribiendo archivos de grupos...')
    _write_csv(work_groups, OUTPUT_DIR / 'labels_groups.csv', group_fields)
    _write_csv(calib_groups, OUTPUT_DIR / 'labels_groups_calib.csv', group_fields)


if __name__ == '__main__':
    main()
