"""
export_standalone.py — Genera un único HTML con el catálogo completo embebido.

Cada dispositivo que abra el HTML se auto-registra en Supabase la primera vez
y recibe automáticamente su partición sin intervención manual.

Uso:
    python pipeline/export_standalone.py

Genera: mobile/GalPairs.html

Flujo:
    1. Corre este script (una sola vez, o cuando cambie el catálogo)
    2. git add mobile/GalPairs.html && git commit && git push
    3. Cada usuario abre fjbautistas.github.io/Galaxy-Pair-Inspector/mobile/GalPairs.html
       → se registra solo en Supabase → clasifica su bloque asignado
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

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

_env              = _load_env()
SUPABASE_URL      = _env.get('SUPABASE_URL', '')
SUPABASE_ANON_KEY = _env.get('SUPABASE_ANON_KEY', '')

# ── Configuración ──────────────────────────────────────────────────────────────
CATALOG_PATH        = _env.get('PAIRS_CATALOG', '')
GROUPS_CATALOG_PATH = _env.get('GROUPS_CATALOG', '')
GROUP_Z_MIN   = 0.01   # excluir grupos con z_center ≤ este valor (artifact FoF local)
PROGRESS_FILE = 'outputs/catalogs/progress.json'
TEMPLATE_HTML = 'mobile/index.html'
OUTPUT_HTML   = 'mobile/GalPairs.html'
RP_MAX_KPC    = 12.0
MAX_GROUP_MEMBERS_MOBILE = 8   # máximo de coords de miembros embebidos por grupo

# ── Catálogo ──────────────────────────────────────────────────────────────────
def _compute_sep(df):
    dec_mid = np.radians((df['dec1'].values + df['dec2'].values) / 2.0)
    dx = (df['ra1'].values - df['ra2'].values) * np.cos(dec_mid) * 3600.0
    dy = (df['dec1'].values - df['dec2'].values) * 3600.0
    return pd.Series(np.hypot(dx, dy), index=df.index)


def _load_desktop_classified(progress_file):
    if not os.path.exists(progress_file):
        return {}
    with open(progress_file) as f:
        state = json.load(f)
    result = {}
    for entry in state.get('false_positives',  []): result[str(entry.get('id_par', ''))] = 'FP'
    for entry in state.get('confirmed_pairs',   []): result[str(entry.get('id_par', ''))] = 'Pair'
    for entry in state.get('possible_mergers',  []): result[str(entry.get('id_par', ''))] = 'PM'
    return result


def build_catalog() -> dict:
    print('Leyendo catálogo…')
    df = pd.read_parquet(CATALOG_PATH)
    print(f'  {len(df):,} pares totales')

    rp_col = next((c for c in ('rp_kpc', 'rp_phys_kpc', 'rp') if c in df.columns), None)
    if rp_col and RP_MAX_KPC:
        df = df[df[rp_col] < RP_MAX_KPC].reset_index(drop=True)
        print(f'  {len(df):,} pares tras filtro rp < {RP_MAX_KPC} kpc')

    if 'sep_arcsec' not in df.columns:
        df['sep_arcsec'] = _compute_sep(df)
    if 'ra_mid' not in df.columns:
        df['ra_mid']  = (df['ra1'] + df['ra2']) / 2.0
    if 'dec_mid' not in df.columns:
        df['dec_mid'] = (df['dec1'] + df['dec2']) / 2.0

    has_z1 = 'z1' in df.columns
    has_z2 = 'z2' in df.columns

    pairs = []
    for _, row in df.iterrows():
        entry = {
            'id_par':     int(row['id_par']) if 'id_par' in row else int(row.name),
            'ra1':        round(float(row['ra1']),    5),
            'dec1':       round(float(row['dec1']),   5),
            'ra2':        round(float(row['ra2']),    5),
            'dec2':       round(float(row['dec2']),   5),
            'ra_mid':     round(float(row['ra_mid']),  6),
            'dec_mid':    round(float(row['dec_mid']), 6),
            'sep_arcsec': round(float(row['sep_arcsec']), 3),
        }
        if rp_col:
            entry['rp'] = round(float(row[rp_col]), 3)
        if has_z1:
            entry['z1'] = round(float(row['z1']), 4)
        if has_z2:
            entry['z2'] = round(float(row['z2']), 4)
        if 'fof_component_id' in row and pd.notna(row['fof_component_id']):
            entry['fof_component_id'] = int(row['fof_component_id'])
        if 'component_size' in row and pd.notna(row['component_size']):
            entry['component_size'] = int(row['component_size'])
        pairs.append(entry)

    desktop_cl = _load_desktop_classified(PROGRESS_FILE)
    print(f'  {len(desktop_cl)} pares ya clasificados en escritorio')

    groups = _build_groups_catalog()

    return {
        'exported_at':        datetime.now().isoformat(),
        'rp_max_kpc':         RP_MAX_KPC,
        'total_pairs':        len(pairs),
        'total_groups':       len(groups),
        'desktop_classified': desktop_cl,
        'pairs':              pairs,
        'groups':             groups,
    }


def _build_groups_catalog() -> list:
    """Construye lista de grupos (una entrada por componente FoF) para embeber en el HTML."""
    if not GROUPS_CATALOG_PATH or not Path(GROUPS_CATALOG_PATH).exists():
        print('  Aviso: GROUPS_CATALOG no encontrado — grupos no incluidos en el HTML')
        return []

    print('Construyendo catálogo de grupos para móvil…')
    df = pd.read_parquet(GROUPS_CATALOG_PATH)
    groups = []
    for gid, edges in df.groupby('fof_component_id'):
        half1 = edges[['id1', 'ra1', 'dec1', 'z1']].rename(
            columns={'id1': 'id', 'ra1': 'ra', 'dec1': 'dec', 'z1': 'z'})
        half2 = edges[['id2', 'ra2', 'dec2', 'z2']].rename(
            columns={'id2': 'id', 'ra2': 'ra', 'dec2': 'dec', 'z2': 'z'})
        members = pd.concat([half1, half2]).drop_duplicates('id')

        ra_c   = float(members['ra'].mean())
        dec_c  = float(members['dec'].mean())
        z_c    = float(members['z'].mean())

        # Excluir grupos en el volumen local (artifact FoF a z≈0)
        if z_c <= GROUP_Z_MIN:
            continue

        # Ordenar miembros por distancia al centroide → los más cercanos primero
        members = members.copy()
        members['_dist'] = np.hypot(members['ra'] - ra_c, members['dec'] - dec_c)
        members = members.sort_values('_dist')

        # Tomar solo los MAX_GROUP_MEMBERS_MOBILE más cercanos para no saturar el HTML
        top = members.head(MAX_GROUP_MEMBERS_MOBILE)

        groups.append({
            'group_id':       int(gid),
            'n_members':      len(members),
            'ra_center':      round(ra_c,  6),
            'dec_center':     round(dec_c, 6),
            'z_center':       round(z_c,   4),
            'max_sep_arcsec': round(float(edges['sep_arcsec'].max()), 2),
            'rp_kpc_max':     round(float(edges['rp_kpc'].max()), 2),
            'member_ra':      [round(float(v), 5) for v in top['ra']],
            'member_dec':     [round(float(v), 5) for v in top['dec']],
        })

    print(f'  {len(groups):,} grupos únicos listos  (filtro z_center > {GROUP_Z_MIN})')
    return groups


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not Path(CATALOG_PATH).exists():
        print(f'ERROR: No se encontró el catálogo en {CATALOG_PATH}')
        sys.exit(1)
    if not Path(TEMPLATE_HTML).exists():
        print(f'ERROR: No se encontró la plantilla {TEMPLATE_HTML}')
        sys.exit(1)

    catalog      = build_catalog()
    catalog_json = json.dumps(catalog, separators=(',', ':'))

    with open(TEMPLATE_HTML, encoding='utf-8') as f:
        html = f.read()

    supabase_js = (
        f'window._SUPABASE_URL={json.dumps(SUPABASE_URL)};'
        f'window._SUPABASE_ANON_KEY={json.dumps(SUPABASE_ANON_KEY)};'
    )
    inject = f'<script>window._CATALOG={catalog_json};{supabase_js}</script>\n  '
    html = html.replace(
        '<script>\n  // ═══════════════════════════════════════════════════════════════════════\n  // CONSTANTS',
        inject + '<script>\n  // ═══════════════════════════════════════════════════════════════════════\n  // CONSTANTS'
    )
    html = html.replace(
        'python export_catalog.py</div>',
        'python export_standalone.py</div>'
    )

    Path(OUTPUT_HTML).parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)

    size_mb = Path(OUTPUT_HTML).stat().st_size / 1e6
    print(f'\n✓  Generado: {OUTPUT_HTML}')
    print(f'   Pares: {catalog["total_pairs"]:,}  |  Grupos: {catalog["total_groups"]:,}')
    print(f'   Tamaño: {size_mb:.1f} MB  |  {len(catalog["pairs"]):,} pares')
    print()
    print('Siguiente paso — publicar en GitHub Pages:')
    print('  git add mobile/GalPairs.html')
    print('  git commit -m "update catalog"')
    print('  git push')
    print()
    print(f'URL pública: https://fjbautistas.github.io/Galaxy-Pair-Inspector/mobile/GalPairs.html')
    print()
    print('Cada usuario que abra esa URL se registra automáticamente en Supabase.')


if __name__ == '__main__':
    main()
