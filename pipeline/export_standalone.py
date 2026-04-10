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

# ── Configuración ──────────────────────────────────────────────────────────────
CATALOG_PATH  = _env.get('PAIRS_CATALOG', '')
PROGRESS_FILE = 'outputs/catalogs/progress.json'
TEMPLATE_HTML = 'mobile/index.html'
OUTPUT_HTML   = 'mobile/GalPairs.html'
RP_MAX_KPC    = 12.0

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
        pairs.append(entry)

    desktop_cl = _load_desktop_classified(PROGRESS_FILE)
    print(f'  {len(desktop_cl)} pares ya clasificados en escritorio')

    return {
        'exported_at':        datetime.now().isoformat(),
        'rp_max_kpc':         RP_MAX_KPC,
        'total_pairs':        len(pairs),
        'desktop_classified': desktop_cl,
        'pairs':              pairs,
    }


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
