"""
export_catalog.py — Exporta el catálogo de pares al formato JSON para la app móvil.

Uso:
    python export_catalog.py

Genera: mobile_app/catalog.json
"""

import json
import os
import random
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

SHUFFLE_SEED = 42   # semilla fija → orden idéntico en todos los dispositivos

# ── Configuración (debe coincidir con pair_inspector_app.py) ──────────────────

CATALOG_PATH = (
    '/Users/frank/Documents/Estudio-PhD/Semestre-2025-II/Tesis_I/'
    'Galaxy_Pairs/Galaxy_pairs/outputs/catalogs/interacting/'
    'DESI_int_legacyID_pairs.parquet'
)
PROGRESS_FILE = 'outputs/catalogs/progress.json'
OUTPUT_PATH   = 'mobile_app/catalog.json'
RP_MAX_KPC    = 12.0

# ─────────────────────────────────────────────────────────────────────────────

def compute_sep_arcsec(df: pd.DataFrame) -> pd.Series:
    """Calcula separación angular en arcsec si no existe en el catálogo."""
    ra1, dec1 = df['ra1'].values, df['dec1'].values
    ra2, dec2 = df['ra2'].values, df['dec2'].values
    dec_mid   = np.radians((dec1 + dec2) / 2.0)
    dx = (ra1 - ra2) * np.cos(dec_mid) * 3600.0
    dy = (dec1 - dec2) * 3600.0
    return pd.Series(np.hypot(dx, dy), index=df.index)


def load_desktop_classified(progress_file: str) -> dict:
    """Lee progress.json y construye un dict id_par → 'FP'|'Pair'|'PM'."""
    if not os.path.exists(progress_file):
        return {}
    with open(progress_file) as f:
        state = json.load(f)
    result = {}
    for id_par in state.get('false_positives',  []): result[str(id_par)] = 'FP'
    for id_par in state.get('confirmed_pairs',   []): result[str(id_par)] = 'Pair'
    for id_par in state.get('possible_mergers',  []): result[str(id_par)] = 'PM'
    return result


def main():
    print(f'Leyendo catálogo: {CATALOG_PATH}')
    if not Path(CATALOG_PATH).exists():
        raise FileNotFoundError(f'No se encontró el catálogo:\n  {CATALOG_PATH}')

    df = pd.read_parquet(CATALOG_PATH)
    print(f'  → {len(df):,} pares en total')

    # Detectar columna rp
    rp_col = next((c for c in ('rp_kpc', 'rp_phys_kpc', 'rp') if c in df.columns), None)
    if rp_col and RP_MAX_KPC:
        df = df[df[rp_col] < RP_MAX_KPC].reset_index(drop=True)
        print(f'  → {len(df):,} pares tras filtro rp < {RP_MAX_KPC} kpc')

    # Separación angular
    if 'sep_arcsec' not in df.columns:
        df['sep_arcsec'] = compute_sep_arcsec(df)

    # Punto medio
    if 'ra_mid' not in df.columns:
        df['ra_mid']  = (df['ra1'] + df['ra2']) / 2.0
    if 'dec_mid' not in df.columns:
        df['dec_mid'] = (df['dec1'] + df['dec2']) / 2.0

    # Redshift (probar varias columnas comunes)
    z_col = next((c for c in ('z', 'z_spec', 'z1', 'redshift') if c in df.columns), None)

    # Construir lista de pares
    pairs = []
    for _, row in df.iterrows():
        entry = {
            'id_par':     int(row['id_par']) if 'id_par' in row else int(row.name),
            'ra1':        float(row['ra1']),
            'dec1':       float(row['dec1']),
            'ra2':        float(row['ra2']),
            'dec2':       float(row['dec2']),
            'ra_mid':     float(row['ra_mid']),
            'dec_mid':    float(row['dec_mid']),
            'sep_arcsec': float(row['sep_arcsec']),
        }
        if rp_col:
            entry['rp'] = float(row[rp_col])
        if z_col:
            entry['z'] = float(row[z_col])
        pairs.append(entry)

    # Orden aleatorio fijo — igual en todos los dispositivos
    random.seed(SHUFFLE_SEED)
    random.shuffle(pairs)

    # Clasificaciones del escritorio
    desktop_cl = load_desktop_classified(PROGRESS_FILE)
    classified_count = len(desktop_cl)

    # Ensamblar JSON
    catalog = {
        'exported_at':         datetime.now().isoformat(),
        'shuffle_seed':        SHUFFLE_SEED,
        'rp_max_kpc':          RP_MAX_KPC,
        'total_pairs':         len(pairs),
        'desktop_classified':  desktop_cl,
        'pairs':               pairs,
    }

    Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(catalog, f, separators=(',', ':'))   # compact para menor tamaño

    size_mb = Path(OUTPUT_PATH).stat().st_size / 1e6
    print(f'\n✓ Exportado: {OUTPUT_PATH}')
    print(f'  {len(pairs):,} pares  |  {classified_count} ya clasificados en escritorio')
    print(f'  Tamaño: {size_mb:.2f} MB')
    print(f'\nSiguiente paso → inicia el servidor:')
    print(f'  python serve_mobile.py')


if __name__ == '__main__':
    main()
