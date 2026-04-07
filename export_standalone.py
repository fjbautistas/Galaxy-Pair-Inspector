"""
export_standalone.py — Genera un único archivo HTML con el catálogo embebido.

El archivo resultante funciona directamente en Safari del iPhone sin servidor.
Solo necesita internet para cargar las imágenes de Legacy Survey.

Uso:
    python export_standalone.py

Genera: mobile_app/GalPairs.html  (~4 MB)

Flujo de uso:
    1. Corre este script en el Mac
    2. AirDrop de  mobile_app/GalPairs.html  al iPhone
    3. En el iPhone: Archivos → GalPairs.html → Abrir con Safari
    4. Clasificar en cualquier lugar con internet
    5. Exportar JSON desde la app → AirDrop de vuelta al Mac
    6. python import_from_mobile.py mobile_cl_YYYY-MM-DD.json
"""

import json
import os
import random
import re
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

# Semilla fija → todos los dispositivos ven el mismo orden aleatorio.
# Cámbiala solo si quieres generar un orden completamente nuevo.
SHUFFLE_SEED     = 42    # orden base idéntico en todos los dispositivos
CALIBRATION_SIZE = 100   # pares iniciales iguales para todos → inter-rater reliability

# ── Configuración ─────────────────────────────────────────────────────────────

CATALOG_PATH = (
    '/Users/frank/Documents/Estudio-PhD/Semestre-2025-II/Tesis_I/'
    'Galaxy_Pairs/Galaxy_pairs/outputs/catalogs/interacting/'
    'DESI_int_legacyID_pairs.parquet'
)
PROGRESS_FILE  = 'outputs/catalogs/progress.json'
TEMPLATE_HTML  = 'mobile_app/index.html'
OUTPUT_HTML    = 'mobile_app/GalPairs.html'
RP_MAX_KPC     = 12.0

# ─────────────────────────────────────────────────────────────────────────────

def compute_sep_arcsec(df):
    ra1, dec1 = df['ra1'].values, df['dec1'].values
    ra2, dec2 = df['ra2'].values, df['dec2'].values
    dec_mid   = np.radians((dec1 + dec2) / 2.0)
    dx = (ra1 - ra2) * np.cos(dec_mid) * 3600.0
    dy = (dec1 - dec2) * 3600.0
    return pd.Series(np.hypot(dx, dy), index=df.index)


def load_desktop_classified(progress_file):
    if not os.path.exists(progress_file):
        return {}
    with open(progress_file) as f:
        state = json.load(f)
    result = {}
    for id_par in state.get('false_positives',  []): result[str(id_par)] = 'FP'
    for id_par in state.get('confirmed_pairs',   []): result[str(id_par)] = 'Pair'
    for id_par in state.get('possible_mergers',  []): result[str(id_par)] = 'PM'
    return result


def build_catalog_dict():
    """Lee el catálogo y devuelve el dict listo para embeber."""
    print(f'Leyendo catálogo...')
    df = pd.read_parquet(CATALOG_PATH)
    print(f'  {len(df):,} pares totales')

    rp_col = next((c for c in ('rp_kpc', 'rp_phys_kpc', 'rp') if c in df.columns), None)
    if rp_col and RP_MAX_KPC:
        df = df[df[rp_col] < RP_MAX_KPC].reset_index(drop=True)
        print(f'  {len(df):,} pares tras filtro rp < {RP_MAX_KPC} kpc')

    if 'sep_arcsec' not in df.columns:
        df['sep_arcsec'] = compute_sep_arcsec(df)
    if 'ra_mid' not in df.columns:
        df['ra_mid']  = (df['ra1'] + df['ra2']) / 2.0
    if 'dec_mid' not in df.columns:
        df['dec_mid'] = (df['dec1'] + df['dec2']) / 2.0

    z_col = next((c for c in ('z', 'z_spec', 'z1', 'redshift') if c in df.columns), None)

    pairs = []
    for _, row in df.iterrows():
        entry = {
            'id_par':     int(row['id_par']) if 'id_par' in row else int(row.name),
            # Coordenadas con 5 decimales (~1 mas precision) — reduce el tamaño del archivo
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
        if z_col:
            entry['z'] = round(float(row[z_col]), 5)
        pairs.append(entry)

    # Orden aleatorio fijo — igual en todos los dispositivos que descarguen
    # el mismo GalPairs.html. Las primeras N galaxias que clasifique cada
    # persona son las mismas → overlap natural para inter-rater reliability.
    random.seed(SHUFFLE_SEED)
    random.shuffle(pairs)
    print(f'  Orden aleatorio aplicado (seed={SHUFFLE_SEED})')

    desktop_cl = load_desktop_classified(PROGRESS_FILE)

    return {
        'exported_at':        datetime.now().isoformat(),
        'shuffle_seed':       SHUFFLE_SEED,
        'calibration_size':   CALIBRATION_SIZE,
        'rp_max_kpc':         RP_MAX_KPC,
        'total_pairs':        len(pairs),
        'desktop_classified': desktop_cl,
        'pairs':              pairs,
    }


def main():
    if not Path(CATALOG_PATH).exists():
        raise FileNotFoundError(f'No se encontró el catálogo:\n  {CATALOG_PATH}')
    if not Path(TEMPLATE_HTML).exists():
        raise FileNotFoundError(f'No se encontró {TEMPLATE_HTML}. Verifica que mobile_app/index.html existe.')

    # Construir datos del catálogo
    catalog = build_catalog_dict()
    print(f'  {len(catalog["desktop_classified"])} ya clasificados en escritorio')

    # Serializar con separadores compactos (sin espacios)
    print('Generando JSON embebido...')
    catalog_json = json.dumps(catalog, separators=(',', ':'))

    # Leer plantilla HTML
    with open(TEMPLATE_HTML, encoding='utf-8') as f:
        html = f.read()

    # Insertar los datos justo antes del primer <script> principal
    # (el que contiene las constantes de la app)
    inject = f'<script>window._CATALOG={catalog_json};</script>\n  '
    html = html.replace('<script>\n  // ═══════════════════════════════════════════════════════════════════════\n  // CONSTANTS',
                        inject + '<script>\n  // ═══════════════════════════════════════════════════════════════════════\n  // CONSTANTS')

    # Actualizar el mensaje "no catálogo" para modo offline
    html = html.replace(
        'python export_catalog.py</div>',
        'python export_standalone.py</div>'
    )

    # Guardar
    Path(OUTPUT_HTML).parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)

    size_mb = Path(OUTPUT_HTML).stat().st_size / 1e6
    print(f'\n✓  Archivo generado: {OUTPUT_HTML}')
    print(f'   Tamaño: {size_mb:.1f} MB  |  {len(catalog["pairs"]):,} pares')
    print()
    print('Próximos pasos:')
    print('  1. AirDrop de  mobile_app/GalPairs.html  al iPhone')
    print('  2. En el iPhone: Archivos → GalPairs.html → Abrir con Safari')
    print('  3. Clasificar en cualquier lugar con internet 🌌')
    print()
    print('Para importar de vuelta al escritorio:')
    print('  python import_from_mobile.py mobile_cl_YYYY-MM-DD.json')


if __name__ == '__main__':
    main()
