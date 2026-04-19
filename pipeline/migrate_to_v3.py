"""
migrate_to_v3.py — Migra las clasificaciones existentes al nuevo catálogo v3.

Problema:
    El catálogo viejo (DESI_int_legacyID_pairs.parquet, v0/v1) y el nuevo
    (DESI_v3_pairs.parquet) tienen ids de par distintos.  Las clasificaciones
    en labels.csv usan el id_par del catálogo viejo, por lo que hay que
    hacer un crossmatch por (id1, id2) — los TARGETID de DESI — para traducir
    los ids.

Salida:
    outputs/catalogs/labels_migrados_v3.csv   — clasificaciones que siguen
                                                 existiendo en v3
    outputs/catalogs/labels_obsoletos.csv     — clasificaciones de pares que
                                                 ya no están en v3

El matching considera la simetría del par: (id1_viejo, id2_viejo) puede
corresponder a (id1_v3, id2_v3) o a (id2_v3, id1_v3).

Uso (desde la raíz de Galaxes_Inspection/):
    conda run -n astro-clean python pipeline/migrate_to_v3.py
"""

import sys
from pathlib import Path

import pandas as pd

# ── Rutas ─────────────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).resolve().parent.parent
OLD_CATALOG    = BASE_DIR / 'data' / 'DESI_int_legacyID_pairs.parquet'
NEW_CATALOG    = BASE_DIR / 'data' / 'DESI_v3_pairs.parquet'
LABELS_WORK    = BASE_DIR / 'outputs' / 'catalogs' / 'labels.csv'
LABELS_CALIB   = BASE_DIR / 'outputs' / 'catalogs' / 'labels_calib.csv'
OUTPUT_DIR     = BASE_DIR / 'outputs' / 'catalogs'

OUTPUT_MIGRADOS  = OUTPUT_DIR / 'labels_migrados_v3.csv'
OUTPUT_OBSOLETOS = OUTPUT_DIR / 'labels_obsoletos.csv'


def _load_and_check(path: Path, label: str) -> pd.DataFrame:
    """Carga un parquet y verifica que exista."""
    if not path.exists():
        print(f'ERROR: no se encontró {label}:\n  {path}')
        sys.exit(1)
    df = pd.read_parquet(path)
    print(f'  {label}: {len(df):,} filas,  columnas: {list(df.columns)}')
    return df


def build_key_map(df: pd.DataFrame) -> dict:
    """Construye un dict (id1, id2) → id_par para búsqueda rápida.
    Como el par es simétrico, se registra también (id2, id1)."""
    kmap = {}
    for _, row in df.iterrows():
        a, b, p = int(row['id1']), int(row['id2']), int(row['id_par'])
        kmap[(a, b)] = p
        kmap[(b, a)] = p   # simetría
    return kmap


def main():
    print('─' * 60)
    print('Migración de clasificaciones: v0/v1 → v3')
    print('─' * 60)

    # ── Cargar catálogos ──────────────────────────────────────────────────────
    print('\nCargando catálogos…')
    df_old = _load_and_check(OLD_CATALOG, 'catálogo viejo')
    df_new = _load_and_check(NEW_CATALOG, 'catálogo v3')

    # ── Cargar clasificaciones ────────────────────────────────────────────────
    print('\nCargando clasificaciones…')
    frames = []
    for p in [LABELS_WORK, LABELS_CALIB]:
        if p.exists():
            df = pd.read_csv(p)
            frames.append(df)
            print(f'  {p.name}: {len(df)} filas')
        else:
            print(f'  {p.name}: no encontrado — se omite')

    if not frames:
        print('No hay clasificaciones que migrar. Saliendo.')
        sys.exit(0)

    df_labels = pd.concat(frames, ignore_index=True)
    print(f'  Total clasificaciones: {len(df_labels)}')

    # ── Recuperar (id1, id2) del catálogo viejo para cada id_par clasificado ──
    print('\nRecuperando id1/id2 del catálogo viejo…')
    old_map = df_old.set_index('id_par')[['id1', 'id2']].to_dict('index')
    new_key_map = build_key_map(df_new)

    migrados  = []
    obsoletos = []

    for _, row in df_labels.iterrows():
        id_par_viejo = int(row['id_par'])
        cls          = row['classification']
        n_votes      = row.get('n_votes', 1)
        agreement    = row.get('agreement', 1.0)

        # Buscar en el catálogo viejo
        if id_par_viejo not in old_map:
            # El id_par ni siquiera existe en el catálogo viejo (raro)
            obsoletos.append({
                'id_par_viejo':    id_par_viejo,
                'id1':             None,
                'id2':             None,
                'id_par_v3':       None,
                'classification':  cls,
                'n_votes':         n_votes,
                'agreement':       agreement,
                'motivo':          'id_par no encontrado en catálogo viejo',
            })
            continue

        id1 = int(old_map[id_par_viejo]['id1'])
        id2 = int(old_map[id_par_viejo]['id2'])

        # Buscar en v3 por (id1, id2) o (id2, id1)
        id_par_v3 = new_key_map.get((id1, id2))

        if id_par_v3 is not None:
            migrados.append({
                'id_par_viejo':    id_par_viejo,
                'id1':             id1,
                'id2':             id2,
                'id_par_v3':       id_par_v3,
                'classification':  cls,
                'n_votes':         n_votes,
                'agreement':       agreement,
            })
        else:
            obsoletos.append({
                'id_par_viejo':    id_par_viejo,
                'id1':             id1,
                'id2':             id2,
                'id_par_v3':       None,
                'classification':  cls,
                'n_votes':         n_votes,
                'agreement':       agreement,
                'motivo':          'par no existe en v3 (criterios de corte cambiaron)',
            })

    # ── Guardar resultados ────────────────────────────────────────────────────
    df_migrados  = pd.DataFrame(migrados)
    df_obsoletos = pd.DataFrame(obsoletos)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df_migrados.to_csv(OUTPUT_MIGRADOS, index=False)
    df_obsoletos.to_csv(OUTPUT_OBSOLETOS, index=False)

    # ── Reporte ───────────────────────────────────────────────────────────────
    print('\n' + '─' * 60)
    print('Resultado:')
    print(f'  Migrados  (existen en v3): {len(df_migrados):>5}  → {OUTPUT_MIGRADOS.name}')
    print(f'  Obsoletos (ya no en v3):   {len(df_obsoletos):>5}  → {OUTPUT_OBSOLETOS.name}')
    pct = 100 * len(df_migrados) / len(df_labels) if df_labels is not None and len(df_labels) else 0
    print(f'  Tasa de migración: {pct:.1f}%')

    if len(df_migrados):
        print('\nDistribución de clasificaciones migradas:')
        for cls, cnt in df_migrados['classification'].value_counts().items():
            print(f'    {cls}: {cnt}')

    if len(df_obsoletos):
        print(f'\n⚠  {len(df_obsoletos)} clasificaciones obsoletas se guardaron en '
              f'{OUTPUT_OBSOLETOS.name} (no se borraron).')

    print('\nLos archivos originales labels.csv y labels_calib.csv NO fueron modificados.')
    print('Para usar las clasificaciones migradas con v3, ver labels_migrados_v3.csv')
    print('─' * 60)


if __name__ == '__main__':
    main()
