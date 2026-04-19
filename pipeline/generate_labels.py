"""
generate_labels.py — Descarga clasificaciones de Supabase y genera labels.csv

Descarga todas las clasificaciones de la tabla `clasificaciones`, aplica
mayoría de votos por par y escribe los siguientes archivos:

    outputs/catalogs/labels.csv          pares de trabajo (id_par < 10_000_000)
    outputs/catalogs/labels_calib.csv    pares de calibración (id_par < 150)
    outputs/catalogs/labels_groups.csv   grupos (id_par >= 10_000_000, desplazados)

Los id_par de grupos están almacenados en Supabase como group_id + 10_000_000
para evitar colisión con los pares.  Este script los detecta automáticamente
y regenera el group_id original.

Categorías de clasificación:
    Pares:  FP, Pair, PM
    Grupos: FP, GROUP, PM, PP (posible par dentro de un grupo)

Reglas:
    - Un solo voto basta para incluir un par o grupo.
    - Empates (dos labels con el mismo número de votos) se omiten.

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

_env         = _load_env()
SUPABASE_URL = _env.get('SUPABASE_URL', '').rstrip('/')
ANON_KEY     = _env.get('SUPABASE_ANON_KEY', '')
CALIB_PAIRS     = 120          # pares de calibración (índices 0–119)
CALIB_GROUPS    = 80           # grupos de calibración (índices 0–79)
GROUPS_OFFSET   = 10_000_000   # group_id + GROUPS_OFFSET = id_par en Supabase
OUTPUT_DIR      = Path('outputs/catalogs')

# ── Supabase ──────────────────────────────────────────────────────────────────
def _fetch_all_classifications() -> list[dict]:
    """Descarga todas las filas de clasificaciones usando paginación."""
    rows   = []
    limit  = 1000
    offset = 0

    while True:
        url = (
            f'{SUPABASE_URL}/rest/v1/clasificaciones'
            f'?select=device_id,id_par,classification'
            f'&limit={limit}&offset={offset}'
        )
        req = urlreq.Request(url, headers={
            'apikey':        ANON_KEY,
            'Authorization': f'Bearer {ANON_KEY}',
        })
        with urlreq.urlopen(req) as resp:
            page = json.loads(resp.read())

        rows.extend(page)
        if len(page) < limit:
            break
        offset += limit

    return rows

# ── Lógica de etiquetas ───────────────────────────────────────────────────────
def _majority_vote(rows: list[dict]) -> list[dict]:
    """
    Agrupa por id_par, aplica mayoría de votos.
    Devuelve lista de dicts con id_par, classification, n_votes, agreement.
    Los empates se omiten.
    """
    # Agrupar votos por par
    votes: dict[int, list[str]] = {}
    for row in rows:
        par_id = int(row['id_par'])
        votes.setdefault(par_id, []).append(row['classification'])

    results = []
    skipped_ties = 0

    for par_id, labels in sorted(votes.items()):
        counts   = Counter(labels)
        n_votes  = len(labels)
        top_two  = counts.most_common(2)

        # Detectar empate
        if len(top_two) == 2 and top_two[0][1] == top_two[1][1]:
            skipped_ties += 1
            continue

        winner    = top_two[0][0]
        agreement = round(top_two[0][1] / n_votes, 4)

        results.append({
            'id_par':         par_id,
            'classification': winner,
            'n_votes':        n_votes,
            'agreement':      agreement,
        })

    if skipped_ties:
        print(f'  Empates omitidos: {skipped_ties} elemento(s)')

    return results

# ── Escritura CSV ─────────────────────────────────────────────────────────────
def _write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ['id_par', 'classification', 'n_votes', 'agreement']
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f'  {len(rows)} pares  →  {path}')

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not SUPABASE_URL or not ANON_KEY:
        print('ERROR: falta SUPABASE_URL o SUPABASE_ANON_KEY en .env')
        sys.exit(1)

    print('Descargando clasificaciones desde Supabase…')
    raw = _fetch_all_classifications()
    print(f'  {len(raw)} filas descargadas')

    # Separar filas de pares y de grupos ANTES de la mayoría de votos
    raw_pairs  = [r for r in raw if int(r['id_par']) <  GROUPS_OFFSET]
    raw_groups = [r for r in raw if int(r['id_par']) >= GROUPS_OFFSET]

    print(f'  Pares: {len(raw_pairs)} votos  |  Grupos: {len(raw_groups)} votos')

    # ── Pares ─────────────────────────────────────────────────────────────────
    print('Aplicando mayoría de votos (pares)…')
    labeled_pairs = _majority_vote(raw_pairs)

    # Separar calibración (id_par < CALIB_PAIRS) y trabajo
    calib = [r for r in labeled_pairs if r['id_par'] <  CALIB_PAIRS]
    work  = [r for r in labeled_pairs if r['id_par'] >= CALIB_PAIRS]

    print('Escribiendo archivos de pares…')
    _write_csv(work,  OUTPUT_DIR / 'labels.csv')
    _write_csv(calib, OUTPUT_DIR / 'labels_calib.csv')

    # ── Grupos ────────────────────────────────────────────────────────────────
    if raw_groups:
        print('Aplicando mayoría de votos (grupos)…')
        labeled_groups_raw = _majority_vote(raw_groups)

        # Revertir el desplazamiento: group_id = id_par - GROUPS_OFFSET
        labeled_groups = []
        for r in labeled_groups_raw:
            labeled_groups.append({
                'group_id':       r['id_par'] - GROUPS_OFFSET,
                'classification': r['classification'],
                'n_votes':        r['n_votes'],
                'agreement':      r['agreement'],
            })

        # Separar calibración (group_id < CALIB_GROUPS) y trabajo
        calib_groups = [r for r in labeled_groups if r['group_id'] <  CALIB_GROUPS]
        work_groups  = [r for r in labeled_groups if r['group_id'] >= CALIB_GROUPS]

        fieldnames = ['group_id', 'classification', 'n_votes', 'agreement']
        for rows, path in [
            (work_groups,  OUTPUT_DIR / 'labels_groups.csv'),
            (calib_groups, OUTPUT_DIR / 'labels_groups_calib.csv'),
        ]:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            print(f'  {len(rows)} grupos  →  {path}')
    else:
        print('  Sin clasificaciones de grupos aún.')

    print('Listo.')

if __name__ == '__main__':
    main()
