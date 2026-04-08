"""
import_from_mobile.py — Importa clasificaciones del dispositivo móvil al sistema principal.

Uso:
    python import_from_mobile.py mobile_cl_2026-04-06.json

Por defecto solo importa pares NUEVOS (no clasificados aún en el escritorio).
Usa --overwrite para que las clasificaciones móviles sobreescriban las del escritorio.
"""

import argparse
import json
import sys
from pathlib import Path

PROGRESS_FILE = 'outputs/catalogs/progress.json'

# ─────────────────────────────────────────────────────────────────────────────

def load_progress(path: str) -> dict:
    if Path(path).exists():
        with open(path) as f:
            return json.load(f)
    return {
        'current_index':    0,
        'false_positives':  [],
        'confirmed_pairs':  [],
        'possible_mergers': [],
        'pending_retry':    [],
    }


def save_progress(state: dict, path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        json.dump(state, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description='Importa clasificaciones móviles.')
    parser.add_argument('mobile_json', help='Archivo JSON exportado desde la app móvil')
    parser.add_argument('--overwrite', action='store_true',
                        help='Sobreescribir clasificaciones existentes del escritorio')
    args = parser.parse_args()

    # Leer JSON móvil
    mobile_path = Path(args.mobile_json)
    if not mobile_path.exists():
        print(f'Error: no se encontró el archivo {mobile_path}')
        sys.exit(1)

    with open(mobile_path) as f:
        mobile_data = json.load(f)

    mobile_entries = mobile_data.get('classifications', [])
    dev = mobile_data.get('device_id', '—')
    print(f'Archivo móvil: {mobile_path.name}')
    print(f'  Dispositivo:  {dev}')
    print(f'  Exportado:    {mobile_data.get("exported_at", "—")}')
    print(f'  Clasificaciones en archivo: {len(mobile_entries)}')

    # Leer progreso del escritorio
    state = load_progress(PROGRESS_FILE)

    fp_set   = set(state.get('false_positives',  []))
    pair_set = set(state.get('confirmed_pairs',   []))
    pm_set   = set(state.get('possible_mergers',  []))
    all_done = fp_set | pair_set | pm_set

    added = {'FP': 0, 'Pair': 0, 'PM': 0}
    skipped = 0

    for entry in mobile_entries:
        id_par = int(entry['id_par'])
        cl     = entry['classification']

        if id_par in all_done and not args.overwrite:
            skipped += 1
            continue

        # Si overwrite: remover de listas anteriores
        if args.overwrite:
            fp_set.discard(id_par)
            pair_set.discard(id_par)
            pm_set.discard(id_par)

        if cl == 'FP':
            fp_set.add(id_par)
        elif cl == 'Pair':
            pair_set.add(id_par)
        elif cl == 'PM':
            pm_set.add(id_par)
        else:
            print(f'  Advertencia: clasificación desconocida "{cl}" para id_par {id_par}')
            continue

        added[cl] += 1

    # Guardar
    state['false_positives']  = sorted(fp_set)
    state['confirmed_pairs']  = sorted(pair_set)
    state['possible_mergers'] = sorted(pm_set)

    save_progress(state, PROGRESS_FILE)

    total_added = sum(added.values())
    print(f'\n✓ Importación completada:')
    print(f'  Nuevas — FP: {added["FP"]}  |  Pares: {added["Pair"]}  |  Mergers: {added["PM"]}')
    if skipped:
        print(f'  Omitidas (ya clasificadas en escritorio): {skipped}')
        print(f'  Usa --overwrite para forzar sobreescritura.')
    print(f'\n  Total en progress.json ahora:')
    print(f'    FP: {len(fp_set)}  |  Pares: {len(pair_set)}  |  Mergers: {len(pm_set)}')

    if total_added > 0:
        print(f'\nNota: las imágenes de los pares nuevos no se descargan automáticamente.')
        print(f'Usa "Limpiar imágenes guardadas" (⚙) en la app de escritorio para obtenerlas.')


if __name__ == '__main__':
    main()
