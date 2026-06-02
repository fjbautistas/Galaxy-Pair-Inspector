"""
identify_users.py — Estadísticas por usuario (campaña v5_3).

Muestra por device_id:
  - votos totales, pares de calibración, pares de trabajo, grupos
  - primer y último voto (created_at servidor)
  - días activos
  - fuente: v5_3_app vs migrated_v3

Por defecto solo muestra actividad real (source = 'v5_3_app').
Para incluir votos migrados de v3 pasa --all.

Uso:
    python pipeline/identify_users.py
    python pipeline/identify_users.py --all
"""

import argparse
import json
import urllib.request as urlreq
from collections import defaultdict
from datetime import datetime
from pathlib import Path


# ── .env ─────────────────────────────────────────────────────────────────────
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


_env             = _load_env()
SUPABASE_URL     = _env.get('SUPABASE_URL', '').rstrip('/')
ANON_KEY         = _env.get('SUPABASE_SERVICE_ROLE_KEY', '') or _env.get('SUPABASE_ANON_KEY', '')

CALIB_PAIRS       = 120   # primeros N pares son calibración base (rp<20 kpc)
SUPP_CALIB_JSON   = 'data/supplementary_calib_ids_v5_3.json'
APP_BASE_URL      = 'https://fjbautistas.github.io/Galaxy-Pair-Inspector/mobile/GalPairs.html'


def _load_supp_calib_ids() -> set:
    """Carga los pair_uid del set de calibración suplementaria v5_3."""
    try:
        with open(SUPP_CALIB_JSON) as f:
            data = json.load(f)
        ids = set(data.get('pair_uid', []))
        print(f'  Calibración suplementaria: {len(ids)} pair_uids cargados')
        return ids
    except FileNotFoundError:
        print(f'  Aviso: {SUPP_CALIB_JSON} no encontrado — supp calib no contada')
        return set()


_SUPP_CALIB_IDS: set = set()  # se inicializa en main()


# ── Supabase ─────────────────────────────────────────────────────────────────
def fetch_all(only_app: bool = True) -> list[dict]:
    rows, limit, offset = [], 1000, 0
    source_filter = '&source=eq.v5_3_app' if only_app else ''
    while True:
        url = (
            f'{SUPABASE_URL}/rest/v1/clasificaciones'
            f'?select=device_id,item_type,item_uid,id_par_v5,classification,source,created_at'
            f'&order=created_at.asc'
            f'{source_filter}'
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


def parse_ts(s: str) -> datetime:
    s = s.replace('Z', '+00:00')
    if '.' in s:
        head, frac_tz = s.split('.', 1)
        for sep in ('+', '-'):
            idx = frac_tz.find(sep, 1)
            if idx != -1:
                frac, tz = frac_tz[:idx], frac_tz[idx:]
                break
        else:
            frac, tz = frac_tz, ''
        frac = (frac + '000000')[:6]
        s = f'{head}.{frac}{tz}'
    return datetime.fromisoformat(s)


# ── Resumen por usuario ──────────────────────────────────────────────────────
def _is_calib(row: dict) -> bool:
    """Par de calibración base (id_par_v5 < CALIB_PAIRS) o suplementaria (pair_uid en _SUPP_CALIB_IDS)."""
    if row.get('item_type') != 'pair':
        return False
    # Calibración base: primeros 120 pares (rp<20 kpc)
    v5 = row.get('id_par_v5')
    if v5 is not None and int(v5) < CALIB_PAIRS:
        return True
    # Calibración suplementaria: pair_uid en el set cargado desde JSON
    uid = row.get('item_uid', '')
    return uid in _SUPP_CALIB_IDS


def summarize(rows: list[dict]) -> list[dict]:
    by_user: dict[str, list] = defaultdict(list)
    for r in rows:
        by_user[r['device_id']].append(r)

    out = []
    for uid, vs in by_user.items():
        vs_sorted = sorted(vs, key=lambda r: r['created_at'])
        first      = parse_ts(vs_sorted[0]['created_at'])
        last       = parse_ts(vs_sorted[-1]['created_at'])
        days       = len({parse_ts(r['created_at']).date() for r in vs_sorted})
        n_calib    = sum(1 for r in vs_sorted if _is_calib(r))
        n_groups   = sum(1 for r in vs_sorted if r.get('item_type') == 'group')
        n_pairs_w  = sum(1 for r in vs_sorted if r.get('item_type') == 'pair' and not _is_calib(r))
        out.append({
            'device_id':   uid,
            'total':       len(vs_sorted),
            'calib':       n_calib,
            'pairs_work':  n_pairs_w,
            'groups':      n_groups,
            'first':       first,
            'last':        last,
            'days_active': days,
        })

    out.sort(key=lambda d: -d['total'])
    return out


# ── Salida ───────────────────────────────────────────────────────────────────
def print_table(summary: list[dict]) -> None:
    fmt = '{:<10} {:>6}  {:>5} {:>10} {:>6}  {:>16}  {:>16}  {:>4}'
    print(fmt.format(
        'deviceId', 'Total', 'Calib', 'Pares(work)', 'Grupos',
        'Primero (UTC)', 'Último (UTC)', 'Días'
    ))
    print('-' * 100)
    for d in summary:
        print(fmt.format(
            d['device_id'],
            d['total'], d['calib'], d['pairs_work'], d['groups'],
            d['first'].strftime('%Y-%m-%d %H:%M'),
            d['last'].strftime('%Y-%m-%d %H:%M'),
            d['days_active'],
        ))


def print_totals(summary: list[dict]) -> None:
    total    = sum(d['total']      for d in summary)
    calib    = sum(d['calib']      for d in summary)
    pairs_w  = sum(d['pairs_work'] for d in summary)
    groups   = sum(d['groups']     for d in summary)
    print(f'\nTotal usuarios: {len(summary)}  |  '
          f'Votos: {total:,}  (calib {calib:,} · pares {pairs_w:,} · grupos {groups:,})')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--all', action='store_true',
                        help='Incluir votos migrados de v3 (source=migrated_v3)')
    args = parser.parse_args()

    if not SUPABASE_URL or not ANON_KEY:
        print('ERROR: falta SUPABASE_URL o SUPABASE_ANON_KEY en .env')
        return

    global _SUPP_CALIB_IDS
    _SUPP_CALIB_IDS = _load_supp_calib_ids()

    only_app = not args.all
    label    = 'actividad real (v5_3_app)' if only_app else 'todos los votos (v5_3_app + migrated_v3)'
    print(f'Descargando clasificaciones desde Supabase — {label}…')
    rows = fetch_all(only_app=only_app)
    print(f'  {len(rows):,} filas, {len({r["device_id"] for r in rows})} usuarios distintos\n')

    summary = summarize(rows)
    print_table(summary)
    print_totals(summary)

    out_path = Path('outputs/catalogs/users_identification.json')
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump([{**d,
                    'first': d['first'].isoformat(),
                    'last':  d['last'].isoformat()}
                   for d in summary], f, indent=2)
    print(f'\nDetalle guardado en {out_path}')


if __name__ == '__main__':
    main()
