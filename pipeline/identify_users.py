"""
identify_users.py — Resumen por usuario para identificar a los 13 humanos
detrás de los deviceIds aleatorios actuales.

El problema: antes del fix de "alias auto-derivado", el deviceId era un código
aleatorio de 5 caracteres (ej. A23BY) generado por el navegador.  Si el usuario
limpia la cache, pierde ese código y vuelve a parecer un usuario nuevo.

Para recuperar el progreso de los usuarios actuales (UNA SOLA VEZ — después
nadie más volverá a perder su ID gracias al alias), Frank necesita identificar
quién es quién.  Este script imprime, por deviceId:

    - cantidad de votos
    - primer y último voto (timestamps)
    - días activos
    - última calibración: pista útil porque todos arrancan por ahí

Con esa info, Frank pregunta a sus contactos: «¿más o menos cuándo
clasificaste y cuántas crees que llevabas?» y cruza contra esta tabla.

También imprime un mensaje plantilla en español para mandar a un grupo o
broadcast — Frank lo copia y pega.

Uso:
    python pipeline/identify_users.py
"""

import json
import urllib.request as urlreq
from collections import defaultdict
from datetime import datetime, timezone
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


_env         = _load_env()
SUPABASE_URL = _env.get('SUPABASE_URL', '').rstrip('/')
ANON_KEY     = _env.get('SUPABASE_ANON_KEY', '')

GROUP_OFFSET   = 10_000_000
CALIB_PAIRS    = 120
APP_BASE_URL   = 'https://fjbautistas.github.io/Galaxy-Pair-Inspector/mobile/GalPairs.html'


# ── Supabase ─────────────────────────────────────────────────────────────────
def fetch_all() -> list[dict]:
    rows, limit, offset = [], 1000, 0
    while True:
        url = (
            f'{SUPABASE_URL}/rest/v1/clasificaciones'
            f'?select=device_id,id_par,classification,created_at'
            f'&order=created_at.asc'
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
    # Supabase devuelve 'YYYY-MM-DDTHH:MM:SS.ffff+00:00' (4 dígitos de fracción
    # según el caso) y datetime.fromisoformat solo acepta 3 o 6.  Truncamos
    # los milisegundos a 6 dígitos máximo.
    s = s.replace('Z', '+00:00')
    if '.' in s:
        head, frac_tz = s.split('.', 1)
        if '+' in frac_tz or '-' in frac_tz[1:]:
            # separar fracción y timezone
            for sep in ('+', '-'):
                idx = frac_tz.find(sep, 1)
                if idx != -1:
                    frac, tz = frac_tz[:idx], frac_tz[idx:]
                    break
            else:
                frac, tz = frac_tz, ''
        else:
            frac, tz = frac_tz, ''
        frac = (frac + '000000')[:6]   # padding/truncado a microsegundos
        s = f'{head}.{frac}{tz}'
    return datetime.fromisoformat(s)


# ── Resumen por usuario ──────────────────────────────────────────────────────
def summarize(rows: list[dict]) -> list[dict]:
    by_user: dict[str, list] = defaultdict(list)
    for r in rows:
        by_user[r['device_id']].append(r)

    out = []
    for uid, vs in by_user.items():
        vs_sorted = sorted(vs, key=lambda r: r['created_at'])
        first     = parse_ts(vs_sorted[0]['created_at'])
        last      = parse_ts(vs_sorted[-1]['created_at'])
        days      = len({parse_ts(r['created_at']).date() for r in vs_sorted})
        n_calib   = sum(1 for r in vs_sorted if int(r['id_par']) < CALIB_PAIRS)
        n_groups  = sum(1 for r in vs_sorted if int(r['id_par']) >= GROUP_OFFSET)
        out.append({
            'device_id':  uid,
            'total':      len(vs_sorted),
            'calib':      n_calib,
            'pairs':      len(vs_sorted) - n_calib - n_groups,
            'groups':     n_groups,
            'first':      first,
            'last':       last,
            'days_active': days,
        })

    out.sort(key=lambda d: -d['total'])
    return out


# ── Salida ───────────────────────────────────────────────────────────────────
def print_table(summary: list[dict]) -> None:
    fmt = '{:<10} {:>6}  {:>5} {:>6} {:>6}  {:>16}  {:>16}  {:>3}'
    print(fmt.format(
        'deviceId', 'Total', 'Calib', 'Pares', 'Grupos',
        'Primero (UTC)', 'Último (UTC)', 'Día'
    ))
    print('-' * 95)
    for d in summary:
        print(fmt.format(
            d['device_id'],
            d['total'], d['calib'], d['pairs'], d['groups'],
            d['first'].strftime('%Y-%m-%d %H:%M'),
            d['last'].strftime('%Y-%m-%d %H:%M'),
            d['days_active'],
        ))


def print_message_template(summary: list[dict]) -> None:
    """Mensaje en español para broadcast — Frank lo copia y pega."""
    print()
    print('=' * 70)
    print('MENSAJE SUGERIDO PARA BROADCAST (copiar y pegar)')
    print('=' * 70)

    print(f"""
Hola, gracias por estar clasificando galaxias 🙌

Hubo un cambio importante en la app: ahora cada usuario elige un *alias*
permanente (tu nombre, un apodo, lo que recuerdes después).  Eso evita
que pierdas tu progreso si limpias la caché del navegador.

A los que ya estaban clasificando antes de este cambio, les asigné un
código aleatorio.  Para que tu progreso no se quede huérfano, necesito
saber cuál es el tuyo.

Por favor respóndeme con:

  1. ¿Más o menos cuántas galaxias has clasificado?
  2. ¿Cuándo empezaste? (día aproximado)

Con eso te mando un enlace personal para recuperar tu progreso, y de
ahí en adelante eliges tu alias y nunca más vuelve a pasar.

Si nunca habías entrado o solo lo probaste un rato, abre el link normal:
  {APP_BASE_URL}
y elige un alias.  Listo.
""".strip())
    print('=' * 70)


def main() -> None:
    if not SUPABASE_URL or not ANON_KEY:
        print('ERROR: falta SUPABASE_URL o SUPABASE_ANON_KEY en .env')
        return

    print('Descargando clasificaciones desde Supabase…')
    rows = fetch_all()
    print(f'  {len(rows)} filas, {len({r["device_id"] for r in rows})} usuarios distintos\n')

    summary = summarize(rows)
    print_table(summary)
    print_message_template(summary)

    # Guardar JSON con detalle para referencia
    out_path = Path('outputs/catalogs/users_identification.json')
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump([{**d, 'first': d['first'].isoformat(), 'last': d['last'].isoformat()}
                   for d in summary], f, indent=2)
    print(f'\nDetalle guardado en {out_path}')


if __name__ == '__main__':
    main()
