"""
plot_dz_vs_sep.py — Distribución 2D de dz vs sep_arcsec coloreada por clasificación

Igual que plot_dz_vs_rp.py pero usando separación angular en vez de separación
proyectada física. Útil para ver el efecto del corte de 1" en los falsos positivos.

Uso:
    python pipeline/plot_dz_vs_sep.py

Requiere: .env con SUPABASE_URL, SUPABASE_ANON_KEY y PAIRS_CATALOG.
Salida:   outputs/plots/dz_vs_sep.png  (no se sube a GitHub)
"""

import json
import sys
import urllib.request as urlreq
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

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
CATALOG_PATH = _env.get('PAIRS_CATALOG', '')
OUTPUT_DIR   = Path('outputs/plots')

COLORS = {
    'Pair': '#4CAF50',
    'FP':   '#F44336',
    'PM':   '#FF9800',
}
LABELS_ES = {
    'Pair': 'Par confirmado',
    'FP':   'Falso positivo',
    'PM':   'Posible merger',
}

# ── Supabase ──────────────────────────────────────────────────────────────────
def _fetch_classifications() -> pd.DataFrame:
    rows   = []
    limit  = 1000
    offset = 0

    while True:
        url = (
            f'{SUPABASE_URL}/rest/v1/clasificaciones'
            f'?select=id_par,classification'
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

    df = pd.DataFrame(rows)
    df['id_par'] = df['id_par'].astype(int)

    def majority(group):
        counts = group['classification'].value_counts()
        if len(counts) >= 2 and counts.iloc[0] == counts.iloc[1]:
            return None
        return counts.index[0]

    labels = (
        df.groupby('id_par')
        .apply(majority)
        .dropna()
        .reset_index()
    )
    labels.columns = ['id_par', 'classification']
    return labels

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not SUPABASE_URL or not ANON_KEY:
        print('ERROR: falta SUPABASE_URL o SUPABASE_ANON_KEY en .env')
        sys.exit(1)
    if not CATALOG_PATH:
        print('ERROR: falta PAIRS_CATALOG en .env')
        sys.exit(1)
    if not Path(CATALOG_PATH).exists():
        print(f'ERROR: no se encontró el catálogo en {CATALOG_PATH}')
        sys.exit(1)

    print('Descargando clasificaciones…')
    labels = _fetch_classifications()
    print(f'  {len(labels)} pares clasificados (sin empates)')

    print('Cargando catálogo…')
    catalog = pd.read_parquet(CATALOG_PATH, columns=['id_par', 'sep_arcsec', 'dz'])
    print(f'  {len(catalog):,} pares en el catálogo')

    df = catalog.merge(labels, on='id_par', how='inner')
    print(f'  {len(df)} pares con clasificación y datos físicos')

    for label, group in df.groupby('classification'):
        if len(group) < 10:
            print(f'  Aviso: {LABELS_ES.get(label, label)} tiene solo {len(group)} puntos — KDE puede ser poco confiable')

    classes = sorted(df['classification'].unique())
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(8, 6))

    for label in classes:
        group = df[df['classification'] == label]
        color = COLORS.get(label, 'gray')

        if len(group) >= 5:
            sns.kdeplot(
                data=group, x='sep_arcsec', y='dz',
                fill=True, thresh=0.05, levels=8,
                color=color, alpha=0.4, ax=ax,
            )
            sns.kdeplot(
                data=group, x='sep_arcsec', y='dz',
                fill=False, thresh=0.05, levels=8,
                color=color, linewidths=0.8, ax=ax,
            )

        ax.scatter(
            group['sep_arcsec'], group['dz'],
            color=color, s=14, alpha=0.5, linewidths=0, zorder=3,
            label=f"{LABELS_ES.get(label, label)} (n={len(group)})",
        )

    # Línea vertical en 1" para referencia del corte
    ax.axvline(x=1.0, color='gray', linewidth=0.8, linestyle='--', alpha=0.7, label='corte 1"')
    ax.set_xlim(0,20)
    
    ax.set_xlabel('sep  [arcsec]', fontsize=11)
    ax.set_ylabel('Δz', fontsize=11)
    ax.legend(fontsize=10, framealpha=0.85)
    ax.grid(True, linewidth=0.3, alpha=0.5)
    fig.suptitle('Separación angular vs diferencia de redshift', fontsize=13)
    plt.tight_layout()

    out = OUTPUT_DIR / 'dz_vs_sep.png'
    fig.savefig(out, dpi=150, bbox_inches='tight')
    print(f'Plot guardado en {out}')
    plt.show()

if __name__ == '__main__':
    main()
