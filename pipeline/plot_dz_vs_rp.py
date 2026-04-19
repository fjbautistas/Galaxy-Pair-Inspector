"""
plot_dz_vs_rp.py — Scatter plot de dz vs rp_kpc coloreado por clasificación

Cruza el catálogo de pares con las clasificaciones de Supabase y genera un
scatter plot para ver dónde se concentran los falsos positivos en el espacio
de parámetros (rp_kpc, dz).

Uso:
    python pipeline/plot_dz_vs_rp.py

Requiere: .env con SUPABASE_URL, SUPABASE_ANON_KEY y PAIRS_CATALOG.
Salida:   outputs/plots/dz_vs_rp.png  (no se sube a GitHub)
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
    'Pair': '#4CAF50',  # verde
    'FP':   '#F44336',  # rojo
    'PM':   '#FF9800',  # naranja
}
LABELS_ES = {
    'Pair': 'Par confirmado',
    'FP':   'Falso positivo',
    'PM':   'Posible merger',
}

# ── Supabase ──────────────────────────────────────────────────────────────────
def _fetch_classifications() -> pd.DataFrame:
    """Descarga clasificaciones con mayoría de votos desde Supabase."""
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

    # Mayoría de votos por par
    def majority(group):
        counts = group['classification'].value_counts()
        if len(counts) >= 2 and counts.iloc[0] == counts.iloc[1]:
            return None  # empate
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
    catalog = pd.read_parquet(CATALOG_PATH, columns=['id_par', 'rp_kpc', 'dz'])
    print(f'  {len(catalog):,} pares en el catálogo')

    # Cruzar
    df = catalog.merge(labels, on='id_par', how='inner')
    print(f'  {len(df)} pares con clasificación y datos físicos')

    # Advertencia si hay pocas muestras por clase
    for label, group in df.groupby('classification'):
        if len(group) < 10:
            print(f'  Aviso: {LABELS_ES.get(label, label)} tiene solo {len(group)} puntos — KDE puede ser poco confiable')

    # Plot: un panel por clasificación con KDE 2D + marginals
    classes    = sorted(df['classification'].unique())
    n          = len(classes)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(1, n, figsize=(6 * n, 5), sharey=True)
    if n == 1:
        axes = [axes]

    for ax, label in zip(axes, classes):
        group = df[df['classification'] == label]
        color = COLORS.get(label, 'gray')

        # KDE 2D relleno
        if len(group) >= 5:
            sns.kdeplot(
                data=group,
                x='rp_kpc',
                y='dz',
                fill=True,
                thresh=0.05,
                levels=8,
                color=color,
                alpha=0.75,
                ax=ax,
            )
            # Contornos encima
            sns.kdeplot(
                data=group,
                x='rp_kpc',
                y='dz',
                fill=False,
                thresh=0.05,
                levels=8,
                color=color,
                linewidths=0.8,
                ax=ax,
            )

        # Puntos individuales superpuestos
        ax.scatter(
            group['rp_kpc'],
            group['dz'],
            color=color,
            s=14,
            alpha=0.5,
            linewidths=0,
            zorder=3,
        )

        ax.set_title(f"{LABELS_ES.get(label, label)}  (n={len(group)})", fontsize=12)
        ax.set_xlabel('rp  [kpc]', fontsize=11)
        ax.grid(True, linewidth=0.3, alpha=0.5)

    axes[0].set_ylabel('Δz', fontsize=11)
    fig.suptitle('Separación proyectada vs diferencia de redshift', fontsize=13, y=1.02)
    plt.tight_layout()

    out = OUTPUT_DIR / 'dz_vs_rp.png'
    fig.savefig(out, dpi=150, bbox_inches='tight')
    print(f'Plot guardado en {out}')
    plt.show()

if __name__ == '__main__':
    main()
