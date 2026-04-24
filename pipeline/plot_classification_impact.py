"""
plot_classification_impact.py — Compara clasificaciones antes y después del catálogo v3.

Muestra tres histogramas:
  - Antes: todas las clasificaciones hechas hasta ahora
  - Después: solo las que tienen par equivalente en el nuevo catálogo (v3)
  - Eliminados: las que desaparecieron con el nuevo catálogo

Uso:
    python pipeline/plot_classification_impact.py

Requiere:
    - outputs/catalogs/labels.csv
    - outputs/catalogs/labels_calib.csv
    - PAIRS_CATALOG en .env (apuntando al parquet v3)
"""

import os
import sys
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
OUTPUT_PLOT  = Path('outputs/plots/before_after_v3.png')
LABELS_PATH  = Path('outputs/catalogs/labels.csv')
CALIB_PATH   = Path('outputs/catalogs/labels_calib.csv')

CATS   = ['FP', 'Pair', 'PM']
COLORS = {'FP': '#FF5A5A', 'Pair': '#50BEFF', 'PM': '#FFFF32'}

# ── Leer .env para obtener PAIRS_CATALOG ──────────────────────────────────────
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


def main():
    env = _load_env()
    pairs_catalog = env.get('PAIRS_CATALOG', '')
    if not pairs_catalog or not Path(pairs_catalog).exists():
        sys.exit(f'ERROR: PAIRS_CATALOG no encontrado en .env o no existe: {pairs_catalog}')

    # ── Cargar datos ──────────────────────────────────────────────────────────
    pairs      = pd.read_parquet(pairs_catalog)
    labels     = pd.read_csv(LABELS_PATH)
    calib      = pd.read_csv(CALIB_PATH)
    all_labels = pd.concat([labels, calib], ignore_index=True)

    new_ids = set(pairs['id_par'])

    before = all_labels['classification'].value_counts().reindex(CATS, fill_value=0)
    kept   = all_labels[all_labels['id_par'].isin(new_ids)]['classification'].value_counts().reindex(CATS, fill_value=0)
    lost   = all_labels[~all_labels['id_par'].isin(new_ids)]['classification'].value_counts().reindex(CATS, fill_value=0)

    print(f'Total clasificados:   {before.sum()}')
    print(f'En catálogo nuevo:    {kept.sum()}  ({100*kept.sum()/before.sum():.1f}%)')
    print(f'Eliminados:           {lost.sum()}  ({100*lost.sum()/before.sum():.1f}%)')

    # ── Plot ──────────────────────────────────────────────────────────────────
    # ── Barras apiladas: kept (sólido) + lost (rayado encima) ─────────────────
    fig, ax = plt.subplots(figsize=(7, 5))
    fig.patch.set_facecolor('#111111')
    ax.set_facecolor('#1a1a1a')

    x = range(len(CATS))
    w = 0.5

    bars_kept = ax.bar(x, kept.values, w,
                       color=[COLORS[c] for c in CATS],
                       alpha=0.9, edgecolor='#111111', linewidth=1.5,
                       label='v3')
    bars_lost = ax.bar(x, lost.values, w,
                       bottom=kept.values,
                       color=[COLORS[c] for c in CATS],
                       alpha=0.35, edgecolor='white', linewidth=1.0,
                       hatch='//', label='Eliminados')

    # Etiquetas: valor kept / valor lost encima de cada barra
    for i, cat in enumerate(CATS):
        k, l, b = int(kept[cat]), int(lost[cat]), int(before[cat])
        # porcentaje eliminado
        pct_lost = 100 * l / b if b else 0
        # número dentro de la parte kept
        ax.text(i, kept.values[i] / 2, str(k),
                ha='center', va='center', color='white', fontsize=11, fontweight='bold')
        # número + % dentro de la parte lost (solo si hay eliminados)
        if l > 0:
            ax.text(i, kept.values[i] + lost.values[i] / 2,
                    f'−{l}\n({pct_lost:.0f}%)',
                    ha='center', va='center', color='white', fontsize=9)

    ax.set_xticks(list(x))
    ax.set_xticklabels(CATS, color='white', fontsize=13, fontweight='bold')
    ax.set_ylabel('N° de pares', color='#cccccc', fontsize=11)
    ax.tick_params(colors='#aaaaaa')
    for spine in ax.spines.values():
        spine.set_color('#444444')

    import matplotlib.patches as mpatches
    leg = ax.legend(
        handles=[
            mpatches.Patch(facecolor='#888888', alpha=0.9,           label='v3'),
            mpatches.Patch(facecolor='#888888', alpha=0.35, hatch='//', label='Eliminados'),
        ],
        facecolor='#222222', edgecolor='#555555', labelcolor='white', fontsize=10,
    )

    plt.tight_layout()

    OUTPUT_PLOT.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUTPUT_PLOT, dpi=150, bbox_inches='tight', facecolor='#111111')
    print(f'Guardado: {OUTPUT_PLOT}')


if __name__ == '__main__':
    main()
