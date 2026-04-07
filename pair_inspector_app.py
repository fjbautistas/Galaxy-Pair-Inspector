"""
pair_inspector_app.py — Clasificador visual de pares de galaxias (escritorio)
"""

import json
import os
import time
import threading
import webbrowser
from io import BytesIO
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import tkinter as tk
from tkinter import ttk, messagebox

import numpy as np
import pandas as pd
import requests
from PIL import Image, ImageDraw, ImageTk

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN — mismo estilo que el notebook
# ══════════════════════════════════════════════════════════════════════════════

CATALOG_PATH = (
    '/Users/frank/Documents/Estudio-PhD/Semestre-2025-II/Tesis_I/'
    'Galaxy_Pairs/Galaxy_pairs/outputs/catalogs/interacting/'
    'DESI_int_legacyID_pairs.parquet'
)

PROGRESS_FILE  = 'outputs/catalogs/progress.json'
OUTPUT_CSV     = 'outputs/catalogs/false_positives.csv'
OUTPUT_CSV_PM  = 'outputs/catalogs/possible_mergers.csv'
OUTPUT_CSV_PAR = 'outputs/catalogs/confirmed_pairs.csv'
FP_IMG_DIR     = 'outputs/fp_images'
PM_IMG_DIR     = 'outputs/pm_images'
PAIR_IMG_DIR   = 'outputs/pair_images'

RP_MAX_KPC = 12.0

GRID_COLS = 4
GRID_ROWS = 2
PAGE_SIZE = GRID_COLS * GRID_ROWS

LS_LAYER       = 'ls-dr10'
IMG_SIZE_PX    = 256   # resolución de descarga — 256×256 para entrenamiento ML
PADDING_FACTOR = 2.5
N_WORKERS      = 8      # reducido para no saturar Legacy Survey
TIMEOUT        = 15     # segundos por intento (aumentado)

CELL_SIZE      = 420    # tamaño de celda en la UI (px) — más grande para monitor grande

CIRCLE_RADIUS  = 4              # radio del anillo exterior (muy transparente)
CIRCLE_ALPHA   = 40             # anillo casi invisible (solo referencia)
CROSS_SIZE     = 4              # semilongitud del brazo de la cruz/X
CROSS_ALPHA    = 130            # marcadores semitransparentes
COLOR_G1       = (255, 90,  90)
COLOR_G2       = (80,  190, 255)
TEXT_COLOR     = (255, 255, 50)

# Registro de errores de descarga: (ra_mid, dec_mid) → mensaje de error
_fetch_errors: dict = {}

# Color neutro para los botones de clasificación (mismo en activo e inactivo)
BTN_GRAY     = '#484848'

# Colores de fondo para celdas según clasificación
BG_DEFAULT   = '#1e1e1e'
BG_FP        = '#5a1010'   # rojo oscuro — Falso positivo
BG_PM        = '#4a3000'   # ámbar oscuro — Merger
BG_PAIR      = '#0f4020'   # verde oscuro — Par confirmado
BG_SELECTED  = '#1a1a50'   # azul oscuro — seleccionada

# ══════════════════════════════════════════════════════════════════════════════
# FUNCIONES DE IMAGEN — reutilizadas del notebook
# ══════════════════════════════════════════════════════════════════════════════

def _adaptive_pixscale(sep_arcsec: float) -> float:
    """Pixscale adaptativo para que el par quepa en la imagen."""
    fov = sep_arcsec * PADDING_FACTOR
    return float(np.clip(fov / IMG_SIZE_PX, 0.3, 2.0))


def _legacy_url(ra: float, dec: float, pixscale: float) -> str:
    return (f'https://www.legacysurvey.org/viewer/cutout.jpg'
            f'?ra={ra:.6f}&dec={dec:.6f}'
            f'&pixscale={pixscale}&layer={LS_LAYER}&size={IMG_SIZE_PX}')


def _skyviewer_url(ra: float, dec: float, sep_arcsec: float) -> str:
    """URL del visor interactivo de Legacy Survey centrado en el midpoint del par."""
    # Zoom adaptativo: separaciones pequeñas → más zoom
    if sep_arcsec < 5:
        zoom = 16
    elif sep_arcsec < 15:
        zoom = 15
    elif sep_arcsec < 40:
        zoom = 14
    else:
        zoom = 13
    return (f'https://www.legacysurvey.org/viewer'
            f'?ra={ra:.6f}&dec={dec:.6f}&layer={LS_LAYER}&zoom={zoom}')


def _fetch_one(ra1, dec1, ra2, dec2, sep_arcsec) -> 'Image.Image | None':
    """Descarga un recorte con hasta 3 reintentos y backoff.
    Registra el error en _fetch_errors si falla."""
    ra_mid  = (ra1 + ra2) / 2.0
    dec_mid = (dec1 + dec2) / 2.0
    ps      = _adaptive_pixscale(sep_arcsec)
    url     = _legacy_url(ra_mid, dec_mid, ps)
    key     = (round(ra_mid, 5), round(dec_mid, 5))
    last_err = ''
    for intento in range(3):
        try:
            resp = requests.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            img  = Image.open(BytesIO(resp.content)).convert('RGB')
            _fetch_errors.pop(key, None)   # éxito — limpiar error previo
            return img
        except requests.exceptions.Timeout:
            last_err = f'Timeout ({TIMEOUT}s) — intento {intento+1}/3'
        except requests.exceptions.ConnectionError:
            last_err = 'Sin conexión a Legacy Survey'
        except Exception as exc:
            last_err = str(exc)[:60]
        if intento < 2:
            time.sleep(0.5 * (intento + 1))
    _fetch_errors[key] = last_err
    return None


def fetch_page_parallel(rows: list) -> list:
    """Descarga todas las imágenes de la página en paralelo."""
    results = [None] * len(rows)
    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        futures = {
            pool.submit(_fetch_one,
                        r['ra1'], r['dec1'], r['ra2'], r['dec2'],
                        r['sep_arcsec']): i
            for i, r in enumerate(rows)
        }
        for fut in as_completed(futures):
            results[futures[fut]] = fut.result()
    return results


def _radec_to_pixel(ra, dec, ra_mid, dec_mid, pixscale):
    cos_dec   = np.cos(np.radians(dec_mid))
    dx_arcsec = (ra_mid - ra) * cos_dec * 3600.0
    dy_arcsec = (dec - dec_mid) * 3600.0
    cx = IMG_SIZE_PX / 2 + dx_arcsec / pixscale
    cy = IMG_SIZE_PX / 2 - dy_arcsec / pixscale
    return cx, cy


def annotate_image(img: Image.Image, row: dict, rp_col: 'str | None') -> Image.Image:
    """Dibuja círculos semitransparentes, línea de conexión y etiqueta rp."""
    base = img.copy().resize((IMG_SIZE_PX, IMG_SIZE_PX)).convert('RGBA')

    ra1, dec1 = row['ra1'], row['dec1']
    ra2, dec2 = row['ra2'], row['dec2']
    sep = float(row.get('sep_arcsec',
                np.hypot((ra1-ra2)*np.cos(np.radians((dec1+dec2)/2))*3600,
                         (dec1-dec2)*3600)))
    ra_mid  = (ra1 + ra2) / 2.0
    dec_mid = (dec1 + dec2) / 2.0
    ps      = _adaptive_pixscale(sep)

    x1, y1 = _radec_to_pixel(ra1, dec1, ra_mid, dec_mid, ps)
    x2, y2 = _radec_to_pixel(ra2, dec2, ra_mid, dec_mid, ps)
    r  = CIRCLE_RADIUS
    a  = CIRCLE_ALPHA     # anillo de referencia — casi invisible
    ca = CROSS_ALPHA      # marcadores de posición — bien visibles
    c  = CROSS_SIZE

    # Capa semitransparente: línea de conexión + marcadores de posición
    overlay = Image.new('RGBA', base.size, (0, 0, 0, 0))
    odraw   = ImageDraw.Draw(overlay)

    # Línea conectora
    odraw.line([x1, y1, x2, y2], fill=(220, 220, 220, 100), width=1)

    # Sombra oscura primero (1 px más grande) → el marcador resalta sobre
    # cualquier fondo, claro u oscuro
    shadow = (0, 0, 0, ca)

    # Galaxia 1 — cruz (+) roja con sombra
    for dx, dy in ((1, 0), (-1, 0), (0, 1), (0, -1)):   # offsets de sombra
        odraw.line([x1-c+dx, y1+dy, x1+c+dx, y1+dy], fill=shadow, width=2)
        odraw.line([x1+dx, y1-c+dy, x1+dx, y1+c+dy], fill=shadow, width=2)
    odraw.line([x1-c, y1,   x1+c, y1  ], fill=(*COLOR_G1, ca), width=1)
    odraw.line([x1,   y1-c, x1,   y1+c], fill=(*COLOR_G1, ca), width=1)
    odraw.ellipse([x1-r, y1-r, x1+r, y1+r], outline=(*COLOR_G1, a), width=1)

    # Galaxia 2 — X azul con sombra
    for dx, dy in ((1, 0), (-1, 0), (0, 1), (0, -1)):
        odraw.line([x2-c+dx, y2-c+dy, x2+c+dx, y2+c+dy], fill=shadow, width=2)
        odraw.line([x2+c+dx, y2-c+dy, x2-c+dx, y2+c+dy], fill=shadow, width=2)
    odraw.line([x2-c, y2-c, x2+c, y2+c], fill=(*COLOR_G2, ca), width=1)
    odraw.line([x2+c, y2-c, x2-c, y2+c], fill=(*COLOR_G2, ca), width=1)
    odraw.ellipse([x2-r, y2-r, x2+r, y2+r], outline=(*COLOR_G2, a), width=1)

    base = Image.alpha_composite(base, overlay)

    # Etiquetas de texto sobre capa opaca (siempre legibles)
    draw   = ImageDraw.Draw(base)
    margin = 4
    if rp_col and rp_col in row:
        label = f'rp={row[rp_col]:.1f} kpc'
    else:
        label = f'{sep:.1f}"'
    draw.rectangle([margin-2, IMG_SIZE_PX-18,
                    margin+len(label)*6+2, IMG_SIZE_PX-margin],
                   fill=(0, 0, 0, 200))
    draw.text((margin, IMG_SIZE_PX-18), label, fill=TEXT_COLOR)

    if 'id_par' in row:
        id_label = f'par #{int(row["id_par"])}'
        draw.rectangle([margin-2, margin-2,
                        margin+len(id_label)*6+2, margin+14],
                       fill=(0, 0, 0, 200))
        draw.text((margin, margin), id_label, fill=TEXT_COLOR)

    return base.convert('RGB')


def make_error_tile(error_msg: str = '') -> Image.Image:
    """Tile naranja-oscuro con diagnóstico del error de descarga."""
    sz   = CELL_SIZE
    img  = Image.new('RGB', (sz, sz), color=(30, 18, 8))
    draw = ImageDraw.Draw(img)
    # Borde naranja tenue
    draw.rectangle([2, 2, sz-3, sz-3], outline=(120, 60, 10), width=2)
    # Icono y título
    draw.text((sz//2 - 52, sz//2 - 60), '⚠  Descarga fallida', fill=(255, 140, 40))
    draw.text((sz//2 - 68, sz//2 - 35), 'Legacy Survey no respondió', fill=(180, 100, 40))
    # Detalle del error (si hay)
    if error_msg:
        words, line, lines = error_msg.split(), '', []
        for w in words:
            if len(line) + len(w) + 1 > 28:
                lines.append(line); line = w
            else:
                line = (line + ' ' + w).strip()
        if line: lines.append(line)
        for i, ln in enumerate(lines[:3]):
            draw.text((12, sz//2 + i*14), ln, fill=(150, 90, 30))
    # Indicación de reintento
    draw.text((sz//2 - 72, sz - 36), '↓  usa el botón Reintentar', fill=(200, 120, 50))
    return img


# ══════════════════════════════════════════════════════════════════════════════
# PAIRVALIDATOR — lógica de persistencia y clasificación (igual que el notebook)
# ══════════════════════════════════════════════════════════════════════════════

class PairValidator:
    """Gestiona el índice de revisión y las tres listas de clasificación."""

    def __init__(self, catalog_path, progress_file,
                 fp_img_dir, pm_img_dir, pair_img_dir, rp_max_kpc=None):
        self.progress_file = progress_file
        self.fp_img_dir    = Path(fp_img_dir)
        self.pm_img_dir    = Path(pm_img_dir)
        self.pair_img_dir  = Path(pair_img_dir)
        for d in (self.fp_img_dir, self.pm_img_dir, self.pair_img_dir):
            d.mkdir(parents=True, exist_ok=True)

        if not Path(catalog_path).exists():
            raise FileNotFoundError(
                f'No se encontró el catálogo:\n{catalog_path}\n'
                'Verifica la variable CATALOG_PATH en el bloque CONFIG.')

        df_full = pd.read_parquet(catalog_path)
        required = {'ra1', 'dec1', 'ra2', 'dec2', 'id1', 'id2'}
        if missing := required - set(df_full.columns):
            raise ValueError(f'Faltan columnas en el catálogo: {missing}')

        self.rp_col = None
        for col in ('rp_kpc', 'rp_phys_kpc', 'rp'):
            if col in df_full.columns:
                self.rp_col = col
                break

        if rp_max_kpc is not None and self.rp_col:
            self.df = df_full[df_full[self.rp_col] < rp_max_kpc].reset_index(drop=True)
            print(f'Filtro rp < {rp_max_kpc} kpc: {len(df_full):,} → {len(self.df):,} pares')
        else:
            self.df = df_full.reset_index(drop=True)

        self._load_progress()

    # ── Persistencia ──────────────────────────────────────────────────────────

    def _load_progress(self):
        if os.path.exists(self.progress_file):
            with open(self.progress_file) as f:
                state = json.load(f)
            self.current_index    = state.get('current_index', 0)
            self.false_positives  = state.get('false_positives', [])
            self.possible_mergers = state.get('possible_mergers', [])
            self.confirmed_pairs  = state.get('confirmed_pairs', [])
            self.pending_retry    = state.get('pending_retry', [])
        else:
            self.current_index    = 0
            self.false_positives  = []
            self.possible_mergers = []
            self.confirmed_pairs  = []
            self.pending_retry    = []

    def save_progress(self):
        Path(self.progress_file).parent.mkdir(parents=True, exist_ok=True)
        with open(self.progress_file, 'w') as f:
            json.dump({
                'current_index'   : self.current_index,
                'false_positives' : self.false_positives,
                'possible_mergers': self.possible_mergers,
                'confirmed_pairs' : self.confirmed_pairs,
                'pending_retry'   : self.pending_retry,
                'last_saved'      : datetime.now().isoformat(),
            }, f, indent=2)

    def export_csv(self):
        results = []
        for data, path, label in [
            (self.false_positives,  OUTPUT_CSV,     'Falsos positivos'),
            (self.possible_mergers, OUTPUT_CSV_PM,  'Posibles mergers'),
            (self.confirmed_pairs,  OUTPUT_CSV_PAR, 'Pares confirmados'),
        ]:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            if data:
                pd.DataFrame(data).to_csv(path, index=False)
                results.append(f'{label}: {len(data)} filas → {path}')
            else:
                results.append(f'{label}: (vacío)')
        return '\n'.join(results)

    # ── Navegación ────────────────────────────────────────────────────────────

    def get_page(self, page_size) -> pd.DataFrame:
        return self.df.iloc[self.current_index:
                            min(self.current_index + page_size, len(self.df))]

    def advance(self, n):
        self.current_index = min(self.current_index + n, len(self.df))

    def go_back(self, n):
        self.current_index = max(self.current_index - n, 0)

    # ── Pending retry ─────────────────────────────────────────────────────────

    def add_pending(self, row: dict):
        id1, id2 = int(row['id1']), int(row['id2'])
        if any(e['id1'] == id1 and e['id2'] == id2 for e in self.pending_retry):
            return
        if self.is_false_positive(row) or self.is_possible_merger(row) or self.is_confirmed_pair(row):
            return
        self.pending_retry.append({
            'id1'   : id1,   'id2'  : id2,
            'id_par': int(row['id_par']) if 'id_par' in row else None,
            'ra1'   : float(row['ra1']), 'dec1': float(row['dec1']),
            'ra2'   : float(row['ra2']), 'dec2': float(row['dec2']),
            'rp_kpc': float(row[self.rp_col]) if self.rp_col and self.rp_col in row else None,
        })

    def remove_pending(self, row: dict):
        id1, id2 = int(row['id1']), int(row['id2'])
        self.pending_retry = [
            e for e in self.pending_retry
            if not (e['id1'] == id1 and e['id2'] == id2)
        ]

    # ── Rutas de imagen ───────────────────────────────────────────────────────

    def _par_id(self, row: dict):
        return int(row['id_par']) if 'id_par' in row else f"{int(row['id1'])}_{int(row['id2'])}"

    def _row_record(self, row: dict, img_path: Path) -> dict:
        return {
            'id1'     : int(row['id1']),
            'id2'     : int(row['id2']),
            'id_par'  : int(row['id_par']) if 'id_par' in row else None,
            'ra1'     : float(row['ra1']),  'dec1': float(row['dec1']),
            'ra2'     : float(row['ra2']),  'dec2': float(row['dec2']),
            'rp_kpc'  : float(row[self.rp_col]) if self.rp_col and self.rp_col in row else None,
            'img_path': str(img_path),
        }

    # ── Clasificación: Falso positivo ─────────────────────────────────────────

    def mark_false_positive(self, row: dict, img: Image.Image):
        id1, id2 = int(row['id1']), int(row['id2'])
        if not any(e['id1'] == id1 and e['id2'] == id2 for e in self.false_positives):
            path = self.fp_img_dir / f'par_{self._par_id(row)}.jpg'
            self.false_positives.append(self._row_record(row, path))
            img.save(path, format='JPEG', quality=92)
        self.remove_pending(row)

    def unmark_false_positive(self, row: dict):
        id1, id2 = int(row['id1']), int(row['id2'])
        path = self.fp_img_dir / f'par_{self._par_id(row)}.jpg'
        self.false_positives = [
            e for e in self.false_positives
            if not (e['id1'] == id1 and e['id2'] == id2)
        ]
        if path.exists():
            path.unlink()

    def is_false_positive(self, row: dict) -> bool:
        id1, id2 = int(row['id1']), int(row['id2'])
        return any(e['id1'] == id1 and e['id2'] == id2 for e in self.false_positives)

    # ── Clasificación: Posible merger ─────────────────────────────────────────

    def mark_possible_merger(self, row: dict, img: Image.Image):
        id1, id2 = int(row['id1']), int(row['id2'])
        if not any(e['id1'] == id1 and e['id2'] == id2 for e in self.possible_mergers):
            path = self.pm_img_dir / f'par_{self._par_id(row)}.jpg'
            self.possible_mergers.append(self._row_record(row, path))
            img.save(path, format='JPEG', quality=92)
        self.remove_pending(row)

    def unmark_possible_merger(self, row: dict):
        id1, id2 = int(row['id1']), int(row['id2'])
        path = self.pm_img_dir / f'par_{self._par_id(row)}.jpg'
        self.possible_mergers = [
            e for e in self.possible_mergers
            if not (e['id1'] == id1 and e['id2'] == id2)
        ]
        if path.exists():
            path.unlink()

    def is_possible_merger(self, row: dict) -> bool:
        id1, id2 = int(row['id1']), int(row['id2'])
        return any(e['id1'] == id1 and e['id2'] == id2 for e in self.possible_mergers)

    # ── Clasificación: Par confirmado ─────────────────────────────────────────

    def mark_confirmed_pair(self, row: dict, img: Image.Image):
        id1, id2 = int(row['id1']), int(row['id2'])
        if not any(e['id1'] == id1 and e['id2'] == id2 for e in self.confirmed_pairs):
            path = self.pair_img_dir / f'par_{self._par_id(row)}.jpg'
            self.confirmed_pairs.append(self._row_record(row, path))
            img.save(path, format='JPEG', quality=92)
        self.remove_pending(row)

    def unmark_confirmed_pair(self, row: dict):
        id1, id2 = int(row['id1']), int(row['id2'])
        path = self.pair_img_dir / f'par_{self._par_id(row)}.jpg'
        self.confirmed_pairs = [
            e for e in self.confirmed_pairs
            if not (e['id1'] == id1 and e['id2'] == id2)
        ]
        if path.exists():
            path.unlink()

    def is_confirmed_pair(self, row: dict) -> bool:
        id1, id2 = int(row['id1']), int(row['id2'])
        return any(e['id1'] == id1 and e['id2'] == id2 for e in self.confirmed_pairs)


# ══════════════════════════════════════════════════════════════════════════════
# CELDA DE IMAGEN — widget Tkinter para un par individual
# ══════════════════════════════════════════════════════════════════════════════

class PairCell:
    """
    Frame que muestra una imagen anotada con sus botones F/P/M.
    Se resalta con un borde cuando está seleccionada.
    """

    def __init__(self, parent, index: int, on_classify_cb, on_select_cb, on_retry_cb):
        self.index         = index
        self.on_classify   = on_classify_cb
        self.on_select     = on_select_cb
        self.on_retry      = on_retry_cb
        self.row_data      = None
        self.pil_img       = None
        self._tk_img       = None
        self.selected      = False

        # Frame contenedor
        self.frame = tk.Frame(parent, bg=BG_DEFAULT,
                              highlightbackground='#444', highlightthickness=2,
                              relief='flat', bd=0)

        # Canvas para la imagen
        self.canvas = tk.Canvas(self.frame, width=CELL_SIZE, height=CELL_SIZE,
                                bg=BG_DEFAULT, highlightthickness=0, cursor='hand2')
        self.canvas.pack(pady=(4, 0))
        self.canvas.bind('<Button-1>',        lambda e: self.on_select(self.index))
        self.canvas.bind('<Double-Button-1>', self._open_skyviewer)
        self.canvas.bind('<Button-2>',        self._show_context_menu)
        self.canvas.bind('<Button-3>',        self._show_context_menu)

        # Campo de coordenadas: readonly pero seleccionable (Cmd+A / Ctrl+A → copiar)
        self._coord_var  = tk.StringVar()
        self.coord_label = tk.Entry(
            self.frame, textvariable=self._coord_var,
            state='readonly', readonlybackground='#141414',
            fg='#eeeeee', font=('Courier', 13),
            relief='flat', justify='center', cursor='xterm', width=36)
        self.coord_label.pack(pady=(4, 0))

        # Botones de clasificación
        btn_frame = tk.Frame(self.frame, bg=BG_DEFAULT)
        btn_frame.pack(pady=5)

        bfont = ('Arial', 11, 'bold')
        self.btn_f = tk.Button(btn_frame, text='[F] Falso pos.',
                               font=bfont, bg=BTN_GRAY, fg='#dddddd',
                               activebackground='#606060', relief='flat',
                               cursor='hand2', padx=8, pady=4,
                               command=lambda: self.on_classify(self.index, 'F'))
        self.btn_f.pack(side='left', padx=4)

        self.btn_p = tk.Button(btn_frame, text='[P] Par',
                               font=bfont, bg=BTN_GRAY, fg='#dddddd',
                               activebackground='#606060', relief='flat',
                               cursor='hand2', padx=8, pady=4,
                               command=lambda: self.on_classify(self.index, 'P'))
        self.btn_p.pack(side='left', padx=4)

        self.btn_m = tk.Button(btn_frame, text='[M] Merger',
                               font=bfont, bg=BTN_GRAY, fg='#dddddd',
                               activebackground='#606060', relief='flat',
                               cursor='hand2', padx=8, pady=4,
                               command=lambda: self.on_classify(self.index, 'M'))
        self.btn_m.pack(side='left', padx=4)

        # Botón de reintento — solo visible cuando la descarga falló
        self.btn_retry_img = tk.Button(
            self.frame, text='🔄  Reintentar descarga',
            font=('Arial', 10), bg='#3a2000', fg='#ffaa44',
            activebackground='#704000', relief='flat', cursor='hand2',
            padx=8, pady=3,
            command=lambda: self.on_retry(self.index))
        # Se muestra/oculta dinámicamente en load()

    def _open_skyviewer(self, event=None):
        """Doble clic: abre el Legacy Survey Sky Viewer en el navegador."""
        if self.row_data is None:
            return
        rd      = self.row_data
        ra_mid  = (rd['ra1'] + rd['ra2']) / 2.0
        dec_mid = (rd['dec1'] + rd['dec2']) / 2.0
        sep     = float(rd.get('sep_arcsec', 10.0))
        url     = _skyviewer_url(ra_mid, dec_mid, sep)
        webbrowser.open(url)

    def _show_context_menu(self, event):
        """Menú contextual al hacer clic derecho sobre la imagen."""
        if self.row_data is None:
            return
        self.on_select(self.index)   # seleccionar primero
        menu = tk.Menu(self.frame, tearoff=0,
                       bg='#2a2a2a', fg='white', activebackground='#4a4aaa',
                       font=('Arial', 10))
        menu.add_command(label='[F]  Falso positivo',
                         command=lambda: self.on_classify(self.index, 'F'))
        menu.add_command(label='[P]  Par confirmado',
                         command=lambda: self.on_classify(self.index, 'P'))
        menu.add_command(label='[M]  Merger',
                         command=lambda: self.on_classify(self.index, 'M'))
        menu.add_separator()
        menu.add_command(label='🔭  Abrir en Sky Viewer',
                         command=self._open_skyviewer)
        menu.tk_popup(event.x_root, event.y_root)

    def load(self, row_data: dict, pil_img: 'Image.Image | None',
             validator: PairValidator, error_msg: str = ''):
        """Carga datos e imagen en la celda. error_msg se muestra si pil_img es None."""
        self.row_data = row_data
        self.pil_img  = pil_img

        if pil_img is not None:
            display_img = pil_img.resize((CELL_SIZE, CELL_SIZE), Image.LANCZOS)
            self.btn_retry_img.pack_forget()
        else:
            display_img = make_error_tile(error_msg)   # ya está en CELL_SIZE
            self.btn_retry_img.pack(pady=(0, 4))       # mostrar botón de reintento

        self._tk_img = ImageTk.PhotoImage(display_img)
        self.canvas.delete('all')
        self.canvas.create_image(0, 0, anchor='nw', image=self._tk_img)
        # Forzar redibujado del canvas inmediatamente (fix macOS)
        self.canvas.update_idletasks()

        # Coordenadas del midpoint y rp
        ra_mid  = (row_data['ra1'] + row_data['ra2']) / 2.0
        dec_mid = (row_data['dec1'] + row_data['dec2']) / 2.0
        rp_val  = row_data.get(validator.rp_col, None) if validator.rp_col else None
        if rp_val is not None:
            self._coord_var.set(f'RA {ra_mid:.5f}  Dec {dec_mid:.5f}  rp={rp_val:.2f} kpc')
        else:
            self._coord_var.set(f'RA {ra_mid:.5f}  Dec {dec_mid:.5f}')

        # Actualizar estado visual de botones
        self._update_btn_state(validator)
        self.set_selected(self.selected)

    def _update_btn_state(self, validator: PairValidator):
        """Resalta el botón activo según la clasificación actual."""
        if self.row_data is None:
            return
        is_fp  = validator.is_false_positive(self.row_data)
        is_pm  = validator.is_possible_merger(self.row_data)
        is_par = validator.is_confirmed_pair(self.row_data)

        # Solo cambia el texto (✓) y el relieve — el color gris se mantiene siempre
        self.btn_f.config(
            relief='groove' if is_fp  else 'flat',
            text='[F] Falso pos. ✓' if is_fp  else '[F] Falso pos.')
        self.btn_p.config(
            relief='groove' if is_par else 'flat',
            text='[P] Par ✓'        if is_par else '[P] Par')
        self.btn_m.config(
            relief='groove' if is_pm  else 'flat',
            text='[M] Merger ✓'     if is_pm  else '[M] Merger')

        # Fondo del frame según clasificación
        if is_fp:
            bg = BG_FP
        elif is_pm:
            bg = BG_PM
        elif is_par:
            bg = BG_PAIR
        else:
            bg = BG_DEFAULT

        # Siempre actualizar el fondo de clasificación; set_selected()
        # solo toca highlightbackground/thickness, no bg, así no hay conflicto.
        self.frame.config(bg=bg)
        self.canvas.config(bg=bg)
        if not self.selected:
            self.frame.config(highlightbackground='#444')

    def set_selected(self, selected: bool):
        self.selected = selected
        if selected:
            self.frame.config(highlightbackground='#6688ff', highlightthickness=3)
        else:
            self.frame.config(highlightbackground='#444', highlightthickness=2)

    def clear(self):
        """Limpia la celda (página con menos de PAGE_SIZE elementos)."""
        self.row_data = None
        self.pil_img  = None
        self._tk_img  = None
        self.canvas.delete('all')
        self.canvas.config(bg=BG_DEFAULT)
        self._coord_var.set('')
        self.btn_retry_img.pack_forget()
        self.btn_f.config(bg=BTN_GRAY, relief='flat', text='[F] Falso pos.')
        self.btn_p.config(bg=BTN_GRAY, relief='flat', text='[P] Par')
        self.btn_m.config(bg=BTN_GRAY, relief='flat', text='[M] Merger')
        self.frame.config(bg=BG_DEFAULT, highlightbackground='#444')


# ══════════════════════════════════════════════════════════════════════════════
# APLICACIÓN PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

class PairInspectorApp:
    """
    Ventana Tkinter fullscreen con grid 4×2.
    Maneja navegación por teclado, prefetch en background y clasificación.
    """

    def __init__(self, root: tk.Tk, validator: PairValidator):
        self.root      = root
        self.v         = validator
        self.cells: list[PairCell] = []
        self.selected_idx = 0          # índice de celda seleccionada (0–7)

        # Estado de prefetch: imágenes de la siguiente página
        self._prefetch_lock    = threading.Lock()
        self._prefetch_rows    = []
        self._prefetch_imgs    = []
        self._prefetch_ready   = False
        self._prefetch_thread  = None

        # Imágenes anotadas para mostrar en pantalla
        self._current_imgs: list = [None] * PAGE_SIZE
        # Recortes LIMPIOS (sin marcas) para guardar al clasificar — lo que
        # va a disco para entrenamiento del modelo
        self._current_raws: list = [None] * PAGE_SIZE

        self._build_ui()
        self._bind_keys()
        # Deferir la primera carga para que Tkinter termine de pintar la ventana.
        # En macOS los Canvas no se renderizan si se cargan antes de que la
        # ventana esté completamente visible — this fixes the "invisible when
        # focused" bug.
        self.root.after(150, lambda: self._load_page(direction='next', first=True))

    # ── Construcción de UI ────────────────────────────────────────────────────

    def _build_ui(self):
        self.root.title('Pair Inspector — Clasificador de Galaxias')
        self.root.configure(bg='#111111')

        # ── Barra superior ────────────────────────────────────────────────────
        top = tk.Frame(self.root, bg='#111111', pady=4)
        top.pack(fill='x', padx=8)

        self.lbl_progress = tk.Label(top, text='', bg='#111111', fg='#cccccc',
                                     font=('Arial', 12))
        self.lbl_progress.pack(side='left', padx=8)

        self.lbl_counts = tk.Label(top, text='', bg='#111111', fg='#cccccc',
                                   font=('Arial', 12))
        self.lbl_counts.pack(side='left', padx=16)

        # Botones de la barra superior
        btn_cfg = dict(font=('Arial', 11, 'bold'), relief='flat',
                       padx=10, pady=4, cursor='hand2')

        self.btn_prev = tk.Button(top, text='◀ Anterior', bg='#2a4a6a', fg='white',
                                  **btn_cfg, command=self._prev_page)
        self.btn_prev.pack(side='left', padx=4)

        self.btn_next = tk.Button(top, text='Siguiente ▶', bg='#2a6a4a', fg='white',
                                  **btn_cfg, command=self._next_page)
        self.btn_next.pack(side='left', padx=4)

        self.btn_retry_all = tk.Button(top, text='🔄 Reintentar página',
                                       bg='#3a2800', fg='#ffaa44',
                                       **btn_cfg, command=self._retry_page)
        self.btn_retry_all.pack(side='left', padx=4)

        tk.Button(top, text='Exportar CSV (Ctrl+E)', bg='#6a3a1a', fg='white',
                  **btn_cfg, command=self._export).pack(side='left', padx=4)

        # ── Menú ⚙ con opciones avanzadas (Limpiar guardadas, etc.) ──────────
        options_mb = tk.Menubutton(top, text='⚙', font=('Arial', 13, 'bold'),
                                   bg='#333333', fg='#aaaaaa',
                                   relief='flat', cursor='hand2',
                                   padx=8, pady=4)
        options_mb.pack(side='left', padx=4)
        options_menu = tk.Menu(options_mb, tearoff=0,
                               bg='#2a2a2a', fg='white',
                               activebackground='#444444',
                               font=('Arial', 11))
        options_menu.add_command(label='🧹  Limpiar imágenes guardadas (una sola vez)',
                                 command=self._clean_saved_images)
        options_mb['menu'] = options_menu

        # ── Barra de búsqueda por ID de par ───────────────────────────────────
        tk.Frame(top, bg='#444444', width=1).pack(side='left', fill='y',
                                                   padx=8, pady=2)
        tk.Label(top, text='Buscar par #', bg='#111111', fg='#aaaaaa',
                 font=('Arial', 11)).pack(side='left')
        self._search_var = tk.StringVar()
        search_entry = tk.Entry(top, textvariable=self._search_var,
                                width=7, font=('Arial', 11),
                                bg='#2a2a2a', fg='white',
                                insertbackground='white', relief='flat')
        search_entry.pack(side='left', padx=(2, 4))
        search_entry.bind('<Return>', lambda e: self._search_pair())
        tk.Button(top, text='Ver', bg='#2a4a6a', fg='white',
                  font=('Arial', 11, 'bold'), relief='flat',
                  cursor='hand2', padx=8, pady=4,
                  command=self._search_pair).pack(side='left')

        self.lbl_status = tk.Label(top, text='Cargando…', bg='#111111',
                                   fg='#888888', font=('Arial', 10, 'italic'))
        self.lbl_status.pack(side='right', padx=8)

        # ── Separador ─────────────────────────────────────────────────────────
        ttk.Separator(self.root, orient='horizontal').pack(fill='x')

        # ── Grid de celdas ────────────────────────────────────────────────────
        self.grid_frame = tk.Frame(self.root, bg='#111111')
        self.grid_frame.pack(fill='both', expand=True, padx=4, pady=4)

        # Filas: sin expansión para que las celdas no se estiren verticalmente
        for row_i in range(GRID_ROWS):
            self.grid_frame.rowconfigure(row_i, weight=0)
        # Columnas: se distribuyen el espacio horizontal por igual
        for col_i in range(GRID_COLS):
            self.grid_frame.columnconfigure(col_i, weight=1)

        for row_i in range(GRID_ROWS):
            for col_i in range(GRID_COLS):
                idx  = row_i * GRID_COLS + col_i
                cell = PairCell(
                    self.grid_frame, idx,
                    on_classify_cb=self._classify,
                    on_select_cb=self._select_cell,
                    on_retry_cb=self._retry_cell,
                )
                # sticky='n': centrado horizontalmente, anclado arriba
                cell.frame.grid(row=row_i, column=col_i, padx=6, pady=6, sticky='n')
                self.cells.append(cell)

        # Resaltar la primera celda
        self.cells[0].set_selected(True)

        # macOS: forzar redibujado completo cada vez que la ventana gana foco.
        # Sin esto los Canvas quedan en blanco mientras la ventana está activa.
        self.root.bind('<FocusIn>', lambda e: self.root.update_idletasks())

    def _bind_keys(self):
        """Atajos de teclado para navegación rápida."""
        self.root.bind('<space>',      lambda e: self._next_page())
        self.root.bind('<Right>',      lambda e: self._next_page())
        self.root.bind('<Left>',       lambda e: self._prev_page())
        self.root.bind('<Tab>',        lambda e: self._move_selection(1))
        self.root.bind('<ISO_Left_Tab>', lambda e: self._move_selection(-1))
        self.root.bind('<f>',          lambda e: self._classify_selected('F'))
        self.root.bind('<F>',          lambda e: self._classify_selected('F'))
        self.root.bind('<p>',          lambda e: self._classify_selected('P'))
        self.root.bind('<P>',          lambda e: self._classify_selected('P'))
        self.root.bind('<m>',          lambda e: self._classify_selected('M'))
        self.root.bind('<M>',          lambda e: self._classify_selected('M'))
        self.root.bind('<Control-e>',  lambda e: self._export())

    # ── Actualización de barra de estado ──────────────────────────────────────

    def _update_status_bar(self):
        idx   = self.v.current_index
        total = len(self.v.df)
        pct   = 100 * idx / total if total else 0
        self.lbl_progress.config(
            text=f'Revisados: {idx:,} / {total:,}  ({pct:.1f}%)')
        self.lbl_counts.config(
            text=(f'FP: {len(self.v.false_positives)}  |  '
                  f'Mergers: {len(self.v.possible_mergers)}  |  '
                  f'Pares: {len(self.v.confirmed_pairs)}  |  '
                  f'Sin img: {len(self.v.pending_retry)}'))

    # ── Navegación de páginas ─────────────────────────────────────────────────

    def _next_page(self):
        self.v.advance(PAGE_SIZE)
        self.v.save_progress()
        self._load_page(direction='next')

    def _prev_page(self):
        self.v.go_back(PAGE_SIZE)
        self.v.save_progress()
        self._load_page(direction='prev')

    def _load_page(self, direction='next', first=False):
        """Renderiza la página actual. Usa prefetch si está disponible."""
        page = self.v.get_page(PAGE_SIZE)

        if page.empty:
            self.lbl_status.config(text='✓ Todos los pares revisados')
            for cell in self.cells:
                cell.clear()
            self._update_status_bar()
            return

        # Construir lista de dicts para la página
        row_list = []
        for _, row in page.iterrows():
            d = row.to_dict()
            if 'sep_arcsec' not in d or pd.isna(d.get('sep_arcsec', float('nan'))):
                d['sep_arcsec'] = float(np.hypot(
                    (d['ra1']-d['ra2'])*np.cos(np.radians((d['dec1']+d['dec2'])/2))*3600,
                    (d['dec1']-d['dec2'])*3600))
            row_list.append(d)

        # ¿Hay prefetch listo para esta dirección?
        with self._prefetch_lock:
            prefetch_ok = (
                direction == 'next' and
                self._prefetch_ready and
                len(self._prefetch_rows) == len(row_list) and
                self._prefetch_rows[0].get('id1') == row_list[0].get('id1')
            )
            if prefetch_ok:
                raw_imgs = list(self._prefetch_imgs)
                self._prefetch_ready = False
            else:
                raw_imgs = None

        if raw_imgs is None:
            self.lbl_status.config(text='Descargando imágenes…')
            self.root.update_idletasks()
            raw_imgs = fetch_page_parallel(row_list)

        # Anotar imágenes y cargar celdas
        self._current_imgs = []
        self._current_raws = []
        for i, (raw, rd) in enumerate(zip(raw_imgs, row_list)):
            if raw is not None:
                annotated = annotate_image(raw, rd, self.v.rp_col)
                err_msg   = ''
            else:
                annotated = None
                self.v.add_pending(rd)
                key     = (round((rd['ra1']+rd['ra2'])/2, 5),
                           round((rd['dec1']+rd['dec2'])/2, 5))
                err_msg = _fetch_errors.get(key, 'Error desconocido')

            self._current_imgs.append(annotated)   # con marcas → solo pantalla
            self._current_raws.append(raw)          # limpio → lo que se guarda

            if i < len(self.cells):
                self.cells[i].load(rd, annotated, self.v, error_msg=err_msg)

        # Limpiar celdas sobrantes si la página es pequeña
        for i in range(len(row_list), PAGE_SIZE):
            self.cells[i].clear()

        # Restablecer selección en primera celda de la página
        self._select_cell(0)
        self._update_status_bar()
        self.lbl_status.config(
            text=f'Pares {self.v.current_index - len(row_list) + 1}–'
                 f'{self.v.current_index} de {len(self.v.df):,}')

        # Forzar redibujado (fix macOS canvas blank on focus)
        self.root.update()

        # Lanzar prefetch de la siguiente página en background
        self._launch_prefetch()

    def _launch_prefetch(self):
        """Descarga la siguiente página en un hilo separado."""
        next_start = self.v.current_index
        next_end   = min(next_start + PAGE_SIZE, len(self.v.df))
        if next_start >= len(self.v.df):
            return

        page = self.v.df.iloc[next_start:next_end]
        row_list = []
        for _, row in page.iterrows():
            d = row.to_dict()
            if 'sep_arcsec' not in d or pd.isna(d.get('sep_arcsec', float('nan'))):
                d['sep_arcsec'] = float(np.hypot(
                    (d['ra1']-d['ra2'])*np.cos(np.radians((d['dec1']+d['dec2'])/2))*3600,
                    (d['dec1']-d['dec2'])*3600))
            row_list.append(d)

        def _do_prefetch():
            imgs = fetch_page_parallel(row_list)
            with self._prefetch_lock:
                self._prefetch_rows  = row_list
                self._prefetch_imgs  = imgs
                self._prefetch_ready = True

        # Cancelar prefetch anterior si sigue corriendo
        if self._prefetch_thread and self._prefetch_thread.is_alive():
            with self._prefetch_lock:
                self._prefetch_ready = False

        self._prefetch_thread = threading.Thread(target=_do_prefetch, daemon=True)
        self._prefetch_thread.start()

    # ── Selección de celda ────────────────────────────────────────────────────

    def _select_cell(self, idx: int):
        if self.cells[self.selected_idx].row_data is not None or True:
            self.cells[self.selected_idx].set_selected(False)
        self.selected_idx = max(0, min(idx, PAGE_SIZE - 1))
        self.cells[self.selected_idx].set_selected(True)

    def _move_selection(self, delta: int):
        new_idx = (self.selected_idx + delta) % PAGE_SIZE
        # Saltar celdas vacías
        attempts = 0
        while self.cells[new_idx].row_data is None and attempts < PAGE_SIZE:
            new_idx = (new_idx + delta) % PAGE_SIZE
            attempts += 1
        self._select_cell(new_idx)
        return 'break'   # evita que Tab cambie el foco de widget

    # ── Clasificación ─────────────────────────────────────────────────────────

    def _classify_selected(self, label: str):
        """Clasifica la celda actualmente seleccionada."""
        self._classify(self.selected_idx, label)

    def _classify(self, cell_idx: int, label: str):
        """Aplica o revierte una clasificación en la celda dada."""
        cell = self.cells[cell_idx]
        if cell.row_data is None:
            return

        row = cell.row_data
        # Se guarda el recorte LIMPIO (sin anotaciones) para entrenamiento
        img = self._current_raws[cell_idx] if cell_idx < len(self._current_raws) else None
        if img is None:
            self.lbl_status.config(text='Sin imagen — no se puede clasificar')
            return

        if label == 'F':
            if self.v.is_false_positive(row):
                self.v.unmark_false_positive(row)
            else:
                self.v.mark_false_positive(row, img)
                # Quitar otras clasificaciones mutuamente excluyentes
                self.v.unmark_possible_merger(row)
                self.v.unmark_confirmed_pair(row)

        elif label == 'P':
            if self.v.is_confirmed_pair(row):
                self.v.unmark_confirmed_pair(row)
            else:
                self.v.mark_confirmed_pair(row, img)
                self.v.unmark_false_positive(row)
                self.v.unmark_possible_merger(row)

        elif label == 'M':
            if self.v.is_possible_merger(row):
                self.v.unmark_possible_merger(row)
            else:
                self.v.mark_possible_merger(row, img)
                self.v.unmark_false_positive(row)
                self.v.unmark_confirmed_pair(row)

        # Refrescar estado visual de la celda
        cell._update_btn_state(self.v)
        self._update_status_bar()
        # Auto-guardar en cada clasificación para no perder datos si la app se cierra
        self.v.save_progress()

    # ── Reintento de página completa y por celda ──────────────────────────────

    def _retry_page(self):
        """Reintenta en paralelo todas las celdas sin imagen de la página actual."""
        failed = [i for i, cell in enumerate(self.cells)
                  if cell.row_data is not None and cell.pil_img is None]
        if not failed:
            self.lbl_status.config(text='Todas las imágenes de esta página están cargadas')
            return

        self.btn_retry_all.config(text='⏳ Descargando…', state='disabled')
        self.lbl_status.config(text=f'Reintentando {len(failed)} imagen(es)…')
        self.root.update_idletasks()

        def _do():
            for idx in failed:
                rd  = self.cells[idx].row_data
                raw = _fetch_one(rd['ra1'], rd['dec1'], rd['ra2'], rd['dec2'],
                                 rd['sep_arcsec'])
                self.root.after(0, lambda i=idx, r=raw, d=rd: self._retry_done(i, d, r))
            self.root.after(0, self._retry_page_done)

        threading.Thread(target=_do, daemon=True).start()

    def _retry_page_done(self):
        self.btn_retry_all.config(text='🔄 Reintentar página', state='normal')
        self._update_status_bar()
        self.root.update()

    def _retry_cell(self, cell_idx: int):
        """Re-descarga la imagen de una celda que falló, en un hilo daemon."""
        cell = self.cells[cell_idx]
        if cell.row_data is None:
            return
        rd = cell.row_data

        # Feedback inmediato
        cell.btn_retry_img.config(text='⏳  Descargando…', state='disabled')
        self.lbl_status.config(text=f'Reintentando par #{rd.get("id_par", "?")}…')
        self.root.update_idletasks()

        def _do():
            raw = _fetch_one(rd['ra1'], rd['dec1'], rd['ra2'], rd['dec2'],
                             rd['sep_arcsec'])
            self.root.after(0, lambda: self._retry_done(cell_idx, rd, raw))

        threading.Thread(target=_do, daemon=True).start()

    def _retry_done(self, cell_idx: int, rd: dict, raw):
        """Callback cuando termina el reintento — ejecutado en hilo UI."""
        cell = self.cells[cell_idx]
        cell.btn_retry_img.config(text='🔄  Reintentar descarga', state='normal')

        if raw is not None:
            annotated = annotate_image(raw, rd, self.v.rp_col)
            self._current_imgs[cell_idx] = annotated
            self._current_raws[cell_idx] = raw          # guardar limpio
            self.v.remove_pending(rd)
            cell.load(rd, annotated, self.v, error_msg='')
            self.lbl_status.config(text='Imagen descargada correctamente')
        else:
            key     = (round((rd['ra1']+rd['ra2'])/2, 5),
                       round((rd['dec1']+rd['dec2'])/2, 5))
            err_msg = _fetch_errors.get(key, 'Error desconocido')
            cell.load(rd, None, self.v, error_msg=err_msg)
            self.lbl_status.config(text=f'Falló de nuevo: {err_msg[:50]}')

        self._update_status_bar()
        self.root.update()

    # ── Guardar y exportar ────────────────────────────────────────────────────

    def _save(self):
        self.v.save_progress()
        self.lbl_status.config(text=f'Guardado  {datetime.now().strftime("%H:%M:%S")}')

    def _export(self):
        self.v.save_progress()
        resultado = self.v.export_csv()
        messagebox.showinfo('Exportar CSV', resultado)
        self.lbl_status.config(text='CSV exportados')

    # ── Búsqueda por ID de par ────────────────────────────────────────────────

    def _search_pair(self):
        query = self._search_var.get().strip()
        if not query:
            return
        try:
            pair_id = int(query)
        except ValueError:
            messagebox.showwarning('Búsqueda', 'Escribe un número entero (ej. 10).')
            return

        col = 'id_par' if 'id_par' in self.v.df.columns else None
        if col is None:
            messagebox.showwarning('Búsqueda', 'El catálogo no tiene columna id_par.')
            return

        mask = self.v.df[col] == pair_id
        if not mask.any():
            messagebox.showwarning('Búsqueda', f'Par #{pair_id} no encontrado.')
            return

        rd = self.v.df[mask].iloc[0].to_dict()
        if 'sep_arcsec' not in rd or pd.isna(rd.get('sep_arcsec', float('nan'))):
            rd['sep_arcsec'] = float(np.hypot(
                (rd['ra1']-rd['ra2'])*np.cos(np.radians((rd['dec1']+rd['dec2'])/2))*3600,
                (rd['dec1']-rd['dec2'])*3600))
        self._open_detail_window(rd)

    def _open_detail_window(self, row_data: dict):
        """Ventana emergente con imagen ampliada, clasificación y acceso al Sky Viewer."""
        par_id  = row_data.get('id_par', f"{int(row_data['id1'])}_{int(row_data['id2'])}")
        ra_mid  = (row_data['ra1'] + row_data['ra2']) / 2.0
        dec_mid = (row_data['dec1'] + row_data['dec2']) / 2.0
        rp_val  = row_data.get(self.v.rp_col) if self.v.rp_col else None

        DETAIL_PX = 520   # tamaño de la imagen en la ventana de detalle

        win = tk.Toplevel(self.root)
        win.title(f'Detalle — Par #{par_id}')
        win.configure(bg='#111111')
        win.resizable(False, False)

        # ── Canvas con imagen ─────────────────────────────────────────────────
        canvas = tk.Canvas(win, width=DETAIL_PX, height=DETAIL_PX,
                           bg='#1e1e1e', highlightthickness=0, cursor='hand2')
        canvas.pack(padx=14, pady=(14, 4))

        _tk_ref = [None]   # mantener referencia para evitar GC
        _raw    = [None]   # recorte limpio para guardar al clasificar

        # Placeholder mientras descarga
        ph = make_error_tile('Descargando…').resize((DETAIL_PX, DETAIL_PX), Image.LANCZOS)
        _tk_ref[0] = ImageTk.PhotoImage(ph)
        canvas.create_image(0, 0, anchor='nw', image=_tk_ref[0])

        # ── Coordenadas copiables ─────────────────────────────────────────────
        coord_var = tk.StringVar()
        if rp_val is not None:
            coord_var.set(f'RA {ra_mid:.5f}  Dec {dec_mid:.5f}  rp={rp_val:.2f} kpc')
        else:
            coord_var.set(f'RA {ra_mid:.5f}  Dec {dec_mid:.5f}')
        tk.Entry(win, textvariable=coord_var, state='readonly',
                 readonlybackground='#141414', fg='#eeeeee',
                 font=('Courier', 12), relief='flat',
                 justify='center', cursor='xterm', width=42
                 ).pack(pady=(4, 0))

        # ── Estado de clasificación ───────────────────────────────────────────
        lbl_class = tk.Label(win, text='', bg='#111111',
                             font=('Arial', 11, 'bold'))
        lbl_class.pack(pady=(4, 0))

        # ── Botones F / P / M ─────────────────────────────────────────────────
        bf = tk.Frame(win, bg='#111111')
        bf.pack(pady=8)
        bfont = ('Arial', 12, 'bold')

        def _refresh_state():
            is_fp  = self.v.is_false_positive(row_data)
            is_par = self.v.is_confirmed_pair(row_data)
            is_pm  = self.v.is_possible_merger(row_data)
            btn_f.config(relief='groove' if is_fp  else 'flat',
                         text='[F] Falso pos. ✓' if is_fp  else '[F] Falso pos.')
            btn_p.config(relief='groove' if is_par else 'flat',
                         text='[P] Par ✓'        if is_par else '[P] Par')
            btn_m.config(relief='groove' if is_pm  else 'flat',
                         text='[M] Merger ✓'     if is_pm  else '[M] Merger')
            if is_fp:
                lbl_class.config(text='● Falso positivo', fg='#ff6666')
            elif is_par:
                lbl_class.config(text='● Par confirmado', fg='#66ff88')
            elif is_pm:
                lbl_class.config(text='● Merger',         fg='#ffaa44')
            else:
                lbl_class.config(text='Sin clasificar',   fg='#666666')

        def _classify(label):
            if _raw[0] is None:
                return
            if label == 'F':
                if self.v.is_false_positive(row_data):
                    self.v.unmark_false_positive(row_data)
                else:
                    self.v.mark_false_positive(row_data, _raw[0])
                    self.v.unmark_possible_merger(row_data)
                    self.v.unmark_confirmed_pair(row_data)
            elif label == 'P':
                if self.v.is_confirmed_pair(row_data):
                    self.v.unmark_confirmed_pair(row_data)
                else:
                    self.v.mark_confirmed_pair(row_data, _raw[0])
                    self.v.unmark_false_positive(row_data)
                    self.v.unmark_possible_merger(row_data)
            elif label == 'M':
                if self.v.is_possible_merger(row_data):
                    self.v.unmark_possible_merger(row_data)
                else:
                    self.v.mark_possible_merger(row_data, _raw[0])
                    self.v.unmark_false_positive(row_data)
                    self.v.unmark_confirmed_pair(row_data)
            self.v.save_progress()
            _refresh_state()
            self._update_status_bar()   # actualizar contadores ventana principal

        btn_f = tk.Button(bf, text='[F] Falso pos.', font=bfont,
                          bg=BTN_GRAY, fg='#dddddd', activebackground='#606060',
                          relief='flat', cursor='hand2', padx=10, pady=5,
                          command=lambda: _classify('F'))
        btn_f.pack(side='left', padx=6)

        btn_p = tk.Button(bf, text='[P] Par', font=bfont,
                          bg=BTN_GRAY, fg='#dddddd', activebackground='#606060',
                          relief='flat', cursor='hand2', padx=10, pady=5,
                          command=lambda: _classify('P'))
        btn_p.pack(side='left', padx=6)

        btn_m = tk.Button(bf, text='[M] Merger', font=bfont,
                          bg=BTN_GRAY, fg='#dddddd', activebackground='#606060',
                          relief='flat', cursor='hand2', padx=10, pady=5,
                          command=lambda: _classify('M'))
        btn_m.pack(side='left', padx=6)

        # ── Teclado en la ventana de detalle ──────────────────────────────────
        win.bind('<f>', lambda e: _classify('F'))
        win.bind('<F>', lambda e: _classify('F'))
        win.bind('<p>', lambda e: _classify('P'))
        win.bind('<P>', lambda e: _classify('P'))
        win.bind('<m>', lambda e: _classify('M'))
        win.bind('<M>', lambda e: _classify('M'))

        # ── Sky Viewer ────────────────────────────────────────────────────────
        def _open_sky(event=None):
            webbrowser.open(_skyviewer_url(ra_mid, dec_mid, row_data['sep_arcsec']))

        canvas.bind('<Double-Button-1>', _open_sky)

        def _ctx(event):
            m = tk.Menu(win, tearoff=0, bg='#2a2a2a', fg='white',
                        activebackground='#4a4aaa', font=('Arial', 11))
            m.add_command(label='[F]  Falso positivo',  command=lambda: _classify('F'))
            m.add_command(label='[P]  Par confirmado',  command=lambda: _classify('P'))
            m.add_command(label='[M]  Merger',          command=lambda: _classify('M'))
            m.add_separator()
            m.add_command(label='🔭  Abrir en Sky Viewer', command=_open_sky)
            m.tk_popup(event.x_root, event.y_root)

        canvas.bind('<Button-2>', _ctx)
        canvas.bind('<Button-3>', _ctx)

        tk.Label(win, text='doble clic → Sky Viewer  |  clic derecho → menú  |  F / P / M',
                 bg='#111111', fg='#444444',
                 font=('Arial', 9, 'italic')).pack(pady=(0, 10))

        _refresh_state()

        # ── Descarga en background ────────────────────────────────────────────
        btn_reload = tk.Button(win, text='🔄  Reintentar descarga',
                               font=('Arial', 10), bg='#3a2000', fg='#ffaa44',
                               activebackground='#704000', relief='flat',
                               cursor='hand2', padx=8, pady=3)
        # Se muestra solo cuando la imagen falla

        def _do():
            raw = _fetch_one(row_data['ra1'], row_data['dec1'],
                             row_data['ra2'], row_data['dec2'],
                             row_data['sep_arcsec'])
            win.after(0, lambda: _on_done(raw))

        def _on_done(raw):
            if not win.winfo_exists():
                return
            _raw[0] = raw
            if raw is not None:
                ann  = annotate_image(raw, row_data, self.v.rp_col)
                disp = ann.resize((DETAIL_PX, DETAIL_PX), Image.LANCZOS)
                btn_reload.pack_forget()          # imagen ok → ocultar botón
            else:
                key  = (round(ra_mid, 5), round(dec_mid, 5))
                err  = _fetch_errors.get(key, 'Error de descarga')
                disp = make_error_tile(err).resize((DETAIL_PX, DETAIL_PX), Image.LANCZOS)
                btn_reload.config(text='🔄  Reintentar descarga', state='normal')
                btn_reload.pack(pady=(0, 8))      # imagen fallida → mostrar botón
            _tk_ref[0] = ImageTk.PhotoImage(disp)
            canvas.delete('all')
            canvas.create_image(0, 0, anchor='nw', image=_tk_ref[0])
            canvas.update_idletasks()

        def _reload():
            btn_reload.config(text='⏳  Descargando…', state='disabled')
            threading.Thread(target=_do, daemon=True).start()

        btn_reload.config(command=_reload)
        threading.Thread(target=_do, daemon=True).start()

    def _clean_saved_images(self):
        """Re-descarga todas las imágenes clasificadas sin marcas y las reemplaza."""
        todas = (self.v.false_positives +
                 self.v.possible_mergers +
                 self.v.confirmed_pairs)
        if not todas:
            messagebox.showinfo('Limpiar guardadas', 'No hay imágenes clasificadas aún.')
            return

        ok = messagebox.askyesno(
            'Limpiar imágenes guardadas',
            f'Se van a re-descargar {len(todas)} imágenes sin marcas y '
            f'reemplazar las actuales.\n\n¿Continuar?')
        if not ok:
            return

        self.btn_clean.config(text='⏳ Limpiando…', state='disabled')
        self.root.update_idletasks()

        def _do():
            ok_count = err_count = 0
            completed = [0]   # contador compartido entre hilos
            lock = threading.Lock()

            def _fetch_and_save(entry):
                ra1, dec1 = entry['ra1'], entry['dec1']
                ra2, dec2 = entry['ra2'], entry['dec2']
                sep = float(np.hypot(
                    (ra1-ra2)*np.cos(np.radians((dec1+dec2)/2))*3600,
                    (dec1-dec2)*3600))
                raw = _fetch_one(ra1, dec1, ra2, dec2, sep)
                if raw is not None:
                    try:
                        raw.save(entry['img_path'], format='JPEG', quality=92)
                        return True
                    except Exception:
                        return False
                return False

            # 24 workers para limpieza batch — operación única, no bloquea la UI
            with ThreadPoolExecutor(max_workers=24) as pool:
                futures = {pool.submit(_fetch_and_save, e): e for e in todas}
                for fut in as_completed(futures):
                    with lock:
                        completed[0] += 1
                        n = completed[0]
                        if fut.result():
                            ok_count += 1
                        else:
                            err_count += 1
                    if n % 5 == 0:
                        self.root.after(0, lambda c=n: self.lbl_status.config(
                            text=f'Limpiando… {c}/{len(todas)}'))

            self.root.after(0, lambda: self._clean_done(ok_count, err_count))

        threading.Thread(target=_do, daemon=True).start()

    def _clean_done(self, ok_count: int, err_count: int):
        self.btn_clean.config(text='🧹 Limpiar guardadas', state='normal')
        msg = f'Reemplazadas: {ok_count} imágenes limpias.'
        if err_count:
            msg += f'\nFallidas (red): {err_count} — puedes reintentar.'
        messagebox.showinfo('Limpiar guardadas', msg)
        self.lbl_status.config(text=f'Limpieza completa: {ok_count} ok, {err_count} errores')


# ══════════════════════════════════════════════════════════════════════════════
# PUNTO DE ENTRADA
# ══════════════════════════════════════════════════════════════════════════════

def main():
    # Inicializar validador (carga catálogo y progreso previo)
    try:
        validator = PairValidator(
            catalog_path  = CATALOG_PATH,
            progress_file = PROGRESS_FILE,
            fp_img_dir    = FP_IMG_DIR,
            pm_img_dir    = PM_IMG_DIR,
            pair_img_dir  = PAIR_IMG_DIR,
            rp_max_kpc    = RP_MAX_KPC,
        )
    except FileNotFoundError as e:
        # Mostrar error con Tkinter en lugar de trazar un crash en terminal
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror('Catálogo no encontrado', str(e))
        root.destroy()
        return
    except Exception as e:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror('Error al cargar datos', str(e))
        root.destroy()
        return

    print(f'Catálogo: {len(validator.df):,} pares  |  '
          f'Revisados: {validator.current_index:,}  |  '
          f'FP: {len(validator.false_positives)}  |  '
          f'Mergers: {len(validator.possible_mergers)}  |  '
          f'Pares confirmados: {len(validator.confirmed_pairs)}')

    # Crear y lanzar ventana
    root = tk.Tk()

    # Fix macOS canvas rendering: ventana 99.9% opaca fuerza GPU compositing,
    # lo que garantiza que los Canvas se redibujen correctamente al ganar foco.
    try:
        root.attributes('-alpha', 0.999)
    except tk.TclError:
        pass

    # Maximizar ventana (macOS y Linux)
    try:
        root.state('zoomed')
    except tk.TclError:
        try:
            root.attributes('-zoomed', True)
        except tk.TclError:
            root.geometry(f'{root.winfo_screenwidth()}x{root.winfo_screenheight()}+0+0')

    app = PairInspectorApp(root, validator)

    # Pulso de redibujado cada 300 ms — garantía extra contra el bug de macOS
    def _redraw_pulse():
        try:
            root.update_idletasks()
        except Exception:
            return
        root.after(300, _redraw_pulse)
    root.after(300, _redraw_pulse)

    # Guardar al cerrar
    def _on_close():
        validator.save_progress()
        root.destroy()

    root.protocol('WM_DELETE_WINDOW', _on_close)
    root.mainloop()


if __name__ == '__main__':
    main()
