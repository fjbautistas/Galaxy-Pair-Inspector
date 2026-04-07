# Galaxy Pair Inspector

Visual inspection and classification tool for galaxy pairs from the DESI Legacy Survey DR10.
Designed for research on interacting galaxies, mergers, and false positives in photometric catalogs.

---

## Overview

This repository contains two complementary tools:

| Tool | Description |
|------|-------------|
| `GalaxyPairInspector.ipynb` | Original Jupyter notebook — exploratory interface using ipywidgets |
| `pair_inspector_app.py` | **Standalone desktop app** (Tkinter) — replaces the notebook for efficient large-scale classification |

The desktop app was built to classify **~5,000+ galaxy pairs** efficiently, generating a clean labeled dataset for training a future CNN/RCNN classifier.

---

## Scientific Context

Galaxy pairs are identified from the DESI spectroscopic catalog by projected separation `rp < 12 kpc`. Each pair must be visually inspected to determine whether it is:

- **Confirmed pair** — two distinct interacting galaxies
- **Possible merger** — morphological evidence of ongoing merger (tidal features, asymmetry, bridges)
- **False positive** — projection effect, star, artifact, or misidentification

The classified cutouts (256×256 px, Legacy Survey DR10 color composites) serve as training data for a supervised ML classifier.

---

## Requirements

```
python >= 3.10
pandas
numpy
requests
Pillow (PIL)
tkinter       # included in standard Python on macOS and Linux
```

Install dependencies:

```bash
pip install pandas numpy requests Pillow
```

> **Note:** `tkinter` is included in most Python distributions. On Linux you may need:
> ```bash
> sudo apt-get install python3-tk
> ```

---

## Quick Start

```bash
python3 pair_inspector_app.py
```

The app automatically restores progress from `outputs/catalogs/progress.json` if it exists, so you can stop and resume at any time.

---

## Configuration

All parameters are defined in the `CONFIG` block at the top of `pair_inspector_app.py`:

```python
CATALOG_PATH  = '/path/to/DESI_int_legacyID_pairs.parquet'
RP_MAX_KPC    = 12.0        # projected separation filter
IMG_SIZE_PX   = 256         # download resolution (pixels) — 256×256 for ML training
CELL_SIZE     = 420         # display size in the grid (pixels)
N_WORKERS     = 8           # parallel download threads
TIMEOUT       = 15          # seconds per download attempt
CROSS_SIZE    = 4           # marker arm length (pixels)
CROSS_ALPHA   = 130         # marker transparency (0=invisible, 255=opaque)
```

The catalog must be a Parquet file with at least these columns:

| Column | Description |
|--------|-------------|
| `id1`, `id2` | Galaxy identifiers |
| `ra1`, `dec1` | RA/Dec of galaxy 1 (degrees) |
| `ra2`, `dec2` | RA/Dec of galaxy 2 (degrees) |
| `id_par` | Pair identifier (optional but recommended) |
| `rp_kpc` / `rp_phys_kpc` / `rp` | Projected separation in kpc |

---

## Interface

### Main Window

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Revisados: N / 22,928 (X%)  │  FP: N  Mergers: N  Pares: N  │  ◀  ▶  🔄  │
├──────────┬──────────┬──────────┬──────────────────────────────────────────  │
│  par #1  │  par #2  │  par #3  │  par #4                                    │
│  image   │  image   │  image   │  image                                     │
│ RA  Dec  │ RA  Dec  │ RA  Dec  │ RA  Dec                                    │
│ [F][P][M]│ [F][P][M]│ [F][P][M]│ [F][P][M]                                 │
├──────────┼──────────┼──────────┼──────────                                  │
│  par #5  │  par #6  │  par #7  │  par #8                                    │
│   ...    │   ...    │   ...    │   ...                                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

- **4×2 grid** — 8 pairs per page
- **Image cutouts** — 256×256 px color composites from Legacy Survey DR10, scaled to 420 px for display
- **Markers** — red cross (+) on galaxy 1, blue X on galaxy 2, semi-transparent with dark outline for contrast
- **RA/Dec field** — selectable and copyable (click + Cmd/Ctrl+A → Cmd/Ctrl+C)
- **Cell background** changes color when classified: red (FP), green (pair), amber (merger)

### Top Bar Buttons

| Button | Action |
|--------|--------|
| `◀ Anterior` | Previous page (auto-saves) |
| `Siguiente ▶` | Next page (auto-saves) |
| `🔄 Reintentar página` | Re-download failed images on current page |
| `Exportar CSV` | Export classification CSVs |
| `⚙` | Advanced options (clean saved images) |
| `Buscar par # [___] Ver` | Search a pair by ID and open detail window |

---

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `Space` / `→` | Next page |
| `←` | Previous page |
| `Tab` | Move selection to next cell |
| `Shift+Tab` | Move selection to previous cell |
| `F` | Toggle **Falso positivo** on selected cell |
| `P` | Toggle **Par confirmado** on selected cell |
| `M` | Toggle **Merger** on selected cell |
| `Ctrl+E` | Export CSV |

---

## Mouse Controls

| Action | Result |
|--------|--------|
| Single click on image | Select cell |
| Double click on image | Open Legacy Survey Sky Viewer in browser |
| Right click on image | Context menu (classify + Sky Viewer) |
| Click `[F]` / `[P]` / `[M]` buttons | Classify pair (toggle) |

---

## Detail Window (Pair Search)

Type a pair ID in the search bar and press **Enter** or **Ver** to open a full-screen detail popup:

- **520×520 px** image for closer inspection
- Same classify buttons with live state feedback (`✓` indicator)
- Keyboard shortcuts `F`, `P`, `M` work directly in the popup
- Double click → Sky Viewer
- Right click → context menu
- **🔄 Reintentar** button appears if the image failed to download
- Reclassifying in the popup immediately moves the image to the correct folder and updates `progress.json`

---

## Output Structure

```
outputs/
├── catalogs/
│   ├── progress.json          # auto-saved state (current index + all classifications)
│   ├── false_positives.csv    # exported via "Exportar CSV"
│   ├── possible_mergers.csv
│   └── confirmed_pairs.csv
├── fp_images/
│   └── par_XXXX.jpg           # clean 256×256 cutouts — false positives
├── pm_images/
│   └── par_XXXX.jpg           # clean 256×256 cutouts — possible mergers
└── pair_images/
    └── par_XXXX.jpg           # clean 256×256 cutouts — confirmed pairs
```

> **Important:** All saved images are **clean cutouts without any annotation marks**.
> Markers (crosses, circles, labels) are rendered only on-screen for inspection purposes
> and are never written to disk, making the images suitable for ML training.

---

## Image Downloading

Images are fetched from the [Legacy Survey DR10 cutout service](https://www.legacysurvey.org/viewer/):

```
https://www.legacysurvey.org/viewer/cutout.jpg
  ?ra=XX.XXXXX&dec=XX.XXXXX
  &pixscale=X.XX
  &layer=ls-dr10
  &size=256
```

- **Adaptive pixscale** — automatically adjusted so both galaxies fit in the frame
- **8 parallel workers** to avoid rate-limiting
- **3 retries** with exponential backoff (0.5 s, 1.0 s)
- **15 s timeout** per attempt
- Failed images show an orange error tile with the error message and a retry button

---

## Auto-save Behavior

Progress is saved automatically:
- On every classification (F / P / M)
- On every page navigation (next / previous)
- On window close

The `progress.json` file is **compatible with the original Jupyter notebook**, so both tools share the same state.

---

## Cleaning Saved Images

If you classified images before version 2 of the app (which saved annotated versions), use:

**⚙ → 🧹 Limpiar imágenes guardadas**

This re-downloads all classified images at 256×256 px without annotations, replacing existing files. Uses 24 parallel workers — typically completes in under 5 minutes even for thousands of images.

---

## ML Training Notes

The saved cutouts are suitable as training data for a CNN image classifier:

| Parameter | Value |
|-----------|-------|
| Resolution | 256 × 256 px |
| Channels | 3 (RGB color composite, grz bands) |
| Format | JPEG, quality=92 |
| Source | Legacy Survey DR10 |
| Classes | `fp_images/`, `pm_images/`, `pair_images/` |

**Recommended architecture:** ResNet-50 or EfficientNet-B3 with ImageNet pre-trained weights and fine-tuning on the 3-class problem.

**Recommended minimum dataset size:**
- ≥ 500 examples per class before training
- ≥ 1,000 per class for robust results
- Data augmentation (rotation, flip, zoom ×8–10) to expand the effective dataset

**Note on pixscale variability:** Each image has a different physical scale depending on the pair's angular separation. Consider storing the pixscale as an auxiliary feature for the model.

---

## Compatibility

| Platform | Status |
|----------|--------|
| macOS (Intel / Apple Silicon) | ✅ Tested |
| Linux | ✅ Supported |
| Windows | ⚠️ Should work, not tested |

---

## Author

Frank Bautista
