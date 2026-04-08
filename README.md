# Galaxy Pair Inspector

Visual classification tool for galaxy pairs from the DESI Legacy Survey.
Built to support the construction of a labeled dataset for training a morphological classifier.

**Author:** Frank Bautista

---

## What it does

Given a catalog of galaxy pairs, the tool displays cutout images from the Legacy Survey and lets the user assign one of three labels:

- **Pair** — confirmed interacting pair
- **False Positive (FP)** — not a real pair
- **Possible Merger (PM)** — merger candidate

Classifications are saved automatically to a cloud database (Supabase), enabling multiple classifiers to work independently and contribute to the same dataset.

---

## Repository structure

```
Galaxy-Pair-Inspector/
  mobile/       Progressive web app (PWA) — runs on any phone via GitHub Pages
  desktop/      Desktop classifier (Tkinter) + exploratory notebook
  pipeline/     Scripts to export the app, import classifications, and sync with the database
  index.html    GitHub Pages entry point
```

---

## Usage

### Mobile app
Open **[fjbautistas.github.io/Galaxy-Pair-Inspector](https://fjbautistas.github.io/Galaxy-Pair-Inspector)** on any phone.
Classifications sync automatically to the cloud as you classify.

To regenerate the standalone HTML after catalog changes:
```bash
python pipeline/export_standalone.py
```

### Desktop app
```bash
python desktop/pair_inspector_app.py
```

### Download all classifications from the database
```bash
python pipeline/download_from_cloud.py
```

---

## Data pipeline

```
DESI catalog
    ↓
Mobile app / Desktop app
    ↓
Supabase (cloud database)
    ↓
pipeline/download_from_cloud.py  →  labels.csv
    ↓
Image generation  →  Google Drive
    ↓
RCNN training
```

---

## Scientific context

Galaxy pairs are identified from the DESI spectroscopic catalog by projected separation `rp < 12 kpc`.
Each pair is visually inspected to determine its morphological category.
The classified cutouts (256×256 px, Legacy Survey DR10 color composites) serve as training data for a supervised morphological classifier.

---

## Requirements

```bash
pip install pandas numpy pillow requests pyarrow
```

> `tkinter` is included in standard Python on macOS and Linux.
> On Linux you may need: `sudo apt-get install python3-tk`

---

## Keyboard shortcuts (desktop app)

| Key | Action |
|-----|--------|
| `Space` / `→` | Next page |
| `←` | Previous page |
| `Tab` / `Shift+Tab` | Move selection between cells |
| `F` | Toggle False Positive |
| `P` | Toggle Confirmed Pair |
| `M` | Toggle Possible Merger |
| `Ctrl+E` | Export CSV |
