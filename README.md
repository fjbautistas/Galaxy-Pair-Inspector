# Galaxy Pair Inspector

Visual classification tool for galaxy pairs from the DESI Legacy Survey.
The goal is to build a labeled dataset to train a morphological classifier.

**Author:** Frank Bautista

---

## What it does

Given a catalog of galaxy pairs, the tool shows cutout images from the Legacy Survey and asks the user to assign one of three labels:

- **Pair** — confirmed interacting pair
- **FP** — false positive, not a real pair
- **PM** — possible merger

Classifications go to a Supabase database. Multiple classifiers can work independently on different subsets of the catalog without overlap.

---

## Repository structure

```
Galaxy-Pair-Inspector/
  mobile/         PWA — runs on any phone via GitHub Pages
  desktop/        Tkinter app + exploratory notebook
  pipeline/       Export, device registration, image upload, label generation
  data/
    DESI_galaxies_base.parquet   All DESI sources, cleaned
    DESI_galaxies_phys.parquet   Physical properties
    raw/                         Pair catalogs and intermediate outputs
  outputs/
    catalogs/                    Classification results and progress files
  index.html      GitHub Pages entry point
```

---

## Setup

### 1. Create `.env` in the project root

```
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_ANON_KEY=your-anon-key
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
PAIRS_CATALOG=data/raw/DESI_int_legacyID_pairs.parquet
```

`PAIRS_CATALOG` is the only place you need to change if you rename or replace the pair catalog. All scripts read from this variable.

`.env` is in `.gitignore` and will never be committed.

### 2. Install dependencies

```bash
pip install pandas numpy pillow requests pyarrow
pip install google-auth google-auth-httplib2 google-api-python-client
```

`tkinter` comes with Python on macOS and Linux. On Linux you may also need:
```bash
sudo apt-get install python3-tk
```

---

## Usage

### Desktop app

```bash
python desktop/pair_inspector_app.py
```

The app reads your device's partition from Supabase and loads only your assigned block of pairs. If no partition exists for your device, register it first (see below).

### Mobile app

Open [fjbautistas.github.io/Galaxy-Pair-Inspector](https://fjbautistas.github.io/Galaxy-Pair-Inspector) on any phone. The app registers the device automatically on the first load and assigns it a unique block of pairs.

To rebuild the standalone HTML after catalog changes:

```bash
python pipeline/export_standalone.py
git add mobile/GalPairs.html && git commit -m "update catalog" && git push
```

---

## Partition system

Each device gets a non-overlapping slice of the catalog:

| Zone | Size | Description |
|------|------|-------------|
| Calibration pool | 150 pairs (indices 0–149) | Shown to every classifier in a different random order. Used later to compute inter-rater agreement (Cohen's/Fleiss' κ). |
| Work block | 3,000 pairs | Unique slice assigned per device, starting where the previous device's block ended. |

The mobile app self-registers. For the desktop app:

```bash
python pipeline/register_device.py --device DESKTOP
```

If the device is already registered, the command prints its existing assignment without changing anything.

---

## Pipeline scripts

### `pipeline/register_device.py`

Registers a device in Supabase and assigns its work block. Reads catalog length from `PAIRS_CATALOG` in `.env`.

```bash
python pipeline/register_device.py --device IPHONE_FRANK
```

### `pipeline/export_standalone.py`

Embeds the full pair catalog into `mobile/GalPairs.html`. The JS in that file handles partitioning at runtime.

```bash
python pipeline/export_standalone.py
```

### `pipeline/generate_and_upload_images.py`

Fetches classified pair IDs from Supabase, downloads 256×256 cutouts from Legacy Survey DR10, and uploads them to a shared Google Drive folder. Already-uploaded images are skipped.

```bash
python pipeline/generate_and_upload_images.py
```

Requires `google_credentials.json` (service account) in the project root.

---

## Data flow

```
DESI_galaxies_base.parquet
    ↓  pair-finding pipeline
data/raw/  (path set via PAIRS_CATALOG in .env)
    ↓
Mobile app / Desktop app  →  Supabase
    ↓
pipeline/generate_and_upload_images.py  →  Google Drive (cutout images)
    ↓
pipeline/generate_labels.py  →  labels.csv
    ↓
RCNN training
```

---

## Supabase setup

### 1. Create a project at [supabase.com](https://supabase.com)

### 2. Run this SQL in the editor

```sql
-- Classifications
create table clasificaciones (
  id             bigserial primary key,
  device_id      text        not null,
  id_par         integer     not null,
  classification text        not null,
  exported_at    timestamptz,
  created_at     timestamptz default now(),
  unique (device_id, id_par)
);

alter table clasificaciones enable row level security;

create policy "public_write"
  on clasificaciones for all
  using (true) with check (true);

grant usage on schema public to anon;
grant insert, update, select on table clasificaciones to anon;
grant usage, select on sequence clasificaciones_id_seq to anon;

-- Device partitions
create table partitions (
  id            bigserial primary key,
  device_id     text        not null unique,
  calib_seed    integer     not null,
  work_start    integer     not null,
  work_end      integer     not null,
  registered_at timestamptz default now()
);

alter table partitions enable row level security;

create policy "public_read"
  on partitions for select using (true);

create policy "auto_register"
  on partitions for insert with check (true);

grant usage on schema public to anon;
grant select, insert on table partitions to anon;
grant usage, select on sequence partitions_id_seq to anon;
```

### 3. Copy keys to `.env`

Keys are under **Settings → API** in your Supabase project.

### Running without Supabase

The apps work without a database. Classifications go to `outputs/catalogs/progress.json` (desktop) and `localStorage` (mobile). Cloud sync does nothing if `.env` is missing.

---

## Google Drive setup

Images are uploaded via a service account, so there's no OAuth prompt.

1. Create a project in [Google Cloud Console](https://console.cloud.google.com)
2. Enable the **Google Drive API**
3. Create a service account and download `google_credentials.json` to the project root
4. Create a folder in Google Drive and share it with the service account email (Editor access)
5. Set `DRIVE_FOLDER_ID` in `pipeline/generate_and_upload_images.py` to the folder ID from the URL

`google_credentials.json` is in `.gitignore` and will never be committed.

---

## Keyboard shortcuts (desktop app)

| Key | Action |
|-----|--------|
| `Space` / `→` | Next page |
| `←` | Previous page |
| `Tab` / `Shift+Tab` | Move between cells |
| `F` | Toggle False Positive |
| `P` | Toggle Confirmed Pair |
| `M` | Toggle Possible Merger |
| `Ctrl+E` | Export CSV |
