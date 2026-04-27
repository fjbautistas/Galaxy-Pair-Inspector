# Galaxy Pair Inspector

Visual inspection tool for DESI galaxy pairs and FoF groups using DESI Legacy
Survey DR10 cutouts. The goal is to build a curated labeled dataset for
training and validating a morphological classifier of interacting systems,
false positives, mergers, and compact groups.

**Author:** Frank Bautista

---

## Current Scope

The repository contains the inspection layer around an external pair-finding
pipeline. It does not generate the original DESI pair catalog; it serves,
partitions, visualizes, records, exports, and analyzes human classifications.

The app supports two object types:

- **Pairs** from `PAIRS_CATALOG`, currently used out to `rp < 50 kpc`.
- **Groups** from a FoF edge catalog, configured through `GROUPS_CATALOG`.

Classifications are saved locally and, when configured, upserted to Supabase.
Multiple classifiers can work in parallel on non-overlapping work blocks while
sharing calibration items for inter-rater agreement checks.

---

## Classification Labels

### Pairs

| Label | Meaning |
|------|---------|
| `FP` | False positive. Usually projection or Legacy Survey deblending where the markers do not indicate two real galaxies. |
| `Pair` | Confirmed pair. Two separate galactic centers are visible and plausibly interacting. |
| `PM` | Possible merger. Two nuclei or disturbed components inside a shared envelope; centers may be hard to separate. |

### Groups

Groups are stored in Supabase as `group_id + 10_000_000` to avoid collisions
with pair `id_par` values.

| Label | Meaning |
|------|---------|
| `FP` | False group, projection, or severe deblending artifact. |
| `GROUP` | Confirmed physical group with 3 or more associated galaxies. |
| `PM` | Possible merger or ambiguous compact interacting system. |
| `PP` | Possible pair inside a group, or group catalog entry where the visual evidence supports a pair but not a full group. |

---

## Repository Structure

```text
Galaxy-Pair-Inspector/
  index.html                         GitHub Pages redirect to the mobile app
  mobile/
    index.html                       PWA source/template
    GalPairs.html                    Generated standalone app for GitHub Pages
    sw.js                            Service worker for Legacy Survey image cache
    manifest.json                    PWA manifest
  desktop/
    pair_inspector_app.py            Tkinter inspection app
    GalaxyPairInspector.ipynb        Original exploratory notebook
  pipeline/
    export_standalone.py             Builds mobile/GalPairs.html
    register_device.py               Manual device registration for desktop
    generate_labels.py               Majority-vote label export from Supabase
    migrate_to_v3.py                 Translate old labels to v3 ids by TARGETID pair
    generate_and_upload_images.py    Download classified cutouts and upload to Drive
    download_from_cloud.py           Export Supabase rows as JSON backup
    identify_users.py                Summarize device activity
    plot_*.py                        Diagnostic plots
  supabase/migrations/
    02_extend_to_50kpc.sql           RPCs and fields for rp<50 two-slice flow
  data/
    supplementary_calib_ids.json     Canonical supplemental calibration pair IDs
  outputs/
    catalogs/                        Local progress and generated label CSVs
    *_images/                        Saved clean cutouts by class
```

Most data, image, CSV, JSON, and Parquet outputs are ignored by Git. The
canonical exception currently versioned is `data/supplementary_calib_ids.json`.

---

## Setup

Create `.env` in the project root:

```bash
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_ANON_KEY=your-anon-key
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
PAIRS_CATALOG=data/DESI_v3_pairs.parquet
GROUPS_CATALOG=data/DESI_v3_groups.parquet
```

Install Python dependencies:

```bash
pip install pandas numpy pillow requests pyarrow
pip install google-auth google-auth-httplib2 google-api-python-client
```

`tkinter` is included with most Python installs on macOS. On Linux you may need:

```bash
sudo apt-get install python3-tk
```

---

## Running The Apps

### Mobile PWA

Open:

[https://fjbautistas.github.io/Galaxy-Pair-Inspector](https://fjbautistas.github.io/Galaxy-Pair-Inspector)

The root `index.html` redirects to `mobile/GalPairs.html` while preserving URL
hashes used for recovery links. On first load, the app creates a local device
ID, asks Supabase for a partition through `assign_partition`, and stores local
progress in `localStorage`.

After changing catalogs or Supabase keys, rebuild the standalone HTML:

```bash
python pipeline/export_standalone.py
git add mobile/GalPairs.html
git commit -m "update catalog"
git push
```

### Desktop App

```bash
python desktop/pair_inspector_app.py
```

The desktop app uses the fixed device id `DESKTOP`. It reads the corresponding
partition from Supabase when available; without a partition or without Supabase,
it falls back to local/full-catalog mode.

Register the desktop device manually when needed:

```bash
python pipeline/register_device.py --device DESKTOP
```

---

## Partition And Calibration Design

The current mobile flow combines pair and group inspection.

| Zone | Size | Purpose |
|------|------|---------|
| Pair calibration base | 120 pairs | Shared by all users in seeded random order. |
| Group calibration | 80 groups | Shared by all users in seeded random order. |
| Pair supplemental calibration | 150 pairs | Canonical `rp in [20, 50] kpc` set from `data/supplementary_calib_ids.json`. Kept in the visible catalog for all users. |
| Pair work block | 1,000 pairs target | New-user mixed assignment: 50% from `5 <= rp < 20 kpc` and 50% from `20 <= rp < 50 kpc`. Existing active users keep their historical block truncated to the first 1,000 pairs. |
| Group work block | 100 groups | Non-overlapping group assignment for active/new partitions. |

The app interleaves items roughly as 10 pairs per 1 group. Calibration items must
be classified by each user independently; work items can count existing desktop
classifications when deciding what to skip.

The current Supabase state is layered through migrations:

- `02_extend_to_50kpc.sql` adds `calib_v`, `work_start_v2`, `work_end_v2`,
  and the initial two-slice flow.
- `05_rebalance_1000_items.sql` rebases the operational target to 1450 visible
  items per user and changes new assignments to 1000 work pairs at 50/50.
- `assign_partition_mixed(...)` atomically creates or returns a mixed 50/50 device partition.
- `assign_partition(...)` remains as a legacy fallback.
- `claim_v2_slice(...)` is called after supplemental calibration is complete and
  closes the supplemental-calibration state without changing active truncated
  partitions.
- `calib_v`, `work_start_v2`, and `work_end_v2` track this state.

---

## Data Flow

```text
External DESI pair/group pipeline
    ↓
PAIRS_CATALOG / GROUPS_CATALOG
    ↓
pipeline/export_standalone.py
    ↓
mobile/GalPairs.html + desktop/pair_inspector_app.py
    ↓
Supabase clasificaciones table
    ↓
pipeline/generate_labels.py
    ↓
outputs/catalogs/labels*.csv
    ↓
ML training / diagnostics
```

Local desktop classifications also save clean image cutouts into the class
folders under `outputs/`.

---

## Supabase Model

Main tables:

- `clasificaciones`: one row per `(device_id, id_par)` with upsert semantics.
- `partitions`: device-level work assignment and calibration state.

Important convention:

```text
pairs:  id_par = catalog id_par
groups: id_par = group_id + 10_000_000
```

For a new Supabase project, create the base tables first:

```sql
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

create table partitions (
  device_id        text primary key,
  calib_seed       integer not null,
  work_start       integer not null,
  work_end         integer not null,
  group_work_start integer,
  group_work_end   integer,
  registered_at    timestamptz default now()
);

alter table partitions enable row level security;

create policy "public_read"
  on partitions for select using (true);

create policy "auto_register"
  on partitions for insert with check (true);

grant usage on schema public to anon;
grant select, insert on table partitions to anon;
```

Then apply the current migration:

```text
supabase/migrations/02_extend_to_50kpc.sql
```

The migration is idempotent and refreshes the `assign_partition` and
`claim_v2_slice` RPCs used by the mobile app.

---

## Pipeline Scripts

### `pipeline/export_standalone.py`

Reads `PAIRS_CATALOG`, optional `GROUPS_CATALOG`, local desktop classifications,
and supplemental calibration IDs. It embeds everything into `mobile/GalPairs.html`.

```bash
python pipeline/export_standalone.py
```

### `pipeline/register_device.py`

Registers or prints an existing device partition. Useful for desktop/manual
setup.

```bash
python pipeline/register_device.py --device IPHONE_FRANK
```

### `pipeline/generate_labels.py`

Downloads Supabase classifications, applies majority vote, omits tied items,
and writes:

- `outputs/catalogs/labels.csv`
- `outputs/catalogs/labels_calib.csv`
- `outputs/catalogs/labels_groups.csv`
- `outputs/catalogs/labels_groups_calib.csv`

```bash
python pipeline/generate_labels.py
```

### `pipeline/migrate_to_v3.py`

Translates existing labels from the old pair catalog to `DESI_v3_pairs.parquet`
by crossmatching the symmetric `(id1, id2)` TARGETID pair.

```bash
python pipeline/migrate_to_v3.py
```

### `pipeline/download_from_cloud.py`

Downloads all rows from Supabase as a JSON backup compatible with older import
workflows.

```bash
python pipeline/download_from_cloud.py
```

### `pipeline/generate_and_upload_images.py`

Downloads 256x256 cutouts for classified pair IDs and uploads missing files to
the configured Google Drive folder. Requires `google_credentials.json`.

```bash
python pipeline/generate_and_upload_images.py
```

### Diagnostic plots

```bash
python pipeline/plot_dz_vs_rp.py
python pipeline/plot_dz_vs_sep.py
python pipeline/plot_classification_impact.py
```

Plots are written to `outputs/plots/`.

---

## Running Without Supabase

The apps still work locally without Supabase:

- Desktop progress: `outputs/catalogs/progress.json` and
  `outputs/catalogs/progress_groups.json`.
- Mobile progress: browser `localStorage`.

Cloud sync and automatic non-overlapping partition assignment require Supabase.

---

## Desktop Shortcuts

| Key | Action |
|-----|--------|
| `Space` / `Right` | Next page |
| `Left` | Previous page |
| `Tab` / `Shift+Tab` | Move selected cell |
| `F` | Toggle false positive |
| `P` | Toggle confirmed pair |
| `M` | Toggle possible merger |
| `G` | Toggle confirmed group in group cells |
| `Ctrl+E` | Export CSV |

Double-click an image to open the Legacy Survey Sky Viewer.
