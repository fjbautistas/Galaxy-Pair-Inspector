# Galaxy Pair Inspector

Visual inspection app for DESI galaxy-pair and compact-group candidates. The
repository contains the inspection layer: it reads a prepared catalog, serves a
mobile/desktop classifier, stores human votes, and exports majority-vote labels
for later analysis or model training.

The app does not build the astronomical pair catalog. Catalog construction lives
outside this repository.

## Core Model

The app works with two item types:

| Type | Stable key | Human display |
|------|------------|---------------|
| Pair | `pair_uid` | `display_id` when available |
| Group | `stable_system_id` / `group_uid` | `group_id` when available |

`pair_uid` is the operational identifier for pairs. It is derived from the two
member galaxy identifiers as `min(TARGETID):max(TARGETID)` when it is not
already present in the catalog. Human votes, local progress, exports, and
Supabase rows should use this key instead of row position or catalog-local
integer IDs.

## Classification Labels

The UI uses user-facing names that avoid confusion with binary-classification
terminology. The stored labels remain compact and stable.

### Pairs

| Stored label | UI label | Meaning |
|--------------|----------|---------|
| `FP` | `NP` / NonPair | Not a usable pair candidate: projection, deblending issue, or markers do not identify two distinct galaxy centers. |
| `Pair` | `P` / Pair | Confirmed pair: two distinct galaxy centers are visible and consistent with a physical pair. |
| `PM` | Merger | Possible merger or strongly disturbed system where the two centers may be difficult to separate. |

### Groups

| Stored label | UI label | Meaning |
|--------------|----------|---------|
| `FP` | `NG` / NonGroup | Not a usable group candidate. |
| `GROUP` | `G` / Group | Confirmed compact group with three or more associated members. |
| `PM` | Merger | Possible merger or ambiguous compact interacting system. |
| `PP` | Pair | Visual evidence favors a pair inside the candidate group. |

## Repository Layout

```text
.
  index.html                         GitHub Pages redirect
  mobile/
    index.html                       PWA source/template
    GalPairs.html                    Generated standalone app
    sw.js                            Legacy Survey cutout cache
    manifest.json                    PWA manifest
  desktop/
    pair_inspector_app.py            Tkinter inspection app
  pipeline/
    export_standalone.py             Builds mobile/GalPairs.html
    generate_labels.py               Exports majority-vote labels
    audit_vote_consistency.py        Audits vote visibility and catalog membership
    identify_users.py                Summarizes classifier activity
    register_device.py               Registers a desktop/manual device
    plot_*.py                        Diagnostic figures
  supabase/
    schema.sql                       Current database schema and RPCs
  data/
    DESI_v5_3_pairs.parquet          Local active pair catalog, ignored by Git
    DESI_v5_3_groups.parquet         Local active group catalog, ignored by Git
    supplementary_calib_ids_v5_3.json Supplemental calibration IDs, versioned when present
  outputs/
    catalogs/                        Generated labels and local progress
    *_images/                        Saved cutouts
```

Most data products, images, CSV, JSON, and Parquet files are ignored by Git.

## Setup

Create `.env` in the repository root:

```bash
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_ANON_KEY=your-anon-key
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
PAIRS_CATALOG=data/DESI_v5_3_pairs.parquet
GROUPS_CATALOG=data/DESI_v5_3_groups.parquet
```

Install dependencies in the project environment:

```bash
conda activate astro-clean
pip install pandas numpy pillow requests pyarrow fastparquet matplotlib seaborn
```

`tkinter` is included with most Python installs on macOS. On Linux it may need
to be installed through the system package manager.

## Running The Apps

### Mobile PWA

The published app is served by GitHub Pages:

[https://fjbautistas.github.io/Galaxy-Pair-Inspector](https://fjbautistas.github.io/Galaxy-Pair-Inspector)

The root page redirects to `mobile/GalPairs.html`. The standalone file embeds
the current catalog and public Supabase settings.

Rebuild the standalone HTML after changing the catalog, template, or public app
configuration:

```bash
python pipeline/export_standalone.py
```

The script writes `mobile/GalPairs.html`. Review the diff before committing.

### Desktop App

```bash
python desktop/pair_inspector_app.py
```

The desktop app uses the fixed device id `DESKTOP`. If Supabase contains a
partition for that device, the app uses it; otherwise it falls back to local
catalog mode.

Register a desktop/manual device:

```bash
python pipeline/register_device.py --device DESKTOP
```

## Data Flow

```text
Prepared pair/group catalogs
    -> pipeline/export_standalone.py
    -> mobile/GalPairs.html and desktop/pair_inspector_app.py
    -> Supabase clasificaciones
    -> pipeline/generate_labels.py
    -> outputs/catalogs/labels*.csv
    -> analysis / model training
```

Local desktop progress is also stored in `outputs/catalogs/progress*.json`, and
desktop image exports are written under `outputs/`.

## Supabase

The current schema is documented in `supabase/schema.sql`.

Main tables:

| Table | Purpose |
|-------|---------|
| `clasificaciones` | One row per `(device_id, item_type, item_uid)` vote. |
| `partitions` | Device-level calibration and work assignment. |

Important columns in `clasificaciones`:

| Column | Meaning |
|--------|---------|
| `device_id` | Classifier/device identifier. |
| `item_type` | `pair` or `group`. |
| `item_uid` | Stable item key used for upserts. |
| `pair_uid` | Pair key, populated for pair rows. |
| `stable_system_id` | Group/system key, populated for group rows. |
| `id_par_v5` | Optional display integer retained for compatibility with the active database schema. |
| `classification` | Stored class label. |
| `source` | Origin of the row, normally `app`. |

Public writes should go through RPCs:

| RPC | Purpose |
|-----|---------|
| `upsert_classification(...)` | Save one vote. |
| `upsert_classifications(jsonb)` | Save a batch of votes. |
| `delete_classification(...)` | Remove one vote for a device/item. |
| `get_device_classifications(text)` | Restore a device's previous votes. |
| `assign_partition_mixed(...)` | Create or return a device work partition. |

Do not use row positions or catalog-local integer IDs as vote keys.

## Label Export

Generate majority-vote labels from Supabase:

```bash
python pipeline/generate_labels.py
```

Outputs:

```text
outputs/catalogs/labels.csv
outputs/catalogs/labels_calib.csv
outputs/catalogs/labels_groups.csv
outputs/catalogs/labels_groups_calib.csv
```

Pair label files use `pair_uid` as the join key. Group label files use
`group_uid` / `stable_system_id`.

## Diagnostics

Useful scripts:

```bash
python pipeline/identify_users.py
python pipeline/audit_vote_consistency.py
python pipeline/plot_dz_vs_rp.py
python pipeline/plot_dz_vs_sep.py
```

Diagnostic scripts write under `outputs/`, which is ignored by Git.
