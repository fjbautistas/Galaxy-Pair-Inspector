# Galaxy Pair Inspector

Visual inspection app for DESI galaxy-pair and compact-group candidates. The
repository contains the inspection layer: it reads a prepared catalog, serves a
mobile/desktop classifier, stores human votes, and exports majority-vote labels
for later analysis or model training.

The app does not build the astronomical pair catalog. Catalog construction lives
outside this repository.

## Current campaign (v6, Jul 2026)

The active campaign classifies **raw v6 pairs across 5–80 kpc** (before the RF
deblending cleaner), sampled **homogeneously in the (rp, z) plane** so the set is
robust across distance and redshift (director's request). Purpose: validate the
transfer of the classifier to 50–80 kpc (break the single-classifier monoculture)
and enable **cross-classifier normalization**.

Each user gets: a shared **550-pair calibration set** (275 single-vote from 5–50 to
lift them to multi-vote + 275 new in 50–80; this is also the RF held-out eval set) +
**900 new pairs** (v1 5–50 / v2 50–80, ~58/42) + interleaved **groups**
(`DESI_v6_groups`, 1 group per 5 pairs). The **pair type** (BGS-BGS, LRG-LRG, …) is
shown in the UI, below the coordinates.

- Builder: `pipeline/build_campaign_v6.py` → `mobile/catalog_v6.json`,
  `data/supplementary_calib_ids_v6.json`, and the deployable `mobile/GalPairs.html`.
- Full notes: [`outputs/campaign_v6_notes.md`](outputs/campaign_v6_notes.md).
- `hidden_triples` are **not** in the visual set (their third member is RELEASE=0,
  i.e. no Legacy imaging → not visually inspectable); they remain a counting systematic.
- Votes and label corrections are keyed by `pair_uid`, so they **persist** across the
  catalog change; the schema is unchanged (`assign_partition_mixed` already existed).

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
    build_campaign_v6.py             Builds the v6 campaign (homogeneous sample + catalog + app)
    export_standalone.py             Generic builder (older path; embeds a catalog into GalPairs.html)
    generate_labels.py               Exports majority-vote labels
    audit_vote_consistency.py        Audits vote visibility and catalog membership
    identify_users.py                Summarizes classifier activity
    register_device.py               Registers a desktop/manual device
    plot_*.py                        Diagnostic figures
  supabase/
    schema.sql                       Current database schema and RPCs
  data/
    supplementary_calib_ids_v6.json  Shared 550 calibration pair_uids (v6), versioned
    (parquets)                       Local pair/group catalogs, ignored by Git
  mobile/
    catalog_v6.json                  Active v6 campaign catalog (embedded into GalPairs.html)
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
# Only for the legacy export_standalone.py path; build_campaign_v6.py uses fixed paths
PAIRS_CATALOG=data/DESI_v5_3_pairs.parquet
GROUPS_CATALOG=data/DESI_v5_3_groups.parquet
```

The Supabase keys are read by both builders (they get injected into `GalPairs.html`).
`build_campaign_v6.py` reads its inputs directly (RF-applied v6 pairs, the live
`labels.csv`, and `DESI_v6_groups`) — see the constants at the top of that file.

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

Rebuild the standalone HTML for the **current v6 campaign** (samples the homogeneous
set, builds the catalog, and embeds it into the app in one step):

```bash
python pipeline/build_campaign_v6.py
```

This writes `mobile/catalog_v6.json`, `data/supplementary_calib_ids_v6.json`, and
`mobile/GalPairs.html`. If you only edited the template/UI, re-run it to re-embed.
Review the diff before committing, then `git push` (GitHub Pages serves GalPairs.html).

Sampling parameters (grid, calib size, block size, users) are constants at the top of
`build_campaign_v6.py`. The legacy `export_standalone.py` remains for the generic
"embed a catalog parquet" path.

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
v6 pairs (RF-applied) + groups + live labels
    -> pipeline/build_campaign_v6.py   (homogeneous (rp,z) sample + catalog + embed)
    -> mobile/GalPairs.html            (deploy to GitHub Pages)
    -> users classify -> Supabase clasificaciones
    -> pipeline/generate_labels.py
    -> outputs/catalogs/labels*.csv
    -> Dawid-Skene normalization + master + RF eval (analysis)
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
