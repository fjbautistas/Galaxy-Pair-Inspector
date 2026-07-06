"""
Construye las tablas maestras de features con etiquetas visuales para los
ensayos de Random Forest (RF_Essays).

Cadena de datos (contrato):
    Galaxy_pairs: find_pairs           -> DESI_<v>_pairs.parquet (posicional)
                  build_pairs_enriched -> DESI_<v>_pairs_enriched.parquet (+ aux/fotometría)
    Aquí:         labels + enriquecido -> master_features_<v>_*.parquet (en exports/)
    RF_Essays consume exports/.
El enriquecido es OBLIGATORIO: las features de deblending vienen de la
fotometría (aux). Sin fotometría no hay RF; es intrínseco, no un defecto.

Versión (`--version vN`):
  - Selecciona el enriquecido de ENTRADA: DESI_<vN>_pairs_enriched.parquet
    (default derivado de la versión; se puede sobreescribir con --pairs).
  - Nombra las SALIDAS: master_features_<vN>_*.parquet, summary_counts_<vN>.csv.

Labels: GENÉRICAS (independientes de versión, se cruzan por `pair_uid`). Fuente viva
= `outputs/catalogs/labels.csv` (salida de generate_labels.py), leída en su lugar sin
copias. Sirve para cualquier versión del catálogo.

Cobertura: las labels que no cruzan por `pair_uid` (p.ej. binarios que al ampliar
rp_max adquieren un 3er vecino y pasan a grupos) se AVISAN y se descartan; el RF
de pares entrena solo con binarios vigentes.

Identificadores estables del par:
  - `pair_uid`  : min(TARGETID):max(TARGETID) del par.
  - `stable_id` : hash FoF del par, invariante entre corridas del pipeline.
No depende de columnas de migración (`id_par_v3`, `id_par_v5`, `stable_id_v5`).
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from rf_features import add_derived_features


INSPECTION_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_DIR = INSPECTION_ROOT / "exports"
# Fuente VIVA de labels (salida de generate_labels.py desde Supabase). Se lee en su
# lugar, sin copias, para evitar drift de versiones.
DEFAULT_LABELS = INSPECTION_ROOT / "outputs/catalogs/labels.csv"
# Directorio del catálogo de pares enriquecido (producido por Galaxy_pairs).
PAIRS_DIR = Path(
    "/Users/frank/Documents/Estudio-PhD/Semestre-2025-II/Tesis_I/Galaxy_Pairs/Galaxy_pairs"
    "/outputs/catalogs/interacting"
)


def default_pairs_path(version: str) -> Path:
    """Enriquecido de la versión: DESI_<version>_pairs_enriched.parquet."""
    return PAIRS_DIR / f"DESI_{version}_pairs_enriched.parquet"

# Claves estables + etiqueta + geometría. Obligatorias.
REQUIRED_COLS = [
    "pair_uid", "stable_id",
    "classification", "n_votes", "agreement",
    "id1", "id2", "ra1", "dec1", "z1", "ra2", "dec2", "z2",
    "rp_kpc", "dv_kms", "sep_arcsec", "dz",
]
# Se arrastran si existen: referencia, metadata FoF (ausente en el catálogo de
# pares lean) y tipo de par (BGS-BGS/LRG-LRG/ELG-ELG/mixto).
OPTIONAL_CARRY = [
    "id_par",                                   # entero por corrida (no estable)
    "component_size", "fof_component_id", "system_group_id",
    "pair_type", "galaxy_type_1", "galaxy_type_2",
]
# Features observacionales/deblending (se incluyen las presentes en el catálogo).
FEATURE_COLS = [
    "flux_g_1", "flux_g_2", "flux_r_1", "flux_r_2", "flux_z_1", "flux_z_2",
    "flux_g_contrast", "flux_r_contrast", "flux_z_contrast",
    "fiberflux_g_1", "fiberflux_g_2", "fiberflux_r_1", "fiberflux_r_2",
    "fiberflux_z_1", "fiberflux_z_2",
    "fiberflux_g_contrast", "fiberflux_r_contrast", "fiberflux_z_contrast",
    "fibertotflux_g_1", "fibertotflux_g_2", "fibertotflux_r_1", "fibertotflux_r_2",
    "fibertotflux_z_1", "fibertotflux_z_2",
    "fibertotflux_g_contrast", "fibertotflux_r_contrast", "fibertotflux_z_contrast",
    "fiber_self_ratio_g_min", "fiber_self_ratio_g_max", "fiber_self_ratio_g_diff",
    "fiber_self_ratio_r_min", "fiber_self_ratio_r_max", "fiber_self_ratio_r_diff",
    "fiber_self_ratio_z_min", "fiber_self_ratio_z_max", "fiber_self_ratio_z_diff",
    "snr_g_1", "snr_g_2", "snr_g_min", "snr_g_diff",
    "snr_r_1", "snr_r_2", "snr_r_min", "snr_r_diff",
    "snr_z_1", "snr_z_2", "snr_z_min", "snr_z_diff",
    "shape_r_1", "shape_r_2", "shape_r_max", "shape_r_diff",
    "deltachi2_1", "deltachi2_2", "zwarn_1", "zwarn_2",
    "morphtype_1", "morphtype_2",
    "is_inner_rp_link", "is_low_dv_link", "is_clean_link",
    "group_has_inner_rp_link", "group_has_low_dv_link",
]


def load_inputs(labels_path: Path, pairs_path: Path,
                corrections_path: Path | None = None) -> pd.DataFrame:
    if not labels_path.exists():
        raise SystemExit(f"Faltan las labels: {labels_path}")
    if not pairs_path.exists():
        raise SystemExit(f"Falta el catálogo de pares: {pairs_path}")

    labels = pd.read_csv(labels_path)
    # Overlay de correcciones manuales (revisión visual): sobrescribe `classification`
    # por `pair_uid`. NO toca labels.csv (fuente viva de Supabase) — es un archivo aparte
    # y versionable. Se aplica solo si se pasa --corrections y el archivo existe.
    if corrections_path is not None and corrections_path.exists():
        corr = pd.read_csv(corrections_path)
        cmap = corr.drop_duplicates("pair_uid").set_index("pair_uid")["classification"]
        before = labels["classification"].to_numpy(copy=True)
        labels["classification"] = labels["pair_uid"].map(cmap).fillna(labels["classification"])
        n_applied = int((labels["classification"].to_numpy() != before).sum())
        print(f"[correcciones] {corrections_path.name}: {len(cmap)} en overlay, "
              f"{n_applied} labels sobrescritas.")
    pairs = pd.read_parquet(pairs_path)
    if pairs["pair_uid"].duplicated().any():
        dup = int(pairs["pair_uid"].duplicated().sum())
        raise SystemExit(f"El catálogo de pares tiene pair_uid duplicados: {dup}")

    merged = labels.merge(
        pairs, on="pair_uid", how="left",
        validate="one_to_one", suffixes=("_label", ""),
    )
    missing = int(merged["stable_id"].isna().sum())
    if missing:
        # Labels que ya no son binarios en esta versión (al ampliar rp_max algunos
        # pares adquieren un 3er vecino y pasan a grupos). Se avisan y se descartan:
        # el RF de pares entrena solo con binarios vigentes.
        print(f"[aviso] {missing} labels no cruzaron por pair_uid "
              f"(ya no son binarios en esta versión) -> se descartan.")
        merged = merged[merged["stable_id"].notna()].copy()
    return merged


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise SystemExit(f"Faltan columnas requeridas en la tabla cruzada: {missing}")

    carry = REQUIRED_COLS + [c for c in OPTIONAL_CARRY if c in df.columns]
    feats = df[carry].copy()
    feats["z_mean"] = 0.5 * (df["z1"] + df["z2"])

    for col in FEATURE_COLS:
        if col in df.columns:
            feats[col] = df[col]

    feats = add_derived_features(feats)

    if "flux_r_contrast" in feats.columns:
        feats["dmag_r"] = np.where(
            feats["flux_r_contrast"].gt(0),
            2.5 * np.log10(feats["flux_r_contrast"]),
            np.nan,
        )

    feats["is_real_pair"] = feats["classification"].map({"FP": 0, "Pair": 1})
    return feats


def write_summary(feats: pd.DataFrame, binary: pd.DataFrame,
                  out_dir: Path, version: str) -> pd.DataFrame:
    rows: list[dict] = []
    for name, frame in [("visible_all", feats), ("binary_pair_fp", binary)]:
        for cls, n in frame["classification"].value_counts(dropna=False).items():
            rows.append({"sample": name, "classification": cls, "n": int(n)})
        rows.append({"sample": name, "classification": "TOTAL", "n": int(len(frame))})
    summary = pd.DataFrame(rows)
    summary.to_csv(out_dir / f"summary_counts_{version}.csv", index=False)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Construye master_features_{version} para los ensayos de RF."
    )
    parser.add_argument("--version", default="v5_3",
                        help="Etiqueta de versión del catálogo (v5_3, v6, ...).")
    parser.add_argument("--pairs", type=Path, default=None,
                        help="Catálogo de pares enriquecido. Default: DESI_<version>_pairs_enriched.parquet.")
    parser.add_argument("--labels", type=Path, default=None,
                        help="CSV de labels (default: outputs/catalogs/labels.csv, salida viva de generate_labels.py).")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR,
                        help="Directorio de salida (default: Galaxes_Inspection/exports).")
    parser.add_argument("--corrections", type=Path, default=None,
                        help="CSV overlay (pair_uid,classification) de correcciones manuales; "
                             "sobrescribe labels por pair_uid SIN tocar labels.csv.")
    args = parser.parse_args()

    version = args.version
    out_dir = args.out_dir
    pairs_path = args.pairs or default_pairs_path(version)
    labels_path = args.labels or DEFAULT_LABELS
    out_dir.mkdir(parents=True, exist_ok=True)

    merged = load_inputs(labels_path, pairs_path, args.corrections)
    feats = build_features(merged)
    binary = feats[feats["classification"].isin(["FP", "Pair"])].copy()

    vis_path = out_dir / f"master_features_{version}_visible.parquet"
    bin_path = out_dir / f"master_features_{version}_binary_pair_fp.parquet"
    feats.to_parquet(vis_path, index=False)
    binary.to_parquet(bin_path, index=False)
    summary = write_summary(feats, binary, out_dir, version)

    print("Escrito:")
    for path in [vis_path, bin_path, out_dir / f"summary_counts_{version}.csv"]:
        print(f"  {path}")
    print("\nResumen:")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
