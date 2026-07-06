"""Shared RF feature definitions and derived feature builders."""
from __future__ import annotations

import numpy as np
import pandas as pd


BASE_FEATURES = [
    "rp_kpc",
    "sep_arcsec",

    "flux_r_contrast",
    "fiberflux_r_contrast",
    "fibertotflux_r_contrast",
    "fiber_self_ratio_r_min",
    "fiber_self_ratio_r_diff",
    "snr_r_min",
    "snr_r_diff",
    "shape_r_max",
    "shape_r_diff",

    "fiber_self_ratio_g_min",
    "fiber_self_ratio_g_diff",
    "fiber_self_ratio_z_min",
    "fiber_self_ratio_z_diff",

    "fiberflux_g_contrast",
    "fiberflux_z_contrast",
    "fibertotflux_g_contrast",
    "fibertotflux_z_contrast",
    "snr_g_min",
    "snr_g_diff",
    "snr_z_min",
    "snr_z_diff",
]

SPECTROSCOPIC_FEATURES = [
    "deltachi2_min",
    "deltachi2_max",
    "deltachi2_diff",
    "deltachi2_contrast",
    "zwarn_any",
]

MORPHOLOGY_DIAGNOSTICS = [
    "morphtype_has_psf",
    "morphtype_same",
]

# Canonical RF feature set: ablation winner for recall >= 90% cleaning.
# Keep spectroscopic, flow-scale and morphology derivatives out of the main RF
# until they improve the operational FP-rejection metric.
FEATURES = BASE_FEATURES


def _numeric_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def _morphtype_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(pd.NA, index=df.index, dtype="string")
    return df[col].astype("string").str.strip().str.upper()


def _add_flux_pair_features(out: pd.DataFrame, prefix: str, band: str) -> None:
    left = _numeric_series(out, f"{prefix}_{band}_1")
    right = _numeric_series(out, f"{prefix}_{band}_2")
    min_col = f"{prefix}_{band}_min"
    max_col = f"{prefix}_{band}_max"
    out[min_col] = np.minimum(left, right)
    out[max_col] = np.maximum(left, right)
    out[f"{prefix}_{band}_diff"] = (left - right).abs()

    for stat, col in [("min", min_col), ("max", max_col)]:
        values = out[col]
        log_values = np.full(len(out), np.nan, dtype="float64")
        valid = np.isfinite(values) & values.gt(0)
        log_values[valid.to_numpy()] = np.log10(values[valid])
        out[f"log10_{prefix}_{band}_{stat}"] = log_values


def add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add order-independent RF diagnostics derived from pair-member columns."""
    out = df.copy()

    for band in ("g", "r", "z"):
        _add_flux_pair_features(out, "flux", band)
        _add_flux_pair_features(out, "fiberflux", band)
        _add_flux_pair_features(out, "fibertotflux", band)

    deltachi2_1 = _numeric_series(out, "deltachi2_1")
    deltachi2_2 = _numeric_series(out, "deltachi2_2")
    out["deltachi2_min"] = np.minimum(deltachi2_1, deltachi2_2)
    out["deltachi2_max"] = np.maximum(deltachi2_1, deltachi2_2)
    out["deltachi2_diff"] = (deltachi2_1 - deltachi2_2).abs()

    valid_contrast = (
        np.isfinite(deltachi2_1)
        & np.isfinite(deltachi2_2)
        & deltachi2_1.gt(0)
        & deltachi2_2.gt(0)
    )
    contrast = np.full(len(out), np.nan, dtype="float64")
    contrast[valid_contrast.to_numpy()] = np.maximum(
        (deltachi2_1 / deltachi2_2)[valid_contrast],
        (deltachi2_2 / deltachi2_1)[valid_contrast],
    )
    out["deltachi2_contrast"] = contrast

    zwarn_1 = _numeric_series(out, "zwarn_1")
    zwarn_2 = _numeric_series(out, "zwarn_2")
    zwarn_any = (zwarn_1.notna() & zwarn_1.ne(0)) | (zwarn_2.notna() & zwarn_2.ne(0))
    zwarn_known = zwarn_1.notna() & zwarn_2.notna()
    out["zwarn_any"] = np.where(
        zwarn_any,
        1.0,
        np.where(zwarn_known, 0.0, np.nan),
    )

    morphtype_1 = _morphtype_series(out, "morphtype_1")
    morphtype_2 = _morphtype_series(out, "morphtype_2")
    morph_known_1 = morphtype_1.notna()
    morph_known_2 = morphtype_2.notna()
    morph_has_psf = morphtype_1.eq("PSF").fillna(False) | morphtype_2.eq("PSF").fillna(False)
    out["morphtype_has_psf"] = np.where(
        morph_has_psf,
        1.0,
        np.where(morph_known_1 & morph_known_2, 0.0, np.nan),
    )
    out["morphtype_same"] = np.where(
        morph_known_1 & morph_known_2,
        morphtype_1.eq(morphtype_2).astype("float64"),
        np.nan,
    )

    return out
