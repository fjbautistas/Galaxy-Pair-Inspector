#!/usr/bin/env python3
"""
build_campaign_v6.py — muestreo de la campaña de inspección visual v6.

Construye el conjunto de pares (y su orden) para la app, muestreando de forma
HOMOGÉNEA en la grilla (rp, z) — no proporcional a la abundancia — como pidió el
director (muestra robusta pareja en distancia y redshift).

Salidas:
  - data/DESI_v6_campaign_pairs.parquet  (pares ORDENADOS: calib | v1 | v2, con pair_type)
  - data/supplementary_calib_ids_v6.json ({"pair_uid": [...]} de los 550 calib)
  - outputs/campaign_v6_coverage.png     (heatmap rp×z para verificar homogeneidad)

NO toca la app ni Supabase. Es la capa de datos de la campaña.
"""
from __future__ import annotations
from pathlib import Path
import json
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
GI = HERE.parent                                   # Galaxes_Inspection/
TESIS_II = GI.parent
GP = (TESIS_II.parents[1] / "Semestre-2025-II" / "Tesis_I" / "Galaxy_Pairs" / "Galaxy_pairs")
RF_APPLIED = TESIS_II / "RF_Essays" / "tables" / "DESI_v6_pairs_rf_applied.parquet"
GROUPS_V6 = GP / "outputs" / "catalogs" / "interacting" / "DESI_v6_groups.parquet"
LABELS = GI / "outputs" / "catalogs" / "labels.csv"
OUT_PARQUET = GI / "data" / "DESI_v6_campaign_pairs.parquet"
OUT_CALIB = GI / "data" / "supplementary_calib_ids_v6.json"
OUT_CATALOG = GI / "mobile" / "catalog_v6.json"
OUT_PNG = GI / "outputs" / "campaign_v6_coverage.png"
CATALOG_VERSION = "v6"

# ── Parámetros de la campaña ────────────────────────────────────────────────
RP_EDGES = np.arange(5, 80 + 5, 5)      # 15 bins de 5 kpc (5..80)
Z_EDGES = np.arange(0.0, 1.0 + 0.1, 0.1)  # 10 bins de 0.1 (0..1)
RP_SPLIT = 50.0                          # frontera v1 (5–50) / v2 (50–80)
N_CALIB = 550                            # set común (todos)
CALIB_SINGLE_FRAC = 0.5                  # mitad single-vote, mitad nuevos
N_USERS = 20
BLOCK_SIZE = 900                         # por usuario
POOL_TARGET = N_USERS * BLOCK_SIZE       # 18,000
RARE_TYPES = ["LRG-LRG", "ELG-ELG", "ELG-LRG", "BGS-LRG"]
RARE_FLOOR_FRAC = 0.05                   # piso mínimo por tipo raro en cada bloque
RANDOM_STATE = 42
# Grupos (track visual = DESI_v6_groups; hidden_triples NO son visuales)
N_CALIB_GROUPS = 80
GROUPS_PER_USER = N_USERS * (BLOCK_SIZE // 5)   # 1 grupo / 5 pares → 180/usuario → 3,600
MAX_GROUP_MEMBERS = 8


def _cell(df):
    """Asigna (rp_bin, z_bin) a cada par. z = media del par."""
    df = df.copy()
    df["z_mean"] = 0.5 * (df["z1"] + df["z2"])
    df["rp_bin"] = pd.cut(df["rp_kpc"], RP_EDGES, right=False, labels=False)
    df["z_bin"] = pd.cut(df["z_mean"], Z_EDGES, right=False, labels=False)
    return df.dropna(subset=["rp_bin", "z_bin"])


def homogeneous_sample(df, n_target, rng, floor_types=None):
    """Muestreo HOMOGÉNEO en (rp_bin, z_bin): ~igual por celda no vacía.
    Con piso mínimo por pair_type raro. Devuelve los pair_uid elegidos."""
    cells = df.groupby(["rp_bin", "z_bin"])
    n_cells = cells.ngroups
    per_cell = max(1, int(np.ceil(n_target / n_cells)))
    chosen = []
    for _, g in cells:
        take = min(per_cell, len(g))
        chosen.append(g.sample(take, random_state=rng))
    out = pd.concat(chosen, ignore_index=True)
    # piso por tipo raro (garantiza representación aunque la celda los sub-muestree)
    if floor_types:
        floor_n = int(RARE_FLOOR_FRAC * n_target)
        for t in floor_types:
            have = (out.pair_type == t).sum()
            if have < floor_n:
                extra = df[(df.pair_type == t) & (~df.pair_uid.isin(out.pair_uid))]
                if len(extra):
                    out = pd.concat([out, extra.sample(min(floor_n - have, len(extra)),
                                                        random_state=rng)], ignore_index=True)
    # si sobra respecto a n_target, recorta manteniendo homogeneidad (shuffle + head)
    out = out.sample(frac=1, random_state=rng).reset_index(drop=True)
    if len(out) > n_target:
        out = out.head(n_target)
    return out


def _pair_entries(df):
    """Entries de pares en el formato que embebe la app (con pair_type)."""
    df = df.copy()
    df["ra_mid"] = (df.ra1 + df.ra2) / 2.0
    df["dec_mid"] = (df.dec1 + df.dec2) / 2.0
    out = []
    for _, r in df.iterrows():
        out.append({
            "pair_uid": str(r.pair_uid),
            "ra1": round(float(r.ra1), 5), "dec1": round(float(r.dec1), 5),
            "ra2": round(float(r.ra2), 5), "dec2": round(float(r.dec2), 5),
            "ra_mid": round(float(r.ra_mid), 5), "dec_mid": round(float(r.dec_mid), 5),
            "sep_arcsec": round(float(r.sep_arcsec), 1),
            "rp": round(float(r.rp_kpc), 1),
            "z1": round(float(r.z1), 4), "z2": round(float(r.z2), 4),
            "pair_type": str(r.pair_type),
        })
    return out


def _sample_and_build_groups(rng):
    """Track visual de grupos: DESI_v6_groups, muestreo homogéneo en (z × tamaño).
    Devuelve (entries_ordenados, n_calib)."""
    e = pd.read_parquet(GROUPS_V6, columns=[
        "fof_component_id", "stable_system_id", "component_size",
        "id1", "ra1", "dec1", "z1", "id2", "ra2", "dec2", "z2", "sep_arcsec", "rp_kpc"])
    summ = (e.groupby("fof_component_id")
              .agg(size=("component_size", "first"), zc=("z1", "mean"),
                   rpmax=("rp_kpc", "max"), maxsep=("sep_arcsec", "max"))
              .reset_index())
    summ = summ[summ.zc > 0.01].copy()                       # excluir volumen local (FoF z≈0)
    summ["z_bin"] = pd.cut(summ.zc, Z_EDGES, right=False, labels=False)
    summ["size_bin"] = summ["size"].clip(upper=6)            # 3,4,5,6+
    summ = summ.dropna(subset=["z_bin"])

    def homog(df, n):                                        # igual-por-celda (z × size)
        cells = df.groupby(["z_bin", "size_bin"])
        per = max(1, int(np.ceil(n / cells.ngroups)))
        picked = pd.concat([g.sample(min(per, len(g)), random_state=rng) for _, g in cells])
        return picked.sample(frac=1, random_state=rng).head(n)

    calib = homog(summ, N_CALIB_GROUPS)
    pool = homog(summ[~summ.fof_component_id.isin(calib.fof_component_id)], GROUPS_PER_USER)
    ordered_ids = list(calib.fof_component_id) + list(pool.fof_component_id)
    print(f"\nGRUPOS: calib {len(calib)} + pool {len(pool)} = {len(ordered_ids)} "
          f"(de {len(summ):,} grupos v6)")
    print("tamaños (calib+pool):",
          pd.concat([calib, pool])["size"].value_counts().sort_index().to_dict())

    # entries en formato app (una por grupo, miembros más cercanos al centroide)
    by_gid = {gid: g for gid, g in e.groupby("fof_component_id")}
    entries = []
    for gid in ordered_ids:
        g = by_gid[gid]
        a = g[["id1", "ra1", "dec1", "z1"]].rename(columns={"id1": "id", "ra1": "ra", "dec1": "dec", "z1": "z"})
        b = g[["id2", "ra2", "dec2", "z2"]].rename(columns={"id2": "id", "ra2": "ra", "dec2": "dec", "z2": "z"})
        m = pd.concat([a, b]).drop_duplicates("id")
        rc, dc, zc = float(m.ra.mean()), float(m.dec.mean()), float(m.z.mean())
        m = m.assign(_d=np.hypot(m.ra - rc, m.dec - dc)).sort_values("_d").head(MAX_GROUP_MEMBERS)
        ssid = g.stable_system_id.dropna().astype(str)
        ssid = ssid.iloc[0] if len(ssid) else None
        entries.append({
            "group_id": int(gid), "group_uid": ssid or str(int(gid)),
            "stable_system_id": ssid, "n_members": int(len(pd.concat([a, b]).drop_duplicates("id"))),
            "ra_center": round(rc, 6), "dec_center": round(dc, 6), "z_center": round(zc, 4),
            "max_sep_arcsec": round(float(g.sep_arcsec.max()), 2),
            "rp_kpc_max": round(float(g.rp_kpc.max()), 2),
            "member_ra": [round(float(v), 5) for v in m.ra],
            "member_dec": [round(float(v), 5) for v in m.dec],
        })
    return entries, len(calib)


def _embed_html(catalog, calib_ids):
    """Inyecta el catálogo v6 + calib + claves Supabase en index.html → GalPairs.html."""
    env = {}
    envp = GI / ".env"
    if envp.exists():
        for line in envp.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    template = (GI / "mobile" / "index.html").read_text(encoding="utf-8")
    inject = (
        "<script>"
        f"window._CATALOG={json.dumps(catalog, separators=(',', ':'))};"
        f"window._SUPP_CALIB_IDS={json.dumps(calib_ids, separators=(',', ':'))};"
        f"window._SUPABASE_URL={json.dumps(env.get('SUPABASE_URL', ''))};"
        f"window._SUPABASE_ANON_KEY={json.dumps(env.get('SUPABASE_ANON_KEY', ''))};"
        "</script>\n  "
    )
    anchor = "// CONSTANTS — match desktop app"
    idx = template.index(anchor)
    script_idx = template.rindex("<script>", 0, idx)
    html = template[:script_idx] + inject + template[script_idx:]
    out = GI / "mobile" / "GalPairs.html"
    out.write_text(html, encoding="utf-8")
    print(f"[ok] APP v6 → {out}  ({out.stat().st_size/1e6:.1f} MB)")
    print(f"     Supabase inyectado: {'sí' if env.get('SUPABASE_URL') else 'NO (revisar .env)'}")


def main():
    rng = np.random.default_rng(RANDOM_STATE)
    rf = pd.read_parquet(RF_APPLIED, columns=[
        "pair_uid", "id1", "id2", "ra1", "dec1", "z1", "ra2", "dec2", "z2",
        "rp_kpc", "sep_arcsec", "pair_type"])
    lab = pd.read_csv(LABELS, usecols=["pair_uid", "n_votes"])
    single = set(lab.loc[lab.n_votes == 1, "pair_uid"].astype(str))
    seen = set(lab.pair_uid.astype(str))
    rf["pair_uid"] = rf.pair_uid.astype(str)
    rf = _cell(rf)
    rf["is_single"] = rf.pair_uid.isin(single)
    rf["is_seen"] = rf.pair_uid.isin(seen)
    print(f"pares v6: {len(rf):,} | single-vote: {rf.is_single.sum():,} | "
          f"nunca vistos: {(~rf.is_seen).sum():,}")

    v1 = rf[rf.rp_kpc < RP_SPLIT]           # 5–50

    # ── CALIB (550): single-vote homogéneo en 5–50 + nuevos homogéneos en 50–80 ──
    n_single = int(N_CALIB * CALIB_SINGLE_FRAC)
    calib_single = homogeneous_sample(v1[v1.is_single], n_single, rng, RARE_TYPES)
    used = set(calib_single.pair_uid)
    new_v2 = rf[(rf.rp_kpc >= RP_SPLIT) & (~rf.is_seen) & (~rf.pair_uid.isin(used))]
    calib_new = homogeneous_sample(new_v2, N_CALIB - len(calib_single), rng, RARE_TYPES)
    calib = pd.concat([calib_single, calib_new], ignore_index=True).drop_duplicates("pair_uid")
    calib = calib.sample(frac=1, random_state=rng).reset_index(drop=True)
    used = set(calib.pair_uid)

    # ── POOL por-usuario: HOMOGÉNEO ESTRICTO en 5–80 (igual-por-celda), luego v1|v2 ──
    pool_src = rf[(~rf.is_seen) & (~rf.pair_uid.isin(used))]
    pool = homogeneous_sample(pool_src, POOL_TARGET, rng, RARE_TYPES)
    pool_v1 = pool[pool.rp_kpc < RP_SPLIT]
    pool_v2 = pool[pool.rp_kpc >= RP_SPLIT]
    work_v1_frac = len(pool_v1) / len(pool)   # el 60/40 sale SOLO de la homogeneidad

    # ── ORDEN FINAL: calib | v1 pool | v2 pool ──
    ordered = pd.concat([calib, pool_v1, pool_v2], ignore_index=True).drop_duplicates("pair_uid")
    n_v1_boundary = len(calib) + len(pool_v1)   # índice frontera v1/v2 para la app

    # ── Reporte ──
    print(f"\nCALIB: {len(calib)} (single-vote 5–50 {calib.is_single.sum()} / nuevos 50–80 {(~calib.is_seen).sum()})")
    print(f"POOL v1 (5–50): {len(pool_v1)} | POOL v2 (50–80): {len(pool_v2)} | "
          f"WORK_V1_FRACTION = {work_v1_frac:.3f}")
    print(f"CATÁLOGO total: {len(ordered)} (frontera v1/v2 en idx {n_v1_boundary})")
    print(f"\npair_type en CALIB:\n{calib.pair_type.value_counts().to_string()}")
    print(f"\npair_type en POOL:\n{pd.concat([pool_v1,pool_v2]).pair_type.value_counts().to_string()}")

    # heatmap de cobertura rp×z (pool + calib)
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    for ax, (name, d) in zip(axes, [("CALIB (550)", calib),
                                    ("POOL (por-usuario)", pd.concat([pool_v1, pool_v2]))]):
        H = np.histogram2d(d.rp_kpc, d.z_mean, bins=[RP_EDGES, Z_EDGES])[0]
        im = ax.imshow(H.T, origin="lower", aspect="auto", cmap="viridis",
                       extent=[5, 80, 0, 1])
        ax.axvline(RP_SPLIT, color="w", ls="--", lw=1)
        ax.set_xlabel("rp [kpc]"); ax.set_ylabel("z"); ax.set_title(f"{name} — N/celda")
        fig.colorbar(im, ax=ax)
    fig.tight_layout(); OUT_PNG.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_PNG, dpi=130, bbox_inches="tight")
    print(f"\n[ok] heatmap → {OUT_PNG}")

    # ── Construir catálogo v6 completo (formato app) ──
    from datetime import datetime
    print("\nConstruyendo entries de pares (formato app)…")
    pair_list = _pair_entries(ordered)
    group_list, n_calib_groups = _sample_and_build_groups(rng)

    catalog = {
        "exported_at": datetime.now().isoformat(),
        "catalog_version": CATALOG_VERSION,
        "cloud_sync_enabled": True,
        "pair_cloud_sync_enabled": True,
        "rp_max_kpc": 80.0,
        "rp_split_kpc": RP_SPLIT,
        "n_pairs_inner": n_v1_boundary,
        "n_pairs_outer": len(ordered) - n_v1_boundary,
        "n_pairs_v1": n_v1_boundary,
        "n_pairs_v2": len(ordered) - n_v1_boundary,
        "total_pairs": len(pair_list),
        "total_groups": len(group_list),
        "desktop_classified": {},
        "pairs": pair_list,
        "groups": group_list,
    }
    ordered.to_parquet(OUT_PARQUET, index=False)
    OUT_CALIB.write_text(json.dumps({"pair_uid": list(calib.pair_uid)}, separators=(",", ":")))
    OUT_CATALOG.write_text(json.dumps(catalog, separators=(",", ":")))
    print(f"\n[ok] catálogo v6 → {OUT_CATALOG}  ({OUT_CATALOG.stat().st_size/1e6:.1f} MB)")
    print(f"[ok] calib ids  → {OUT_CALIB}")
    _embed_html(catalog, list(calib.pair_uid))
    print("\n══ CONSTANTES PARA LA APP (mobile/index.html) ══")
    print(f"  CATALOG_VERSION      = '{CATALOG_VERSION}'")
    print(f"  CALIB_PAIRS          = {len(calib)}")
    print(f"  BLOCK_SIZE           = {BLOCK_SIZE}")
    print(f"  WORK_V1_FRACTION     = {work_v1_frac:.2f}")
    print(f"  CALIB_GROUPS         = {n_calib_groups}")
    print(f"  GROUP_BLOCK_SIZE     = {BLOCK_SIZE // 5}")
    print(f"  GROUP_INTERLEAVE_RATIO = 5")
    print(f"  (catálogo: n_pairs_v1={n_v1_boundary}, total_pairs={len(ordered)}, total_groups={len(group_list)})")


if __name__ == "__main__":
    main()
