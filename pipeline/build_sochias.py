"""Build de GalPairs para la demo SOCHIAS (28-oct-2026).

Reutiliza el catálogo de la campaña v6 tal cual (mobile/catalog_v6.json y
data/supplementary_calib_ids_v6.json, generados por build_campaign_v6.py) y solo
cambia `catalog_version` a 'sochias', para que los votos del público no se
mezclen con v5_3/v6 (congeladas en Supabase).

El catálogo va compacto: columnas en vez de objetos y sin display_id, ra_mid,
dec_mid (la app los reconstruye en _expandCatalog). ~40 % menos de HTML.

Uso:  python pipeline/build_sochias.py
"""
import json
from datetime import datetime
from pathlib import Path

GI = Path(__file__).resolve().parent.parent
CATALOG_V6 = GI / "mobile" / "catalog_v6.json"
CALIB_V6 = GI / "data" / "supplementary_calib_ids_v6.json"
TEMPLATE = GI / "mobile" / "index.html"
OUT = GI / "mobile" / "GalPairs.html"
CATALOG_VERSION = "sochias"
DERIVED_PAIR_KEYS = ("display_id", "ra_mid", "dec_mid")


def _read_env():
    env = {}
    for line in (GI / ".env").read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def _columns(rows, drop=()):
    keys = [k for k in rows[0] if k not in drop]
    return {k: [r[k] for r in rows] for k in keys}


def main():
    cat = json.loads(CATALOG_V6.read_text())
    calib_ids = json.loads(CALIB_V6.read_text())["pair_uid"]
    pairs, groups = cat.pop("pairs"), cat.pop("groups")

    # Las columnas derivadas deben coincidir con lo que reconstruye la app
    assert all(p["display_id"] == i + 1 for i, p in enumerate(pairs))
    assert all(abs(p["ra_mid"] - (p["ra1"] + p["ra2"]) / 2) < 1e-4 for p in pairs)
    assert all(abs(p["dec_mid"] - (p["dec1"] + p["dec2"]) / 2) < 1e-4 for p in pairs)

    cat["catalog_version"] = CATALOG_VERSION
    cat["work_first"] = True  # demo: bloque propio primero, calibración al final
    cat["exported_at"] = datetime.now().isoformat()
    cat["pairs_cols"] = _columns(pairs, drop=DERIVED_PAIR_KEYS)
    cat["groups_cols"] = _columns(groups)

    env = _read_env()
    key = env.get("SUPABASE_ANON_KEY", "")
    # La anon key es un JWT con role=anon; nunca inyectar la service_role
    assert '"role":"anon"' in __import__("base64").urlsafe_b64decode(
        key.split(".")[1] + "==").decode().replace(" ", ""), "SUPABASE_ANON_KEY no es la anon key"

    template = TEMPLATE.read_text(encoding="utf-8")
    inject = (
        "<script>"
        f"window._CATALOG={json.dumps(cat, separators=(',', ':'))};"
        f"window._SUPP_CALIB_IDS={json.dumps(calib_ids, separators=(',', ':'))};"
        f"window._SUPABASE_URL={json.dumps(env.get('SUPABASE_URL', ''))};"
        f"window._SUPABASE_ANON_KEY={json.dumps(key)};"
        "</script>\n  "
    )
    anchor = "// CONSTANTS — match desktop app"
    idx = template.index(anchor)
    script_idx = template.rindex("<script>", 0, idx)
    OUT.write_text(template[:script_idx] + inject + template[script_idx:], encoding="utf-8")
    print(f"[ok] GalPairs '{CATALOG_VERSION}' → {OUT}  ({OUT.stat().st_size/1e6:.1f} MB)")
    print(f"     {len(pairs)} pares, {len(groups)} grupos, {len(calib_ids)} calib")


if __name__ == "__main__":
    main()
