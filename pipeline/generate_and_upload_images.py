"""
generate_and_upload_images.py — Descarga imágenes de pares clasificados y las sube a Google Drive.

Flujo:
    1. Descarga lista de id_par únicos clasificados desde Supabase
    2. Busca coordenadas en el catálogo local (data/DESI_int_legacyID_pairs.parquet)
    3. Descarga imagen 256×256 desde Legacy Survey para cada par
    4. Sube a Google Drive/GalaxyPairs/ como par_{id_par}.jpg
    5. Salta los pares cuya imagen ya existe en Drive (incremental)

Uso:
    python pipeline/generate_and_upload_images.py

Requiere:
    - .env con SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY
    - google_credentials.json en la raíz del proyecto
    - pip install google-auth google-auth-httplib2 google-api-python-client
"""

import io
import json
import sys
import time
import urllib.request as urlreq
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# ── Configuración ──────────────────────────────────────────────────────────────
CATALOG_PATH        = _env.get('PAIRS_CATALOG', '')
CREDENTIALS_FILE    = 'google_credentials.json'
DRIVE_FOLDER_ID     = '1IQBbltVsN1r-VvYQ6nF9yhOV8NChXJif'

IMG_SIZE_PX         = 256
PADDING_FACTOR      = 2.5
LS_LAYER            = 'ls-dr10'
N_WORKERS           = 6      # descargas paralelas desde Legacy Survey
TIMEOUT             = 15     # segundos por intento
MAX_RETRIES         = 3

# ── Leer .env ─────────────────────────────────────────────────────────────────
def _load_env(path='.env'):
    env = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    env[k.strip()] = v.strip()
    except FileNotFoundError:
        pass
    return env

_env             = _load_env()
SUPABASE_URL     = _env.get('SUPABASE_URL', '').rstrip('/')
SERVICE_ROLE_KEY = _env.get('SUPABASE_SERVICE_ROLE_KEY', '')

# ── Supabase ──────────────────────────────────────────────────────────────────
def fetch_classified_ids() -> set:
    """Devuelve el conjunto de id_par únicos con al menos una clasificación."""
    url = f'{SUPABASE_URL}/rest/v1/clasificaciones?select=id_par'
    req = urlreq.Request(url, headers={
        'apikey':        SERVICE_ROLE_KEY,
        'Authorization': f'Bearer {SERVICE_ROLE_KEY}',
        'Accept':        'application/json',
    })
    with urlreq.urlopen(req, timeout=20) as resp:
        rows = json.loads(resp.read().decode('utf-8'))
    return {int(r['id_par']) for r in rows}

# ── Google Drive ──────────────────────────────────────────────────────────────
def build_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=['https://www.googleapis.com/auth/drive.file'],
    )
    return build('drive', 'v3', credentials=creds, cache_discovery=False)


def fetch_existing_files(service) -> set:
    """Devuelve el conjunto de nombres de archivos ya existentes en la carpeta de Drive."""
    existing = set()
    page_token = None
    while True:
        params = {
            'q':        f"'{DRIVE_FOLDER_ID}' in parents and trashed=false",
            'fields':   'nextPageToken, files(name)',
            'pageSize': 1000,
        }
        if page_token:
            params['pageToken'] = page_token
        result = service.files().list(**params).execute()
        for f in result.get('files', []):
            existing.add(f['name'])
        page_token = result.get('nextPageToken')
        if not page_token:
            break
    return existing


def upload_image(service, filename: str, img_bytes: bytes) -> bool:
    """Sube una imagen a Google Drive. Devuelve True si tuvo éxito."""
    try:
        media = MediaIoBaseUpload(
            io.BytesIO(img_bytes),
            mimetype='image/jpeg',
            resumable=False,
        )
        service.files().create(
            body={'name': filename, 'parents': [DRIVE_FOLDER_ID]},
            media_body=media,
            fields='id',
        ).execute()
        return True
    except Exception as exc:
        print(f'  ERROR subiendo {filename}: {exc}')
        return False

# ── Legacy Survey ─────────────────────────────────────────────────────────────
def _pixscale(sep_arcsec: float) -> float:
    fov = sep_arcsec * PADDING_FACTOR
    return float(np.clip(fov / IMG_SIZE_PX, 0.3, 2.0))


def _legacy_url(ra_mid: float, dec_mid: float, sep_arcsec: float) -> str:
    ps = _pixscale(sep_arcsec)
    return (f'https://www.legacysurvey.org/viewer/cutout.jpg'
            f'?ra={ra_mid:.6f}&dec={dec_mid:.6f}'
            f'&pixscale={ps:.4f}&layer={LS_LAYER}&size={IMG_SIZE_PX}')


def download_image(ra_mid: float, dec_mid: float, sep_arcsec: float) -> 'bytes | None':
    """Descarga imagen de Legacy Survey con reintentos. Devuelve bytes o None si falla."""
    url = _legacy_url(ra_mid, dec_mid, sep_arcsec)
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp.content
        except Exception:
            if attempt < MAX_RETRIES - 1:
                time.sleep(0.5 * (attempt + 1))
    return None

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # Validaciones
    if not SUPABASE_URL or not SERVICE_ROLE_KEY:
        print('ERROR: falta SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY en .env')
        sys.exit(1)
    if not Path(CREDENTIALS_FILE).exists():
        print(f'ERROR: no se encontró {CREDENTIALS_FILE}')
        sys.exit(1)
    if not Path(CATALOG_PATH).exists():
        print(f'ERROR: no se encontró el catálogo en {CATALOG_PATH}')
        sys.exit(1)

    # 1. Pares clasificados en Supabase
    print('Consultando Supabase…')
    classified_ids = fetch_classified_ids()
    print(f'  {len(classified_ids):,} pares clasificados en Supabase')

    # 2. Cargar catálogo y filtrar solo los clasificados
    print('Cargando catálogo…')
    df = pd.read_parquet(CATALOG_PATH)
    if 'id_par' not in df.columns:
        print('ERROR: el catálogo no tiene columna id_par')
        sys.exit(1)

    df = df[df['id_par'].isin(classified_ids)].copy()
    print(f'  {len(df):,} pares con coordenadas encontradas')

    # Calcular sep_arcsec y midpoint si no existen
    if 'sep_arcsec' not in df.columns:
        dec_mid = np.radians((df['dec1'].values + df['dec2'].values) / 2.0)
        dx = (df['ra1'].values - df['ra2'].values) * np.cos(dec_mid) * 3600.0
        dy = (df['dec1'].values - df['dec2'].values) * 3600.0
        df['sep_arcsec'] = np.hypot(dx, dy)
    df['ra_mid']  = (df['ra1'] + df['ra2']) / 2.0
    df['dec_mid'] = (df['dec1'] + df['dec2']) / 2.0

    # 3. Conectar a Drive y ver qué ya existe
    print('Conectando a Google Drive…')
    service = build_drive_service()
    existing = fetch_existing_files(service)
    print(f'  {len(existing):,} imágenes ya en Drive')

    # Filtrar los que ya están subidos
    pending = df[~df['id_par'].apply(lambda x: f'par_{int(x)}.jpg' in existing)]
    print(f'  {len(pending):,} imágenes pendientes de subir')

    if pending.empty:
        print('\nTodo al día — no hay imágenes nuevas que subir.')
        return

    # 4. Descargar y subir en paralelo
    print(f'\nDescargando y subiendo {len(pending):,} imágenes…')
    ok = 0
    fail = 0
    rows = pending.to_dict('records')

    def _process(row):
        img_bytes = download_image(row['ra_mid'], row['dec_mid'], row['sep_arcsec'])
        if img_bytes is None:
            return False
        filename = f'par_{int(row["id_par"])}.jpg'
        return upload_image(service, filename, img_bytes)

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        futures = {pool.submit(_process, row): row['id_par'] for row in rows}
        for i, fut in enumerate(as_completed(futures), 1):
            id_par = futures[fut]
            success = fut.result()
            if success:
                ok += 1
            else:
                fail += 1
            if i % 50 == 0 or i == len(rows):
                print(f'  {i}/{len(rows)}  ✓ {ok}  ✗ {fail}')

    print(f'\n✓  Subidas: {ok}  |  Fallidas: {fail}')
    if fail:
        print(f'   Las {fail} fallidas se pueden reintentar corriendo el script de nuevo.')


if __name__ == '__main__':
    main()
