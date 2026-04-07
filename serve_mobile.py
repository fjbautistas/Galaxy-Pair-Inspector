"""
serve_mobile.py — Servidor HTTP local para la app móvil.

Uso:
    python serve_mobile.py

Luego abre en Safari desde tu iPhone:
    http://<IP-que-aparece>:8080
"""

import http.server
import socket
import os
import sys
from pathlib import Path

PORT      = 8080
SERVE_DIR = Path(__file__).parent / 'mobile_app'


def get_local_ip() -> str:
    """Detecta la IP local en la red WiFi."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80))
        return s.getsockname()[0]
    except Exception:
        return '127.0.0.1'
    finally:
        s.close()


class _Handler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, fmt, *args):
        # Solo mostrar accesos a archivos principales (no assets)
        if any(args[0].startswith(p) for p in ('GET /catalog', 'GET /index', 'GET /')):
            print(f'  [{self.client_address[0]}] {args[0]} {args[1]}')


def main():
    catalog = SERVE_DIR / 'catalog.json'
    if not catalog.exists():
        print('⚠️  No se encontró mobile_app/catalog.json')
        print('   Primero ejecuta:  python export_catalog.py')
        print()

    if not SERVE_DIR.exists():
        print(f'Error: no existe el directorio {SERVE_DIR}')
        sys.exit(1)

    os.chdir(SERVE_DIR)

    ip = get_local_ip()
    handler = _Handler

    try:
        server = http.server.HTTPServer(('', PORT), handler)
    except OSError:
        print(f'El puerto {PORT} ya está en uso. Prueba otro puerto:')
        print(f'  python serve_mobile.py  # edita la variable PORT en el script')
        sys.exit(1)

    print()
    print('🌌  Galaxy Pairs Mobile — servidor iniciado')
    print('─' * 45)
    print(f'   Abre en Safari (iPhone, misma red WiFi):')
    print(f'   ▶  http://{ip}:{PORT}')
    print()
    print('   Tip: en Safari → Compartir → "Añadir a pantalla')
    print('        de inicio" para usarla como app nativa.')
    print()
    print('   Ctrl+C para detener el servidor.')
    print('─' * 45)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\n   Servidor detenido.')


if __name__ == '__main__':
    main()
