/**
 * sw.js — Service Worker para Galaxy Pair Inspector
 *
 * Cachea permanentemente las imágenes de legacysurvey.org en disco.
 * La primera vez que se carga una imagen va al servidor (lento).
 * Todas las veces siguientes se sirven desde cache local (instantáneo).
 *
 * Límite: MAX_IMAGES imágenes. Cuando se llena, se borran las más antiguas (FIFO).
 */

const CACHE_NAME   = 'galaxy-images-v1';
const MAX_IMAGES   = 600;   // ~6 MB estimado (≈10 KB por imagen JPEG 192px)
const LEGACY_HOST  = 'www.legacysurvey.org';

// ── Instalación: activar inmediatamente sin esperar ────────────────────────
self.addEventListener('install',  () => self.skipWaiting());
self.addEventListener('activate', e  => e.waitUntil(self.clients.claim()));

// ── Interceptar fetch ──────────────────────────────────────────────────────
self.addEventListener('fetch', event => {
  const url = event.request.url;

  // Solo interceptar imágenes de Legacy Survey
  if (!url.includes(LEGACY_HOST)) return;

  event.respondWith(
    caches.open(CACHE_NAME).then(async cache => {

      // Cache hit → devolver inmediatamente (sin red)
      const cached = await cache.match(event.request);
      if (cached) return cached;

      // Cache miss → fetch al servidor
      try {
        // mode: 'no-cors' porque Legacy Survey no envía cabeceras CORS;
        // produce una "opaque response" que el browser puede mostrar en <img>
        const response = await fetch(event.request, { mode: 'no-cors' });

        // Guardar en cache (opaque responses tienen type === 'opaque')
        if (response.type === 'opaque' || response.ok) {
          cache.put(event.request, response.clone());

          // Eviction FIFO: mantener el cache dentro del límite
          const keys = await cache.keys();
          if (keys.length > MAX_IMAGES) {
            const toDelete = keys.slice(0, keys.length - MAX_IMAGES);
            await Promise.all(toDelete.map(k => cache.delete(k)));
          }
        }

        return response;
      } catch (err) {
        // Sin red y sin cache → dejar que el browser maneje el error normalmente
        throw err;
      }
    })
  );
});
