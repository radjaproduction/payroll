const CACHE_NAME = "payroll-radja-v3";

const STATIC_ASSETS = [
  "./",
  "./index.html",
  "./manifest.json",
  "./icon-192.png",
  "./icon-512.png"
];

self.addEventListener("install", event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => {
        // PENTING: jangan pakai cache.addAll() polos.
        // addAll() itu all-or-nothing -- kalau SATU SAJA file di
        // STATIC_ASSETS gagal di-fetch (404 / rename / dsb), SELURUH
        // proses install gagal, SW baru jadi "redundant", dan admin
        // tidak akan pernah lihat banner update sampai hapus cache manual.
        // Dengan Promise.allSettled, kalau ada 1 file gagal, yang lain
        // tetap ke-cache dan install tetap sukses.
        return Promise.allSettled(
          STATIC_ASSETS.map(url =>
            fetch(url, { cache: "no-store" })
              .then(res => {
                if (res && res.ok) return cache.put(url, res);
                console.warn("[SW] gagal cache asset (dilewati):", url, res && res.status);
              })
              .catch(err => console.warn("[SW] gagal fetch asset (dilewati):", url, err))
          )
        );
      })
  );

  // TIDAK panggil self.skipWaiting() di sini.
  // SW baru dibiarkan "waiting" sampai admin klik tombol Update di banner
  // (lihat listener 'message' di bawah). Ini mencegah reload paksa
  // saat admin sedang input data.
});

// Terima perintah dari halaman (klik tombol "Update" di banner)
self.addEventListener("message", event => {
  if (event.data && event.data.type === "SKIP_WAITING") {
    self.skipWaiting();
  }
});

self.addEventListener("activate", event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys.map(key => {
          if (key !== CACHE_NAME) {
            return caches.delete(key);
          }
        })
      )
    )
  );

  self.clients.claim();
});

self.addEventListener("fetch", event => {

  const url = new URL(event.request.url);

  // JANGAN CACHE SUPABASE
  if (
    url.hostname.includes("supabase.co") ||
    url.hostname.includes("supabase.in")
  ) {
    event.respondWith(fetch(event.request));
    return;
  }

  // CDN
  if (
    url.hostname.includes("fonts.googleapis.com") ||
    url.hostname.includes("fonts.gstatic.com") ||
    url.hostname.includes("cdnjs.cloudflare.com")
  ) {
    event.respondWith(
      caches.match(event.request).then(cached => {
        return (
          cached ||
          fetch(event.request).then(response => {
            const clone = response.clone();

            caches.open(CACHE_NAME).then(cache => {
              cache.put(event.request, clone);
            });

            return response;
          })
        );
      })
    );
    return;
  }

  // HTML, CSS, JS
  event.respondWith(
    fetch(event.request, { cache: "no-store" })
      .then(response => {

        if (
          response &&
          response.status === 200 &&
          event.request.method === "GET"
        ) {
          const clone = response.clone();

          caches.open(CACHE_NAME).then(cache => {
            cache.put(event.request, clone);
          });
        }

        return response;
      })
      .catch(() => {
        return caches.match(event.request)
          .then(cached => {
            return cached || caches.match("./index.html");
          });
      })
  );
});