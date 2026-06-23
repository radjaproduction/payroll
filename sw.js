const CACHE_NAME = "payroll-radja-v2";

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
      .then(cache => cache.addAll(STATIC_ASSETS))
  );

  self.skipWaiting();
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
    fetch(event.request)
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