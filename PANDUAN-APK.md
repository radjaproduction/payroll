# Panduan Build APK — Payroll Radja Production

## Isi folder ini
- `index.html`, `manifest.json`, `sw.js` — app inti
- `icon-192.png`, `icon-512.png` — icon biasa (sudah diperbaiki ke ukuran pas)
- `icon-192-maskable.png`, `icon-512-maskable.png` — icon versi maskable (aman untuk crop bentuk Android)
- `.nojekyll` — **wajib ada**, supaya GitHub Pages tidak mengabaikan folder `.well-known`
- `.well-known/assetlinks.json` — placeholder, harus diisi manual (lihat Langkah 4)

## Langkah 1 — Push ke GitHub
1. Buat repo baru (public), misal `payroll-radja`
2. Push semua file di folder ini ke root repo tersebut (jangan taruh di subfolder, biar path relatif `./` tetap benar)

## Langkah 2 — Aktifkan GitHub Pages
1. Settings → Pages
2. Source: `Deploy from a branch` → pilih branch `main`, folder `/ (root)`
3. Tunggu ±1 menit, catat URL yang muncul, contoh:
   `https://username.github.io/payroll-radja/`
4. Buka URL itu, pastikan app terbuka normal (test login, dsb)

## Langkah 3 — Generate APK via PWABuilder
1. Buka https://www.pwabuilder.com
2. Masukkan URL GitHub Pages kamu → Start
3. PWABuilder akan audit manifest & service worker (harusnya lolos karena sudah lengkap)
4. Pilih platform **Android**
5. Pilih package type **TWA (Trusted Web Activity)**
6. Isi:
   - Package ID, misal `com.radja.payroll`
   - App name: `Payroll Radja Production`
7. PWABuilder akan generate APK/AAB **dan keystore baru** (atau kamu bisa upload keystore sendiri kalau sudah punya) — **download & simpan keystore ini baik-baik**, dibutuhkan setiap update APK ke depannya
8. PWABuilder juga akan kasih file `assetlinks.json` yang sudah terisi fingerprint asli

## Langkah 4 — Pasang assetlinks.json yang asli
1. Buka `assetlinks.json` hasil dari PWABuilder (Langkah 3)
2. Copy isinya, replace isi file `.well-known/assetlinks.json` di repo (ganti placeholder)
3. Commit & push lagi → tunggu GitHub Pages redeploy
4. Cek file sudah bisa diakses di:
   `https://username.github.io/payroll-radja/.well-known/assetlinks.json`
   (harus muncul JSON, bukan 404)

## Langkah 5 — Install & Test APK
1. Install APK hasil download ke HP Android (aktifkan "Install dari sumber tidak dikenal" kalau perlu)
2. Buka app — kalau `assetlinks.json` sudah benar, app akan tampil **tanpa address bar** (full app-like, bukan seperti browser)
3. Kalau masih muncul address bar, cek lagi: package name & fingerprint di `assetlinks.json` harus sama persis dengan yang dipakai APK

## Catatan penting
- Supabase tetap dipanggil via internet (bukan file lokal), jadi tidak akan kena masalah CORS `file://` seperti versi WebView lama
- Kalau nanti update `index.html`, cukup push ke GitHub → Pages otomatis update → user tinggal buka app lagi (service worker akan fetch versi terbaru)
- Kalau mau ganti isi APK secara signifikan (icon, nama app), APK perlu di-generate ulang dari PWABuilder — tapi **pakai keystore yang sama** dari Langkah 3, jangan generate baru, supaya bisa update APK yang sudah terinstall (bukan dianggap app baru)
