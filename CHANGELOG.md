# Ringkasan Perubahan

## 1. FIX BUG UTAMA — Session userbot tidak tersimpan/ditemukan saat redeploy

**File: `antigcast.py`**

Akar masalah: `_DEPLOY_ID` sebelumnya hanya `str(os.getpid())`. Di container
Docker/Railway, proses pertama yang dijalankan di container baru hampir selalu
mendapat PID 1 (PID namespace baru per container). Akibatnya deploy LAMA dan
deploy BARU bisa punya `_DEPLOY_ID` yang SAMA PERSIS ("1").

Dampaknya: pengecekan `data.get("by") != _DEPLOY_ID` di `_deploy_watch_and_release()`
selalu `False` — instance lama menyangka sinyal "pending" dari deploy baru itu
datang dari dirinya sendiri, sehingga `graceful_shutdown()` (yang menyimpan
session userbot lewat `stop_userbot()`) TIDAK PERNAH terpanggil lewat jalur
handshake MongoDB. Satu-satunya jalur penyelamat yang tersisa adalah SIGTERM,
yang tidak selalu sempat selesai sebelum Railway mengirim SIGKILL — sehingga
sesi userbot kadang tidak ke-save sebelum container mati, dan deploy baru
"tidak menemukan" sesi userbot.

**Perbaikan:** `_DEPLOY_ID` sekarang gabungan `PID-waktu_start-token_acak`,
sehingga praktis mustahil sama antara dua proses berbeda, walau kebetulan
PID-nya sama.

## 2. Pengaman tambahan — backup periodik sesi userbot

**File: `security_os/video_call.py`**

Sebelumnya sesi userbot (`userbot_security_os.session`) HANYA disimpan ke
MongoDB saat:
- berhasil login/start, dan
- saat `stop_userbot()` dipanggil dari `graceful_shutdown()`.

Tidak ada backup berkala seperti yang sudah dimiliki bot utama
(`_periodic_session_backup` di `antigcast.py`, setiap 20 menit) dan bot
pemantau (`save_all_sessions` periodik di `monitor_bot_reference.py`).

Sekarang ditambahkan `_periodic_ub_session_backup()` — backup sesi userbot
setiap 20 menit, jadi walau container mati paksa (SIGKILL) sebelum
`graceful_shutdown()` selesai, sesi yang tersimpan di Mongo tetap relatif
baru, bukan basi sejak login pertama.

Dipasang otomatis baik saat `start_userbot()` (startup) maupun
`change_userbot()` (ganti manual via panel "Ganti Userbot"), dengan guard
supaya tidak berjalan dobel.

## 3. Reorganisasi folder

File-file berikut dipindah ke folder baru `security_os/` agar tidak bercampur
dengan file utama di root:
- `video_call.py` → `security_os/video_call.py`
- `monitor_bot_reference.py` → `security_os/monitor_bot_reference.py`
- `admin_session.py` → `security_os/admin_session.py`

**Cara kerja agar semua import lama tetap berfungsi tanpa diubah satu-satu:**
`antigcast.py` (entry point) menambahkan folder `security_os/` ke `sys.path` di
awal startup (sebelum modul apa pun di-import). Karena itu, import lama yang
tersebar di banyak file plugin — `from video_call import ...`,
`import admin_session as ...`, `from monitor_bot_reference import ...` —
tetap berfungsi tepat seperti sebelumnya, karena Python menemukan modul itu
lewat `sys.path`, bukan lewat package path.

**Penyesuaian path internal** (supaya lokasi file `.env` dan file `.session`
tidak berubah meski modul Python-nya pindah folder):
- `security_os/video_call.py`: `_BOT_DIR`, `load_dotenv(...)`, dan `env_path` di
  `change_userbot()` diarahkan ke `parent.parent` (root proyek), bukan
  `parent` (folder `security_os/`).
- `security_os/monitor_bot_reference.py`: `load_dotenv(...)`, `sys.path.insert`
  untuk `database`, dan `self._session_path` per-grup juga diarahkan ke
  root proyek.
- `security_os/admin_session.py`: tidak ada path internal — sesi disimpan
  in-memory, tidak ada perubahan.

File session (`.session`) tetap tersimpan di root proyek seperti sebelumnya
— tidak ada perubahan lokasi file session, hanya lokasi modul Python-nya
yang berubah.

## Yang TIDAK diubah

- Tidak ada logika bisnis, handler, atau alur fitur yang diubah selain yang
  disebut di atas.
- Semua fungsi (`start_userbot`, `stop_userbot`, `change_userbot`,
  `graceful_shutdown`, deploy handshake, restore/save session, dll) tetap
  terpanggil di tempat yang sama seperti sebelumnya — hanya lokasi file dan
  satu nilai variabel (`_DEPLOY_ID`) yang diperbaiki.

## 4. Update lanjutan — simplifikasi pemanggilan start_userbot

**File: `antigcast.py`**

Ditemukan kasus nyata di produksi: setelah `[Rewarm] ✅ Chat: ... berhasil`
tercetak, baris log `[UB] ▶️ start_userbot() mulai berjalan.` (baris PALING
PERTAMA di dalam `start_userbot()`, dicetak sebelum operasi apa pun) tidak
pernah muncul — padahal bot utama tetap berjalan normal (idle tercapai).

Wrapper `_run_start_userbot_safely()` yang membungkus `create_task` (FIX
sebelumnya) seharusnya setara secara fungsional dengan panggilan langsung,
tapi sebagai langkah pencegahan, baris ini disederhanakan kembali ke pola
versi lama yang terbukti selalu bekerja di produksi:

```python
asyncio.create_task(start_userbot(app))
```

`start_userbot()` sendiri sudah membungkus setiap langkah internalnya
dengan try/except masing-masing, jadi wrapper tambahan di `antigcast.py`
tidak diperlukan.

Ditambahkan juga 2 baris log diagnostik eksplisit tepat sebelum dan sesudah
`create_task` ini (`[Startup] ▶️ Memanggil create_task(start_userbot)...`
dan `[Startup] ✅ create_task(start_userbot) berhasil dijadwalkan.`). Jika
kasus "tidak ada log [UB] sama sekali" terulang, log baru ini akan
menunjukkan dengan pasti apakah `create_task` sendiri tercapai/berhasil,
mempersempit pencarian akar masalah jauh lebih cepat.
