"""
antigcast.py — Entry Point Bot Antispam + Nexus AI
Jalankan: python antigcast.py

Sistem yang berjalan:
  [REFACTOR] plugins/filters/    → antispam, bio, cas  (group filter)
  [REFACTOR] plugins/commands/   → settings, regex, free, log, antigcast_group
  [REFACTOR] plugins/ui/         → DM panel interaktif (pages, handlers_dm, handlers_fsm)
  [NEXUS]    plugins/nexus/      → nexus_group.py, nexus_handlers.py
             core/               → engine.py (komputasi AI)

Database (otomatis dipilih saat startup):
  1. MongoDB  — jika MONGO_URL ada di .env dan bisa tersambung
  2. SQLite   — fallback ke penyimpanan internal HP (Termux)
"""

import os
import sys
import asyncio
import threading
from pathlib import Path as _Path
import dns.resolver
from pyrogram import Client, idle
from pyrogram.types import BotCommand, BotCommandScopeAllPrivateChats, BotCommandScopeAllGroupChats
from http.server import BaseHTTPRequestHandler, HTTPServer

# ── Path fix: pastikan semua import lokal bisa ditemukan dari CWD manapun ─────
# _BOT_DIR adalah folder tempat antigcast.py berada (misal: /sdcard/bot-main/).
# sys.path.insert memastikan Python selalu menemukan modules lokal (database,
# plugins/, core/, dll) meskipun script dijalankan dari direktori lain.
_BOT_DIR = _Path(__file__).resolve().parent
if str(_BOT_DIR) not in sys.path:
    sys.path.insert(0, str(_BOT_DIR))

# ── Folder security_os/ ────────────────────────────────────────────────────────
# video_call.py, monitor_bot_reference.py, dan admin_session.py dipindah ke
# subfolder security_os/ agar tidak bercampur dengan file utama di root proyek.
# Ditambahkan ke sys.path (bukan diimpor sebagai package security_os.xxx) supaya
# SEMUA import lama yang sudah ada di seluruh proyek — `from video_call import
# ...`, `import admin_session as ...`, `from monitor_bot_reference import ...`
# — tetap berfungsi tanpa perlu diubah satu per satu di setiap file plugin.
_SECURITY_OS_DIR = _BOT_DIR / "security_os"
if str(_SECURITY_OS_DIR) not in sys.path:
    sys.path.insert(0, str(_SECURITY_OS_DIR))

from database import setup_db, delete_worker, panel_write_worker, close_db, get_bot_config, save_bot_config, get_active_backend
from admin_session import start_cleanup_task as _adm_cleanup
from video_call import start_userbot, stop_userbot

# ── Termux: ambil OWNER_ID ────────────────────────────────────────────────────
OWNER_ID = int(os.environ.get("OWNER_ID", 0))

# ── Fix DNS Termux ────────────────────────────────────────────────────────────
dns.resolver.default_resolver = dns.resolver.Resolver(configure=False)
dns.resolver.default_resolver.nameservers = ['223.5.5.5', '223.6.6.6']

# ── Env ───────────────────────────────────────────────────────────────────────
API_ID    = int(os.environ.get("API_ID", 0))
API_HASH  = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CODE_BOT  = os.environ.get("CODE_BOT", "").strip()

# ── Session name — berbasis CODE_BOT jika tersedia, fallback ke bot_id ────────
# Jika CODE_BOT diset:
#   • Semua bot dengan CODE_BOT yang sama berbagi satu file session.
#   • Ganti BOT_TOKEN → session lama tetap dipakai, pengaturan grup tidak reset.
# Jika CODE_BOT kosong:
#   • Fallback ke bot_id dari token (perilaku lama) agar tidak patah.
_BOT_ID = BOT_TOKEN.split(":")[0] if ":" in BOT_TOKEN else "default"

# ── Session suffix: selalu berbasis CODE_BOT + BOT_ID ─────────────────────────
# Tujuan: 2 bot clone (CODE_BOT sama, BOT_TOKEN beda) bisa jalan bersamaan
# tanpa berebut file session. Data grup/regex/dll tetap berbagi lewat CODE_BOT.
# Contoh:
#   Bot 1: CODE_BOT=produksi, BOT_ID=111 → session: antispam_bot_produksi_111
#   Bot 2: CODE_BOT=produksi, BOT_ID=222 → session: antispam_bot_produksi_222
#   Keduanya baca/tulis database namespace "produksi" yang sama.
_SESSION_SUFFIX = f"{CODE_BOT}_{_BOT_ID}" if CODE_BOT else f"token_{_BOT_ID}"
_SESSION_NAME = str(_BOT_DIR / f"antispam_bot_{_SESSION_SUFFIX}")


def _print_startup_banner():
    """Tampilkan banner info bot saat startup di Termux."""
    print(f"\n")
    print(f"{'  BOT ANTISPAM + NEXUS AI  ':^52}")

    token_display = (BOT_TOKEN[:8] + "…" + BOT_TOKEN[-4:]) if len(BOT_TOKEN) > 12 else "(tidak diset)"
    sess_display  = f"antispam_bot_{_SESSION_SUFFIX}.session"
    print(f"  API_ID    : {str(API_ID) if API_ID else '(tidak diset)':<39}")
    print(f"  BOT_TOKEN : {token_display:<39}")
    print(f"  BOT_ID    : {_BOT_ID:<39}")
    print(f"  Session   : {sess_display:<39}")
    print(f"  OWNER_ID  : {str(OWNER_ID) if OWNER_ID else '(tidak diset)':<39}")
    if CODE_BOT:
        print(f"  CODE_BOT  : [{CODE_BOT}]{'':>{39 - len(CODE_BOT) - 2}}")
        print(f"  Namespace : aktif — data & session berbagi per CODE_BOT")
    else:
        print(f"  CODE_BOT  : (kosong — tidak ada isolasi)        ")
        print(f"  ⚠️  Set CODE_BOT di .env agar data tidak campur ")

    print(f"  Info backend database menyusul di bawah...      ")
    print(f"\n")

# ── Client ────────────────────────────────────────────────────────────────────
# Session name = path absolut + bot_id suffix.
# Tiap BOT_TOKEN punya file .session sendiri → tidak pernah bentrok.
# plugins root tetap "plugins" (nama modul Python, bukan path filesystem) —
# Python sudah tahu mencarinya lewat sys.path yang sudah diset di atas.
_SESSION_DB_KEY = f"pyrogram_session_{_SESSION_SUFFIX}"

app: Client = None  # diinisialisasi di _build_client() dalam main()


async def _build_client() -> Client:
    """
    Buat Pyrogram Client pakai file session lokal seperti biasa.
    Setelah login, file session disimpan ke MongoDB sebagai backup.
    """
    client = Client(
        _SESSION_NAME,
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        plugins=dict(root="plugins"),
    )
    return client


async def _restore_session_from_mongo() -> bool:
    """
    Pulihkan file .session dari MongoDB jika file lokal tidak ada.
    Hanya restore jika file lokal TIDAK ADA (misal setelah Railway redeploy).
    Jika BOT_TOKEN berubah sejak session terakhir disimpan → hapus session lama
    dan biarkan bot login ulang dengan token baru.
    """
    import base64, os as _os

    if get_active_backend() != "mongo":
        return False

    session_path = _SESSION_NAME + ".session"
    if _os.path.exists(session_path):
        return False  # File lokal ada, tidak perlu restore

    # ── Cek apakah BOT_TOKEN berubah sejak session terakhir disimpan ──────────
    _TOKEN_DB_KEY = f"last_bot_token_{_SESSION_SUFFIX}"
    saved_token = await get_bot_config(_TOKEN_DB_KEY)
    if saved_token and saved_token != BOT_TOKEN:
        print(f"[Session] ⚠️  BOT_TOKEN berubah — session lama dihapus, bot login ulang.")
        await save_bot_config(_SESSION_DB_KEY, None)
        await save_bot_config(_TOKEN_DB_KEY, None)
        return False

    saved_bytes = await get_bot_config(_SESSION_DB_KEY)
    if not saved_bytes:
        print(f"[Session] ℹ️  Belum ada session di MongoDB, bot akan login baru.")
        return False

    try:
        raw = base64.b64decode(saved_bytes.encode())
        with open(session_path, "wb") as _f:
            _f.write(raw)
        print(f"[Session] ✅ File session dipulihkan dari MongoDB.")
        return True
    except Exception as e:
        print(f"[Session] ⚠️  Gagal pulihkan session: {e}")
        return False


async def _clear_session_from_mongo() -> None:
    """Hapus session dari MongoDB — dipanggil jika session yang dipulihkan ditolak Telegram."""
    try:
        await save_bot_config(_SESSION_DB_KEY, None)
        print(f"[Session] 🗑️  Session lama dihapus dari MongoDB.")
    except Exception as e:
        print(f"[Session] ⚠️  Gagal hapus session dari MongoDB: {e}")


async def _save_session_to_mongo() -> None:
    """
    Baca file .session dari disk dan simpan isinya (base64) ke MongoDB.
    Dipanggil setelah app.start() berhasil — MongoDB selalu diupdate dari file lokal.
    Juga menyimpan BOT_TOKEN aktif agar saat redeploy bisa deteksi token berubah.
    """
    import base64, os as _os

    if get_active_backend() != "mongo":
        return
    try:
        session_path = _SESSION_NAME + ".session"
        if not _os.path.exists(session_path):
            return
        with open(session_path, "rb") as _f:
            raw = _f.read()
        encoded = base64.b64encode(raw).decode()
        await save_bot_config(_SESSION_DB_KEY, encoded)
        # Simpan token aktif untuk deteksi perubahan di deploy berikutnya
        _TOKEN_DB_KEY = f"last_bot_token_{_SESSION_SUFFIX}"
        await save_bot_config(_TOKEN_DB_KEY, BOT_TOKEN)
        print(f"[Session] ✅ Session disimpan ke MongoDB.")
    except Exception as e:
        print(f"[Session] ⚠️  Gagal simpan session ke MongoDB: {e}")


async def _periodic_session_backup() -> None:
    """
    Simpan session ke MongoDB setiap 20 menit secara berkala.

    Tujuan: peer cache di .session terus bertambah saat bot berjalan
    (setiap user/grup/channel baru yang ditemui langsung masuk ke SQLite lokal).
    Tanpa backup berkala, redeploy berikutnya hanya mendapat snapshot saat startup —
    semua peer baru yang ditemui setelah itu hilang → PeerIdInvalid.

    Interval 20 menit = trade-off antara write ke MongoDB vs freshness peer cache.
    """
    while True:
        await asyncio.sleep(20 * 60)  # 20 menit
        await _save_session_to_mongo()
        print("[Session] 🔄 Periodic backup session selesai.")

# ── Deploy Handshake via MongoDB ──────────────────────────────────────────────
# Masalah: Railway start instance baru SEBELUM instance lama benar-benar mati.
# Dua koneksi aktif ke Telegram → AuthKeyDuplicated → session invalid.
#
# Solusi: instance baru sinyal ke MongoDB, instance lama deteksi dan disconnect
# lebih dulu, baru instance baru lanjut app.start().
#
# Flag MongoDB yang dipakai (key = f"deploy_{_SESSION_SUFFIX}"):
#   "pending"  → instance baru sudah siap, minta instance lama shutdown
#   "released" → instance lama sudah disconnect, instance baru boleh start
#   "active"   → instance baru sudah running (tulis setelah app.start())

_DEPLOY_FLAG_KEY = f"deploy_{_SESSION_SUFFIX}"

# FIX (bug: session userbot tidak tersimpan saat redeploy): _DEPLOY_ID
# SEBELUMNYA hanya str(os.getpid()). Di container Docker/Railway, proses
# pertama yang dijalankan di dalam container baru hampir selalu mendapat
# PID 1 (PID namespace baru per container). Akibatnya deploy LAMA dan
# deploy BARU bisa punya _DEPLOY_ID yang SAMA PERSIS ("1"). Pengecekan
# `data.get("by") != _DEPLOY_ID` di _deploy_watch_and_release() jadi
# False terus — instance lama menyangka sinyal "pending" itu datang dari
# dirinya sendiri, sehingga graceful_shutdown() (yang menyimpan session
# userbot via stop_userbot()) TIDAK PERNAH terpanggil lewat jalur ini.
# Satu-satunya jalur penyelamat tersisa adalah SIGTERM, yang tidak selalu
# sempat selesai sebelum Railway mengirim SIGKILL.
#
# Solusi: _DEPLOY_ID sekarang gabungan PID + waktu proses dimulai + token
# acak — kombinasi ini praktis mustahil sama antara dua proses berbeda,
# bahkan jika kebetulan keduanya mendapat PID yang sama.
import time as _time_deploy_id
import uuid as _uuid_deploy_id
_DEPLOY_ID = f"{os.getpid()}-{int(_time_deploy_id.time())}-{_uuid_deploy_id.uuid4().hex[:8]}"


async def _deploy_signal_new() -> None:
    """
    Instance BARU: cek dulu apakah ada instance aktif (state='active') di MongoDB.
    - Tidak ada flag / flag bukan 'active'  → deploy pertama atau script lama
                                               → langsung lanjut, tidak perlu tunggu.
    - Flag 'active' ada (script baru sudah jalan sebelumnya)
                                               → tulis 'pending', tunggu 'released'
                                                 maks 30 detik.
    """
    if get_active_backend() != "mongo":
        return

    import json, time

    # ── Cek apakah ada instance aktif ────────────────────────────────────────
    raw = await get_bot_config(_DEPLOY_FLAG_KEY)
    if raw:
        try:
            existing = json.loads(raw)
        except Exception:
            existing = {}
    else:
        existing = {}

    if existing.get("state") != "active":
        # Tidak ada instance lama yang pakai script baru → lanjut langsung
        print(f"[Deploy] ℹ️  Tidak ada instance aktif di MongoDB (state={existing.get('state', 'kosong')!r}). "
              f"Lanjut start tanpa tunggu.")
        return

    # ── Ada instance aktif → sinyal dan tunggu ───────────────────────────────
    payload = json.dumps({"state": "pending", "by": _DEPLOY_ID, "ts": time.time()})
    await save_bot_config(_DEPLOY_FLAG_KEY, payload)
    print(f"[Deploy] 🆕 Instance aktif ditemukan. Flag 'pending' ditulis (deploy_id={_DEPLOY_ID}). "
          f"Tunggu instance lama release (maks 30 detik)...")

    deadline = asyncio.get_event_loop().time() + 30
    while asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(1)
        raw = await get_bot_config(_DEPLOY_FLAG_KEY)
        if not raw:
            break
        try:
            data = json.loads(raw)
        except Exception:
            break
        if data.get("state") == "released":
            print(f"[Deploy] ✅ Instance lama sudah release. Lanjut start...")
            return

    print(f"[Deploy] ⏰ Timeout 30 detik — lanjut start paksa "
          f"(instance lama tidak merespons atau sudah mati).")


async def _deploy_watch_and_release() -> None:
    """
    Instance LAMA: poll MongoDB setiap 2 detik. Jika ada flag 'pending' dari
    deploy baru (bukan dari diri sendiri), lakukan graceful_shutdown() lalu
    tulis flag 'released' agar instance baru bisa lanjut.
    Berjalan sebagai background task sejak awal.
    """
    if get_active_backend() != "mongo":
        return

    import json
    print(f"[Deploy] 👀 Deploy watcher aktif (deploy_id={_DEPLOY_ID}).")
    while True:
        await asyncio.sleep(2)
        try:
            raw = await get_bot_config(_DEPLOY_FLAG_KEY)
            if not raw:
                continue
            data = json.loads(raw)
        except Exception:
            continue

        # Ada permintaan deploy baru, bukan dari diri sendiri
        if data.get("state") == "pending" and data.get("by") != _DEPLOY_ID:
            print(f"[Deploy] 🔄 Deploy baru terdeteksi. Instance lama mulai shutdown...")
            globals()["_shutdown_triggered"] = True

            # Simpan flag 'released' SEBELUM shutdown penuh agar instance baru
            # tidak menunggu sampai timeout 30 detik
            import time
            released = json.dumps({"state": "released", "by": _DEPLOY_ID, "ts": time.time()})
            try:
                await save_bot_config(_DEPLOY_FLAG_KEY, released)
            except Exception:
                pass

            await graceful_shutdown()
            # Hentikan event loop — instance ini selesai
            asyncio.get_event_loop().stop()
            return


async def _deploy_mark_active() -> None:
    """Instance baru setelah app.start() berhasil: tulis flag 'active'."""
    if get_active_backend() != "mongo":
        return
    import json, time
    payload = json.dumps({"state": "active", "by": _DEPLOY_ID, "ts": time.time()})
    await save_bot_config(_DEPLOY_FLAG_KEY, payload)
    print(f"[Deploy] ✅ Flag 'active' ditulis (deploy_id={_DEPLOY_ID}).")


async def _rewarm_known_peers(client) -> None:
    """
    Setelah redeploy, session baru tidak punya peer cache sama sekali.
    Fungsi ini resolve ulang semua grup/channel yang sudah dikenal di DB
    agar langsung masuk ke peer cache — mencegah PeerIdInvalid saat bot
    pertama kali mencoba kirim pesan ke chat tersebut.

    Dipanggil sekali setelah app.start() + _restore_session_from_mongo().
    Jika session berhasil di-restore dari MongoDB, rewarm tetap dijalankan
    untuk memastikan semua peer yang mungkin hilang ter-resolve ulang.
    """
    from database import config_db, nexus_grup_db, get_active_backend as _backend
    from database import group_action_log_db, local_mute_db

    if _backend() != "mongo":
        return

    peer_ids: set[int] = set()
    # username_map: chat_id → "@username" — dipakai sebagai jalur resolve
    # utama saat sesi baru (username tidak butuh access hash)
    username_map: dict[int, str] = {}
    user_ids: set[int] = set()

    # Grup/channel dari config_db
    try:
        async for doc in config_db.find({}):
            cid = doc.get("chat_id")
            if cid:
                cid = int(cid)
                peer_ids.add(cid)
                uname = doc.get("username")
                if uname:
                    username_map[cid] = f"@{uname.lstrip('@')}"
    except Exception as e:
        print(f"[Rewarm] ⚠️  Gagal baca config_db: {e}")

    # Grup dari nexus_grup_db
    try:
        async for doc in nexus_grup_db.find({}):
            cid = doc.get("chat_id")
            if cid:
                cid = int(cid)
                peer_ids.add(cid)
                uname = doc.get("username")
                if uname and cid not in username_map:
                    username_map[cid] = f"@{uname.lstrip('@')}"
    except Exception as e:
        print(f"[Rewarm] ⚠️  Gagal baca nexus_grup_db: {e}")

    # CHANNEL_OWNER, LOG_CHANNEL, LOG_OS dari env
    for _env_key in ("CHANNEL_OWNER", "LOG_CHANNEL", "LOG_OS"):
        try:
            _ch_id = int(os.environ.get(_env_key, 0))
            if _ch_id:
                peer_ids.add(_ch_id)
        except Exception:
            pass

    # User dari dm_users
    try:
        from database import get_all_dm_users
        dm_users = await get_all_dm_users()
        for uid in dm_users:
            if uid:
                user_ids.add(int(uid))
    except Exception as e:
        print(f"[Rewarm] ⚠️  Gagal baca dm_users_db: {e}")

    # User dari group_action_log
    try:
        async for doc in group_action_log_db.find({}):
            uid = doc.get("user_id")
            if uid:
                user_ids.add(int(uid))
    except Exception as e:
        print(f"[Rewarm] ⚠️  Gagal baca group_action_log_db: {e}")
    
    # Resolve grup/channel — prioritas @username (tidak butuh access hash di sesi baru),
    # fallback ke integer ID (butuh access hash; mungkin gagal di sesi baru).
    ok, fail = 0, 0
    for cid in peer_ids:
        resolved = False
        # Coba via @username dulu — lebih andal di sesi baru
        if cid in username_map:
            try:
                await client.get_chat(username_map[cid])
                ok += 1
                resolved = True
            except Exception:
                pass
        # Fallback ke integer ID (berhasil jika access hash masih ada di session)
        if not resolved:
            try:
                await client.get_chat(cid)
                ok += 1
            except Exception:
                fail += 1
        await asyncio.sleep(0.3)  # jeda kecil cegah rate-limit
    print(f"[Rewarm] ✅ Chat: {ok} berhasil, {fail} gagal ({len(peer_ids)} total)")

    # Resolve user — dijalankan sebagai background task agar TIDAK memblokir
    # startup. Sebelumnya loop ini di-await langsung; jika dm_users atau
    # group_action_log_db punya banyak entri, startup bisa macet di sini
    # puluhan detik sebelum sempat menjalankan blok monitor bot.
    user_list = list(user_ids)

    async def _rewarm_users_bg():
        u_ok, u_fail = 0, 0
        for uid in user_list:
            try:
                await client.get_users(uid)
                u_ok += 1
            except Exception:
                u_fail += 1
            await asyncio.sleep(0.3)
        print(f"[Rewarm] ✅ User: {u_ok} berhasil, {u_fail} gagal ({len(user_list)} total)")

    asyncio.create_task(_rewarm_users_bg())

# ── Health Check ──────────────────────────────────────────────────────────────
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot Antispam + Nexus AI Online 2026")

    def log_message(self, *args):
        pass


def run_health_check():
    try:
        port = int(os.environ.get("PORT", 8000))
        server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
        server.serve_forever()
    except Exception as e:
        print(f"[HealthCheck] Error: {e}")


# ── Set Bot Commands ──────────────────────────────────────────────────────────
async def _setup_commands():
    try:
        await app.set_bot_commands(
            commands=[
                BotCommand("unmutemic", "hps / priv link bio sebelum klik ini"),
                BotCommand("antigcast", "anti spam cerdas abad ini"),
                BotCommand("spam", "balas pesan n masukin ke database AI"),
            ],
            scope=BotCommandScopeAllGroupChats(),
        )
        await app.set_bot_commands(
            commands=[
                BotCommand("antigcast", "anti spam cerdas"),
            ],
            scope=BotCommandScopeAllPrivateChats(),
        )
        print("✅ Bot commands berhasil diset (grup & DM).")
    except Exception as e:
        print(f"⚠️  Gagal set bot commands: {e}")


# ── Resolve Channel Peer ──────────────────────────────────────────────────────
async def _resolve_channel_peer(client):
    """
    Resolve CHANNEL_OWNER, LOG_CHANNEL, dan LOG_OS dari .env ke Telegram peer,
    lalu simpan info CHANNEL_OWNER (title + username) ke database cloud.

    Tujuan:
      • Sesi baru belum pernah "melihat" channel → PeerIdInvalid saat kirim log
      • Resolve di sini memaksa Telegram mengembalikan access hash → masuk peer cache
      • Username di DB memungkinkan resolve ulang via @username saat bot restart

    Dipanggil sekali setelah app.start() di main().
    """
    from database import save_bot_config

    # ── Resolve LOG_CHANNEL dan LOG_OS ────────────────────────────────────────────
    # Cukup get_chat() — tujuannya hanya agar access hash masuk peer cache session.
    for _env_key in ("LOG_CHANNEL", "LOG_OS"):
        try:
            _ch_id = int(os.environ.get(_env_key, 0))
            if not _ch_id:
                continue
            await client.get_chat(_ch_id)
            print(f"[Startup] ✅ {_env_key} ({_ch_id}) berhasil di-resolve ke peer cache.")
        except Exception as _e:
            print(f"[Startup] ⚠️  Gagal resolve {_env_key}: {_e}")

    # ── Resolve CHANNEL_OWNER + simpan title/username ke DB ────────────────────
    ch_id = int(os.environ.get("CHANNEL_OWNER", 0))
    if not ch_id:
        return
    try:
        ch = await client.get_chat(ch_id)
        title    = ch.title or ""
        username = getattr(ch, "username", None) or ""
        await save_bot_config("channel_owner_id",       ch_id)
        await save_bot_config("channel_owner_title",    title)
        await save_bot_config("channel_owner_username", username)
        label = f"@{username}" if username else f"(no username, id={ch_id})"
        print(f"[Startup] ✅ CHANNEL_OWNER '{title}' {label} berhasil di-cache ke DB.")
    except Exception as e:
        print(f"[Startup] ⚠️  Gagal resolve CHANNEL_OWNER ({ch_id}): {e}")
        print(f"           Info channel akan diambil dari cache DB (jika sudah pernah disimpan sebelumnya).")


# ── Graceful Shutdown ─────────────────────────────────────────────────────────
async def _notify_owner():
    """Kirim notif ke owner lalu return. Dibatasi timeout 8 detik."""
    if not OWNER_ID:
        return
    try:
        await asyncio.wait_for(
            app.send_message(OWNER_ID, "⚠️ Bot offline — shutdown/maintenance."),
            timeout=8.0,
        )
        print("📢 Notifikasi shutdown terkirim ke owner.")
    except Exception as e:
        print(f"[Shutdown] Gagal kirim notif owner: {e}")


async def graceful_shutdown():
    """
    Tutup bot dengan bersih. Urutan:
      1. Simpan session terbaru ke MongoDB (peer cache yang ditemui sejak start
         ikut terbawa — PALING PENTING, harus sebelum app.stop()/close_db())
      2. Kirim notif ke owner (timeout 8 detik)
      3. Cancel semua background task
      4. Tutup koneksi database
      5. Stop Pyrogram (timeout 5 detik)
    """
    print("\n🛑 Memulai prosedur shutdown...")

    # ── Tulis flag 'released' ke MongoDB SEKARANG JUGA ───────────────────────
    # Harus dilakukan PERTAMA sebelum apapun — termasuk sebelum simpan session.
    # Tujuan: instance baru yang sedang menunggu (poll 1 detik) langsung tahu
    # instance ini sudah siap dilepas dan bisa lanjut app.start().
    # Jika ini ditunda sampai setelah simpan session/stop pyrogram,
    # instance baru akan timeout 30 detik karena Railway kill container
    # lebih cepat dari proses shutdown selesai.
    try:
        import json as _json, time as _time
        _released = _json.dumps({"state": "released", "by": _DEPLOY_ID, "ts": _time.time()})
        await save_bot_config(_DEPLOY_FLAG_KEY, _released)
        print("[Deploy] 🔓 Flag 'released' ditulis — instance baru boleh start.")
    except Exception as _e:
        print(f"[Deploy] ⚠️  Gagal tulis flag released: {_e}")

    # Simpan dulu sebelum apapun lain — ini yang mencegah peer cache (CHANNEL_OWNER,
    # grup, dll yang ditemui selama bot berjalan) hilang saat Railway redeploy.
    # Tanpa ini, MongoDB hanya punya snapshot session terakhir kali backup periodik
    # 20-menit jalan, sehingga peer baru yang ditemui setelah itu selalu hilang
    # tiap kali container di-restart/redeploy.
    try:
        await _save_session_to_mongo()
    except Exception as e:
        print(f"[Shutdown] ⚠️  Gagal simpan session sebelum shutdown: {e}")

    # Backup juga session semua bot pemantau (monitor) yang aktif — sama alasannya:
    # mencegah peer cache per-grup hilang setiap kali container di-redeploy.
    try:
        from monitor_bot_reference import save_all_sessions
        await save_all_sessions()
    except Exception as e:
        print(f"[Shutdown] ⚠️  Gagal simpan session monitor: {e}")

    # FIX (bug: sesi userbot lama tidak terbaca saat redeploy): backup juga
    # session userbot (Security OS) ke MongoDB. Sebelumnya graceful_shutdown()
    # (dipanggil dari SIGTERM handler — jalur redeploy Railway yang sebenarnya)
    # tidak pernah menyentuh session userbot sama sekali; stop_userbot() hanya
    # dipanggil di finally block main(), yang TIDAK TENTU tereksekusi saat
    # proses dimatikan paksa lewat SIGTERM. Tanpa baris ini, peer cache userbot
    # (termasuk login yang baru saja berhasil) hilang setiap redeploy.
    try:
        from video_call import stop_userbot as _stop_ub
        await _stop_ub()
    except Exception as e:
        print(f"[Shutdown] ⚠️  Gagal simpan/stop session userbot: {e}")

    await _notify_owner()

    current = asyncio.current_task()
    tasks   = [t for t in asyncio.all_tasks() if t is not current]
    if tasks:
        print(f"🔄 Membatalkan {len(tasks)} background task...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        print("✅ Semua task dibatalkan.")

    await close_db()

    try:
        if app.is_connected:
            await asyncio.wait_for(app.stop(), timeout=5.0)
            print("✅ Koneksi Telegram berhasil diputus.")
    except asyncio.TimeoutError:
        print("⚠️  app.stop() timeout — paksa keluar.")
    except Exception as e:
        print(f"[Shutdown] app.stop error (diabaikan): {e}")

    print("🛑 Bot berhasil dimatikan dengan bersih.")


# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    global app

    # Banner startup — tampil sebelum apapun
    _print_startup_banner()

    # Health check thread (daemon)
    threading.Thread(target=run_health_check, daemon=True).start()

    # Setup database (auto-pilih MongoDB atau SQLite)
    await setup_db()

    # ── Deploy Handshake ──────────────────────────────────────────────────────
    # Sinyal ke instance lama bahwa deploy baru siap — tunggu sampai instance lama
    # disconnect dari Telegram (maks 30 detik) agar tidak terjadi AuthKeyDuplicated.
    await _deploy_signal_new()

    # Pulihkan session dari MongoDB jika file lokal tidak ada (misal setelah Railway redeploy)
    await _restore_session_from_mongo()

    # Bangun Client
    app = await _build_client()

    # Admin session cleanup — hapus sesi kedaluwarsa setiap 10 menit
    asyncio.create_task(_adm_cleanup())

    # Deploy watcher — deteksi jika ada deploy baru selama bot berjalan → auto shutdown
    asyncio.create_task(_deploy_watch_and_release())

    # Nexus midnight scheduler
    from plugins.nexus.engine import cron_midnight_scheduler
    asyncio.create_task(cron_midnight_scheduler())

    # Jalankan bot
    try:
        await app.start()
    except Exception as _start_err:
        # Jika session yang dipulihkan dari MongoDB ditolak Telegram → hapus dan login fresh
        if "AUTH_KEY_DUPLICATED" in str(_start_err) or "AUTH_KEY_UNREGISTERED" in str(_start_err):
            print(f"[Session] ⚠️  Session dari MongoDB tidak valid ({type(_start_err).__name__}), hapus dan login ulang...")
            import os as _os
            session_path = _SESSION_NAME + ".session"
            if _os.path.exists(session_path):
                _os.remove(session_path)
            await _clear_session_from_mongo()
            # Buat client baru tanpa session lama
            app = Client(
                _SESSION_NAME,
                api_id=API_ID,
                api_hash=API_HASH,
                bot_token=BOT_TOKEN,
                plugins=dict(root="plugins"),
            )
            await app.start()
        else:
            raise

    # Background task delete_worker dijalankan SETELAH app.start() agar client
    # sudah terkoneksi saat worker pertama kali mencoba menghapus pesan.
    asyncio.create_task(delete_worker(app))

    # Background task moderation_worker_loop — eksekusi mute/unmute/ban satu
    # per satu dengan jeda kecil antar aksi, agar tidak ada banyak aksi
    # moderasi ditembak bersamaan ke Telegram API saat raid terjadi.
    from core.moderation_queue import moderation_worker_loop
    asyncio.create_task(moderation_worker_loop(app))

    # Background task panel_write_worker — menulis ke DB hasil tombol panel
    # (toggle, +/-, dsb) secara antri. Client diteruskan agar worker bisa
    # mengoreksi tampilan panel di DM admin jika penulisan gagal permanen.
    asyncio.create_task(panel_write_worker(app))

    try:
        # Tandai instance ini sebagai aktif di MongoDB
        try:
            await _deploy_mark_active()
        except Exception as e:
            print(f"[Startup] ⚠️  _deploy_mark_active gagal (dilanjutkan): {e}")

        # Simpan session lokal ke MongoDB setelah login berhasil
        try:
            await _save_session_to_mongo()
        except Exception as e:
            print(f"[Startup] ⚠️  _save_session_to_mongo gagal (dilanjutkan): {e}")

        try:
            await _setup_commands()
        except Exception as e:
            print(f"[Startup] ⚠️  _setup_commands gagal (dilanjutkan): {e}")

        # Resolve CHANNEL_OWNER peer → simpan ke DB agar dikenal sesi baru
        try:
            await _resolve_channel_peer(app)
        except Exception as e:
            print(f"[Startup] ⚠️  _resolve_channel_peer gagal (dilanjutkan): {e}")

        # Isi ulang peer cache dari semua grup/channel yang dikenal di DB
        # → mencegah PeerIdInvalid setelah Railway redeploy (filesystem bersih)
        try:
            await _rewarm_known_peers(app)
        except Exception as e:
            print(f"[Startup] ⚠️  _rewarm_known_peers gagal (dilanjutkan): {e}")

        # Backup session ke MongoDB setiap 20 menit
        # → peer baru yang ditemui saat bot berjalan ikut tersimpan
        try:
            asyncio.create_task(_periodic_session_backup())
        except Exception as e:
            print(f"[Startup] ⚠️  Gagal create_task _periodic_session_backup: {e}")

        # ── Userbot Security OS ───────────────────────────────────────────────
        # Dijalankan SETELAH bot biasa start & siap agar OTP bisa dikirim ke owner.
        # start_userbot tidak blocking — ia menjalankan task sendiri di background.
        #
        # FIX (disederhanakan kembali ke pola versi lama yang terbukti selalu
        # bekerja): sebelumnya ada wrapper _run_start_userbot_safely() di sekitar
        # create_task ini. Wrapper itu seharusnya setara secara fungsional, tapi
        # untuk menyingkirkan kemungkinan ada interaksi tak terduga, baris ini
        # dikembalikan ke bentuk paling sederhana — create_task langsung pada
        # start_userbot(app), identik dengan versi yang sudah terbukti membuat
        # userbot selalu aktif sebelumnya. start_userbot() sendiri SUDAH
        # membungkus setiap langkah internalnya dengan try/except masing-masing
        # (lihat video_call.py) sehingga tidak butuh wrapper tambahan di sini.
        #
        # Print log EKSPLISIT ditambahkan tepat sebelum & sesudah create_task
        # ini — jika suatu saat baris "[UB] ▶️" di video_call.py tidak pernah
        # muncul di log lagi, baris print di bawah ini akan menunjukkan dengan
        # pasti apakah create_task ini sendiri tercapai atau tidak.
        print("[Startup] ▶️  Memanggil create_task(start_userbot)...", flush=True)
        try:
            asyncio.create_task(start_userbot(app))
            print("[Startup] ✅ create_task(start_userbot) berhasil dijadwalkan.", flush=True)
        except Exception as e:
            import traceback
            print(f"[UB] ❌ Gagal create_task start_userbot: {e}", flush=True)
            traceback.print_exc()

        # ── NewsCore Time-Checker Loop ────────────────────────────────────────
        try:
            from plugins.commands.newscore import newscore_checker_loop
            asyncio.create_task(newscore_checker_loop(app))
        except Exception as e:
            print(f"[Startup] ⚠️  newscore_checker_loop gagal dimulai: {e}")

        # ── NewsCore Score Buffer Flush Worker ────────────────────────────────
        # Flush skor yang di-buffer di memory ke MongoDB secara batch,
        # setiap NS_FLUSH_INTERVAL detik (default 10 detik).
        try:
            from database import ns_flush_worker_loop
            asyncio.create_task(ns_flush_worker_loop())
        except Exception as e:
            print(f"[Startup] ⚠️  ns_flush_worker_loop gagal dimulai: {e}")

        # ── LOG_CHANNEL Flush Worker ───────────────────────────────────────────
        # Flush antrian log (spam lokal/global, regex, sistem) ke LOG_CHANNEL
        # secara batch setiap LOG_FLUSH_INTERVAL detik (default 8 detik).
        # FIXED: Mencegah FloodWait menumpuk saat grup ramai — semua log
        # dikumpulkan dulu lalu dikirim sebagai 1 pesan gabungan per siklus.
        try:
            from plugins.commands.log import log_flush_worker_loop
            asyncio.create_task(log_flush_worker_loop(app))
        except Exception as e:
            print(f"[Startup] ⚠️  log_flush_worker_loop gagal dimulai: {e}")

        # ── Bot Pemantau (Monitor) — independen dari userbot ──────────────────
        # FIX: Sebelumnya _load_instances_from_db() hanya dipanggil dari dalam
        # _voice_chat_monitor_loop() di video_call.py — yang hanya berjalan jika
        # userbot berhasil start. Akibatnya, jika userbot off atau belum punya
        # session, bot pemantau yang sudah di-generate tidak pernah aktif.
        #
        # Solusi: panggil _load_instances_from_db() langsung di sini, setelah
        # bot utama siap, TANPA menunggu userbot. Bot pemantau berjalan
        # independen — mereka hanya butuh token di DB, bukan sesi userbot.
        # _voice_chat_monitor_loop() di video_call.py tetap memanggil
        # _load_instances_from_db() juga, tapi karena fungsi itu idempotent
        # (grup yang sudah ada di _active_instances dilewati), tidak ada duplikasi.
        try:
            from monitor_bot_reference import (
                _load_instances_from_db as _monitor_load,
                _periodic_session_backup as _monitor_session_backup,
            )
            await _monitor_load()
            asyncio.create_task(_monitor_session_backup())
            print("[Startup] ✅ Bot pemantau (monitor) dimuat dari DB — independen dari userbot.", flush=True)
        except Exception as e:
            print(f"[Startup] ⚠️  Gagal load bot pemantau (monitor): {e}", flush=True)

        print("🚀 Bot Antispam + Nexus AI aktif! Tekan Ctrl+C untuk berhenti.", flush=True)
        await idle()
    except (KeyboardInterrupt, asyncio.CancelledError):
        # graceful_shutdown mungkin sudah dipanggil via SIGTERM handler —
        # _shutdown_triggered mencegah pemanggilan ganda
        if not globals().get("_shutdown_triggered", False):
            await graceful_shutdown()
    finally:
        # Hentikan userbot dengan bersih sebelum tutup program
        try:
            await stop_userbot()
        except Exception:
            pass
        try:
            if app.is_connected:
                await app.stop()
        except Exception:
            pass

if __name__ == "__main__":
    import signal

    loop = asyncio.get_event_loop()

    # ── Exception handler global — redam noise PeerIdInvalid dari Pyrogram ──
    # Bot pemantau (monitor_bot_reference.py) menjalankan banyak Client
    # Pyrogram sekaligus. Saat Telegram mengirim raw update untuk sebuah
    # channel yang BELUM dikenal sesi monitor tertentu (belum punya peer
    # cache/access_hash-nya), Client.handle_updates() internal Pyrogram
    # melempar exception (PeerIdInvalid / KeyError: ID not found) di dalam
    # task-nya sendiri — bukan di kode kita, jadi tidak tertangkap try/except
    # manapun di aplikasi. Exception ini TIDAK FATAL (peer akan dikenal
    # dengan sendirinya begitu monitor benar-benar berinteraksi dengan
    # channel itu), tapi membanjiri log sebagai "Task exception was never
    # retrieved" lengkap dengan traceback panjang.
    #
    # Handler ini meredam KHUSUS exception jenis itu (cukup 1 baris info),
    # dan tetap menampilkan traceback lengkap untuk exception lain yang
    # benar-benar perlu diperhatikan.
    def _global_exception_handler(loop, context):
        exc = context.get("exception")
        msg = context.get("message", "")
        if isinstance(exc, (KeyError, ValueError)) and (
            "Peer id invalid" in str(exc) or "ID not found" in str(exc)
        ):
            print(f"[Pyrogram] ℹ️  Peer belum dikenal sesi monitor (diabaikan, tidak fatal): {exc}")
            return
        # Exception lain yang tidak dikenali — tetap tampilkan penuh seperti default asyncio
        loop.default_exception_handler(context)

    loop.set_exception_handler(_global_exception_handler)

    # ── SIGTERM handler ───────────────────────────────────────────────────────
    # Railway (dan Docker) mengirim SIGTERM saat redeploy/stop — bukan SIGINT.
    # Tanpa handler ini, proses lama tidak sempat disconnect dari Telegram
    # sebelum instance baru start → Telegram deteksi dua koneksi → AuthKeyDuplicated
    # → session baru (tanpa peer cache) → rewarm selalu gagal.
    #
    # Solusi: tangkap SIGTERM, jalankan graceful_shutdown() (simpan session +
    # disconnect Telegram), lalu stop loop — proses selesai sebelum instance baru naik.
    _shutdown_triggered = False

    def _handle_sigterm():
        if globals().get("_shutdown_triggered", False):
            return
        globals()["_shutdown_triggered"] = True
        print("\n[Signal] SIGTERM diterima — memulai graceful shutdown...")
        # Schedule graceful_shutdown sebagai task di loop yang sedang berjalan
        loop.create_task(graceful_shutdown())

    loop.add_signal_handler(signal.SIGTERM, _handle_sigterm)

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        # 1. Ambil semua task yang masih menggantung/pending
        pending_tasks = asyncio.all_tasks(loop)

        # 2. Batalkan semua task tersebut
        for task in pending_tasks:
            task.cancel()

        # 3. Berikan waktu sejenak agar sistem memproses pembatalan task
        if pending_tasks:
            try:
                loop.run_until_complete(asyncio.gather(*pending_tasks, return_exceptions=True))
            except Exception:
                pass

        # 4. Baru setelah itu tutup loop dengan aman
        try:
            loop.close()
        except Exception:
            pass

        print("🛑 Bot berhasil dimatikan dengan bersih.")

