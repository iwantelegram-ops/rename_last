"""
video_call.py — Userbot Security OS
════════════════════════════════════════════════════════════════════════════════
Modul userbot Pyrogram yang berjalan berdampingan dengan bot biasa (antigcast.py).

ARSITEKTUR (Database-driven — tidak ada komunikasi di grup):
  ┌─────────────────────────────────────────────────────────────┐
  │  Bot Pemantau (monitor_bot_reference.py)                    │
  │  Scan semua member → simpan bio_profiles ke DB bersama      │
  └────────────────────────┬────────────────────────────────────┘
                           │ DB bersama (MONGO_URL / SQLite sama)
           ┌───────────────┴───────────────────────┐
           ▼                                       ▼
  ┌────────────────┐                    ┌──────────────────────┐
  │   Bot Utama    │  query bio_profiles│      Userbot (ini)   │
  │  (pesan grup)  │  → hapus jika link │  (obrolan suara/VC)  │
  └────────────────┘                    └──────────────────────┘
                                               │ kick dari VC
                                               ↓ (jika has_link)

ATURAN UTAMA:
  - Userbot TIDAK mengirim /checkbio ke grup — query DB langsung.
  - Bot pemantau mengisi bio_profiles secara berkala & saat user join.
  - Userbot hanya memantau obrolan SUARA — pesan/typing ditangani bot biasa.
  - Semua data disimpan ke DB (MongoDB/SQLite) via db[] seperti bot asli.
  - Logika penyimpanan asli tidak diubah sama sekali.

ARSITEKTUR VC (Scheduled Join — bukan keepalive):
  - Security OS aktif → userbot join VC tiap 30 menit (bukan stay permanen).
  - Saat join: scan semua peserta VC sekarang + peserta baru (UpdateGroupCallParticipants).
  - Bot pemantau cek profil tiap user (cache 1 menit) → mute jika link, unmute jika bersih.
  - Telegram kick userbot setelah ~30 detik — tidak masalah, tugas sudah selesai.
  - Tidak ada keepalive, tidak ada rejoin loop — sudah terjadwal.

FLOW STARTUP:
  1. antigcast.py start → bot biasa aktif
  2. start_userbot(app) dipanggil → cek session userbot
  3a. Session ada → userbot langsung aktif
  3b. Session tidak ada → bot TIDAK memaksa OTP login otomatis.
      Bot hanya kirim DM info ke OWNER_ID bahwa session userbot tidak
      ditemukan. Owner login userbot baru kapan saja via panel:
      Nexus AI → Owner Bot → Ganti Userbot (flow OTP interaktif ada di
      sana, lihat ganti_userbot() / nx_setuserbot di nexus_handlers.py).

VARIABEL .env BARU:
  USERBOT_PHONE — nomor HP akun userbot (format: +62xxx)
                  Jika kosong → Security OS tidak tersedia, bot berjalan normal.
                  Tidak lagi dipakai untuk OTP login otomatis saat startup —
                  hanya dipakai sebagai nilai awal/fallback; nomor aktif
                  yang dipakai login selalu yang terbaru lewat Ganti Userbot.
"""

from __future__ import annotations

import sys as _sys_path_fix
from pathlib import Path as _Path_fix
_BOT_DIR_VC = str(_Path_fix(__file__).resolve().parent)
if _BOT_DIR_VC not in _sys_path_fix.path:
    _sys_path_fix.path.insert(0, _BOT_DIR_VC)

import os
import asyncio
import time
import re as _re
from pathlib import Path as _Path
from datetime import datetime as _dt_vc, timezone as _tz_vc, timedelta as _td_vc

_WIB_VC = _tz_vc(_td_vc(hours=7))

from pyrogram import Client as _Client, filters as _filters
from pyrogram.enums import ParseMode
from pyrogram.types import Message as _Message, ChatMemberUpdated as _ChatMemberUpdated
from pyrogram.errors import (
    FloodWait,
    PhoneCodeInvalid,
    PhoneCodeExpired,
    SessionPasswordNeeded,
    PhoneNumberInvalid,
    PeerIdInvalid,
)
from dotenv import load_dotenv

# FIX (pemindahan ke folder security_os/): file ini sekarang berada di
# <root>/security_os/video_call.py, bukan lagi di <root>/video_call.py. Jika
# proyek memakai file .env fisik (bukan env var Railway), .env tersebut
# tetap berada di ROOT proyek — bukan di dalam security_os/. parent.parent
# (bukan parent saja) supaya tetap menemukan .env yang benar apapun
# urutan modul ini diimpor.
load_dotenv(dotenv_path=_Path(__file__).resolve().parent.parent / ".env", override=False)

# ── Env ───────────────────────────────────────────────────────────────────────
API_ID        = int(os.environ.get("API_ID", 0))
API_HASH      = os.environ.get("API_HASH", "")
OWNER_ID      = int(os.environ.get("OWNER_ID", 0))
USERBOT_PHONE = os.environ.get("USERBOT_PHONE", "").strip()
LOG_OS        = int(os.environ.get("LOG_OS", 0))

# FIX (pemindahan ke folder security_os/): _BOT_DIR sekarang sengaja diarahkan
# ke ROOT proyek (parent.parent), bukan ke folder security_os/ tempat file ini
# berada. Tujuan: file session userbot (userbot_security_os.session) tetap
# tersimpan di lokasi yang sama seperti sebelum pemindahan — konsisten
# dengan _SESSION_NAME bot utama di antigcast.py yang juga di root, dan
# tidak mengubah perilaku apa pun selain lokasi modul Python-nya.
_BOT_DIR    = _Path(__file__).resolve().parent.parent
_UB_SESSION = str(_BOT_DIR / "userbot_security_os")

# ── State global ──────────────────────────────────────────────────────────────
userbot: _Client | None = None   # instance userbot Pyrogram
_bot_ref: _Client | None = None  # referensi bot biasa (untuk kirim peringatan)
_ub_ready: bool = False
_ub_self_id: int = 0             # user_id akun userbot agar tidak kick diri sendiri
_ub_backup_loop_running: bool = False  # guard — cegah _periodic_ub_session_backup dobel

# ── OTP flow state ────────────────────────────────────────────────────────────
_otp_event: asyncio.Event | None = None
_otp_value: str = ""
_otp_handler_registered: bool = False   # cegah register_otp_handler() terpasang dobel

# ── Rate limit per grup — minimum jeda antar pengecekan ──────────────────────
_last_vc_check: dict[int, float] = {}
_VC_CHECK_INTERVAL = 15.0   # detik minimum antar scan VC per grup

# ── Pelacak user yang sedang diproses (hindari double-kick) ──────────────────
_processing_kick: set[tuple[int, int]] = set()   # {(chat_id, user_id)} — cegah double-proses

# ── Cache status member grup (TTL 2 menit) ────────────────────────────────────
_member_cache: dict[tuple[int, int], tuple[bool, float]] = {}
_MEMBER_CACHE_TTL = 120.0   # detik

# ── Cache status VIP per user per grup (TTL 3 menit) ─────────────────────────
# VIP = ada di free_per_group collection → bebas dari SEMUA tindakan Security OS.
_vip_cache: dict[tuple[int, int], tuple[bool, float]] = {}
_VIP_CACHE_TTL = 180.0   # detik

# ── Alasan warning tertunda (dihapus setelah _do_send_warning memakai) ────────
_pending_warn_reason: dict[tuple[int, int], str] = {}

# ── Global lock inspeksi dadakan via /unmutemic (hindari concurrent floodwait) ─
_vc_inspection_lock: asyncio.Lock | None = None

# ══════════════════════════════════════════════════════════════════════════════
# VC JOIN/LEAVE WORKER — Antrean global join/leave obrolan suara
# ══════════════════════════════════════════════════════════════════════════════
#
# MASALAH YANG DISELESAIKAN:
#   Tanpa worker ini, setiap panggilan _vc_scan_and_enforce dan
#   _join_vc_for_group dari banyak grup berjalan paralel serentak.
#   Telegram API tidak toleran terhadap banyak join/leave VC bersamaan
#   dari satu akun userbot — bisa menyebabkan FloodWait, PEER_FLOOD,
#   atau bahkan suspensi sementara akun userbot.
#
# SOLUSI:
#   Semua permintaan join VC (dari scheduler 30 menit, dari enable Security OS,
#   dari follow-up recheck) dimasukkan ke _vc_join_queue.
#   Worker tunggal _vc_join_queue_worker mengeksekusi satu per satu
#   dengan jeda _VC_WORKER_JOIN_DELAY detik antar join ke VC berbeda.
#
# ITEM QUEUE FORMAT:
#   ("scan",    chat_id: int)          → jalankan _vc_scan_and_enforce
#   ("leave",   chat_id: int)          → jalankan _leave_vc_for_group_direct
#   ("join_now", chat_id: int)         → jalankan _join_vc_for_group_direct (darurat/enable)
#
# DEDUPLICATION:
#   _vc_join_pending menyimpan set chat_id yang sudah antri. Jika chat_id
#   sudah di-queue dan belum dikerjakan, permintaan baru TIDAK ditambah
#   kecuali tipenya "leave" atau "join_now" (prioritas lebih tinggi).
# ══════════════════════════════════════════════════════════════════════════════

# Antrean global join/leave VC — tuple (action, chat_id)
_vc_join_queue: asyncio.Queue | None = None

# Set chat_id yang sedang antri "scan" — cegah duplikat scan
_vc_join_pending: set[int] = set()

# Task worker join/leave VC
_vc_join_worker_task: asyncio.Task | None = None

# Jeda antar join VC ke grup berbeda (detik) — aman dari FloodWait
_VC_WORKER_JOIN_DELAY = 8.0   # 8 detik jeda antar join ke grup berbeda

# Jeda antar leave VC ke grup berbeda (detik)
_VC_WORKER_LEAVE_DELAY = 3.0


def _get_vc_join_queue() -> asyncio.Queue:
    """Lazy-init antrean join VC dalam event loop aktif."""
    global _vc_join_queue
    if _vc_join_queue is None:
        _vc_join_queue = asyncio.Queue()
    return _vc_join_queue


def _enqueue_vc_scan(chat_id: int) -> None:
    """
    Masukkan permintaan scan VC grup ke antrean global.
    Jika chat_id sudah antri (tipe scan), request baru diabaikan (deduplicated).
    Spawn worker jika belum ada.
    """
    global _vc_join_worker_task
    q = _get_vc_join_queue()
    if chat_id in _vc_join_pending:
        # Sudah antri — tidak perlu double-queue
        print(f"[VC-Worker] Grup {chat_id} sudah antri scan — skip duplikat.")
        return
    _vc_join_pending.add(chat_id)
    q.put_nowait(("scan", chat_id))
    # Spawn worker jika belum ada atau sudah selesai
    if _vc_join_worker_task is None or _vc_join_worker_task.done():
        _vc_join_worker_task = _safe_task(_vc_join_queue_worker(), tag="vc-join-worker")
        print("[VC-Worker] Worker join VC di-spawn.")


def _enqueue_vc_join_now(chat_id: int) -> None:
    """
    Masukkan permintaan join segera (saat enable Security OS).
    Tipe 'join_now' selalu masuk antrean (tidak deduplicated) — prioritas enable.
    """
    global _vc_join_worker_task
    q = _get_vc_join_queue()
    q.put_nowait(("join_now", chat_id))
    if _vc_join_worker_task is None or _vc_join_worker_task.done():
        _vc_join_worker_task = _safe_task(_vc_join_queue_worker(), tag="vc-join-worker")
        print("[VC-Worker] Worker join VC di-spawn (join_now).")


def _enqueue_vc_leave(chat_id: int) -> None:
    """
    Masukkan permintaan leave VC grup ke antrean global.
    Tipe 'leave' selalu masuk (tidak cek deduplicated) — harus dieksekusi.
    """
    global _vc_join_worker_task
    q = _get_vc_join_queue()
    # Hapus dari pending scan jika ada — leave lebih prioritas
    _vc_join_pending.discard(chat_id)
    q.put_nowait(("leave", chat_id))
    if _vc_join_worker_task is None or _vc_join_worker_task.done():
        _vc_join_worker_task = _safe_task(_vc_join_queue_worker(), tag="vc-join-worker")
        print("[VC-Worker] Worker join VC di-spawn (leave).")


async def _vc_join_queue_worker() -> None:
    """
    Worker tunggal antrean join/leave VC.

    Mengeksekusi satu permintaan pada satu waktu — tidak pernah dua grup
    join/leave VC bersamaan. Jeda _VC_WORKER_JOIN_DELAY detik setelah
    setiap join, _VC_WORKER_LEAVE_DELAY setelah setiap leave.

    Loop berhenti saat antrean kosong — akan di-spawn ulang saat ada item baru.
    """
    q = _get_vc_join_queue()
    print("[VC-Worker] Worker join VC mulai berjalan.")
    while True:
        try:
            action, chat_id = q.get_nowait()
        except asyncio.QueueEmpty:
            break

        try:
            if action == "scan":
                print(f"[VC-Worker] Eksekusi scan VC grup {chat_id}...")
                await _vc_scan_and_enforce(chat_id)
                _vc_join_pending.discard(chat_id)
                if not q.empty():
                    await asyncio.sleep(_VC_WORKER_JOIN_DELAY)

            elif action == "join_now":
                print(f"[VC-Worker] Eksekusi join_now VC grup {chat_id}...")
                await _join_vc_for_group_direct(chat_id)
                if not q.empty():
                    await asyncio.sleep(_VC_WORKER_JOIN_DELAY)

            elif action == "leave":
                print(f"[VC-Worker] Eksekusi leave VC grup {chat_id}...")
                await _leave_vc_for_group_direct(chat_id)
                if not q.empty():
                    await asyncio.sleep(_VC_WORKER_LEAVE_DELAY)

        except FloodWait as fw:
            wait_sec = fw.value + 2
            print(f"[VC-Worker] FloodWait {fw.value}s saat action={action} grup={chat_id} — menunggu {wait_sec}s...")
            _vc_join_pending.discard(chat_id)
            await asyncio.sleep(wait_sec)
        except Exception as e:
            print(f"[VC-Worker] Error saat proses action={action} grup={chat_id}: {e}")
            _vc_join_pending.discard(chat_id)

        q.task_done()

    print("[VC-Worker] Worker join VC selesai (antrean kosong).")


# ══════════════════════════════════════════════════════════════════════════════
# MIC ACTION WORKER — Antrean mute/unmute mic per grup
# ══════════════════════════════════════════════════════════════════════════════
#
# MASALAH YANG DISELESAIKAN:
#   _kick_from_voice dan _unmute_user_in_vc bisa terpanggil bersamaan
#   untuk banyak user di grup yang sama dalam satu siklus scan.
#   Eksekusi bersamaan membebani API dan menyebabkan FloodWait.
#   Juga: tindakan mic untuk grup yang berbeda bisa berjalan paralel
#   tanpa batas, memperparah kondisi flood.
#
# SOLUSI:
#   Setiap tindakan mute/unmute mic dimasukkan ke _mic_queues[chat_id].
#   Satu worker per grup mengeksekusi tindakan mic satu per satu
#   dengan jeda _MIC_WORKER_DELAY detik antar tindakan.
#
# ITEM QUEUE FORMAT:
#   ("mute",   user_id: int, call_input, reason: str)
#   ("unmute", user_id: int, call_input, reason: str)
# ══════════════════════════════════════════════════════════════════════════════

# Antrean mic per grup — {chat_id: asyncio.Queue}
_mic_queues: dict[int, asyncio.Queue] = {}

# Task worker mic per grup — {chat_id: asyncio.Task}
_mic_workers: dict[int, asyncio.Task] = {}

# Set (chat_id, user_id) yang sudah antri mic action — cegah duplikat per user per grup
_mic_pending: set[tuple[int, int]] = set()

# Jeda antar tindakan mic dalam satu grup (detik)
_MIC_WORKER_DELAY = 2.0


def _get_mic_queue(chat_id: int) -> asyncio.Queue:
    """Dapatkan / buat antrean mic untuk grup ini."""
    if chat_id not in _mic_queues:
        _mic_queues[chat_id] = asyncio.Queue()
    return _mic_queues[chat_id]


def _enqueue_mute_mic(
    chat_id: int,
    user_id: int,
    call_input,
    reason: str = "bio mengandung link",
) -> None:
    """
    Masukkan permintaan mute mic ke antrean grup ini.
    Jika (chat_id, user_id) sudah antri tindakan mic, skip duplikat.
    Spawn worker jika belum ada.
    """
    key = (chat_id, user_id)
    if key in _mic_pending:
        print(f"[Mic-Worker] uid={user_id} grup={chat_id} sudah antri mic action — skip duplikat.")
        return
    _mic_pending.add(key)
    q = _get_mic_queue(chat_id)
    q.put_nowait(("mute", user_id, call_input, reason))
    _ensure_mic_worker(chat_id)


def _enqueue_unmute_mic(
    chat_id: int,
    user_id: int,
    call_input,
    reason: str = "bio bersih",
) -> None:
    """
    Masukkan permintaan unmute mic ke antrean grup ini.
    Jika (chat_id, user_id) sudah antri tindakan mic, skip duplikat.
    Spawn worker jika belum ada.
    """
    key = (chat_id, user_id)
    if key in _mic_pending:
        print(f"[Mic-Worker] uid={user_id} grup={chat_id} sudah antri unmute — skip duplikat.")
        return
    _mic_pending.add(key)
    q = _get_mic_queue(chat_id)
    q.put_nowait(("unmute", user_id, call_input, reason))
    _ensure_mic_worker(chat_id)


def _ensure_mic_worker(chat_id: int) -> None:
    """Spawn worker mic untuk grup jika belum ada atau sudah selesai."""
    existing = _mic_workers.get(chat_id)
    if existing is None or existing.done():
        task = _safe_task(_mic_action_worker(chat_id), tag=f"mic-worker-{chat_id}")
        _mic_workers[chat_id] = task


async def _mic_action_worker(chat_id: int) -> None:
    """
    Worker antrean mute/unmute mic per grup.

    Mengeksekusi tindakan mic satu per satu dengan jeda _MIC_WORKER_DELAY
    detik antar tindakan. Berhenti saat antrean kosong — akan di-spawn
    ulang oleh _enqueue_mute_mic/_enqueue_unmute_mic saat ada item baru.
    """
    q = _get_mic_queue(chat_id)
    print(f"[Mic-Worker] Worker mic grup {chat_id} mulai berjalan.")
    while True:
        try:
            item = q.get_nowait()
        except asyncio.QueueEmpty:
            break

        action, user_id, call_input, reason = item
        key = (chat_id, user_id)
        try:
            if action == "mute":
                print(f"[Mic-Worker] Mute mic uid={user_id} grup={chat_id} — alasan: {reason}")
                await _kick_from_voice(chat_id, user_id, call_input)
            elif action == "unmute":
                print(f"[Mic-Worker] Unmute mic uid={user_id} grup={chat_id} — alasan: {reason}")
                await _unmute_user_in_vc(chat_id, user_id, call_input)
        except FloodWait as fw:
            wait_sec = fw.value + 2
            print(f"[Mic-Worker] FloodWait {fw.value}s saat {action} uid={user_id} grup={chat_id} — menunggu {wait_sec}s...")
            await asyncio.sleep(wait_sec)
        except Exception as e:
            print(f"[Mic-Worker] Error {action} uid={user_id} grup={chat_id}: {e}")
        finally:
            _mic_pending.discard(key)

        q.task_done()
        if not q.empty():
            await asyncio.sleep(_MIC_WORKER_DELAY)

    # Worker selesai — hapus referensi agar bisa dibuat ulang
    _mic_workers.pop(chat_id, None)
    print(f"[Mic-Worker] Worker mic grup {chat_id} selesai (antrean kosong).")


def get_vc_inspection_lock() -> asyncio.Lock:
    """Return (atau buat) lock inspeksi dadakan. Aman dipanggil dari event loop manapun."""
    global _vc_inspection_lock
    if _vc_inspection_lock is None:
        _vc_inspection_lock = asyncio.Lock()
    return _vc_inspection_lock


def _safe_task(coro, *, tag: str = "") -> asyncio.Task:
    """
    asyncio.create_task dengan exception logging agar task tidak crash diam-diam.
    Semua exception dicatat ke console tanpa membunuh event loop.
    Gunakan untuk semua create_task di hot path Security OS.
    """
    async def _guarded():
        try:
            await coro
        except Exception as _exc:
            _pfx = f"[{tag}] " if tag else ""
            print(f"[UB-Task] {_pfx}{type(_exc).__name__}: {_exc}")
    return asyncio.create_task(_guarded())

# ── Pelacak keberadaan userbot di VC per grup ─────────────────────────────────
# Di-set saat join berhasil, dihapus saat leave/disabled.
_ub_in_vc_groups: set[int] = set()   # {chat_id}

# ══════════════════════════════════════════════════════════════════════════════
# SECOS MUTE TRACKING — Pelacak mute khusus Security OS (Fitur 1-4)
# ══════════════════════════════════════════════════════════════════════════════
#
# "peer invalid" = bot pemantau (bot biasa, bukan userbot) GAGAL resolve peer
#   user karena user belum pernah berinteraksi di grup, sehingga _fetch_bio
#   return None (semua 4 fallback gagal). Kondisi ini berbeda dari has_link=True.
#
# Dua kondisi yang dicatat di _secos_muted_users (cache 30 detik):
#   "non_member"   → user bukan anggota grup tapi naik ke VC
#   "peer_invalid" → user member grup tapi bot pemantau return None (gagal fetch)
#
# Flow (Fitur 2, 3, 4):
#   Mute terjadi → tunggu 1 menit → naik VC cek ulang (Fitur 2)
#   Jika masih invalid → tunggu 5 menit → naik VC lagi (Fitur 3)
#   Setelah itu → kembali ke jadwal 30 menit normal (Fitur 4)
# ══════════════════════════════════════════════════════════════════════════════

# Cache mute khusus Security OS (non-member / peer_invalid) — TTL 30 detik
# Key: (chat_id, user_id), Value: (reason_type: str, muted_at: float)
_SECOS_MUTE_CACHE_TTL = 30.0   # detik — sesuai spesifikasi Fitur 1
_secos_muted_users: dict[tuple[int, int], tuple[str, float]] = {}

# Pelacak follow-up task per grup — cegah double-spawn
# Key: chat_id, Value: asyncio.Task yang sedang berjalan
_secos_followup_tasks: dict[int, asyncio.Task] = {}

# ── Cooldown join per grup ────────────────────────────────────────────────────
# Mencegah multi-join cepat dari jalur manapun (UpdateGroupCall, OnJoin, keepalive).
# Value: waktu monotonic saat join terakhir.
_vc_join_last_ts: dict[int, float] = {}   # {chat_id: monotonic_time}
_VC_JOIN_COOLDOWN      = 15.0      # detik — minimal jeda antar join ke VC yang sama
_VC_SCHEDULED_INTERVAL = 30 * 60   # 30 menit — jeda antar siklus join per grup
_VC_SCAN_DURATION      = 20        # detik stay di VC untuk scan peserta saat ini

# ── Guard tumpang tindih scan antar siklus scheduler ─────────────────────────
# Mencegah _vc_scan_and_enforce dijalankan 2x bersamaan untuk grup yang sama,
# misalnya ketika siklus 30 menit berikutnya mulai sebelum siklus sebelumnya
# selesai (30 grup × stagger 10 detik = 300 detik = 5 menit blocking di VC).
_vc_scanning_groups: set[int] = set()   # {chat_id} yang sedang aktif di-scan

# ── Cache admin grup per chat_id (TTL 5 menit) ──────────────────────────────
_admin_cache: dict[int, tuple[set[int], float]] = {}   # {chat_id: (admin_ids, ts)}
_ADMIN_CACHE_TTL = 300.0   # 5 menit — refresh admin list tiap 5 menit

# ── Cache bio per user per grup (dua lapis) ──────────────────────────────────
# Lapisan 1 (di sini, video_call.py): cache in-memory userbot, TTL 60 detik.
#   → Setelah 60 detik, saat user naik VC lagi → trigger force_check_vc_join().
# Lapisan 2 (di MonitorInstance): cache VC khusus, juga TTL 60 detik.
#   → MonitorInstance tidak hit Telegram API jika < 60 detik sejak cek VC.
#
# Kombinasi dua lapis ini memastikan:
#   • Data bio SELALU fresh (≤ 60 detik) saat user naik VC.
#   • Telegram API tidak di-spam jika user keluar-masuk VC berulang.
# Key: (chat_id, user_id) — cache TIDAK pernah dipakai lintas grup.
_bio_cache: dict[tuple[int, int], tuple[bool, float]] = {}
# FIX: sebelumnya hardcoded 60.0 — tidak ikut berubah saat user mengubah
# BIO_TTL_SECS di .env, sehingga cache userbot bisa lebih basi/lebih segar
# daripada TTL Mongo & throttle bot pemantau. Sekarang ikut env yang sama
# agar semua lapisan cache (Mongo TTL, bot pemantau, bot utama, userbot)
# selalu konsisten satu nilai.
# Satu-satunya sumber kebenaran TTL bio: BIO_TTL_SECS di .env.
# Default 300 — konsisten dengan monitor_bot_reference.py dan bio.py.
_BIO_CACHE_TTL = float(os.environ.get("BIO_TTL_SECS", 300))

# ── Penanda pesan jawaban bot pemantau ───────────────────────────────────────
_pending_checks: dict[tuple[int, int], int] = {}

# ── Mapping call_id → chat_id untuk UpdateGroupCallParticipants ──────────────
# Dideklarasikan di sini (global) agar _on_vc_update bisa mengaksesnya.
_call_id_to_chat: dict[int, int] = {}

# ── Mapping call_id → access_hash (wajib untuk InputGroupCall di raw API) ────
# update.call di UpdateGroupCallParticipants hanya berisi .id (GroupCallReference),
# TIDAK mengandung access_hash. access_hash hanya ada di UpdateGroupCall (saat VC
# dimulai) dan di GetFullChannel. Kita simpan di sini agar bisa build InputGroupCall
# yang valid saat memanggil phone.EditGroupCallParticipant.
_call_id_to_access_hash: dict[int, int] = {}

# ── Global semaphore — batasi concurrent /checkbio ke seluruh Telegram API ───
# Maks 3 query paralel di seluruh sistem (lintas semua grup).
# Diinisialisasi lazy di start_userbot().
_api_semaphore: asyncio.Semaphore | None = None
_API_CONCURRENCY = 3   # konservatif: 3 checkbio parallel max

# ── Per-grup semaphore — batasi checkbio berurutan per grup ──────────────────
# Setiap grup punya semaphore sendiri: maks 1 /checkbio berjalan di waktu yg sama
# per grup. Ini agar bot pemantau di grup A tidak dibanjiri pertanyaan serentak.
_group_semaphores: dict[int, asyncio.Semaphore] = {}

def _get_group_semaphore(chat_id: int) -> asyncio.Semaphore:
    """1 slot per grup — /checkbio diproses satu per satu per grup."""
    if chat_id not in _group_semaphores:
        _group_semaphores[chat_id] = asyncio.Semaphore(1)
    return _group_semaphores[chat_id]

# ── Per-grup antrean notifikasi (warn) ───────────────────────────────────────
# Notifikasi kick dikumpulkan per grup, lalu dikirim dengan jeda.
# Mencegah bot utama mengirim 10 pesan beruntun ke grup dalam 1 detik.
_warn_queues: dict[int, asyncio.Queue] = {}
_warn_workers: dict[int, asyncio.Task] = {}

# Jeda minimum antar pesan warn dalam 1 grup (detik)
_WARN_INTERVAL = 2.5

def _get_warn_queue(chat_id: int) -> asyncio.Queue:
    """Dapatkan / buat antrean warn untuk grup ini."""
    if chat_id not in _warn_queues:
        _warn_queues[chat_id] = asyncio.Queue()
    return _warn_queues[chat_id]

# Set chat_id yang sedang di-fetch admin list-nya — cegah concurrent fetch
_admin_fetch_in_progress: set[int] = set()

async def _get_group_admin_ids(chat_id: int) -> set[int]:
    """
    Ambil set user_id admin grup, dengan cache 5 menit.
    Return set kosong jika error — lebih aman skip check daripada false-kick admin.
    Dipanggil sebelum loop scan peserta VC untuk skip admin dari pengecekan.
    """
    cached = _admin_cache.get(chat_id)
    if cached:
        ids, ts = cached
        if time.monotonic() - ts < _ADMIN_CACHE_TTL:
            return ids
    if not userbot:
        return set()
    # Guard: jika sudah ada fetch in-progress untuk grup ini, pakai cache lama
    if chat_id in _admin_fetch_in_progress:
        return _admin_cache.get(chat_id, (set(), 0.0))[0]
    _admin_fetch_in_progress.add(chat_id)
    try:
        from pyrogram.enums import ChatMembersFilter
        admin_ids: set[int] = set()
        async for member in userbot.get_chat_members(chat_id, filter=ChatMembersFilter.ADMINISTRATORS):
            if member.user and member.user.id:
                admin_ids.add(member.user.id)
        _admin_cache[chat_id] = (admin_ids, time.monotonic())
        print(f"[UB-VC] Admin grup {chat_id}: {len(admin_ids)} admin di-cache.")
        return admin_ids
    except FloodWait as fw:
        await asyncio.sleep(fw.value + 1)
        return _admin_cache.get(chat_id, (set(), 0.0))[0]
    except Exception as e:
        print(f"[UB-VC] Gagal ambil admin grup {chat_id}: {e}")
        return _admin_cache.get(chat_id, (set(), 0.0))[0]
    finally:
        _admin_fetch_in_progress.discard(chat_id)


async def _warn_worker(chat_id: int) -> None:
    """
    Worker per-grup: ambil user_id dari antrean, kirim peringatan, tunggu jeda.
    Berjalan sampai antrean kosong, lalu berhenti (worker-on-demand).
    """
    q = _get_warn_queue(chat_id)
    while True:
        try:
            user_id = q.get_nowait()
        except asyncio.QueueEmpty:
            break
        try:
            await _do_send_warning(chat_id, user_id)
        except FloodWait as fw:
            wait_sec = fw.value + 2
            print(f"[UB-Warn] FloodWait {fw.value}s di warn worker grup={chat_id} uid={user_id} — menunggu {wait_sec}s lalu retry...")
            await asyncio.sleep(wait_sec)
            # Retry sekali setelah FloodWait — jangan buang item
            try:
                await _do_send_warning(chat_id, user_id)
            except Exception as e_retry:
                print(f"[UB-Warn] Retry warn gagal uid={user_id} grup={chat_id}: {e_retry}")
        except Exception as e:
            print(f"[UB-Warn] Worker error uid={user_id} grup={chat_id}: {e}")
        q.task_done()
        if not q.empty():
            await asyncio.sleep(_WARN_INTERVAL)
    # Worker selesai — hapus referensi agar bisa dibuat ulang
    _warn_workers.pop(chat_id, None)

def _enqueue_warning(chat_id: int, user_id: int) -> None:
    """Masukkan user_id ke antrean warn grup. Spawn worker jika belum ada."""
    q = _get_warn_queue(chat_id)
    q.put_nowait(user_id)
    # Spawn worker hanya jika tidak ada yang berjalan
    existing = _warn_workers.get(chat_id)
    if existing is None or existing.done():
        task = _safe_task(_warn_worker(chat_id), tag=f"warn-worker-{chat_id}")
        _warn_workers[chat_id] = task

# ── Throttle scan grup aktif — cegah spawn task tak terbatas ─────────────────
# Maks grup yang di-scan paralel per siklus monitor (10 detik).
_MAX_PARALLEL_GROUP_SCANS = 4


def _get_api_semaphore() -> asyncio.Semaphore:
    """Lazy-init semaphore di dalam event loop yang aktif."""
    global _api_semaphore
    if _api_semaphore is None:
        _api_semaphore = asyncio.Semaphore(_API_CONCURRENCY)
    return _api_semaphore


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE HELPERS — pakai db[] dari database.py (logika asli TIDAK diubah)
# ══════════════════════════════════════════════════════════════════════════════

def _get_db():
    """Lazy import untuk menghindari circular import saat modul pertama di-load."""
    from database import db, save_bot_config, get_bot_config
    return db, save_bot_config, get_bot_config


# ── Cache untuk _sec_os_get (mempercepat panel DM — tidak query DB tiap klik) ─
import time as _time_mod
_sec_os_cache: dict[int, tuple[dict, float]] = {}
_SEC_OS_TTL = 30  # detik


async def _sec_os_get(chat_id: int) -> dict:
    """
    Ambil dokumen Security OS untuk satu grup dari DB.

    Schema:
      chat_id        : int   — ID grup Telegram
      enabled        : bool  — apakah Security OS aktif untuk grup ini
      monitor_token  : str   — token bot pemantau (disimpan di DB)
      monitor_bot_id : int   — user_id Telegram bot pemantau
      monitor_chat   : int   — chat_id grup (sama dengan chat_id, redundan tapi eksplisit)
    """
    now = _time_mod.monotonic()
    hit = _sec_os_cache.get(chat_id)
    if hit and (now - hit[1]) < _SEC_OS_TTL:
        return hit[0]
    db, _, _ = _get_db()
    doc = await db["security_os"].find_one({"chat_id": chat_id})
    if doc is None:
        doc = {
            "chat_id":        chat_id,
            "enabled":        False,
            "monitor_token":  "",
            "monitor_bot_id": 0,
            "monitor_chat":   chat_id,
        }
    _sec_os_cache[chat_id] = (doc, now)
    return doc


async def _sec_os_save(doc: dict) -> None:
    db, _, _ = _get_db()
    # Exclude _id dari $set — MongoDB tidak izinkan update field immutable _id
    payload = {k: v for k, v in doc.items() if k != "_id"}
    await db["security_os"].update_one(
        {"chat_id": doc["chat_id"]},
        {"$set": payload},
        upsert=True,
    )
    _sec_os_cache.pop(doc["chat_id"], None)  # invalidasi cache panel


async def _sec_os_set_enabled(chat_id: int, enabled: bool) -> None:
    doc = await _sec_os_get(chat_id)
    doc["enabled"] = enabled
    await _sec_os_save(doc)


async def _sec_os_set_monitor(chat_id: int, token: str, bot_id: int) -> None:
    doc = await _sec_os_get(chat_id)
    doc["monitor_token"]  = token
    doc["monitor_bot_id"] = bot_id
    doc["monitor_chat"]   = chat_id
    await _sec_os_save(doc)


# ── DB helpers: lacak mute yang dilakukan userbot ─────────────────────────────
# Collection: vc_muted_by_ub → {chat_id, user_id, ts}
# Tujuan:
#   - Userbot HANYA membuka mute user yang dia sendiri yang mute-kan.
#   - Jika admin lain mute, userbot tidak ikut campur (tidak unmute).
#   - Saat userbot unmute → entri dihapus dari collection ini.

async def _record_ub_muted(chat_id: int, user_id: int) -> None:
    """Catat bahwa userbot yang mute user ini di grup ini."""
    try:
        db, _, _ = _get_db()
        await db["vc_muted_by_ub"].update_one(
            {"chat_id": chat_id, "user_id": user_id},
            {"$set": {"chat_id": chat_id, "user_id": user_id, "ts": time.time()}},
            upsert=True,
        )
    except Exception as e:
        print(f"[UB-MuteDB] Gagal catat mute uid={user_id} grup={chat_id}: {e}")


async def _remove_ub_muted(chat_id: int, user_id: int) -> None:
    """Hapus catatan mute userbot untuk user ini di grup ini (setelah unmute)."""
    try:
        db, _, _ = _get_db()
        await db["vc_muted_by_ub"].delete_one({"chat_id": chat_id, "user_id": user_id})
    except Exception as e:
        print(f"[UB-MuteDB] Gagal hapus mute uid={user_id} grup={chat_id}: {e}")


async def _ub_muted_this_user(chat_id: int, user_id: int) -> bool:
    """Return True jika userbot yang pernah mute user ini di grup ini."""
    try:
        db, _, _ = _get_db()
        doc = await db["vc_muted_by_ub"].find_one({"chat_id": chat_id, "user_id": user_id})
        return doc is not None
    except Exception:
        return False


# ── Session userbot ke/dari MongoDB ──────────────────────────────────────────

async def _save_ub_session() -> None:
    """Simpan .session userbot ke MongoDB (sama polanya dengan bot biasa)."""
    import base64
    _, save_bot_config, _ = _get_db()
    try:
        from database import get_active_backend
        backend = get_active_backend()
        if backend != "mongo":
            print(f"[UB] ℹ️  Backend aktif bukan MongoDB ({backend}) — simpan session dilewati.", flush=True)
            return
        path = _UB_SESSION + ".session"
        if not _Path(path).exists():
            print("[UB] ℹ️  File session lokal tidak ditemukan — tidak ada yang disimpan.", flush=True)
            return
        with open(path, "rb") as f:
            raw = f.read()
        await save_bot_config("ub_session_data", base64.b64encode(raw).decode())
        print(f"[UB] ✅ Session userbot disimpan ke MongoDB ({len(raw)} bytes).", flush=True)
    except Exception as e:
        import traceback
        print(f"[UB] ⚠️  Gagal simpan session ke MongoDB: {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()


async def _restore_ub_session() -> bool:
    """Pulihkan .session userbot dari MongoDB jika file lokal tidak ada."""
    import base64
    _, _, get_bot_config = _get_db()
    try:
        from database import get_active_backend
        backend = get_active_backend()
        if backend != "mongo":
            print(f"[UB] ℹ️  Backend aktif bukan MongoDB ({backend}) — restore session dilewati.", flush=True)
            return False
        path = _UB_SESSION + ".session"
        if _Path(path).exists():
            print("[UB] ℹ️  File session lokal sudah ada — restore dari MongoDB dilewati.", flush=True)
            return False
        saved = await get_bot_config("ub_session_data")
        if not saved:
            print("[UB] ℹ️  Tidak ada session userbot tersimpan di MongoDB (key 'ub_session_data' kosong/tidak ada).", flush=True)
            return False
        with open(path, "wb") as f:
            f.write(base64.b64decode(saved.encode()))
        print("[UB] ✅ Session userbot dipulihkan dari MongoDB.", flush=True)
        return True
    except Exception as e:
        import traceback
        print(f"[UB] ⚠️  Gagal pulihkan session: {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        return False


async def _periodic_ub_session_backup(interval_secs: int = 20 * 60) -> None:
    """
    Backup session userbot ke MongoDB secara berkala (default 20 menit),
    sama seperti pola yang sudah ada untuk bot utama (_periodic_session_backup
    di antigcast.py) dan bot pemantau (_periodic_session_backup di
    monitor_bot_reference.py).

    Tujuan: peer cache (grup/channel yang userbot temui sejak login terakhir)
    tetap ter-backup berkala, bukan HANYA saat graceful_shutdown(). Jika
    container mati paksa (SIGKILL sebelum shutdown selesai), session yang
    tersimpan tetap relatif baru (maks setua interval ini) — bukan basi
    sejak login pertama kali.

    Guard _ub_backup_loop_running mencegah lebih dari satu loop berjalan
    bersamaan — bisa terjadi jika start_userbot() (saat startup) dan
    change_userbot() (ganti manual via panel) keduanya memicu task ini
    dalam proses yang sama.
    """
    global _ub_backup_loop_running
    if _ub_backup_loop_running:
        return
    _ub_backup_loop_running = True
    try:
        while True:
            await asyncio.sleep(interval_secs)
            if not userbot or not _ub_ready:
                continue  # userbot sedang tidak aktif — tidak ada yang perlu dibackup
            await _save_ub_session()
            print("[UB] 🔄 Periodic backup session userbot selesai.", flush=True)
    finally:
        _ub_backup_loop_running = False


# ══════════════════════════════════════════════════════════════════════════════
# OTP LOGIN FLOW
# Saat session belum ada:
#   bot biasa → kirim instruksi ke OWNER_ID
#   owner → balas OTP
#   bot biasa → teruskan ke receive_otp_from_bot()
#   userbot → login dengan OTP
# ══════════════════════════════════════════════════════════════════════════════

def receive_otp_from_bot(text: str) -> None:
    """
    Dipanggil dari handler bot biasa saat owner membalas OTP/2FA.

    FIX: Selalu simpan nilai ke _otp_value terlepas dari state _otp_event.
    Jika event sudah ada → set langsung (path normal).
    Jika event belum ada (timing edge, dipanggil sebelum _do_login siap) →
    nilai tersimpan di _otp_value dan akan dibaca oleh _prompt_owner saat
    mulai berjalan (lihat pengecekan _otp_value di awal _prompt_owner).
    """
    global _otp_value
    _otp_value = text.strip()
    if _otp_event and not _otp_event.is_set():
        _otp_event.set()


def register_otp_handler(bot: _Client) -> None:
    """
    DEPRECATED — TIDAK DIPAKAI LAGI.

    FIX TOTAL (bug: /otp diam total): pendekatan lama mendaftarkan handler
    /otp secara MANUAL dari dalam start_userbot()/change_userbot() — yang
    keduanya berjalan sebagai task/coroutine SETELAH app.start(). Ada celah
    waktu nyata antara bot mulai menerima update dari Telegram dan baris
    register_otp_handler() benar-benar tereksekusi; jika owner mengirim
    /otp tepat di celah itu (atau race lain pada dispatcher Pyrogram saat
    add_handler() dipanggil dari dalam handler callback — lihat catatan di
    plugins/ui/handler_otp.py), pesan /otp tidak punya pendengar sama sekali.

    Sekarang handler /otp didaftarkan permanen lewat plugin auto-load
    (plugins/ui/handler_otp.py) — terpasang SAAT Client() dibuat, jauh
    SEBELUM app.start() dipanggil. Tidak bergantung pada start_userbot(),
    change_userbot(), atau timing task apapun. Fungsi ini dibiarkan ada
    (no-op) agar pemanggilan lama yang mungkin masih tersisa tidak error.
    """
    return


def _otp_is_waiting() -> bool:
    """True jika _do_login() sedang menunggu OTP dari owner saat ini."""
    return _otp_event is not None and not _otp_event.is_set()


async def _prompt_owner(bot: _Client, html_msg: str) -> str:
    """
    Tunggu OTP dari owner (maks 10 menit).
    Owner harus mengirim /otp <kode> ke bot ini secara DM.
    Return teks OTP, atau "" jika timeout.

    FIX race condition: _otp_event dibuat di _do_login SEBELUM send_code.
    Jika owner kirim /otp sebelum fungsi ini dipanggil, _otp_value sudah
    terisi (buffered oleh _catch_otp). Fungsi ini langsung return nilainya
    tanpa perlu wait — tidak ada OTP yang hilang.
    """
    global _otp_event, _otp_value

    # Cek apakah OTP sudah di-buffer saat kita belum siap
    if _otp_value:
        val = _otp_value
        _otp_value = ""
        _otp_event = None
        print(f"[UB-OTP] OTP sudah di-buffer sebelumnya, langsung dipakai.")
        return val

    # Hanya init jika belum disiapkan
    if _otp_event is None or _otp_event.is_set():
        _otp_event = asyncio.Event()
        _otp_value = ""

    print("[UB-OTP] Menunggu owner kirim OTP via DM bot dengan format: /otp <kode>")

    try:
        await asyncio.wait_for(_otp_event.wait(), timeout=600.0)
        return _otp_value
    except asyncio.TimeoutError:
        print("[UB-OTP] Timeout menunggu OTP dari owner (10 menit). Restart bot untuk mencoba lagi.")
        return ""
    finally:
        _otp_event = None
        _otp_value = ""


async def _do_login(bot: _Client) -> bool:
    """
    Login userbot dengan flow OTP interaktif.
    Owner harus mengirim /otp <kode> ke bot ini via DM.
    Return True jika berhasil, False jika gagal/timeout.
    """
    global userbot

    if not USERBOT_PHONE:
        print("[UB] ⚠️  USERBOT_PHONE tidak diset — Security OS tidak tersedia.")
        return False

    print("[UB] 🔄 Session userbot belum ada. Meminta kode OTP ke Telegram...")
    print(f"[UB] 📱 Nomor: {USERBOT_PHONE}")
    print("[UB] ⏳ Kirim OTP via DM bot dengan format: /otp <kode>")

    # Buat client userbot (mode user, bukan bot)
    ub = _Client(
        _UB_SESSION,
        api_id=API_ID,
        api_hash=API_HASH,
        device_model="Samsung Galaxy S24",
        system_version="Android 14",
        app_version="10.14.5",
    )

    try:
        await ub.connect()
    except Exception as e:
        print(f"[UB] Gagal connect: {e}")
        return False

    # FIX: Inisialisasi _otp_event SEBELUM send_code agar owner yang kirim
    # /otp sangat cepat (saat OTP Telegram tiba) tidak melewatkan event.
    # _prompt_owner akan skip re-init jika event sudah ada.
    #
    # FIX (bug: OTP yang sudah dibuffer terhapus): sebelumnya baris ini
    # SELALU mengosongkan _otp_value, termasuk jika owner sudah mengirim
    # /otp lebih dulu (tersimpan di buffer oleh plugins/ui/handler_otp.py)
    # sebelum baris ini sempat jalan — buffer itu langsung terhapus
    # sebelum _prompt_owner() punya kesempatan membacanya. Sekarang buffer
    # yang sudah terisi TIDAK disentuh di sini.
    global _otp_event, _otp_value
    if _otp_event is None or _otp_event.is_set():
        _otp_event = asyncio.Event()
        if not _otp_value:
            _otp_value = ""

    # Minta kode OTP ke Telegram
    try:
        sent = await ub.send_code(USERBOT_PHONE)
    except PhoneNumberInvalid:
        print(f"[UB] \u274c USERBOT_PHONE tidak valid: '{USERBOT_PHONE}' — periksa format di .env (contoh: +628123456789)")
        await ub.disconnect()
        return False
    except FloodWait as fw:
        print(f"[UB] FloodWait {fw.value}s saat send_code.")
        await asyncio.sleep(fw.value)
        await ub.disconnect()
        return False
    except Exception as e:
        print(f"[UB] Gagal send_code: {e}")
        await ub.disconnect()
        return False

    # Tampilkan petunjuk di console — owner harus kirim /otp sendiri ke bot
    phone_hint = (
        USERBOT_PHONE[:3] + "****" + USERBOT_PHONE[-3:]
        if len(USERBOT_PHONE) > 6 else "****"
    )
    print(f"[UB-OTP] \U0001f510 OTP Telegram dikirim ke {phone_hint}")
    print("[UB-OTP] Kirim OTP ke bot via DM dengan format: /otp <kode>")
    print("[UB-OTP] Menunggu owner kirim OTP... (timeout 10 menit)")
    otp = await _prompt_owner(bot, "")

    if not otp:
        await ub.disconnect()
        return False

    # Sign in dengan OTP
    try:
        await ub.sign_in(USERBOT_PHONE, sent.phone_code_hash, otp)

    except PhoneCodeInvalid:
        print("[UB-OTP] \u274c OTP salah. Restart bot untuk mencoba lagi.")
        await ub.disconnect()
        return False

    except PhoneCodeExpired:
        print("[UB-OTP] \u274c OTP sudah kadaluarsa. Restart bot untuk mencoba lagi.")
        await ub.disconnect()
        return False

    except SessionPasswordNeeded:
        # Akun menggunakan 2FA
        print("[UB-OTP] \U0001f511 Akun menggunakan 2FA. Kirim password via DM bot: /otp <password>")
        print("[UB-OTP] Menunggu password 2FA dari owner... (timeout 10 menit)")
        pw = await _prompt_owner(bot, "")
        if not pw:
            await ub.disconnect()
            return False
        try:
            await ub.check_password(pw)
        except Exception as e2:
            print(f"[UB-OTP] \u274c Password 2FA salah: {e2} — Restart bot untuk mencoba lagi.")
            await ub.disconnect()
            return False

    except Exception as e:
        print(f"[UB] Gagal sign_in: {e}")
        await ub.disconnect()
        return False

    # Login berhasil — userbot sudah connected via connect()+sign_in()
    # JANGAN panggil start() lagi, karena client sudah connected
    userbot = ub
    await _save_ub_session()

    try:
        me = await ub.get_me()
        _ub_self_id_val = me.id
        print(f"[UB] \u2705 Userbot Security OS berhasil login! Akun: {me.first_name} (id={me.id})")
        print("[UB] \U0001f6e1\ufe0f Security OS siap dikonfigurasi di panel grup.")
        return True, _ub_self_id_val
    except Exception as e:
        print(f"[UB] ⚠️  Login berhasil tapi gagal get_me: {e}")
        return True, 0


# ══════════════════════════════════════════════════════════════════════════════
# USERBOT — START & STOP
# ══════════════════════════════════════════════════════════════════════════════

async def start_userbot(bot: _Client) -> None:
    """
    Entry point dipanggil dari antigcast.py setelah bot biasa aktif.
    Non-blocking — langsung return setelah create_task background loop.

    FIX (debug total silence pada /otp): setiap langkah di bawah dibungkus
    try/except SENDIRI dan WAJIB mencetak sesuatu, baik sukses maupun
    gagal. Sebelumnya jika salah satu langkah ini melempar exception
    SEBELUM baris print pertama tercapai, seluruh fungsi berhenti tanpa
    bekas di log — termasuk register_otp_handler() yang dipanggil paling
    awal, sehingga /otp tidak pernah punya handler sama sekali (bukan
    soal urutan/ketelan handler lain, handler-nya memang tidak terpasang).
    """
    global userbot, _bot_ref, _ub_ready, _ub_self_id, _ub_dead_notified
    _bot_ref = bot
    print("[UB] ▶️  start_userbot() mulai berjalan.", flush=True)

    # Inisialisasi semaphore di dalam event loop yang aktif
    try:
        _get_api_semaphore()
    except Exception as e:
        print(f"[UB] ❌ Gagal init semaphore: {type(e).__name__}: {e}")

    # NOTE: Handler /otp TIDAK lagi dipasang manual di sini — sekarang
    # permanen lewat plugin auto-load (plugins/ui/handler_otp.py), aktif
    # sejak Client() dibuat, jauh sebelum app.start(). Lihat catatan FIX
    # lengkap di register_otp_handler() (deprecated, no-op) di atas.

    # Pasang handler auto-kenali bot pemantau saat masuk grup
    try:
        register_monitor_join_handler(bot)
    except Exception as e:
        import traceback
        print(f"[UB] ❌ Gagal pasang monitor-join handler: {type(e).__name__}: {e}")
        traceback.print_exc()

    # Coba pulihkan session dari MongoDB (setelah Railway redeploy)
    try:
        restored = await _restore_ub_session()
        if not restored:
            print("[UB] ℹ️  Tidak ada session untuk dipulihkan dari MongoDB (atau file lokal sudah ada).")
    except Exception as e:
        import traceback
        print(f"[UB] ❌ _restore_ub_session() error: {type(e).__name__}: {e}")
        traceback.print_exc()

    session_file = _UB_SESSION + ".session"

    if _Path(session_file).exists():
        # Session tersedia — coba langsung start
        try:
            ub = _Client(
                _UB_SESSION,
                api_id=API_ID,
                api_hash=API_HASH,
                device_model="Samsung Galaxy S24",
                system_version="Android 14",
                app_version="10.14.5",
            )
            await ub.start()
            me = await ub.get_me()
            userbot    = ub
            _ub_self_id = me.id
            _ub_ready  = True
            _ub_dead_notified = False   # reset — userbot baru berhasil aktif lagi
            print(f"[UB] ✅ Userbot aktif: {me.first_name} (id={me.id})", flush=True)
            await _save_ub_session()
            # Log berapa grup Security OS yang sudah terdaftar di DB
            await _log_registered_groups()
            _safe_task(_voice_chat_monitor_loop(), tag="vc-monitor-loop")
            # FIX (pengaman tambahan — session userbot tidak ke-track saat redeploy):
            # sebelumnya session userbot HANYA disimpan saat start() & stop_userbot()
            # (lihat graceful_shutdown()). Jika container di-SIGKILL paksa sebelum
            # graceful_shutdown() selesai (mis. timeout Railway, atau bug PID
            # collision pada deploy handshake — sudah diperbaiki di antigcast.py),
            # session yang tersimpan di Mongo bisa basi/hilang. Backup periodik ini
            # meniru pola yang sudah ada di bot utama & bot pemantau (_save_session_to_mongo
            # / save_all_sessions setiap 20 menit) — peer cache userbot ikut ter-update
            # berkala, bukan cuma di titik shutdown yang rawan gagal.
            _safe_task(_periodic_ub_session_backup(), tag="ub-session-backup")
            return
        except Exception as e:
            print(f"[UB] ⚠️  Session ada tapi gagal start ({type(e).__name__}): {e}")
            # Hapus session rusak agar bisa login ulang
            try:
                _Path(session_file).unlink(missing_ok=True)
            except Exception:
                pass

    # Tidak ada session / session rusak
    # FIX (startup tidak boleh memaksa OTP): sebelumnya jika USERBOT_PHONE
    # diset, bot langsung memulai _do_login() (flow OTP paksa) setiap kali
    # session tidak ditemukan — termasuk tiap kali redeploy. Ini mengganggu
    # karena owner harus selalu siap kirim /otp tiap restart, padahal akun
    # userbot sudah bisa diganti kapan saja lewat Nexus AI > Owner Bot >
    # Ganti Userbot (fungsi itu sudah berjalan baik, lihat ganti_userbot /
    # nx_setuserbot — TIDAK disentuh oleh perubahan ini).
    #
    # Sekarang: jika session tidak ditemukan saat startup, bot HANYA
    # mengirim DM info ke OWNER_ID — tidak ada OTP flow otomatis yang
    # dipaksa berjalan. Owner login manual via panel saat siap.
    print("[UB] ℹ️  Session userbot tidak ditemukan saat startup.")
    await _notify_owner_session_missing(bot)
    return


async def _notify_owner_session_missing(bot: _Client) -> None:
    """
    Kirim DM ke OWNER_ID memberitahu bahwa session userbot tidak ditemukan
    saat startup/redeploy — TANPA memulai OTP flow otomatis.

    Owner login userbot baru secara manual via panel:
    Nexus AI > Owner Bot > Ganti Userbot.
    """
    if not OWNER_ID:
        print("[UB] ⚠️  OWNER_ID tidak diset — tidak bisa kirim notifikasi.")
        return
    try:
        await asyncio.wait_for(
            bot.send_message(
                OWNER_ID,
                "⚠️ <b>Session Userbot Tidak Ditemukan</b>\n\n"
                "Bot baru saja start/redeploy, tapi session akun userbot "
                "(Security OS) tidak ditemukan di database maupun lokal.\n\n"
                "Security OS untuk semua grup <b>tidak aktif</b> sampai "
                "userbot login kembali.\n\n"
                "Login akun userbot baru via:\n"
                "<b>Nexus AI → Owner Bot → Ganti Userbot</b>",
                parse_mode=ParseMode.HTML,
            ),
            timeout=8.0,
        )
        print("[UB] 📢 Notifikasi session userbot hilang terkirim ke owner.")
    except Exception as e:
        print(f"[UB] ⚠️  Gagal kirim notifikasi ke owner: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# DETEKSI SESSION USERBOT MATI SAAT SEDANG BERJALAN
# (logout paksa dari Telegram, akun dinonaktifkan/dibanned, dll — bukan saat
# startup, tapi di tengah operasi normal setelah sebelumnya berhasil login)
# ══════════════════════════════════════════════════════════════════════════════

# Anti-spam: jangan kirim DM berulang kali untuk kematian session yang sama.
# Reset otomatis setiap kali userbot berhasil login ulang (_ub_ready jadi True
# lagi lewat start_userbot() / ganti_userbot()).
_ub_dead_notified: bool = False


def _is_session_dead_error(exc: BaseException) -> bool:
    """
    True jika exception ini menandakan session userbot sudah TIDAK VALID
    lagi di sisi Telegram — logout paksa dari device lain, sesi dicabut,
    akun dinonaktifkan, atau akun dibanned.

    Dicek lewat nama class (bukan hanya isinstance) supaya tetap terdeteksi
    walau ada perbedaan kecil di exception hierarchy antar versi pyrogram —
    isinstance tetap dicoba dulu sebagai jalur utama yang lebih akurat.
    """
    try:
        from pyrogram.errors import (
            AuthKeyUnregistered,
            UserDeactivated,
            UserDeactivatedBan,
        )
        if isinstance(exc, (AuthKeyUnregistered, UserDeactivated, UserDeactivatedBan)):
            return True
    except ImportError:
        pass

    # Fallback berbasis nama class — aman dari perbedaan versi pyrogram
    name = type(exc).__name__
    return name in (
        "AuthKeyUnregistered",
        "UserDeactivated",
        "UserDeactivatedBan",
        "SessionRevoked",
        "AuthKeyInvalid",
    )


async def _handle_userbot_session_dead(reason: str) -> None:
    """
    Dipanggil saat terdeteksi session userbot sudah tidak valid di tengah
    operasi normal (bukan saat startup). Menandai userbot sebagai tidak
    siap dan mengirim DM sekali ke OWNER_ID — tidak diulang tiap siklus.

    Owner login userbot baru via: Nexus AI → Owner Bot → Ganti Userbot
    (TIDAK ada OTP otomatis yang dipaksa di sini, sama seperti perilaku
    saat startup session tidak ditemukan).
    """
    global _ub_ready, _ub_dead_notified

    was_ready = _ub_ready
    _ub_ready = False

    if _ub_dead_notified:
        # Sudah pernah diberitahu untuk kematian session ini — jangan spam DM
        return
    _ub_dead_notified = True

    if was_ready:
        print(f"[UB] 🛑 Session userbot tidak valid lagi ({reason}). Security OS dihentikan.")

    if not OWNER_ID or not _bot_ref:
        print("[UB] ⚠️  OWNER_ID/_bot_ref tidak tersedia — tidak bisa kirim notifikasi.")
        return
    try:
        await asyncio.wait_for(
            _bot_ref.send_message(
                OWNER_ID,
                "🛑 <b>Session Userbot Terputus</b>\n\n"
                "Akun userbot (Security OS) terdeteksi <b>logout/sesi dicabut</b> "
                "dari sisi Telegram saat sedang berjalan.\n\n"
                f"<i>Detail:</i> <code>{reason}</code>\n\n"
                "Security OS untuk semua grup <b>tidak aktif</b> sampai "
                "userbot login kembali.\n\n"
                "Login akun userbot baru via:\n"
                "<b>Nexus AI → Owner Bot → Ganti Userbot</b>",
                parse_mode=ParseMode.HTML,
            ),
            timeout=8.0,
        )
        print("[UB] 📢 Notifikasi session userbot terputus terkirim ke owner.")
    except Exception as e:
        print(f"[UB] ⚠️  Gagal kirim notifikasi session terputus ke owner: {e}")


async def stop_userbot() -> None:
    """
    Hentikan userbot dengan bersih. Dipanggil dari graceful_shutdown().

    FIX (bug: sesi userbot lama tidak terbaca saat redeploy): sebelumnya
    fungsi ini langsung memanggil userbot.stop() tanpa menyimpan dulu
    peer cache terbaru ke MongoDB. Akibatnya, peer/grup baru yang dikenal
    userbot sejak login terakhir (atau sejak backup periodik 20 menit
    yang lalu) hilang begitu container di-redeploy — sama persis masalah
    yang sudah diperbaiki untuk bot biasa di antigcast.py (_save_session_to_mongo
    dipanggil SEBELUM app.stop() di graceful_shutdown). Sekarang userbot
    mengikuti pola yang sama: simpan dulu, baru stop.
    """
    global userbot, _ub_ready
    _ub_ready = False
    if userbot:
        try:
            await _save_ub_session()
        except Exception as e:
            print(f"[UB] ⚠️  Gagal simpan session sebelum stop: {e}")
        try:
            await userbot.stop()
            print("[UB] ✅ Userbot berhenti dengan bersih.")
        except Exception as e:
            print(f"[UB] stop error: {e}")
        userbot = None


# ══════════════════════════════════════════════════════════════════════════════
# VOICE CHAT MONITOR LOOP
# Polling ringan per-grup, hanya mengamati obrolan SUARA.
# Pesan/typing tetap sepenuhnya di tangan bot biasa (tidak disentuh).
# ══════════════════════════════════════════════════════════════════════════════


async def _log_registered_groups() -> None:
    """
    Saat startup, log berapa grup Security OS yang sudah tersimpan di MongoDB,
    lalu lakukan warm-up BERTAHAP (staggered) — resolve peer setiap grup dengan
    jeda kecil agar userbot tidak memicu FloodWait karena mengakses
    banyak grup sekaligus saat redeploy.
    """
    db, _, _ = _get_db()
    try:
        total  = await db["security_os"].count_documents({})
        active = await db["security_os"].count_documents({"enabled": True})
        print(
            f"[UB] 📋 Security OS DB: {total} grup terdaftar, "
            f"{active} aktif — semua dikenali otomatis dari MongoDB."
        )
    except Exception as e:
        print(f"[UB] ⚠️  Tidak bisa baca hitungan grup dari DB: {e}")
        return

    # ── Warm-up bertahap: resolve peer setiap grup dengan jeda ───────────────
    # Mencegah userbot "hadir" di banyak grup sekaligus saat redeploy,
    # yang bisa memicu FloodWait atau deteksi anomali Telegram.
    _STARTUP_STAGGER = 3.0   # detik jeda antar grup
    try:
        docs = await db["security_os"].find({}, {"chat_id": 1}).to_list(None)
    except Exception:
        return

    if not docs:
        return

    print(f"[UB] ⏳ Startup stagger: warm-up {len(docs)} grup "
          f"(jeda {_STARTUP_STAGGER}s per grup)...")
    for i, doc in enumerate(docs):
        if not userbot or not _ub_ready:
            break
        chat_id = doc.get("chat_id")
        if not chat_id:
            continue
        try:
            await userbot.resolve_peer(chat_id)
        except FloodWait as fw:
            print(f"[UB-Startup] FloodWait {fw.value}s saat resolve grup {chat_id} — menunggu...")
            await asyncio.sleep(fw.value + 1)
        except Exception:
            pass   # Grup mungkin dihapus/userbot tidak ada — lewati
        if i < len(docs) - 1:
            await asyncio.sleep(_STARTUP_STAGGER)

    print("[UB] ✅ Startup stagger selesai — userbot siap.")
    # Join grup dilakukan oleh _vc_scheduled_loop setiap 30 menit


async def _voice_chat_monitor_loop() -> None:
    """
    Background task — pasang handler raw update untuk menangkap
    UpdateGroupCallParticipants secara event-driven.

    ── CARA KERJA: MENURUNKAN USER BIO-LINK DARI OBROLAN SUARA ─────────────
    1. Userbot menjadi member grup (bukan peserta VC).
    2. Telegram API secara otomatis mengirim UpdateGroupCallParticipants
       ke semua member grup setiap ada user yang JOIN obrolan suara/video.
       ➜ Ini adalah perilaku resmi Telegram API — tidak memerlukan join VC.
    3. Setiap user yang join VC dicek: apakah bio-nya mengandung link?
       • Cek cache in-memory dulu (TTL 10 menit).
       • Jika tidak ada cache → query bio_profiles di DB (diisi bot pemantau).
    4. Jika has_link=True → userbot memanggil phone.EditGroupCallParticipant
       (muted=True, video_stopped=True) → user diturunkan dari obrolan suara.
    5. Bot biasa mengirim peringatan di grup lalu menghapus pesan setelah 10 detik.

    ── KENAPA USERBOT HARUS JOIN VC ─────────────────────────────────────────
    UpdateGroupCallParticipants HANYA dikirim Telegram ke klien yang sudah
    berada di dalam VC (subscriber aktif call). Userbot yang hanya jadi member
    grup biasa TIDAK akan menerima event peserta join/leave.

    Oleh karena itu:
      • Saat VC baru dimulai (UpdateGroupCall) → userbot join VC otomatis.
      • Saat startup/redeploy dan VC sudah aktif → userbot join via startup scan.
      • phone.EditGroupCallParticipant (mute mic) wajib dipanggil dari dalam VC
        ATAU oleh admin dengan izin "Kelola Obrolan Video" — userbot join VC
        memastikan kedua kondisi terpenuhi.
    """
    print("[UB] \U0001f3a4 Voice chat monitor dimulai (event-driven).")

    if not userbot:
        return

    # ── Init MonitorInstance dari DB DULU sebelum handler VC aktif ───────────
    # Wajib sebelum @on_raw_update didaftarkan agar force_check_vc_join tidak
    # return None karena _active_instances masih kosong saat event pertama masuk.
    try:
        from monitor_bot_reference import _load_instances_from_db, _periodic_session_backup
        await _load_instances_from_db()
        print("[UB-Monitor] ✅ MonitorInstance dimuat dari DB.")
        # Backup periodik session tiap monitor (peer cache ikut terbawa saat redeploy)
        import asyncio as _asyncio_mon
        _asyncio_mon.create_task(_periodic_session_backup())
    except Exception as _e_mon:
        print(f"[UB-Monitor] ⚠️  Gagal load MonitorInstance: {_e_mon}")

    # ── Sync dialog agar Telegram kirim UpdateGroupCall ke sesi ini ──────────
    try:
        print("[UB-VC] Sinkronisasi dialog untuk subscribe update VC...")
        async for _ in userbot.get_dialogs():
            pass
        print("[UB-VC] ✅ Dialog tersinkronisasi.")
    except FloodWait as fw:
        print(f"[UB-VC] FloodWait {fw.value}s saat get_dialogs")
        await asyncio.sleep(fw.value + 1)
    except Exception as e:
        print(f"[UB-VC] get_dialogs error (tidak fatal): {e}")

    @userbot.on_raw_update()
    async def _on_vc_update(client, update, users, chats):
        if not _ub_ready:
            return
        try:
            from pyrogram.raw.types import (
                UpdateGroupCallParticipants,
                UpdateGroupCall,
                GroupCallParticipant,
                GroupCallDiscarded,
            )
        except ImportError:
            return

        # ── Tangkap voice chat baru dimulai → daftarkan call_id + access_hash ─
        if isinstance(update, UpdateGroupCall):
            chat_id_raw = getattr(update, "chat_id", None)
            if chat_id_raw:
                # Telegram kirim chat_id sebagai angka positif untuk supergroup
                chat_id_neg = int(f"-100{chat_id_raw}") if chat_id_raw > 0 else chat_id_raw
                call_obj = getattr(update, "call", None)
                if call_obj:
                    # ── FILTER: skip VC yang sudah berakhir (GroupCallDiscarded) ──
                    # Telegram kirim UpdateGroupCall + GroupCallDiscarded saat VC selesai.
                    # Jangan proses sebagai VC baru — cukup bersihkan mapping.
                    if isinstance(call_obj, GroupCallDiscarded):
                        disc_id = getattr(call_obj, "id", None)
                        if disc_id:
                            _call_id_to_chat.pop(disc_id, None)
                            _call_id_to_access_hash.pop(disc_id, None)
                        return

                    # ── FILTER: skip live stream channel (bukan obrolan suara grup) ──
                    # GroupCall.is_stream = True  → Live stream / channel broadcast → skip
                    # GroupCall.is_stream = False/None → Obrolan suara grup → proses
                    # Telegram membedakan keduanya via flag ini di object GroupCall.
                    is_stream = getattr(call_obj, "is_stream", False)
                    if is_stream:
                        print(
                            f"[UB-VC] Skip live stream (bukan obrolan suara grup) "
                            f"di chat {chat_id_neg}"
                        )
                        return

                    call_id = getattr(call_obj, "id", None)
                    # BUG FIX: simpan access_hash — hanya tersedia di UpdateGroupCall,
                    # TIDAK ada di UpdateGroupCallParticipants (GroupCallReference).
                    # Tanpa access_hash, phone.EditGroupCallParticipant akan gagal
                    # dengan ACCESS_HASH_INVALID atau serupa.
                    access_hash = getattr(call_obj, "access_hash", None)
                    if call_id:
                        # Simpan selalu — filter enabled dicek saat ada peserta join
                        _call_id_to_chat[call_id] = chat_id_neg
                        if access_hash is not None:
                            _call_id_to_access_hash[call_id] = access_hash
                        # Log semua VC yang terdeteksi (debug)
                        sec = await _sec_os_get(chat_id_neg)
                        enabled = sec.get("enabled", False)
                        print(
                            f"[UB-VC] Obrolan suara grup {chat_id_neg} "
                            f"(call_id={call_id}, enabled={enabled}, "
                            f"access_hash={'✅' if access_hash else '⚠️ tidak ada'})"
                        )
                        # Userbot harus JOIN VC segera saat VC dimulai.
                        #
                        # KENAPA WAJIB JOIN:
                        # UpdateGroupCallParticipants HANYA dikirim Telegram ke klien
                        # yang sudah berada di dalam VC (subscriber aktif call).
                        # Jika userbot tidak join, ia tidak akan pernah menerima event
                        # peserta join/leave — sehingga pemantauan bio-link tidak berjalan.
                        #
                        # UpdateGroupCall (event VC mulai) dikirim ke SEMUA member grup,
                        # sehingga inilah satu-satunya kesempatan reliable untuk join.
                        # Scheduler 30 menit akan join pada waktunya — tidak auto-join di sini.
                        if enabled:
                            print(
                                f"[UB-VC] VC dimulai di grup {chat_id_neg} "
                                f"(call_id={call_id}) — dijadwal tiap 30 menit."
                            )
            return

        if not isinstance(update, UpdateGroupCallParticipants):
            return

        call_id = update.call.id
        chat_id = _call_id_to_chat.get(call_id)
        if not chat_id:
            # ── FALLBACK: mapping belum terisi (warmup gagal/terlewat) ───────
            # Coba resolve langsung dengan cocokkan call.id ke grup Security OS
            # yang terdaftar. Hasil yang cocok di-cache agar event berikutnya
            # tidak perlu resolve ulang.
            chat_id = await _resolve_chat_for_call_id(call_id)
            if not chat_id:
                return
            _call_id_to_chat[call_id] = chat_id
            print(f"[UB-VC] Fallback resolve: call_id={call_id} → grup {chat_id}")

        sec_doc = await _sec_os_get(chat_id)
        if not sec_doc.get("enabled"):
            return

        # ARSITEKTUR DB-DRIVEN: monitor_bot_id tidak wajib untuk query bio.
        # Userbot langsung baca collection bio_profiles yang diisi bot pemantau.
        # Catatan: Security OS tetap membutuhkan bot pemantau untuk mengisi DB,
        # tapi userbot tidak perlu tahu monitor_bot_id untuk cek bio.
        monitor_id = sec_doc.get("monitor_bot_id", 0)  # dipertahankan untuk logging

        # Tidak ada auto-join — scheduler 30 menit yang menangani join VC.

        # FIX 4: Ambil daftar admin grup (cached 5 menit) — admin di-skip
        _vc_admin_ids = await _get_group_admin_ids(chat_id)

        for p in update.participants:
            if not isinstance(p, GroupCallParticipant):
                continue
            if getattr(p, "left", False):
                # User keluar dari VC — skip
                continue

            peer = getattr(p, "peer", None)
            if peer is None:
                continue
            uid = getattr(peer, "user_id", None)
            if not uid or uid == _ub_self_id:
                continue
            # FIX 4: Skip admin grup
            if uid in _vc_admin_ids:
                continue

            # Pisahkan "muted mic oleh admin" vs "mute sendiri" vs "muted di typing (chat)"
            # muted=True + can_self_unmute=False → admin mute mic (yang userbot pedulikan)
            # muted=True + can_self_unmute=True  → self-mute (BUKAN urusan userbot)
            # Restrict typing (chat ban) TIDAK ada kaitannya dengan field VC ini.
            _p_muted    = bool(getattr(p, "muted", False))
            _can_self   = bool(getattr(p, "can_self_unmute", True))
            is_muted    = _p_muted and not _can_self   # True hanya jika admin-muted
            # BUG 2: muted_by_you = field Telegram API, True jika userbot sendiri yang mute
            muted_by_you = bool(getattr(p, "muted_by_you", False))

            key = (chat_id, uid)
            if key in _processing_kick:
                continue

            # Cek in-memory cache dulu (TTL 1 menit)
            cached = _bio_cache.get(key)
            if cached:
                has_link, cache_ts = cached
                if time.monotonic() - cache_ts < _BIO_CACHE_TTL:
                    if has_link:
                        # Jika admin lain sudah unmute (is_muted=False) tapi
                        # bio masih ada link → mute ulang dengan notifikasi baru.
                        _processing_kick.add(key)
                        call_input = _build_input_group_call(call_id)
                        if not is_muted:
                            print(
                                f"[UB-VC] uid={uid} grup={chat_id}: di-unmute admin lain "
                                "tapi bio masih ada link → mute mic ulang."
                            )
                        _safe_task(
                            _execute_kick(chat_id, uid, call_input, was_already_muted=is_muted),
                            tag="exec-kick-cache",
                        )
                    elif is_muted:
                        # bio bersih/kosong tapi mic muted → cek fresh dari bot pemantau
                        _processing_kick.add(key)
                        call_input = _build_input_group_call(call_id)
                        _safe_task(
                            _query_monitor_then_kick(
                                chat_id, uid, monitor_id, call_input,
                                is_muted=True, muted_by_you=muted_by_you,
                            ),
                            tag="query-muted",
                        )
                    continue

            # Query DB (bot pemantau sudah mengisi bio_profiles)
            _processing_kick.add(key)
            call_input = _build_input_group_call(call_id)
            _safe_task(
                _query_monitor_then_kick(
                    chat_id, uid, monitor_id, call_input,
                    is_muted=is_muted, muted_by_you=muted_by_you,
                )
            )

    # Warmup: isi _call_id_to_chat dari grup Security OS yang sudah punya VC aktif
    await _warmup_active_calls()

    # Join VC yang sudah aktif saat startup/redeploy.
    #
    # KENAPA WAJIB JOIN SAAT STARTUP:
    # UpdateGroupCallParticipants HANYA dikirim Telegram ke klien yang sudah
    # berada di dalam VC. Jika VC sudah aktif sebelum bot start (dan tidak ada
    # UpdateGroupCall baru yang diterima), userbot tidak akan pernah masuk VC
    # kecuali join manual di sini.
    asyncio.ensure_future(_safe_task(_vc_scheduled_loop(), tag="vc-scheduled-loop"))
    print("[UB-VC] Scheduler join VC 30 menit dimulai.")

    # Cache cleanup loop — bersihkan entri cache kedaluwarsa tiap 10 menit
    asyncio.ensure_future(_safe_task(_cache_cleanup_loop(), tag="cache-cleanup-loop"))
    print("[UB-Cache] Cache cleanup loop dimulai (interval 10 menit).")

    # Jaga task tetap hidup
    while _ub_ready and userbot:
        await asyncio.sleep(30)
    print("[UB] \U0001f507 Voice chat monitor berhenti.")



async def _vc_join_raw(chat_id: int, call_id: int, access_hash: int) -> bool:
    """
    Join VC via raw MTProto pyrogram.
    Telegram akan kick userbot setelah ~30 detik — tidak masalah, tugasnya sudah selesai.
    Return True jika berhasil (atau sudah ada di VC), False jika gagal.
    """
    if not userbot:
        return False
    import random as _random
    import json as _json
    from pyrogram.raw import functions as _rf
    from pyrogram.raw.types import InputGroupCall, DataJSON

    ssrc  = _random.randint(1, 0xFFFFFFFF)
    ufrag = "".join(_random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=8))
    pwd   = "".join(_random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=22))
    params = DataJSON(data=_json.dumps({
        "ufrag": ufrag,
        "pwd":   pwd,
        "fingerprints": [],
        "ssrc": ssrc,
    }))
    input_call = InputGroupCall(id=call_id, access_hash=access_hash)
    try:
        await userbot.invoke(
            _rf.phone.JoinGroupCall(
                call=input_call,
                join_as=await userbot.resolve_peer("me"),
                params=params,
                muted=True,
                video_stopped=True,
            )
        )
        print(f"[UB-VC-Join] ✅ Join VC grup {chat_id} berhasil (raw MTProto, ssrc={ssrc})")
        return True
    except FloodWait as fw:
        print(f"[UB-VC-Join] FloodWait {fw.value}s saat join VC grup {chat_id}")
        await asyncio.sleep(fw.value + 1)
        return False
    except Exception as e:
        if _is_session_dead_error(e):
            await _handle_userbot_session_dead(f"{type(e).__name__}: {e}")
            return False
        err_str = str(e).lower()
        if "already" in err_str:
            return True   # Sudah di VC — anggap berhasil
        print(f"[UB-VC-Join] Gagal join VC grup {chat_id}: {e}")
        return False


async def _vc_get_call_info(chat_id: int):
    """
    Ambil (call_id, access_hash) dari GetFullChannel.
    Return (call_id, access_hash) atau (None, None) jika tidak ada VC aktif.
    """
    if not userbot:
        return None, None
    from pyrogram.raw import functions as _rf
    try:
        chat_peer = await userbot.resolve_peer(chat_id)
        full = await userbot.invoke(_rf.channels.GetFullChannel(channel=chat_peer))
        call_obj = getattr(full.full_chat, "call", None)
        if not call_obj:
            return None, None
        call_id     = call_obj.id
        access_hash = getattr(call_obj, "access_hash", None)
        if access_hash:
            _call_id_to_chat[call_id]        = chat_id
            _call_id_to_access_hash[call_id] = access_hash
        return call_id, access_hash
    except FloodWait as fw:
        print(f"[UB-VC] FloodWait {fw.value}s saat GetFullChannel grup {chat_id}")
        await asyncio.sleep(fw.value + 1)
        return None, None
    except Exception as e:
        if _is_session_dead_error(e):
            await _handle_userbot_session_dead(f"{type(e).__name__}: {e}")
            return None, None
        print(f"[UB-VC] Gagal GetFullChannel grup {chat_id}: {e}")
        return None, None


async def _vc_scan_and_enforce(chat_id: int) -> None:
    """
    Satu siklus Security OS untuk satu grup:
      1. Ambil info VC aktif (GetFullChannel)
      2. Join VC via raw MTProto
      3. Ambil semua peserta VC via GetGroupParticipants
      4. Cek bio tiap peserta (cache 1 menit) → mute jika link, unmute jika bersih
      5. Tunggu _VC_SCAN_DURATION detik (sambil handle user baru yg join via UpdateGroupCallParticipants)
      6. Leave VC (Telegram mungkin sudah kick duluan — tidak masalah)

    Semua langkah diproteksi FloodWait. Antar grup ada stagger 10 detik di scheduler.
    Guard _vc_scanning_groups memastikan tidak ada 2 siklus berjalan bersamaan
    untuk grup yang sama (mencegah tumpang tindih antar siklus 30 menit).
    """
    if not userbot or not _ub_ready:
        return

    # ── Guard tumpang tindih: skip jika siklus sebelumnya masih berjalan ────
    if chat_id in _vc_scanning_groups:
        print(f"[UB-VC-Sched] Grup {chat_id}: scan sebelumnya masih berjalan — skip siklus ini.")
        return
    _vc_scanning_groups.add(chat_id)

    try:
        await _vc_scan_and_enforce_impl(chat_id)
    finally:
        _vc_scanning_groups.discard(chat_id)


async def _vc_scan_and_enforce_impl(chat_id: int) -> None:
    """Implementasi inti _vc_scan_and_enforce — dipanggil setelah guard aktif."""
    sec_doc = await _sec_os_get(chat_id)
    if not sec_doc.get("enabled"):
        return
    monitor_id = sec_doc.get("monitor_bot_id", 0)

    print(f"[UB-VC-Sched] Grup {chat_id}: mulai siklus scan VC...")

    # ── 1. Ambil info VC aktif ───────────────────────────────────────────────
    call_id, access_hash = await _vc_get_call_info(chat_id)
    if not call_id or not access_hash:
        print(f"[UB-VC-Sched] Grup {chat_id}: tidak ada VC aktif — skip siklus ini.")
        return

    # ── 2. Join VC ───────────────────────────────────────────────────────────
    ok = await _vc_join_raw(chat_id, call_id, access_hash)
    if not ok:
        print(f"[UB-VC-Sched] Grup {chat_id}: gagal join VC — skip siklus ini.")
        return

    _ub_in_vc_groups.add(chat_id)
    _vc_join_last_ts[chat_id] = time.monotonic()

    # ── 3. Scan peserta saat ini via GetGroupParticipants ────────────────────
    from pyrogram.raw import functions as _rf
    from pyrogram.raw.types import InputGroupCall
    input_call = InputGroupCall(id=call_id, access_hash=access_hash)
    try:
        result = await userbot.invoke(
            _rf.phone.GetGroupParticipants(
                call=input_call,
                ids=[],
                sources=[],
                offset="",
                limit=200,
            )
        )
        participants = getattr(result, "participants", [])
        print(f"[UB-VC-Sched] Grup {chat_id}: {len(participants)} peserta ditemukan di VC.")

        # Ambil daftar admin grup — admin di-skip, tidak di-mute oleh userbot
        admin_ids = await _get_group_admin_ids(chat_id)

        for p in participants:
            peer = getattr(p, "peer", None)
            if peer is None:
                continue
            uid = getattr(peer, "user_id", None)
            if not uid or uid == _ub_self_id:
                continue
            # FIX 4: Skip admin grup — userbot tidak memeriksa atau mute admin
            if uid in admin_ids:
                continue
            # Hanya admin-muted yang dihitung — bukan self-muted atau chat restriction (typing ban)
            _pm  = bool(getattr(p, "muted", False))
            _cs  = bool(getattr(p, "can_self_unmute", True))
            is_muted     = _pm and not _cs       # True hanya jika mic di-mute oleh admin
            # BUG 2: field Telegram API — True jika userbot sendiri yang mute mic user ini
            muted_by_you = bool(getattr(p, "muted_by_you", False))
            key = (chat_id, uid)
            if key in _processing_kick:
                continue
            _processing_kick.add(key)
            call_input = _build_input_group_call(call_id)
            _safe_task(
                _query_monitor_then_kick(
                    chat_id, uid, monitor_id, call_input,
                    is_muted=is_muted, muted_by_you=muted_by_you,
                ),
                tag="scan-enforce",
            )
    except FloodWait as fw:
        print(f"[UB-VC-Sched] FloodWait {fw.value}s saat GetGroupParticipants grup {chat_id}")
        await asyncio.sleep(fw.value + 1)
    except Exception as e:
        print(f"[UB-VC-Sched] Gagal GetGroupParticipants grup {chat_id}: {e}")

    # ── 4. Tunggu sambil handle user baru yang join via UpdateGroupCallParticipants ─
    # Handler _on_vc_update sudah aktif — user baru yang join selama window ini
    # akan otomatis dicek oleh handler tersebut (karena chat_id in _ub_in_vc_groups).
    await asyncio.sleep(_VC_SCAN_DURATION)

    # ── 5. Leave VC (Telegram mungkin sudah kick duluan) ────────────────────
    _ub_in_vc_groups.discard(chat_id)
    print(f"[UB-VC-Sched] Grup {chat_id}: siklus selesai, keluar dari VC.")
    try:
        await userbot.invoke(_rf.phone.LeaveGroupCall(call=input_call, source=0))
    except Exception:
        pass   # Sudah dikick atau tidak di VC — tidak masalah


async def _vc_scheduled_loop() -> None:
    """
    Scheduler utama Security OS:
    Setiap _VC_SCHEDULED_INTERVAL (30 menit), untuk tiap grup yang Security OS-nya aktif
    → jalankan satu siklus _vc_scan_and_enforce.

    Stagger antar grup: 10 detik jeda untuk cegah FloodWait ke Telegram API.
    Siklus pertama dimulai 60 detik setelah startup (beri waktu warmup selesai).
    """
    print("[UB-VC-Sched] ⏰ Scheduler join VC 30 menit aktif.")
    await asyncio.sleep(60)   # beri waktu startup/warmup selesai

    while _ub_ready and userbot:
        db, _, _ = _get_db()
        try:
            docs = await db["security_os"].find({"enabled": True}).to_list(None)
        except Exception:
            await asyncio.sleep(60)
            continue

        if docs:
            print(f"[UB-VC-Sched] Mulai siklus — {len(docs)} grup aktif → antri ke VC worker.")
            for doc in docs:
                if not userbot or not _ub_ready:
                    break
                chat_id = doc.get("chat_id")
                if not chat_id:
                    continue
                # Antri ke worker — worker yang atur jeda antar grup, tidak parallel
                _enqueue_vc_scan(chat_id)
        else:
            print("[UB-VC-Sched] Tidak ada grup aktif — tidur 60 detik.")
            await asyncio.sleep(60)
            continue

        print(f"[UB-VC-Sched] Tidur {_VC_SCHEDULED_INTERVAL // 60} menit hingga siklus berikutnya...")
        await asyncio.sleep(_VC_SCHEDULED_INTERVAL)



async def _resolve_chat_for_call_id(call_id: int) -> int | None:
    """
    Fallback saat _call_id_to_chat tidak punya entri untuk call_id ini
    (warmup gagal/terlewat, atau VC dimulai sebelum warmup selesai).

    Iterasi grup Security OS aktif, GetFullChannel tiap grup, cocokkan
    call.id dengan call_id yang sedang diproses. Sekali ketemu langsung
    return — hasil di-cache oleh caller ke _call_id_to_chat.

    Tidak dipanggil sering: hanya saat terjadi cache-miss pada
    _call_id_to_chat, jadi aman dari segi rate limit (di-throttle
    dengan sleep kecil + FloodWait handling).
    """
    if not userbot:
        return None
    db, _, _ = _get_db()
    try:
        docs = await db["security_os"].find({"enabled": True}).to_list(None)
    except Exception:
        return None

    from pyrogram.raw import functions as _rf
    for doc in docs:
        chat_id = doc.get("chat_id")
        if not chat_id:
            continue
        try:
            chat_peer = await userbot.resolve_peer(chat_id)
            full = await userbot.invoke(_rf.channels.GetFullChannel(channel=chat_peer))
            call_obj = getattr(full.full_chat, "call", None)
            if call_obj and call_obj.id == call_id:
                # BUG FIX: simpan access_hash dari fallback resolve juga
                access_hash = getattr(call_obj, "access_hash", None)
                if access_hash is not None:
                    _call_id_to_access_hash[call_id] = access_hash
                return chat_id
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 1)
        except Exception:
            pass
        await asyncio.sleep(0.5)

    return None


async def _warmup_active_calls() -> None:
    """
    Saat startup, cari grup Security OS aktif yang sudah punya voice chat
    berjalan dan isi _call_id_to_chat agar event pertama langsung dikenali.
    """
    if not userbot:
        return
    db, _, _ = _get_db()
    try:
        docs = await db["security_os"].find({"enabled": True}).to_list(None)
    except Exception:
        return

    from pyrogram.raw import functions as _rf
    for doc in docs:
        chat_id = doc.get("chat_id")
        if not chat_id:
            continue
        try:
            chat_peer = await userbot.resolve_peer(chat_id)
            full = await userbot.invoke(_rf.channels.GetFullChannel(channel=chat_peer))
            call_obj = getattr(full.full_chat, "call", None)
            if call_obj:
                _call_id_to_chat[call_obj.id] = chat_id
                # BUG FIX: simpan access_hash dari GetFullChannel — ini sumber
                # access_hash yang valid untuk InputGroupCall saat warmup.
                access_hash = getattr(call_obj, "access_hash", None)
                if access_hash is not None:
                    _call_id_to_access_hash[call_obj.id] = access_hash
                print(
                    f"[UB-VC] Warmup: grup {chat_id} punya voice chat aktif "
                    f"(call_id={call_obj.id}, access_hash={'✅' if access_hash else '⚠️ tidak ada'})"
                )
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 1)
        except Exception:
            pass
        await asyncio.sleep(2)


async def _leave_vc_for_group_direct(chat_id: int) -> None:
    """
    [INTERNAL] Paksa userbot keluar dari obrolan suara grup ini.
    Dipanggil HANYA oleh _vc_join_queue_worker — jangan panggil langsung.
    Gunakan _enqueue_vc_leave(chat_id) untuk antri permintaan leave.

    Menggunakan phone.LeaveGroupCall (MTProto raw API).
    Jika userbot tidak ada di VC, operasi ini aman (tidak error fatal).
    """
    if not userbot or not _ub_ready:
        return

    from pyrogram.raw import functions as _rf
    from pyrogram.raw.types import InputGroupCall

    try:
        chat_peer = await userbot.resolve_peer(chat_id)
        full = await userbot.invoke(_rf.channels.GetFullChannel(channel=chat_peer))
        call_obj = getattr(full.full_chat, "call", None)
        if not call_obj:
            # Tidak ada VC aktif di grup — tidak perlu leave
            print(f"[UB-VC-Leave] Grup {chat_id}: tidak ada VC aktif — skip leave.")
            return
        call_id     = call_obj.id
        access_hash = getattr(call_obj, "access_hash", None)
        if not access_hash:
            print(f"[UB-VC-Leave] Grup {chat_id}: access_hash tidak tersedia — skip leave.")
            return

        # Dapatkan call_id dari mapping
        _lv_call_id = None
        for _cid, _chid in list(_call_id_to_chat.items()):
            if _chid == chat_id:
                _lv_call_id = _cid
                break
        if _lv_call_id:
            from pyrogram.raw import functions as _rf_lv
            from pyrogram.raw.types import InputGroupCall as _IPC_lv
            _lv_ah = _call_id_to_access_hash.get(_lv_call_id)
            if _lv_ah:
                try:
                    await userbot.invoke(
                        _rf_lv.phone.LeaveGroupCall(
                            call=_IPC_lv(id=_lv_call_id, access_hash=_lv_ah),
                            source=0,
                        )
                    )
                except Exception:
                    pass
        _ub_in_vc_groups.discard(chat_id)
        print(f"[UB-VC-Leave] ✅ Userbot keluar dari VC grup {chat_id} (Security OS dinonaktifkan).")
    except FloodWait as fw:
        print(f"[UB-VC-Leave] FloodWait {fw.value}s saat leave VC grup {chat_id}.")
        await asyncio.sleep(fw.value + 1)
    except Exception as e:
        err_str = str(e).lower()
        if "not_in_call" in err_str or "not in call" in err_str:
            print(f"[UB-VC-Leave] Grup {chat_id}: userbot memang tidak di VC — OK.")
        else:
            print(f"[UB-VC-Leave] Grup {chat_id}: error leave VC — {e}")


async def _join_vc_for_group_direct(chat_id: int) -> None:
    """
    [INTERNAL] Dipanggil saat admin mengaktifkan Security OS.
    Dipanggil HANYA oleh _vc_join_queue_worker — jangan panggil langsung.
    Gunakan _enqueue_vc_join_now(chat_id) untuk antri permintaan join darurat.
    """
    if not userbot or not _ub_ready:
        return
    print(f"[UB-VC-Join] Security OS diaktifkan grup {chat_id} — jalankan siklus scan segera.")
    await _vc_scan_and_enforce(chat_id)


async def _leave_vc_for_group(chat_id: int) -> None:
    """
    Antri permintaan leave VC untuk grup ini ke _vc_join_queue_worker.
    Dieksekusi berurutan — tidak pernah bersamaan dengan join/leave grup lain.
    """
    _enqueue_vc_leave(chat_id)


async def _join_vc_for_group(chat_id: int) -> None:
    """
    Antri permintaan join/scan VC untuk grup ini ke _vc_join_queue_worker.
    Dieksekusi berurutan — tidak pernah bersamaan dengan join/leave grup lain.
    """
    _enqueue_vc_join_now(chat_id)


def _build_input_group_call(call_id: int):
    """
    Bangun InputGroupCall yang valid untuk raw API phone.EditGroupCallParticipant.

    UpdateGroupCallParticipants hanya membawa GroupCallReference (.id saja).
    phone.EditGroupCallParticipant WAJIB menerima InputGroupCall (.id + .access_hash).
    Tanpa access_hash yang benar, Telegram mengembalikan ACCESS_HASH_INVALID.

    access_hash di-cache dari UpdateGroupCall (saat VC mulai) dan dari
    GetFullChannel (saat warmup). Jika tidak ditemukan (cache miss), gunakan 0
    sebagai fallback — beberapa implementasi Pyrogram versi lama toleran terhadap
    ini, tapi idealnya selalu tersedia dari cache.
    """
    from pyrogram.raw.types import InputGroupCall
    access_hash = _call_id_to_access_hash.get(call_id, 0)
    if not access_hash:
        print(
            f"[UB-VC] ⚠️  access_hash untuk call_id={call_id} tidak ditemukan di cache. "
            "Pastikan UpdateGroupCall (VC start) diterima sebelum UpdateGroupCallParticipants."
        )
    return InputGroupCall(id=call_id, access_hash=access_hash)


async def _scan_active_groups() -> None:
    """Stub — arsitektur lama (polling). Tidak dipakai lagi."""
    pass


async def _check_one_group(sec_doc: dict) -> None:
    """Stub — arsitektur lama (polling). Tidak dipakai lagi."""
    pass


# ══════════════════════════════════════════════════════════════════════════════
# KOMUNIKASI USERBOT ↔ BOT PEMANTAU (DI DALAM GRUP)
#
# Mekanisme:
#   1. Userbot mengirim `/checkbio <user_id>` ke bot pemantau DI GRUP ITU SENDIRI
#      via pesan grup (mention bot pemantau agar hanya ia yang merespons)
#   2. Userbot memantau pesan baru di grup, menunggu jawaban dari bot pemantau
#   3. Bot pemantau menjawab: "HAS_LINK" atau "NO_LINK"
#   4. Userbot memproses jawaban
#
# Catatan keamanan:
#   - Pesan /checkbio dikirim sebagai pesan grup biasa (userbot sebagai member).
#   - Bot pemantau HARUS sudah join di grup itu agar bisa menerima & membalas.
#   - Jika bot pemantau tidak ada di grup, tidak ada jawaban → tidak ada eksekusi.
# ══════════════════════════════════════════════════════════════════════════════

async def _query_monitor_then_kick(
    chat_id: int,
    user_id: int,
    monitor_bot_id: int,
    call_input,
    is_muted: bool = False,
    muted_by_you: bool = False,
) -> None:
    """
    Perintahkan bot pemantau cek bio user → mute mic jika ada link, unmute jika bersih.

    ARSITEKTUR DB-DRIVEN (Security OS — BUKAN kick dari grup, hanya mute mic VC):
      Userbot memerintahkan bot pemantau (via force_check_vc_join) untuk
      fetch bio fresh dari Telegram API saat user naik ke voice chat.
      Hasilnya disimpan ke DB dan dikembalikan ke sini.

    Alur (sesuai 4 poin aturan moderasi mic VC):
      1. Tidak dikenali sama sekali / bukan member grup → mute mic langsung,
         terlepas ada link atau tidak (mengabaikan syarat bio).

      2. Member, TAPI bio tidak dapat dilihat bot pemantau — baik karena:
         - monitor_unavailable: bot pemantau grup ini BELUM TERDAFTAR di
           registry sama sekali, ATAU
         - has_link=None: bot pemantau AKTIF tapi gagal resolve bio user ini
           (peer tidak dikenal / privasi / semua fallback gagal)
         → dianggap AMAN: unmute mic jika sebelumnya di-mute userbot, skip
           (tidak ada tindakan) jika mic sudah unmuted.

      3. Member dengan link di bio (has_link=True) → mute mic (via _execute_kick).

      4. Member VIP → unmute tanpa syarat (ditangani VIP Guard di awal fungsi,
         sebelum poin 1-3 dievaluasi).

      has_link=False (bio bersih, berhasil dibaca, tidak ada link) → sama
      seperti poin 2: unmute mic jika sebelumnya di-mute userbot, abaikan jika
      muted oleh admin lain.

    Isolasi per grup: chat_id memastikan setiap grup hanya diperiksa
    oleh bot pemantau grup tersebut. Data grup A tidak mencemari grup B.

    BUG 2 FIX — Deteksi "siapa yang mute" dua lapis (dipakai untuk skenario
    monitor_unavailable dan has_link=False, BUKAN untuk has_link=None/stranger):
      1. muted_by_you (bool dari Telegram API GroupCallParticipant.muted_by_you)
         — field langsung dari Telegram, paling andal, tapi hanya ada saat scan/event
      2. _ub_muted_this_user (DB collection vc_muted_by_ub)
         — persisten antar siklus, backup jika field Telegram tidak tersedia
      Unmute dibolehkan jika SALAH SATU dari keduanya True.
      Jika admin lain yang mute (muted_by_you=False AND DB miss) → tidak di-unmute.
    """
    try:
        # ── VIP Guard: cek paling awal — sebelum non-member dan bio check ─────
        # User VIP (free_per_group) bebas dari aturan mute Security OS.
        # TAPI: jika VIP sedang/pernah di-mute oleh userbot (vc_muted_by_ub)
        # → unmute mic VIP tersebut.
        # Ini dipakai saat VIP kirim /unmutemic dan userbot naik VC untuk scan.
        #
        # FIX: jangan gate dengan `is_muted` (snapshot live can_self_unmute dari
        # Telegram) — field ini tidak selalu sinkron tepat setelah userbot baru
        # join VC, sehingga unmute bisa gagal terpicu meski DB sudah mencatat
        # bahwa userbot yang mute user ini. DB vc_muted_by_ub / muted_by_you
        # adalah sumber kebenaran yang lebih andal; is_muted hanya dipakai
        # sebagai info log, bukan syarat wajib.
        if await _is_vip_user(chat_id, user_id):
            was_ub_muted = muted_by_you or await _ub_muted_this_user(chat_id, user_id)
            if was_ub_muted:
                print(
                    f"[UB-VC] uid={user_id} grup={chat_id}: VIP + tercatat di-mute userbot "
                    f"(is_muted_live={is_muted}) → unmute mic VIP."
                )
                _enqueue_unmute_mic(chat_id, user_id, call_input, "VIP — mic diaktifkan kembali")
            elif is_muted:
                print(
                    f"[UB-VC] uid={user_id} grup={chat_id}: VIP muted oleh admin lain "
                    "→ tidak di-unmute userbot."
                )
            else:
                print(f"[UB-VC] uid={user_id} grup={chat_id}: VIP → skip semua cek Security OS.")
            _processing_kick.discard((chat_id, user_id))
            return

        # ── Perubahan 1: Non-member → mute mic langsung tanpa cek bio ────────
        # User yang bukan anggota grup tidak boleh di obrolan suara grup.
        # Mute dilakukan terlepas ada/tidaknya link di bio, lalu dicatat di DB.
        is_member = await _is_group_member(chat_id, user_id)
        if is_member is False:
            reason_nm = "non-member grup naik ke obrolan suara"
            print(
                f"[UB-VC] uid={user_id} grup={chat_id}: "
                f"non-member → mute mic langsung (tanpa cek bio)."
            )
            # Invalidasi cache member agar dicek ulang jika kondisi berubah
            _member_cache.pop((chat_id, user_id), None)
            await _execute_kick(
                chat_id, user_id, call_input,
                was_already_muted=is_muted,
                reason=reason_nm,
            )
            # Fitur 1: Catat ke secos_muted_users (TTL 30 detik)
            _secos_record_mute(chat_id, user_id, "non_member")
            # Fitur 2: Jadwalkan follow-up recheck 1 menit kemudian
            _secos_schedule_followup(chat_id, [(user_id, "non_member")])
            return

        has_link, monitor_unavailable = await _query_bio_from_db(chat_id, user_id)

        # Cache hanya hasil definitif True/False, dan HANYA jika benar-benar
        # hasil cek bio (bukan dari monitor_unavailable — itu bukan hasil cek,
        # harus selalu dicek ulang begitu bot pemantau terdaftar).
        if not monitor_unavailable:
            if has_link is True:
                _bio_cache[(chat_id, user_id)] = (True, time.monotonic())
            elif has_link is False:
                _bio_cache[(chat_id, user_id)] = (False, time.monotonic())

        if monitor_unavailable:
            # ── Skenario 1: bot pemantau grup ini BELUM TERDAFTAR di registry ──
            # Tidak ada cara untuk memverifikasi bio sama sekali — ini bukan
            # kesalahan/kecurigaan terhadap user, melainkan keterbatasan sistem.
            # Diperlakukan sebagai NO LINK (sama seperti bio bersih):
            #   mic muted (tercatat di-mute userbot) → unmute.
            #   mic sudah unmuted                    → skip, tidak ada tindakan.
            _processing_kick.discard((chat_id, user_id))
            was_ub_muted = muted_by_you or await _ub_muted_this_user(chat_id, user_id)
            if was_ub_muted:
                print(
                    f"[UB-VC] uid={user_id} grup={chat_id}: bio tidak tersedia dari bot "
                    f"pemantau (belum terdaftar) — dianggap no link → unmute mic."
                )
                _enqueue_unmute_mic(
                    chat_id, user_id, call_input,
                    "bio tidak tersedia dari bot pemantau — dianggap no link",
                )
            elif is_muted:
                print(
                    f"[UB-VC] uid={user_id} grup={chat_id}: muted oleh admin lain — "
                    "userbot tidak membuka mute mic (bot pemantau belum terdaftar)."
                )
            else:
                print(
                    f"[UB-VC] uid={user_id} grup={chat_id}: bio tidak tersedia dari bot "
                    f"pemantau (belum terdaftar) — dianggap no link, mic sudah unmuted, skip."
                )
                # Tidak di-mute live DAN tidak ada record DB → bersihkan
                # record stale jika ada.
                _safe_task(_remove_ub_muted(chat_id, user_id), tag="rm-muted-stale")
            return

        if has_link is True:
            await _execute_kick(
                chat_id, user_id, call_input,
                was_already_muted=is_muted,
                reason="bio mengandung link",
            )
            # has_link=True dari bot pemantau = bio berhasil dibaca & ada link.
            # Ini BUKAN peer_invalid — tidak perlu follow-up khusus.
        elif has_link is None:
            # ── Golongan 1: member TAPI benar-benar TIDAK DIKENALI bot
            # pemantau — semua fallback fetch bio gagal total (peer tidak
            # bisa di-resolve sama sekali). Ini BUKAN bio kosong/privasi
            # (itu Golongan 2, lihat has_link=False di bawah — _fetch_bio
            # mengembalikan string kosong "" untuk privasi/kosong, BUKAN None).
            # Golongan 1 → MUTE mic, mengabaikan status mute sebelumnya.
            print(
                f"[UB-VC] uid={user_id} grup={chat_id}: tidak dikenali sama sekali "
                f"oleh bot pemantau (semua fallback gagal resolve peer) → mute mic."
            )
            await _execute_kick(
                chat_id, user_id, call_input,
                was_already_muted=is_muted,
                reason="tidak dikenali bot pemantau (peer tidak dapat di-resolve)",
            )
            _secos_record_mute(chat_id, user_id, "peer_invalid")
            _secos_schedule_followup(chat_id, [(user_id, "peer_invalid")])
        else:
            # has_link = False → bio bersih, tidak ada link
            _processing_kick.discard((chat_id, user_id))

            # FIX: jangan gate cek unmute dengan `is_muted` (snapshot live
            # can_self_unmute dari Telegram) — field ini tidak selalu sinkron
            # tepat setelah userbot baru join VC, sehingga unmute via
            # /unmutemic bisa gagal terpicu meski DB vc_muted_by_ub sudah
            # mencatat userbot yang mute user ini. Cek DB/muted_by_you dulu
            # sebagai sumber kebenaran utama; is_muted hanya info pendukung.
            was_ub_muted = muted_by_you or await _ub_muted_this_user(chat_id, user_id)
            if was_ub_muted:
                src_label = "Telegram API" if muted_by_you else "DB record"
                print(
                    f"[UB-Unmute] uid={user_id} grup={chat_id}: "
                    f"bio bersih ({src_label}, is_muted_live={is_muted}) → antri unmute mic ke worker."
                )
                # Antri unmute ke worker (bukan panggil langsung — aman API per grup)
                _enqueue_unmute_mic(chat_id, user_id, call_input, "bio bersih")
            elif is_muted:
                print(
                    f"[UB-Unmute] uid={user_id} grup={chat_id}: "
                    "muted oleh admin lain — userbot tidak membuka mute mic"
                )
            else:
                # Tidak di-mute live DAN tidak ada record DB → bersihkan
                # record stale jika ada (misal: admin sudah unmute duluan,
                # record userbot belum terhapus).
                _safe_task(_remove_ub_muted(chat_id, user_id), tag="rm-muted-stale")

    except Exception as e:
        print(f"[UB-Query] Error uid={user_id} chat={chat_id}: {e}")
        _processing_kick.discard((chat_id, user_id))


async def _query_bio_from_db(chat_id: int, user_id: int) -> tuple[bool | None, bool]:
    """
    Perintahkan bot pemantau cek bio user secara fresh saat naik ke VC.

    ALUR:
      Selalu panggil force_check_vc_join() → bot pemantau fetch bio fresh
      dari Telegram API → simpan ke DB → kembalikan hasilnya.

      force_check_vc_join sudah punya cache internal 60 detik (VC_JOIN_RECHECK_SECS):
        • Jika user naik VC lagi dalam 60 detik → pakai cache, tidak spam API.
        • Setelah 60 detik → fetch fresh dari Telegram API.

      Data lama di DB TIDAK dipakai langsung — userbot selalu tunggu konfirmasi
      fresh dari bot pemantau sebelum memutuskan mute/unmute.

    Return: (has_link, monitor_unavailable)
      has_link:
        True  → ada link di bio (data fresh dari bot pemantau)
        False → tidak ada link di bio (data fresh dari bot pemantau), ATAU
                 bot pemantau grup ini belum terdaftar (lihat monitor_unavailable
                 — has_link dipaksa False sebagai default aman)
        None  → bot pemantau AKTIF tapi gagal resolve peer user ini sepenuhnya
                 (semua fallback gagal / peer belum dikenal bot / belum ada
                 di DB / FloodWait) → TIDAK bertindak seolah no-link, dianggap
                 orang asing oleh pemanggil.
      monitor_unavailable:
        True  → instance bot pemantau untuk grup ini TIDAK ADA sama sekali di
                 registry (belum terdaftar / belum disetup) — ini keterbatasan
                 sistem, bukan kesalahan/kecurigaan terhadap user.
        False → instance ada (baik berhasil fetch atau gagal resolve peer).
    """
    from monitor_bot_reference import force_check_vc_join, _active_instances
    # FIX 5: Isolasi per grup — HANYA gunakan bot pemantau milik chat_id ini.
    # force_check_vc_join(chat_id, user_id) membaca _active_instances[chat_id],
    # sehingga data grup A tidak pernah dicek oleh bot pemantau grup B.
    instance = _active_instances.get(chat_id)
    if instance is None:
        print(
            f"[UB-Bio] chat={chat_id} uid={user_id} "
            "bio tidak tersedia dari bot pemantau (belum terdaftar di registry) "
            "— dianggap no link"
        )
        return False, True
    # ── Instance ada → minta fresh check dari bot pemantau GRUP INI saja ─────
    result = await force_check_vc_join(chat_id, user_id)
    if result is None:
        # None dari force_check_vc_join = bot AKTIF tapi bio tidak tersedia
        # (semua fallback gagal / peer belum dikenal bot / belum ada di DB /
        # FloodWait). Ini BUKAN "instance mati" — jangan log menyesatkan.
        print(
            f"[UB-Bio] chat={chat_id} uid={user_id} "
            "bio tidak tersedia (peer belum dikenal bot / belum ada di DB) — skip"
        )
    else:
        print(
            f"[UB-Bio] chat={chat_id} uid={user_id} "
            f"has_link={result} (fresh dari bot pemantau)"
        )

    # ── Bio Admin Wajib (NewsCore) — independen dari hasil has_link di atas.
    # force_check_vc_join (lewat check_and_save_vc → check_and_save) sudah
    # menulis field admin_bio_ok ke bio_profiles untuk user ini jika relevan.
    # Tidak mempengaruhi mute/unmute mic VC sama sekali — hanya unadmin.
    try:
        from monitor_bot_reference import query_admin_bio_ok
        admin_bio_ok = await query_admin_bio_ok(chat_id, user_id)
        if admin_bio_ok is False and _bot_ref:
            from core.ns_bio_guard import enforce_admin_bio
            _safe_task(
                enforce_admin_bio(_bot_ref, chat_id, user_id, admin_bio_ok),
                tag="ns-bio-guard-vc",
            )
    except Exception as e:
        print(f"[NS-BioGuard] gagal cek admin_bio_ok via VC chat={chat_id} uid={user_id}: {e}")

    return result, False


# ── _get_monitor_username dipertahankan untuk kebutuhan setup_monitor_bot ─────
# (tidak dipakai lagi untuk checkbio, tapi masih dipakai di panel Security OS)

# ══════════════════════════════════════════════════════════════════════════════
# SECOS MUTE HELPERS — Fitur 1: Pencatatan & pembacaan mute khusus keamanan
# ══════════════════════════════════════════════════════════════════════════════

def _secos_record_mute(chat_id: int, user_id: int, reason_type: str) -> None:
    """
    Catat mute Security OS khusus ke cache 30 detik.

    reason_type:
      "non_member"   — user bukan anggota grup tapi naik VC
      "peer_invalid" — user member grup tapi bot pemantau gagal fetch bio
                       (user belum pernah berinteraksi di grup → PeerIdInvalid)

    Hanya dua kondisi ini yang memicu follow-up recheck (Fitur 2 & 3).
    Mute biasa karena has_link=True TIDAK dicatat di sini.
    """
    _secos_muted_users[(chat_id, user_id)] = (reason_type, time.monotonic())
    print(
        f"[SecOS-Mute] uid={user_id} grup={chat_id}: "
        f"dicatat sebagai '{reason_type}' (TTL {_SECOS_MUTE_CACHE_TTL:.0f}s)"
    )


def _secos_get_mute_reason(chat_id: int, user_id: int) -> str | None:
    """
    Cek apakah user masih tercatat dalam mute Security OS (dalam 30 detik).

    Return:
      "non_member" | "peer_invalid" — jika masih dalam TTL 30 detik
      None                          — jika sudah lewat 30 detik atau tidak ada
    """
    entry = _secos_muted_users.get((chat_id, user_id))
    if entry is None:
        return None
    reason_type, ts = entry
    if time.monotonic() - ts <= _SECOS_MUTE_CACHE_TTL:
        return reason_type
    # Expired — hapus otomatis
    _secos_muted_users.pop((chat_id, user_id), None)
    return None


def _secos_clear_mute(chat_id: int, user_id: int) -> None:
    """Hapus catatan mute Security OS untuk user ini (status sudah berubah jadi valid)."""
    _secos_muted_users.pop((chat_id, user_id), None)


# ══════════════════════════════════════════════════════════════════════════════
# SECOS FOLLOW-UP RECHECK — Fitur 2, 3, 4
# ══════════════════════════════════════════════════════════════════════════════

async def _secos_followup_recheck(chat_id: int, muted_users: list[tuple[int, str]]) -> None:
    """
    Follow-up recheck Security OS setelah mute non-member atau peer-invalid.

    Fitur 2: Tunggu 1 menit → naik VC cek ulang (di luar jadwal 30 menit rutin).
    Fitur 3: Jika user yang sama masih belum valid → tunggu 5 menit → naik lagi.
    Fitur 4: Setelah itu kembali ke jadwal 30 menit normal (tidak perlu aksi,
             _vc_scheduled_loop tetap berjalan sendiri).

    muted_users: list of (user_id, reason_type) yang baru di-mute
    """
    if not userbot or not _ub_ready:
        _secos_followup_tasks.pop(chat_id, None)
        return

    print(
        f"[SecOS-FollowUp] Grup {chat_id}: menunggu 1 menit sebelum cek ulang "
        f"({len(muted_users)} user: {[u for u, _ in muted_users]})"
    )

    # ── Fitur 2: Tunggu 1 menit ──────────────────────────────────────────────
    await asyncio.sleep(60)

    if not userbot or not _ub_ready:
        _secos_followup_tasks.pop(chat_id, None)
        return

    sec_doc = await _sec_os_get(chat_id)
    if not sec_doc.get("enabled"):
        _secos_followup_tasks.pop(chat_id, None)
        return

    print(f"[SecOS-FollowUp] Grup {chat_id}: 1 menit berlalu — antri scan VC ke worker.")

    # Invalidasi cache member & bio semua user yang di-mute agar cek fresh
    for uid, _ in muted_users:
        _member_cache.pop((chat_id, uid), None)
        _bio_cache.pop((chat_id, uid), None)

    # Antri scan ke worker (bukan langsung — aman API, tidak bentrok dengan siklus lain)
    _enqueue_vc_scan(chat_id)
    # Tunggu sebentar agar worker sempat proses scan sebelum kita cek hasilnya
    await asyncio.sleep(_VC_WORKER_JOIN_DELAY + _VC_SCAN_DURATION + 15)   # jeda join + estimasi durasi scan + buffer

    # ── Periksa siapa yang masih belum valid setelah cek 1 menit ─────────────
    # Catatan: hanya reason_type "non_member" yang bisa masuk sini sekarang.
    # "peer_invalid" tidak lagi di-mute (lihat poin 2 spek moderasi mic VC —
    # member dengan bio tidak terbaca dianggap aman/unmute), sehingga tidak
    # pernah lagi tercatat via _secos_record_mute dengan reason ini.
    still_invalid: list[tuple[int, str]] = []
    for uid, reason_type in muted_users:
        if reason_type == "non_member":
            # Cek apakah sudah bergabung jadi member
            is_mem = await _is_group_member(chat_id, uid)
            if is_mem is True:
                print(f"[SecOS-FollowUp] uid={uid} grup={chat_id}: sudah jadi member ✓ — clear.")
                _secos_clear_mute(chat_id, uid)
            else:
                still_invalid.append((uid, reason_type))
                print(f"[SecOS-FollowUp] uid={uid} grup={chat_id}: masih non-member setelah 1 menit.")

    if not still_invalid:
        print(
            f"[SecOS-FollowUp] Grup {chat_id}: semua user sudah valid — "
            f"kembali ke jadwal 30 menit normal. ✓"
        )
        _secos_followup_tasks.pop(chat_id, None)
        return

    # ── Fitur 3: Ada yang masih invalid → tunggu 5 menit, naik lagi ─────────
    print(
        f"[SecOS-FollowUp] Grup {chat_id}: {len(still_invalid)} user masih invalid "
        f"({[u for u, _ in still_invalid]}) — menunggu 5 menit sebelum cek terakhir."
    )
    await asyncio.sleep(5 * 60)

    if not userbot or not _ub_ready:
        _secos_followup_tasks.pop(chat_id, None)
        return

    sec_doc = await _sec_os_get(chat_id)
    if not sec_doc.get("enabled"):
        _secos_followup_tasks.pop(chat_id, None)
        return

    print(f"[SecOS-FollowUp] Grup {chat_id}: 5 menit berlalu — antri scan VC terakhir ke worker.")

    # Invalidasi cache lagi sebelum scan ulang
    for uid, _ in still_invalid:
        _member_cache.pop((chat_id, uid), None)
        _bio_cache.pop((chat_id, uid), None)

    # Antri scan ke worker untuk cek terakhir
    _enqueue_vc_scan(chat_id)
    # Tunggu agar worker sempat proses sebelum follow-up task selesai
    await asyncio.sleep(_VC_WORKER_JOIN_DELAY + _VC_SCAN_DURATION + 15)   # jeda join + estimasi durasi scan + buffer

    # ── Fitur 4: Selesai — kembali ke jadwal 30 menit normal ─────────────────
    # _vc_scheduled_loop tetap berjalan sendiri, tidak perlu tindakan tambahan.
    print(
        f"[SecOS-FollowUp] Grup {chat_id}: follow-up selesai — "
        f"kembali ke jadwal 30 menit normal. ✓"
    )
    _secos_followup_tasks.pop(chat_id, None)


def _secos_schedule_followup(chat_id: int, muted_users: list[tuple[int, str]]) -> None:
    """
    Jadwalkan follow-up recheck Security OS untuk grup ini.

    Jika sudah ada follow-up task yang aktif untuk grup ini, tambahkan
    user baru ke dalam follow-up yang sudah berjalan dengan meluncurkan
    task terpisah (karena task yang berjalan sudah punya snapshot muted_users-nya).

    Dalam praktik, skenario overlap sangat jarang (mute terjadi dalam hitungan
    detik satu sama lain). Task lama yang sudah sleep(60) tidak terpengaruh.

    muted_users: list of (user_id, reason_type) yang baru di-mute
    """
    if not muted_users:
        return

    existing = _secos_followup_tasks.get(chat_id)
    if existing and not existing.done():
        # Sudah ada follow-up berjalan — spawn task terpisah untuk batch baru
        # agar tidak kehilangan user yang di-mute belakangan.
        # Simpan ke dict dengan key turunan agar cache_cleanup bisa membersihkannya.
        print(
            f"[SecOS-FollowUp] Grup {chat_id}: sudah ada follow-up berjalan, "
            f"spawn task tambahan untuk {[u for u, _ in muted_users]}."
        )
        extra_key = f"{chat_id}_extra"
        extra_task = _safe_task(
            _secos_followup_recheck(chat_id, muted_users),
            tag=f"secos-followup-extra-{chat_id}",
        )
        # Overwrite slot extra — hanya butuh satu extra task per grup sekaligus
        _secos_followup_tasks[extra_key] = extra_task  # type: ignore[assignment]
        return

    task = _safe_task(
        _secos_followup_recheck(chat_id, muted_users),
        tag=f"secos-followup-{chat_id}",
    )
    _secos_followup_tasks[chat_id] = task
    print(
        f"[SecOS-FollowUp] Grup {chat_id}: follow-up task di-spawn untuk "
        f"{len(muted_users)} user ({[u for u, _ in muted_users]})."
    )


_monitor_username_cache: dict[int, str] = {}


# ══════════════════════════════════════════════════════════════════════════════
# LOG OS — kirim log mute/unmute userbot ke channel khusus LOG_OS
# ══════════════════════════════════════════════════════════════════════════════

async def _log_os_action(chat_id: int, user_id: int, action: str, reason: str) -> None:
    """
    Kirim log tindakan userbot (mute/unmute mic) ke channel LOG_OS.

    action : label singkat, contoh "MUTE-MIC" atau "UNMUTE-MIC"
    reason : keterangan detail, contoh "bio mengandung link" atau "non-member grup"
    """
    if not LOG_OS or not _bot_ref:
        return
    try:
        name  = str(user_id)
        uname = f"id:{user_id}"
        try:
            u = await _bot_ref.get_users(user_id)
            name  = u.first_name or str(user_id)
            uname = f"@{u.username}" if u.username else f"id:{user_id}"
        except Exception:
            pass

        icon   = "🔇" if "MUTE" in action.upper() and "UNMUTE" not in action.upper() else "🔊"
        waktu  = _dt_vc.now(_WIB_VC).strftime("%H:%M:%S · %d %b %Y WIB")
        text = (
            f"{icon} <b>Security OS — {action}</b>\n"
            f"<code>Grup : {chat_id}</code>\n"
            f"👤 {name} (<code>{user_id}</code>) {uname}\n"
            f"📌 Alasan : {reason}\n"
            f"🕐 {waktu}"
        )
        await _bot_ref.send_message(LOG_OS, text, parse_mode=ParseMode.HTML)
    except FloodWait as fw:
        await asyncio.sleep(fw.value + 1)
    except Exception as e:
        print(f"[UB-LogOS] Gagal kirim log ke LOG_OS: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# CEK KEANGGOTAAN GRUP — untuk mute non-member di obrolan suara
# ══════════════════════════════════════════════════════════════════════════════

async def _is_group_member(chat_id: int, user_id: int) -> bool | None:
    """
    Cek apakah user adalah anggota grup.

    Return:
      True  → user adalah anggota grup (owner/admin/member/restricted)
      False → user bukan anggota (LEFT/BANNED atau UserNotParticipant)
      None  → tidak bisa menentukan (error lain, FloodWait, dsb)

    Hasil di-cache 2 menit.
    API call di-throttle via _get_api_semaphore() agar aman di banyak grup.
    """
    if not userbot:
        return None

    key = (chat_id, user_id)
    cached = _member_cache.get(key)
    if cached:
        is_mem, ts = cached
        if time.monotonic() - ts < _MEMBER_CACHE_TTL:
            return is_mem

    # Gunakan semaphore global untuk batasi concurrent API call (maks 3 paralel)
    sem = _get_api_semaphore()
    try:
        from pyrogram.enums import ChatMemberStatus
        async with sem:
            member = await userbot.get_chat_member(chat_id, user_id)
        is_member = member.status not in (
            ChatMemberStatus.BANNED,
            ChatMemberStatus.LEFT,
        )
        _member_cache[key] = (is_member, time.monotonic())
        return is_member
    except FloodWait as fw:
        wait = fw.value + 1
        print(f"[UB-Member] FloodWait {fw.value}s saat cek member uid={user_id} grup={chat_id} — tunggu {wait}s")
        await asyncio.sleep(wait)
        return None   # Tidak retry — caller akan skip atau cek ulang di siklus berikutnya
    except Exception as e:
        err = str(e).lower()
        if "user_not_participant" in err or "not_participant" in err or "member_not_found" in err:
            _member_cache[key] = (False, time.monotonic())
            return False
        # Error lain (peer tidak dikenal, dsb) → tidak bisa menentukan
        return None


async def _is_vip_user(chat_id: int, user_id: int) -> bool:
    """
    Cek apakah user adalah Member VIP di grup ini.

    VIP = ada di collection free_per_group {user_id, chat_id}.
    User VIP BEBAS dari semua tindakan Security OS (mute mic non-member dan bio-link).

    Return True jika VIP, False jika bukan (termasuk saat error — aman ke bawah).
    Hasil di-cache 3 menit untuk performa.
    """
    key = (chat_id, user_id)
    cached = _vip_cache.get(key)
    if cached:
        is_vip, ts = cached
        if time.monotonic() - ts < _VIP_CACHE_TTL:
            return is_vip

    try:
        db, _, _ = _get_db()
        doc = await db["free_per_group"].find_one(
            {"user_id": user_id, "chat_id": chat_id},
        )
        is_vip = doc is not None
        _vip_cache[key] = (is_vip, time.monotonic())
        return is_vip
    except Exception as e:
        print(f"[UB-VIP] Gagal cek VIP uid={user_id} grup={chat_id}: {e}")
        return False   # safe default: anggap bukan VIP jika cek gagal


def invalidate_vip_cache(chat_id: int, user_id: int) -> None:
    """
    Hapus entri cache VIP untuk (chat_id, user_id) ini.

    WAJIB dipanggil setelah /vip, /unvip, atau tombol UI VIP/unvip mengubah
    status VIP di DB (free_per_group). Tanpa ini, _is_vip_user() bisa
    mengembalikan status VIP yang sudah basi selama TTL cache (3 menit) belum
    habis — contoh kasus nyata: user baru di-VIP-kan lalu langsung /unmutemic
    dalam window 3 menit tersebut, bot masih membaca cache lama "bukan VIP"
    sehingga /unmutemic salah jalur (cek bio dulu) dan userbot tidak/lambat
    naik ke voice chat.

    Aman dipanggil meski entri belum ada di cache (no-op).
    """
    _vip_cache.pop((chat_id, user_id), None)


async def _cache_cleanup_loop() -> None:
    """
    Bersihkan entri cache kedaluwarsa setiap 10 menit — cegah memory leak.

    Cache yang dibersihkan:
      - _bio_cache        : TTL 60 detik (bersihkan entri > 3x TTL)
      - _member_cache     : TTL 2 menit
      - _vip_cache        : TTL 3 menit
      - _admin_cache      : TTL 5 menit
      - _warn_workers     : hapus referensi task yang sudah selesai
      - _processing_kick  : log jika terlalu besar (kemungkinan stuck)

    Loop ini berjalan selama userbot aktif (_ub_ready=True).
    """
    _CLEANUP_INTERVAL = 600  # 10 menit
    while _ub_ready and userbot:
        await asyncio.sleep(_CLEANUP_INTERVAL)
        if not _ub_ready:
            break
        now = time.monotonic()
        cleaned = 0

        # Bio cache — entri yang sudah 3× TTL pasti tidak akan dipakai lagi
        stale = [k for k, (_, ts) in list(_bio_cache.items()) if now - ts > _BIO_CACHE_TTL * 3]
        for k in stale:
            _bio_cache.pop(k, None)
        cleaned += len(stale)

        # Member cache
        stale = [k for k, (_, ts) in list(_member_cache.items()) if now - ts > _MEMBER_CACHE_TTL * 3]
        for k in stale:
            _member_cache.pop(k, None)
        cleaned += len(stale)

        # VIP cache
        stale = [k for k, (_, ts) in list(_vip_cache.items()) if now - ts > _VIP_CACHE_TTL * 3]
        for k in stale:
            _vip_cache.pop(k, None)
        cleaned += len(stale)

        # Admin cache
        stale = [cid for cid, (_, ts) in list(_admin_cache.items()) if now - ts > _ADMIN_CACHE_TTL * 3]
        for cid in stale:
            _admin_cache.pop(cid, None)
        cleaned += len(stale)

        # Warn workers: bersihkan referensi task yang sudah selesai
        done_workers = [cid for cid, t in list(_warn_workers.items()) if t.done()]
        for cid in done_workers:
            _warn_workers.pop(cid, None)

        # SecOS muted users cache — bersihkan entri yang sudah > TTL 30 detik
        now_mono = time.monotonic()
        stale_secos = [
            k for k, (_, ts) in list(_secos_muted_users.items())
            if now_mono - ts > _SECOS_MUTE_CACHE_TTL * 3
        ]
        for k in stale_secos:
            _secos_muted_users.pop(k, None)
        cleaned += len(stale_secos)

        # SecOS follow-up tasks — bersihkan referensi task yang sudah selesai
        done_followup = [cid for cid, t in list(_secos_followup_tasks.items()) if t.done()]
        for cid in done_followup:
            _secos_followup_tasks.pop(cid, None)

        # Mic workers — bersihkan referensi task yang sudah selesai
        done_mic = [cid for cid, t in list(_mic_workers.items()) if t.done()]
        for cid in done_mic:
            _mic_workers.pop(cid, None)

        # Mic pending — bersihkan entri yang mungkin stuck (worker crash tanpa finally)
        # Normalnya _mic_pending dibersihkan di finally worker, ini safety net.
        if len(_mic_pending) > 50:
            print(f"[UB-Cache] ⚠️ _mic_pending: {len(_mic_pending)} entri — kemungkinan ada yang stuck, reset.")
            _mic_pending.clear()

        # VC join pending — bersihkan jika terlalu besar (worker restart atau crash)
        if len(_vc_join_pending) > 20:
            print(f"[UB-Cache] ⚠️ _vc_join_pending: {len(_vc_join_pending)} entri — reset.")
            _vc_join_pending.clear()

        # Processing kick safety: clear entri yang stuck (guard > 30 entri)
        stuck_count = len(_processing_kick)
        if stuck_count > 30:
            print(f"[UB-Cache] ⚠️ _processing_kick: {stuck_count} entri stuck — force clear untuk cegah blokir permanen.")
            _processing_kick.clear()

        total = (
            len(_bio_cache) + len(_member_cache) + len(_vip_cache) +
            len(_admin_cache) + len(_processing_kick)
        )
        if cleaned > 0:
            print(f"[UB-Cache] Cleanup: {cleaned} entri dihapus — total cache aktif: {total}")


async def _get_monitor_username(monitor_bot_id: int) -> str:
    """Ambil username bot pemantau (cache di memory). Masih dipakai di panel UI."""
    if monitor_bot_id in _monitor_username_cache:
        return _monitor_username_cache[monitor_bot_id]
    try:
        if userbot:
            user = await userbot.get_users(monitor_bot_id)
            uname = user.username or str(monitor_bot_id)
        else:
            uname = str(monitor_bot_id)
    except Exception:
        uname = str(monitor_bot_id)
    _monitor_username_cache[monitor_bot_id] = uname
    return uname


# ══════════════════════════════════════════════════════════════════════════════
# EKSEKUSI: MUTE MIC DI VOICE CHAT + PERINGATAN
# (Security OS: BUKAN kick dari grup — hanya mute mic VC)
# ══════════════════════════════════════════════════════════════════════════════

async def _execute_kick(
    chat_id: int,
    user_id: int,
    call_input,
    was_already_muted: bool = False,
    reason: str = "bio mengandung link",
) -> None:
    """
    Mute mic user dari voice chat, lalu antrekan peringatan ke grup.

    was_already_muted=True berarti user SUDAH di-mute sebelum userbot bertindak.
    Dalam kasus ini: SKIP sepenuhnya — tidak mute ulang, tidak LOG_OS, tidak notif.
    LOG_OS dan notif hanya dikirim untuk PERUBAHAN STATUS NYATA (unmuted → muted).

    reason: alasan mute — diteruskan ke _do_send_warning dan LOG_OS.

    Alur (dengan Mic Worker Queue):
      1. Jika was_already_muted=True → skip seluruhnya (tidak ada perubahan status)
      2. Antri mute mic ke _mic_action_worker → _kick_from_voice
         → Jika API return sudah-muted (GROUP_CALL_NOT_MODIFIED) → skip LOG_OS
      3. LOG_OS hanya jika benar-benar terjadi perubahan status (unmuted → muted)
      4. Catat ke DB (vc_muted_by_ub)
      5. Antrekan notifikasi grup hanya jika perubahan status nyata
    """
    try:
        # ── VIP Guard — cek sebelum APAPUN ────────────────────────────────────
        if await _is_vip_user(chat_id, user_id):
            print(f"[UB-Exec] uid={user_id} grup={chat_id}: VIP → skip mute mic Security OS.")
            return

        # ── Poin 1: Skip jika sudah muted — tidak ada perubahan status ────────
        # was_already_muted=True berarti Telegram sudah melaporkan user ini muted.
        # Mute ulang tidak akan mengubah apapun; LOG_OS dan notif tidak relevan.
        if was_already_muted:
            print(
                f"[UB-Exec] uid={user_id} grup={chat_id}: "
                "sudah muted sebelumnya → skip (tidak mute ulang, tidak LOG_OS, tidak notif)."
            )
            return

        # ── Antri mute mic ke worker — eksekusi berurutan per grup ────────────
        _enqueue_mute_mic(chat_id, user_id, call_input, reason)
        # LOG_OS: perubahan status nyata (unmuted → muted) — dicatat di sini
        _safe_task(_log_os_action(chat_id, user_id, "MUTE-MIC", reason), tag="log-os")
        # Catat ke DB bahwa userbot yang mute-kan user ini
        _safe_task(_record_ub_muted(chat_id, user_id), tag="record-muted")
        # Notifikasi grup: perubahan status nyata
        _pending_warn_reason[(chat_id, user_id)] = reason
        _enqueue_warning(chat_id, user_id)

    except Exception as e:
        print(f"[UB-Exec] Error saat kick uid={user_id} di grup {chat_id}: {e}")
    finally:
        _processing_kick.discard((chat_id, user_id))


async def _kick_from_voice(chat_id: int, user_id: int, call_input) -> None:
    """
    Mute mic user di obrolan suara menggunakan raw API Telegram.

    ── CATATAN PERUBAHAN ────────────────────────────────────────────────────
    Telegram tidak lagi mengizinkan kick paksa dari VC oleh admin/userbot
    (error: VIDEO_STOP_FORBIDDEN). Sebagai gantinya, userbot akan mute mic
    user saja (muted=True) — user masih di VC tapi tidak bisa berbicara.

    Metode API: phone.EditGroupCallParticipant (MTProto)
      • Parameter yang diset: muted=True SAJA.
      • Efek: mic user di-mute paksa — user tidak bisa berbicara di VC.
      • Userbot harus punya izin "Kelola Obrolan Video" (manage_video_chats).
      • Userbot TIDAK perlu berada di dalam VC.

    Setelah mute berhasil, _execute_kick() mengantrekan notifikasi teks
    ke grup via _enqueue_warning() dengan jeda antar pesan.
    """
    if not userbot:
        return
    try:
        from pyrogram.raw import functions as _rf
        peer = await userbot.resolve_peer(user_id)
        await userbot.invoke(
            _rf.phone.EditGroupCallParticipant(
                call=call_input,
                participant=peer,
                muted=True,
            )
        )
        print(f"[UB-VC] ✅ Mic user {user_id} di-mute di voice chat grup {chat_id}")
    except FloodWait as fw:
        print(f"[UB-VC] FloodWait {fw.value}s saat mute mic uid={user_id} — menunggu & retry...")
        await asyncio.sleep(fw.value + 1)
        # Coba sekali lagi setelah FloodWait
        try:
            from pyrogram.raw import functions as _rf2
            peer2 = await userbot.resolve_peer(user_id)
            await userbot.invoke(
                _rf2.phone.EditGroupCallParticipant(
                    call=call_input,
                    participant=peer2,
                    muted=True,
                )
            )
            print(f"[UB-VC] ✅ Retry mute mic uid={user_id} di grup {chat_id} berhasil")
        except Exception as e2:
            print(f"[UB-VC] Retry mute mic uid={user_id} gagal: {e2}")
    except Exception as e:
        print(f"[UB-VC] Gagal mute mic uid={user_id} dari voice chat: {e}")


async def _unmute_user_in_vc(chat_id: int, user_id: int, call_input) -> None:
    """
    Unmute mic user di obrolan suara grup.

    Dipanggil dari _query_monitor_then_kick HANYA jika:
      1. User sedang di-mute (is_muted=True) saat naik VC
      2. Bio sudah bersih (has_link=False)
      3. Userbot yang dulu mute user ini (DB collection vc_muted_by_ub)

    Alur setelah unmute berhasil:
      → Hapus catatan mute dari DB (vc_muted_by_ub)
      → Hapus cache bio user ini
      → Kirim notifikasi ke grup (perubahan status: muted → unmuted)
      → Auto-hapus notifikasi setelah 10 detik

    Jika API mengembalikan GROUP_CALL_NOT_MODIFIED (user sudah unmuted) →
      TIDAK kirim notifikasi (tidak ada perubahan status).

    Userbot harus punya izin "Kelola Obrolan Video" (manage_video_chats).
    """
    if not userbot:
        return

    async def _do_unmute() -> bool:
        """Lakukan unmute via raw API. Return True jika berhasil, False jika user sudah unmuted."""
        from pyrogram.raw import functions as _rf
        peer = await userbot.resolve_peer(user_id)
        try:
            await userbot.invoke(
                _rf.phone.EditGroupCallParticipant(
                    call=call_input,
                    participant=peer,
                    muted=False,
                )
            )
            return True
        except Exception as e:
            err_str = str(e).lower()
            if "not_modified" in err_str or "group_call_not_modified" in err_str:
                # User sudah tidak di-mute — tidak ada perubahan status → skip notif
                print(
                    f"[UB-VC] uid={user_id} grup={chat_id}: "
                    "sudah unmuted sebelumnya — skip notifikasi ke grup"
                )
                return False
            raise   # lempar ke caller untuk penanganan lain

    try:
        changed = await _do_unmute()
        print(
            f"[UB-VC] ✅ Mic user {user_id} di-unmute di obrolan suara grup {chat_id} "
            f"(bio bersih, {'notif dikirim' if changed else 'sudah unmuted sebelumnya'})"
        )

        # Hapus catatan mute userbot dari DB
        _safe_task(_remove_ub_muted(chat_id, user_id), tag="rm-muted-unmute")

        # Hapus cache bio agar status selalu dicek fresh berikutnya
        _bio_cache.pop((chat_id, user_id), None)

        # Perubahan 2: log unmute ke channel LOG_OS
        if changed:
            _safe_task(
                _log_os_action(chat_id, user_id, "UNMUTE-MIC", "bio bersih / tidak ada link"),
                tag="log-os-unmute",
            )

        # Kirim notifikasi ke grup HANYA jika ini perubahan status
        if changed and _bot_ref:
            try:
                u = await _bot_ref.get_users(user_id)
                name = u.first_name or str(user_id)
            except Exception:
                name = str(user_id)
            mention = f"<a href='tg://user?id={user_id}'>{name}</a>"
            notif_text = (
                f"🔊 {mention} mic-nya telah diaktifkan kembali.\n"
                f"<i>Bio sudah tidak mengandung link.</i>"
            )
            try:
                sent = await _bot_ref.send_message(chat_id, notif_text, parse_mode=ParseMode.HTML)
                async def _auto_del(msg=sent):
                    await asyncio.sleep(10)
                    try:
                        await msg.delete()
                    except Exception:
                        pass
                asyncio.create_task(_auto_del())
            except FloodWait as fw:
                print(f"[UB-Unmute] FloodWait {fw.value}s saat kirim notif unmute uid={user_id}")
                await asyncio.sleep(fw.value + 1)
                try:
                    sent = await _bot_ref.send_message(chat_id, notif_text, parse_mode=ParseMode.HTML)
                    async def _auto_del2(msg=sent):
                        await asyncio.sleep(10)
                        try:
                            await msg.delete()
                        except Exception:
                            pass
                    asyncio.create_task(_auto_del2())
                except Exception as e2:
                    print(f"[UB-Unmute] Retry notif unmute uid={user_id} gagal: {e2}")
            except Exception as e:
                print(f"[UB-Unmute] Gagal kirim notif unmute uid={user_id}: {e}")

            # Catat ke log aktivitas grup — perubahan status nyata (muted → unmuted)
            try:
                from database import insert_group_action_log
                await insert_group_action_log(
                    chat_id,
                    "UNMUTE-VC-MIC",
                    "Security OS: mic diaktifkan kembali (bio bersih / tidak ada link)",
                    user_id,
                    name[:50],
                )
            except Exception as _e_log:
                print(f"[UB-Unmute] Gagal catat log unmute uid={user_id}: {_e_log}")

    except FloodWait as fw:
        print(f"[UB-VC] FloodWait {fw.value}s saat unmute uid={user_id} — menunggu & retry...")
        await asyncio.sleep(fw.value + 1)
        try:
            changed = await _do_unmute()
            if changed:
                print(f"[UB-VC] ✅ Retry unmute mic uid={user_id} di grup {chat_id} berhasil")
                _safe_task(_remove_ub_muted(chat_id, user_id), tag="rm-muted-retry")
            _bio_cache.pop((chat_id, user_id), None)
        except Exception as e2:
            print(f"[UB-VC] Retry unmute uid={user_id} gagal: {e2}")
    except Exception as e:
        print(f"[UB-VC] Gagal unmute mic uid={user_id} dari obrolan suara: {e}")


async def _do_send_warning(chat_id: int, user_id: int) -> None:
    """
    Bot biasa mengirim peringatan di grup kepada user yang diturunkan.
    Juga mencatat ke group_action_log (pakai fungsi asli database.py).

    DIPANGGIL OLEH _warn_worker — tidak langsung, selalu via _enqueue_warning().
    FloodWait ditangani di sini: tunggu dan coba ulang sekali.
    """
    if not _bot_ref:
        return
    try:
        from database import insert_group_action_log

        # Ambil nama user
        name = str(user_id)
        try:
            u = await _bot_ref.get_users(user_id)
            name = u.first_name or str(user_id)
        except Exception:
            pass

        mention = f"<a href='tg://user?id={user_id}'>{name}</a>"

        # Kirim peringatan di grup via bot biasa — tangani FloodWait
        # Perubahan 2: ambil alasan dari _pending_warn_reason
        warn_reason = _pending_warn_reason.pop((chat_id, user_id), "bio mengandung link")

        # Tentukan warn_type untuk cek 1× seumur hidup
        if "non-member" in warn_reason:
            warn_type = "vc_nonmember"
        elif "peer tidak dikenal" in warn_reason or "peer_invalid" in warn_reason:
            warn_type = "vc_peer"
        else:
            warn_type = "vc_bio"

        if "non-member" in warn_reason:
            warn_msg = (
                f"🔇 {mention} mic-nya di-mute di obrolan suara.\n"
                f"<i>Anda bukan anggota grup ini. "
                f"Bergabunglah ke grup terlebih dahulu agar mic dapat diaktifkan.</i>"
            )
        elif "peer tidak dikenal" in warn_reason or "peer_invalid" in warn_reason:
            warn_msg = (
                f"🔇 {mention} mic-nya di-mute sementara di obrolan suara.\n"
                f"<i>Profil Anda belum dapat diverifikasi. "
                f"Kirim pesan di grup ini terlebih dahulu agar sistem dapat mengenali Anda, "
                f"kemudian mic akan diaktifkan kembali secara otomatis.</i>"
            )
        else:
            warn_msg = (
                f"🔇 {mention} mic-nya di-mute di obrolan suara.\n"
                f"<i>Bio Anda mengandung link/username. "
                f"Hapus link atau privatkan bio agar mic dapat diaktifkan kembali.</i>"
            )

        # Pemberitahuan 1× seumur hidup per (user, grup, jenis mute VC)
        from database import has_warned_user, mark_warned_user
        already_warned = await has_warned_user(chat_id, user_id, warn_type)

        sent_warn = None
        if not already_warned:
            try:
                sent_warn = await _bot_ref.send_message(chat_id, warn_msg, parse_mode=ParseMode.HTML)
                await mark_warned_user(chat_id, user_id, warn_type)
            except FloodWait as fw_warn:
                print(f"[UB-Warn] FloodWait {fw_warn.value}s saat kirim warn ke grup {chat_id} — menunggu...")
                await asyncio.sleep(fw_warn.value + 1)
                try:
                    sent_warn = await _bot_ref.send_message(chat_id, warn_msg, parse_mode=ParseMode.HTML)
                    await mark_warned_user(chat_id, user_id, warn_type)
                except Exception as e2:
                    print(f"[UB-Warn] Retry warn gagal uid={user_id}: {e2}")

        # Hapus pesan peringatan otomatis setelah 10 detik
        if sent_warn:
            async def _auto_delete_warn(msg=sent_warn):
                await asyncio.sleep(10)
                try:
                    await msg.delete()
                except Exception:
                    pass
            asyncio.create_task(_auto_delete_warn())

        # Catat ke log aktivitas grup (fungsi asli database.py)
        # Tentukan label alasan yang tepat berdasarkan warn_reason
        if "non-member" in warn_reason:
            log_alasan = "Security OS: bukan anggota grup, mic di-mute di obrolan suara"
        elif "peer tidak dikenal" in warn_reason or "peer_invalid" in warn_reason:
            log_alasan = "Security OS: profil belum terverifikasi, mic di-mute sementara"
        else:
            log_alasan = "Security OS: bio mengandung link, mic di-mute di obrolan suara"

        await insert_group_action_log(
            chat_id,
            "MUTE-VC-MIC",
            log_alasan,
            user_id,
            name[:50],
        )

        # Hapus cache bio user ini agar bisa naik lagi setelah benahi bio
        _bio_cache.pop((chat_id, user_id), None)

    except Exception as e:
        print(f"[UB-Warn] Gagal kirim peringatan uid={user_id}: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# SETUP BOT PEMANTAU
# Dipanggil dari handler UI saat admin memasukkan token bot pemantau baru.
# ══════════════════════════════════════════════════════════════════════════════

async def change_userbot(
    new_phone: str,
    bot: _Client,
) -> tuple[bool, str]:
    """
    Ganti akun userbot dengan nomor HP baru.

    ── ALUR ────────────────────────────────────────────────────────────────
    1. Hentikan userbot lama (jika aktif).
    2. Hapus session lama dari disk dan DB.
    3. Tulis USERBOT_PHONE baru ke variabel global dan file .env (jika ada).
    4. Mulai OTP login flow untuk nomor baru — owner kirim /otp <kode> via DM.
    5. Setelah login berhasil, simpan session baru dan aktifkan voice monitor.

    Dipanggil dari handler UI secos_setuserbot_{chat_id} di handlers_secos.py.
    Return: (berhasil: bool, pesan_hasil: str)
    """
    global userbot, _ub_ready, _ub_self_id, USERBOT_PHONE, _ub_dead_notified

    # NOTE: Handler /otp permanen lewat plugin auto-load — tidak perlu
    # dipasang di sini lagi. Lihat plugins/ui/handler_otp.py.

    # ── 1. Validasi format nomor ─────────────────────────────────────────
    clean_phone = new_phone.strip()
    if not _re.match(r"^\+\d{7,15}$", clean_phone):
        return False, (
            "Format nomor tidak valid. Gunakan format internasional, "
            "contoh: <code>+628123456789</code>"
        )

    # ── 2. Hentikan userbot lama ─────────────────────────────────────────
    _ub_ready = False
    if userbot:
        try:
            await userbot.stop()
        except Exception:
            pass
        userbot = None
    _ub_self_id = 0

    # Hapus session lama dari disk
    session_file = _UB_SESSION + ".session"
    try:
        _Path(session_file).unlink(missing_ok=True)
    except Exception:
        pass

    # Hapus session lama dari DB (simpan di bot_config dengan key "ub_session_data")
    try:
        _, _, _ = _get_db()   # pastikan db lazy-init
        from database import save_bot_config as _sbc
        await _sbc("ub_session_data", "")   # kosongkan — bukan delete agar key tetap ada
    except Exception:
        pass

    # ── 3. Set nomor baru ────────────────────────────────────────────────
    USERBOT_PHONE = clean_phone

    # Perbarui .env jika file ada (best-effort)
    # FIX (pemindahan ke folder security_os/): .env tetap di ROOT proyek,
    # bukan di dalam security_os/ tempat file ini sekarang berada.
    env_path = _Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        try:
            env_text = env_path.read_text()
            import re as _re2
            if _re2.search(r"^USERBOT_PHONE\s*=", env_text, _re2.MULTILINE):
                env_text = _re2.sub(
                    r"^(USERBOT_PHONE\s*=).*$",
                    rf"\g<1>{clean_phone}",
                    env_text,
                    flags=_re2.MULTILINE,
                )
            else:
                env_text += f"\nUSERBOT_PHONE={clean_phone}\n"
            env_path.write_text(env_text)
        except Exception as e:
            print(f"[UB-Change] Gagal update .env: {e} (tidak fatal)")

    # ── 4. Login dengan nomor baru ───────────────────────────────────────
    print(f"[UB-Change] 🔄 Ganti userbot → nomor baru: {clean_phone}")
    result = await _do_login(bot)

    if isinstance(result, tuple):
        ok, self_id = result
    else:
        ok, self_id = result, 0

    if not ok or not userbot:
        return False, (
            "Login userbot baru gagal. Pastikan nomor benar dan OTP dikirim "
            "via DM bot dengan format <code>/otp &lt;kode&gt;</code>."
        )

    # ── 5. Aktifkan ──────────────────────────────────────────────────────
    _ub_self_id = self_id
    _ub_ready   = True
    _ub_dead_notified = False   # reset — userbot baru berhasil aktif lagi
    try:
        me = await userbot.get_me()
        uname = me.username or me.first_name or str(me.id)
    except Exception:
        uname = "userbot baru"

    await _log_registered_groups()
    asyncio.ensure_future(_safe_task(_voice_chat_monitor_loop(), tag="vc-monitor-loop"))
    # Pastikan backup periodik session userbot tetap berjalan untuk akun baru
    # ini juga — bukan hanya saat session dipulihkan otomatis di startup().
    _safe_task(_periodic_ub_session_backup(), tag="ub-session-backup")

    print(f"[UB-Change] ✅ Userbot berhasil diganti → @{uname} (id={self_id})")
    return True, (
        f"✅ Userbot berhasil diganti ke <b>@{uname}</b> (id: <code>{self_id}</code>).\n"
        f"Voice chat monitor sudah aktif kembali."
    )


async def setup_monitor_bot(
    chat_id: int,
    token: str,
    inviter_bot: _Client,
) -> tuple[bool, str]:
    """
    Validasi token bot pemantau dan simpan ke DB.
    Bot pemantau TIDAK langsung di-join ke grup — admin menambahkannya manual.
    Saat bot pemantau masuk ke grup, handler on_chat_member_updated akan
    mengenalinya otomatis dari DB.

    Jika grup ini sudah punya bot pemantau LAMA (token berbeda),
    bot lama di-kick dulu dari grup sebelum yang baru disimpan.

    Return: (berhasil: bool, pesan_hasil: str)
    """
    import httpx

    db, _, _ = _get_db()

    # ── 1. Validasi token via Telegram getMe ─────────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=10) as hc:
            resp = await hc.get(f"https://api.telegram.org/bot{token}/getMe")
            data = resp.json()
        if not data.get("ok"):
            desc = data.get("description", "unknown error")
            return False, f"Token tidak valid: {desc}"
        info           = data["result"]
        monitor_bot_id = int(info["id"])
        monitor_uname  = info.get("username", str(monitor_bot_id))
    except Exception as e:
        return False, f"Gagal menghubungi Telegram API: {e}"

    # ── 2. Pastikan bot pemantau belum dipakai grup lain ─────────────────────
    mon_col  = db["security_os_monitors"]
    existing = await mon_col.find_one({"monitor_bot_id": monitor_bot_id})
    if existing:
        existing_chat = int(existing.get("chat_id", 0))
        if existing_chat != chat_id:
            return False, (
                f"Bot @{monitor_uname} sudah terdaftar di grup lain "
                f"(<code>{existing_chat}</code>).\n"
                f"1 bot pemantau hanya boleh digunakan di 1 grup."
            )
        # Bot pemantau sudah terdaftar di grup ini — update saja (token baru)

    # ── 2b. Kick bot pemantau LAMA jika token berbeda ────────────────────────
    old_doc    = await _sec_os_get(chat_id)
    old_mon_id = old_doc.get("monitor_bot_id", 0)
    if old_mon_id and old_mon_id != monitor_bot_id:
        old_uname = _monitor_username_cache.get(old_mon_id, f"id:{old_mon_id}")
        try:
            await inviter_bot.ban_chat_member(chat_id, old_mon_id)
            await asyncio.sleep(1)
            await inviter_bot.unban_chat_member(chat_id, old_mon_id)
            print(f"[SecOS] Bot lama @{old_uname} ({old_mon_id}) di-kick dari grup {chat_id}")
        except Exception as e_kick:
            print(f"[SecOS] Kick bot lama gagal (mungkin sudah tidak ada): {e_kick}")
        # Hapus entri lama dari monitor index
        await mon_col.delete_one({"monitor_bot_id": old_mon_id})
        _monitor_username_cache.pop(old_mon_id, None)

    # ── 3. Simpan ke DB — bot pemantau dikonfigurasi, belum harus join ───────
    await _sec_os_set_monitor(chat_id, token, monitor_bot_id)

    # Index global: 1 bot pemantau → 1 grup
    await mon_col.update_one(
        {"monitor_bot_id": monitor_bot_id},
        {"$set": {"monitor_bot_id": monitor_bot_id, "chat_id": chat_id}},
        upsert=True,
    )

    # Cache username
    _monitor_username_cache[monitor_bot_id] = monitor_uname

    print(f"[SecOS] Bot pemantau @{monitor_uname} ({monitor_bot_id}) dikonfigurasi untuk grup {chat_id}")
    print(f"[SecOS] Menunggu @{monitor_uname} ditambahkan ke grup secara manual...")

    # ── Langsung spawn instance bot pemantau baru ─────────────────────────────
    # Instance ini akan mulai scan berkala setelah bot pemantau join ke grup.
    # Tidak perlu restart proses — instance jalan dalam proses yang sama.
    try:
        from monitor_bot_reference import spawn_monitor_for_group
        _safe_task(
            spawn_monitor_for_group(chat_id, token, monitor_bot_id),
            tag=f"spawn-monitor-{chat_id}",
        )
        print(f"[SecOS] MonitorInstance untuk grup {chat_id} di-spawn.")
    except Exception as e_spawn:
        print(f"[SecOS] Gagal spawn MonitorInstance: {e_spawn}")
        # Tidak fatal — instance akan di-load ulang saat restart proses

    return True, (
        f"Bot @{monitor_uname} berhasil dikonfigurasi.\n"
        f"Sekarang tambahkan <b>@{monitor_uname}</b> ke grup secara manual,\n"
        f"dan bot akan dikenali otomatis saat masuk."
    )


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API — fungsi yang dipanggil dari luar modul ini
# ══════════════════════════════════════════════════════════════════════════════

async def security_os_enable(chat_id: int) -> None:
    """
    Aktifkan Security OS untuk grup ini (per-grup, tidak mempengaruhi grup lain).

    Urutan yang benar:
      1. Simpan enabled=True ke DB
      2. Reset cache bio grup ini
      3. Pastikan userbot member grup ini (tunggu selesai)
      4. Baru join VC grup ini (jika ada VC aktif)
      5. Spawn MonitorInstance untuk grup ini
    """
    await _sec_os_set_enabled(chat_id, True)

    # Reset cache bio grup ini saja
    keys_to_del = [k for k in _bio_cache if k[0] == chat_id]
    for k in keys_to_del:
        _bio_cache.pop(k, None)

    if userbot and _ub_ready:
        _safe_task(_enable_secos_for_group(chat_id), tag=f"enable-secos-{chat_id}")


async def _enable_secos_for_group(chat_id: int) -> None:
    """
    Task sequential per grup saat Security OS diaktifkan:
    join VC dulu → pastikan monitor aktif (spawn hanya jika belum ada).
    Userbot sudah admin grup — tidak perlu join_chat.
    """
    # Join VC grup ini (guard inside akan skip jika sudah di VC)
    await _join_vc_for_group(chat_id)

    # Spawn MonitorInstance hanya jika belum aktif
    try:
        from monitor_bot_reference import spawn_monitor_for_group, _active_instances
        if chat_id in _active_instances:
            print(f"[SecOS] Bot pemantau grup {chat_id} sudah aktif — skip spawn ulang.")
            return
        db, _, _ = _get_db()
        sec_doc = await db["security_os"].find_one({"chat_id": chat_id}) or {}
        token  = sec_doc.get("monitor_token", "").strip()
        bot_id = sec_doc.get("monitor_bot_id", 0)
        if token and bot_id:
            try:
                await asyncio.wait_for(
                    spawn_monitor_for_group(chat_id, token, bot_id),
                    timeout=30.0,
                )
            except asyncio.TimeoutError:
                print(f"[SecOS] Timeout 30s spawn MonitorInstance grup {chat_id} — akan di-load saat restart.")
        else:
            print(f"[SecOS] Grup {chat_id}: belum ada token monitor — bot pemantau belum dikonfigurasi.")
    except Exception as _e_mon:
        print(f"[SecOS] Gagal spawn MonitorInstance grup {chat_id}: {_e_mon}")


async def security_os_disable(chat_id: int) -> None:
    """
    Nonaktifkan Security OS untuk grup ini.

    Userbot dipaksa KELUAR dari obrolan suara agar tidak ada di VC
    saat Security OS tidak aktif (persisten meski redeploy).

    PENTING: bot pemantau (MonitorInstance) TIDAK dihentikan.
    Bot pemantau wajib selalu hidup karena juga dipakai oleh bio.py,
    terlepas dari status Security OS.
    """
    await _sec_os_set_enabled(chat_id, False)

    # ── Paksa userbot turun dari VC via worker (antri — tidak langsung) ──────
    if userbot and _ub_ready:
        _enqueue_vc_leave(chat_id)

    # ── Bot pemantau TIDAK dimatikan — selalu standby (bio.py juga memakainya)
    print(f"[SecOS] Security OS dinonaktifkan grup {chat_id} — bot pemantau tetap aktif.")


async def security_os_get_status(chat_id: int) -> dict:
    """Ambil status Security OS untuk grup. Return dict dokumen DB."""
    return await _sec_os_get(chat_id)


def is_userbot_ready() -> bool:
    """Return True jika userbot sudah login dan siap memantau."""
    return _ub_ready and userbot is not None


async def check_monitor_is_member(client: _Client, chat_id: int) -> bool:
    """
    Cek apakah bot pemantau sudah menjadi anggota (atau admin) di grup.

    Menggunakan bot utama (client) untuk get_chat_member karena userbot mungkin
    tidak selalu ada di grup target.

    Return True jika bot pemantau sudah ada di grup, False jika belum.
    """
    sec_doc = await _sec_os_get(chat_id)
    monitor_bot_id = sec_doc.get("monitor_bot_id", 0)
    if not monitor_bot_id:
        return False

    # Force resolve peer dulu agar sesi bot utama kenal grup ini
    try:
        await client.get_chat(chat_id)
    except Exception:
        pass

    try:
        from pyrogram.enums import ChatMemberStatus
        member = await client.get_chat_member(chat_id, monitor_bot_id)
        return member.status in (
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.OWNER,
        )
    except (PeerIdInvalid, ValueError, KeyError):
        # Peer belum dikenal sesi ini bahkan setelah get_chat — return False (safe)
        print(f"[SecOS] check_monitor_is_member: peer {chat_id} belum dikenal sesi bot — anggap belum join.")
        return False
    except Exception as e:
        # USER_NOT_PARTICIPANT atau error lain → belum jadi anggota
        print(f"[SecOS] check_monitor_is_member error chat={chat_id}: {e}")
        return False


async def check_activation_prerequisites(
    client: _Client,
    chat_id: int,
) -> tuple[bool, list[str]]:
    """
    Periksa syarat wajib sebelum Security OS boleh diaktifkan.

    Syarat WAJIB (memblokir aktivasi):
      1. Userbot sudah online
      2. Bot pemantau sudah dikonfigurasi di DB

    Syarat OPSIONAL (warning saja, tidak memblokir):
      3. Bot pemantau sudah jadi anggota grup
         (bisa diaktifkan dulu, bot dikenali otomatis saat masuk)

    Return: (syarat_wajib_terpenuhi: bool, daftar_pesan: list[str])
    """
    blockers: list[str] = []
    warnings: list[str] = []

    # ── Syarat wajib 1: userbot online ───────────────────────────────────────
    if not is_userbot_ready():
        blockers.append(
            "⚠️ <b>Userbot belum online.</b>\n"
            "└ Pastikan <code>USERBOT_PHONE</code> sudah diisi di <code>.env</code> "
            "dan bot sudah di-restart. Kemudian kirim OTP yang dikirim Telegram ke HP Anda."
        )

    # ── Syarat wajib 2: bot pemantau sudah dikonfigurasi di DB ───────────────
    sec_doc = await _sec_os_get(chat_id)
    has_monitor_config = bool(sec_doc.get("monitor_bot_id", 0))

    if not has_monitor_config:
        blockers.append(
            "🤖 <b>Bot pemantau belum dikonfigurasi.</b>\n"
            "└ Buat bot baru via @BotFather, salin tokennya, lalu tekan "
            "<b>🤖 Pasang Bot Pemantau</b> dan masukkan token tersebut.\n"
            "   Setelah token disimpan, tambahkan bot pemantau ke grup secara manual."
        )
    else:
        # ── Warning opsional: bot pemantau belum join grup ───────────────────
        is_member = await check_monitor_is_member(client, chat_id)
        if not is_member:
            monitor_bot_id = sec_doc.get("monitor_bot_id", 0)
            uname = _monitor_username_cache.get(monitor_bot_id, f"id:{monitor_bot_id}")
            warnings.append(
                f"ℹ️ <b>Bot pemantau @{uname} belum ada di grup.</b>\n"
                f"└ Tambahkan ke grup agar fitur checkbio berfungsi.\n"
                f"   Bot akan dikenali otomatis saat masuk.\n"
                f"   <i>(Security OS tetap bisa diaktifkan sekarang.)</i>"
            )

    all_ok = len(blockers) == 0
    # Blockers dulu, lalu warnings — caller menampilkan semuanya
    return all_ok, blockers + warnings


# ══════════════════════════════════════════════════════════════════════════════
# AUTO-KENALI BOT PEMANTAU SAAT DITAMBAHKAN KE GRUP
# Saat bot pemantau masuk ke grup, cocokkan dengan DB → log konfirmasi.
# group=10 — jalan setelah handler nexus (8, 9) tapi tidak mengganggu mereka.
# ══════════════════════════════════════════════════════════════════════════════

def register_monitor_join_handler(bot: _Client) -> None:
    """
    Pasang handler on_chat_member_updated di bot utama untuk mendeteksi
    bot pemantau yang baru ditambahkan ke grup.
    Dipanggil dari start_userbot() setelah bot biasa aktif.
    """

    @bot.on_chat_member_updated(group=10)
    async def _on_monitor_joined(client: _Client, update: _ChatMemberUpdated):
        try:
            from pyrogram.enums import ChatMemberStatus

            new = update.new_chat_member
            if not new or not new.user or not new.user.is_bot:
                return  # bukan bot → skip

            # Hanya tangkap event JOIN (bukan kick/ban/promote)
            if new.status not in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR):
                return

            bot_id  = new.user.id
            chat_id = update.chat.id

            # Cek apakah bot ini adalah bot pemantau yang terdaftar untuk grup ini
            sec_doc = await _sec_os_get(chat_id)
            registered_monitor_id = sec_doc.get("monitor_bot_id", 0)

            if not registered_monitor_id or registered_monitor_id != bot_id:
                return  # bukan bot pemantau kita → skip

            uname = new.user.username or str(bot_id)
            _monitor_username_cache[bot_id] = uname

            print(f"[SecOS] ✅ Bot pemantau @{uname} ({bot_id}) terdeteksi masuk grup {chat_id} — dikenali otomatis.")

            # Jika Security OS sudah enabled, tidak perlu lakukan apa-apa lagi
            # Jika belum enabled, beri tahu di console saja
            if not sec_doc.get("enabled", False):
                print(f"[SecOS] ℹ️  Security OS grup {chat_id} belum diaktifkan. Aktifkan via panel.")

        except Exception as e:
            print(f"[SecOS] _on_monitor_joined error: {e}")
