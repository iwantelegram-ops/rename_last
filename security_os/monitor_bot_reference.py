"""
monitor_bot_reference.py
════════════════════════════════════════════════════════════════════════════════
MANAJER BOT PEMANTAU — Security OS (Multi-Instance, Database-Driven)

ARSITEKTUR:
  Tiap grup Security OS punya 1 bot pemantau sendiri (token berbeda).
  Token disimpan di DB: security_os.monitor_token (diisi saat admin setup).
  File ini menjalankan SEMUA bot pemantau dalam SATU proses — tiap bot
  berjalan sebagai Pyrogram Client tersendiri (instance terpisah).

  ┌────────────────────────────────────────────────────────────────────┐
  │  monitor_bot_reference.py (proses ini)                             │
  │                                                                    │
  │   MonitorInstance(chat_id=grupA, token=tokenA)  ← pantau grupA      │
  │   MonitorInstance(chat_id=grupB, token=tokenB)  ← pantau grupB      │
  │   MonitorInstance(chat_id=grupC, token=tokenC)  ← pantau grupC      │
  │                                                                    │
  │   Semua tulis ke collection bio_profiles dengan field chat_id      │
  └──────────────────────────────┬─────────────────────────────────────┘
                                 │ DB bersama (MongoDB)
              ┌──────────────────┴──────────────────┐
              ▼                                     ▼
    ┌──────────────────┐                 ┌──────────────────────┐
    │   Bot Utama      │  query          │      Userbot         │
    │  bio_filter      │  bio_profiles   │   (VC kick)          │
    │  (chat_id=grupA) │  {user_id,      │   (chat_id=grupA)    │
    └──────────────────┘   chat_id}      └──────────────────────┘

COLLECTION bio_profiles:
  {
    chat_id    : int,    # ID grup (tiap grup data terpisah)
    user_id    : int,    # ID user
    has_link   : bool,   # True = ada link di bio
    bio        : str,    # isi bio saat dicek
    checked_at : float,  # unix timestamp terakhir dicek
    updated_at : float,  # unix timestamp terakhir berubah status
    expires_at : datetime, # TTL — dokumen otomatis dihapus MongoDB setelah 5 menit
  }
  Index unik: (chat_id, user_id)
  Index TTL : expires_at (expireAfterSeconds=0) → MongoDB hapus otomatis

FLOW TOKEN:
  1. Admin aktifkan Security OS di grup → bot utama minta token bot pemantau
  2. Admin kirim token via DM ke bot utama
  3. Bot utama validasi token → simpan ke security_os.monitor_token
  4. Bot utama panggil reload_monitor_instances() (fungsi di file ini)
  5. File ini spawn MonitorInstance baru untuk token/grup tersebut

VARIABEL .env:
  API_ID, API_HASH   — sama dengan bot utama
  MONGO_URL          — HARUS SAMA dengan bot utama (DB bersama)
  MONGO_DB_NAME      — HARUS SAMA dengan bot utama
  CODE_BOT           — HARUS SAMA dengan bot utama
  BIO_TTL_SECS           — TTL data bio di DB sebelum dihapus (default: 60 detik).
                            Semua throttle in-memory (BIO_RECHECK_SECS,
                            VC_JOIN_RECHECK_SECS, TYPING_RECHECK_SECS) serta
                            cache di bio.py dan video_call.py ikut nilai ini
                            secara default — ubah satu env var ini cukup untuk
                            menyamakan seluruh lapisan cache/TTL bio di sistem.
"""

from __future__ import annotations

import os
import re
import time
import asyncio
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.types import Message, ChatMemberUpdated
from pyrogram.raw import functions as raw_fns, types as raw_types
from pyrogram.errors import FloodWait, PeerIdInvalid, ChatAdminRequired

# FIX (pemindahan ke folder security_os/): file ini sekarang berada di
# <root>/security_os/monitor_bot_reference.py. .env tetap di ROOT proyek,
# jadi pakai parent.parent (bukan parent saja).
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env", override=False)

# ── Env ───────────────────────────────────────────────────────────────────────
API_ID    = int(os.environ.get("API_ID", 0))
API_HASH  = os.environ.get("API_HASH", "")

# TTL data bio di MongoDB — dokumen dihapus otomatis setelah N detik (default 60)
BIO_TTL_SECS = int(os.environ.get("BIO_TTL_SECS", 300))

# FIX: Semua throttle in-memory di bawah ini SEBELUMNYA punya default sendiri
# yang jauh lebih besar dari BIO_TTL_SECS (600 & 300 detik vs TTL 60 detik).
# Akibatnya: dokumen bio sudah dihapus MongoDB (sesuai TTL), tapi throttle
# in-memory _last_checked / _last_typing_checked masih menganggap "baru saja
# dicek" sehingga check_and_save(force=False) SKIP fetch ulang ke Telegram API
# dan mengembalikan None / data DB basi — bio yang sudah dibersihkan user
# tetap dianggap "ada link" sampai throttle lama itu habis (bisa 5-10 menit).
# Sekarang semua throttle ini default-nya MENGIKUTI BIO_TTL_SECS, supaya satu
# env var BIO_TTL_SECS mengatur seluruh lapisan cache secara konsisten.
# Tetap bisa di-override individual lewat env var masing-masing jika perlu.
BIO_RECHECK_SECS      = int(os.environ.get("BIO_RECHECK_SECS", BIO_TTL_SECS))

# ── Throttle khusus per skenario ──────────────────────────────────────────────
VC_JOIN_RECHECK_SECS = int(os.environ.get("VC_JOIN_RECHECK_SECS", BIO_TTL_SECS))
TYPING_RECHECK_SECS  = int(os.environ.get("TYPING_RECHECK_SECS", BIO_TTL_SECS))

# ── Pola deteksi link di bio ──────────────────────────────────────────────────
LINK_PATTERN = re.compile(
    r"(@\S+|https?://\S+|t\.me/\S+|bit\.ly/\S+|linktr\.ee/\S+)",
    re.IGNORECASE,
)

TZ_WIB = timezone(timedelta(hours=7))

# ── Database — pakai modul yang sama dengan bot utama ────────────────────────
# FIX (pemindahan ke folder security_os/): tambahkan ROOT proyek ke sys.path
# (bukan folder security_os/ tempat file ini berada) — database.py tetap di
# root. Ini membuat modul ini tetap bisa diimpor mandiri (tidak rapuh
# terhadap urutan import dari antigcast.py), persis seperti sebelum
# pemindahan.
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database import db, _init_backend, get_bot_config, save_bot_config, get_active_backend  # noqa: E402

bio_col = db["bio_profiles"]   # Collection hasil scan — dibaca bot utama & userbot
sec_col = db["security_os"]    # Untuk ambil daftar grup + token

# ── Registry instance aktif ───────────────────────────────────────────────────
_active_instances: dict[int, "MonitorInstance"] = {}
_instances_lock = asyncio.Lock()

# ── Flag: TTL index sudah dibuat ──────────────────────────────────────────────
_ttl_index_created = False

# ── Rate-limit config untuk bio-check queue ───────────────────────────────────────────────────────────────────────────────────────
# Jeda minimum antar request GetFullUser PER INSTANCE (bot pemantau per grup).
# Default 0.5 detik → maks ~120 req/menit per token, aman dari limit Telegram.
_BIO_QUEUE_DELAY = float(os.environ.get("BIO_QUEUE_DELAY", 0.5))



async def _ensure_ttl_index() -> None:
    """
    Buat TTL index pada field expires_at di bio_profiles.
    MongoDB akan otomatis hapus dokumen saat expires_at sudah lewat.
    Dipanggil sekali saat startup — aman dipanggil berulang (idempotent).
    """
    global _ttl_index_created
    if _ttl_index_created:
        return
    try:
        await bio_col.create_index(
            "expires_at",
            expireAfterSeconds=0,
        )
        print("[Monitor] ✅ TTL index bio_profiles.expires_at siap.")
        _ttl_index_created = True
    except Exception as e:
        print(f"[Monitor] ⚠️  Gagal buat TTL index: {e}")


def _make_expires_at() -> datetime:
    """Return datetime UTC kapan dokumen bio harus dihapus (sekarang + BIO_TTL_SECS)."""
    return datetime.now(timezone.utc) + timedelta(seconds=BIO_TTL_SECS)


# ══════════════════════════════════════════════════════════════════════════════
# KELAS UTAMA — SATU INSTANCE PER GRUP
# ══════════════════════════════════════════════════════════════════════════════

class MonitorInstance:
    """
    Satu bot pemantau untuk satu grup.
    Punya Pyrogram Client sendiri (token unik per grup).
    Bereaksi terhadap event: pesan masuk, user join, typing, perubahan profil.
    """

    def __init__(self, chat_id: int, token: str, bot_id: int):
        self.chat_id    = chat_id
        self.token      = token
        self.bot_id     = bot_id
        self._stopped   = False
        self._last_checked: dict[int, float] = {}   # user_id → timestamp

        session_name = f"monitor_{abs(chat_id)}"
        # FIX (pemindahan ke folder security_os/): file .session tetap di ROOT
        # proyek (parent.parent), bukan di dalam security_os/, agar lokasi file
        # session monitor tidak berubah dibanding sebelum pemindahan.
        self._session_path  = str(Path(__file__).resolve().parent.parent / session_name) + ".session"
        self._session_db_key = f"monitor_session_{abs(chat_id)}"
        self.client = Client(
            session_name,
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=token,
        )

        self._raw_handler_registered = False

        self._last_vc_checked: dict[int, float] = {}
        self._last_typing_checked: dict[int, float] = {}

        # ── Bio-check queue: antrian agar request GetFullUser tidak burst ─────
        # Item: (user_id, asyncio.Future) — worker proses 1 per 1 + jeda
        self._bio_queue: asyncio.Queue = asyncio.Queue()
        self._bio_queue_pending: set[int] = set()   # dedup: sedang antri/diproses
        self._bio_worker_task: "Optional[asyncio.Task]" = None

    async def _restore_session(self) -> None:
        """
        Pulihkan file .session monitor ini dari MongoDB jika file lokal belum ada
        (misal setelah Railway redeploy — filesystem container selalu bersih).
        Tanpa ini, tiap monitor selalu login dari nol dan peer cache-nya kosong
        setiap kali container di-restart.
        """
        import base64

        if get_active_backend() != "mongo":
            return
        if os.path.exists(self._session_path):
            return  # file lokal sudah ada, tidak perlu restore
        try:
            saved = await get_bot_config(self._session_db_key)
            if not saved:
                return
            raw = base64.b64decode(saved.encode())
            with open(self._session_path, "wb") as f:
                f.write(raw)
            print(f"[Monitor {self.chat_id}] ✅ Session dipulihkan dari MongoDB.")
        except Exception as e:
            print(f"[Monitor {self.chat_id}] ⚠️  Gagal pulihkan session: {e}")

    async def _save_session(self) -> None:
        """
        Backup file .session monitor ini (termasuk peer cache di dalamnya) ke
        MongoDB. Dipanggil setelah start() berhasil dan saat stop() — sehingga
        peer baru yang ditemui monitor selama berjalan ikut terbawa ke redeploy
        berikutnya.
        """
        import base64

        if get_active_backend() != "mongo":
            return
        try:
            if not os.path.exists(self._session_path):
                return
            with open(self._session_path, "rb") as f:
                raw = f.read()
            encoded = base64.b64encode(raw).decode()
            await save_bot_config(self._session_db_key, encoded)
        except Exception as e:
            print(f"[Monitor {self.chat_id}] ⚠️  Gagal simpan session: {e}")

    async def start(self) -> bool:
        try:
            await self._restore_session()
            await self.client.start()
            await self._save_session()
            self._register_handlers()
            # Jalankan bio-check worker (antrian rate-limit aman)
            self._bio_worker_task = asyncio.create_task(
                self._bio_worker(), name=f"bio_worker_{abs(self.chat_id)}"
            )
            print(f"[Monitor {self.chat_id}] ✅ Bot pemantau aktif (bio worker started).")
            return True
        except Exception as e:
            print(f"[Monitor {self.chat_id}] ❌ Gagal start: {e}")
            return False

    async def stop(self) -> None:
        self._stopped = True
        # Hentikan bio worker
        if self._bio_worker_task and not self._bio_worker_task.done():
            self._bio_worker_task.cancel()
            try:
                await self._bio_worker_task
            except asyncio.CancelledError:
                pass
        # Simpan session terbaru (peer cache yang sempat terbentuk) sebelum
        # client benar-benar berhenti — kalau ini dipanggil saat SIGTERM/redeploy.
        await self._save_session()
        try:
            if self.client.is_connected:
                await self.client.stop()
        except Exception:
            pass
        print(f"[Monitor {self.chat_id}] 🛑 Bot pemantau dihentikan.")

    async def _fetch_bio(self, user_id: int) -> str | None:
        """
        Ambil bio user via Telegram API. Return None jika gagal.

        Bekerja untuk SIAPAPUN yang ada di VC — termasuk user yang BUKAN member
        grup. Fallback 2 (get_chat_member) akan gagal untuk non-member tapi
        ditangkap dan dilanjutkan ke fallback 3 & 4.

        Strategi EMPAT langkah:
        1. resolve_peer(user_id) → GetFullUser  ← cepat
        2. get_chat_member(chat_id, user_id) → warm session → GetFullUser
           ← GAGAL untuk non-member (UserNotParticipant), dilanjutkan ke F3
        3. InputUser(user_id, access_hash=0) → GetFullUser
           ← bekerja untuk user publik
        4. get_users([user_id]) → resolve_peer → GetFullUser
           ← last resort, paksa Telegram kirim access_hash
        """
        async def _getfull(peer) -> str | None:
            try:
                full = await self.client.invoke(raw_fns.users.GetFullUser(id=peer))
                return getattr(full.full_user, "about", None) or ""
            except Exception:
                return None

        # ── 1. Cepat: resolve_peer langsung ─────────────────────────────────
        try:
            peer = await self.client.resolve_peer(user_id)
            bio = await _getfull(peer)
            if bio is not None:
                return bio
        except FloodWait as fw:
            print(f"[Monitor {self.chat_id}] FloodWait {fw.value}s uid={user_id}")
            await asyncio.sleep(fw.value + 1)
            return None
        except (PeerIdInvalid, KeyError):
            pass
        except Exception as e:
            print(f"[Monitor {self.chat_id}] Gagal ambil bio uid={user_id}: {e}")
            return None

        # ── 2. Fallback: get_chat_member → warm session → resolve_peer ──────
        #    CATATAN: Gagal untuk non-member (UserNotParticipant) — ditangkap,
        #    lanjut ke fallback 3 & 4. Non-member di VC tetap diperiksa.
        try:
            member = await self.client.get_chat_member(self.chat_id, user_id)
            if member and member.user:
                try:
                    peer = await self.client.resolve_peer(member.user.id)
                    bio = await _getfull(peer)
                    if bio is not None:
                        print(f"[Monitor {self.chat_id}] uid={user_id}: bio via get_chat_member ✓")
                        return bio
                except Exception:
                    pass
        except FloodWait as fw2:
            print(f"[Monitor {self.chat_id}] FloodWait (fallback2) {fw2.value}s uid={user_id}")
            await asyncio.sleep(fw2.value + 1)
            return None
        except Exception as e2:
            # UserNotParticipant = non-member; error lain = peer tak terjangkau
            # Tetap lanjut ke fallback 3 & 4 — jangan berhenti di sini
            print(
                f"[Monitor {self.chat_id}] Fallback2 gagal uid={user_id} "
                f"({type(e2).__name__}) — lanjut fallback 3+4"
            )

        # ── 3. Fallback: InputUser(access_hash=0) ───────────────────────────
        try:
            from pyrogram.raw.types import InputUser as _RawInputUser
            peer = _RawInputUser(user_id=user_id, access_hash=0)
            bio = await _getfull(peer)
            if bio is not None:
                print(f"[Monitor {self.chat_id}] uid={user_id}: bio via access_hash=0 ✓")
                return bio
        except Exception:
            pass

        # ── 4. Fallback: get_users → paksa Pyrogram fetch access_hash ────────
        try:
            users_result = await self.client.get_users(user_id)
            u = users_result[0] if isinstance(users_result, list) else users_result
            if u:
                peer = await self.client.resolve_peer(u.id)
                bio = await _getfull(peer)
                if bio is not None:
                    print(f"[Monitor {self.chat_id}] uid={user_id}: bio via get_users ✓")
                    return bio
        except FloodWait as fw4:
            print(f"[Monitor {self.chat_id}] FloodWait (fallback4) {fw4.value}s uid={user_id}")
            await asyncio.sleep(fw4.value + 1)
        except Exception:
            pass

        print(f"[Monitor {self.chat_id}] ⚠️  Semua fallback gagal uid={user_id} — bio tidak tersedia")
        return None


    # ── Bio-check queue worker ───────────────────────────────────────────────

    async def _bio_worker(self) -> None:
        """
        Worker tunggal per-instance — konsumsi _bio_queue satu per satu.
        Jeda _BIO_QUEUE_DELAY detik antar request untuk menghindari FloodWait
        saat banyak grup ramai dan force_check_user dipanggil beruntun.
        """
        while not self._stopped:
            try:
                user_id, future = await asyncio.wait_for(
                    self._bio_queue.get(), timeout=5.0
                )
            except asyncio.TimeoutError:
                continue
            except Exception:
                continue

            try:
                result = await self._check_and_save_impl(user_id)
                if not future.done():
                    future.set_result(result)
            except Exception as e:
                if not future.done():
                    try:
                        future.set_exception(e)
                    except Exception:
                        pass
            finally:
                self._bio_queue_pending.discard(user_id)
                self._bio_queue.task_done()
                if not self._stopped:
                    await asyncio.sleep(_BIO_QUEUE_DELAY)

    async def _enqueue_bio_check(self, user_id: int) -> "bool | None":
        """
        Masukkan user_id ke antrian bio-check dan tunggu hasilnya.
        Jika user sudah dalam antrian (dedup), langsung baca DB yang ada
        daripada antri dua kali.
        """
        if user_id in self._bio_queue_pending:
            # Sudah antri — kembalikan data DB sementara (tidak menambah antrian)
            doc = await bio_col.find_one({"chat_id": self.chat_id, "user_id": user_id})
            return doc.get("has_link", False) if doc else None

        loop = asyncio.get_event_loop()
        future: asyncio.Future = loop.create_future()
        self._bio_queue_pending.add(user_id)
        await self._bio_queue.put((user_id, future))
        try:
            return await asyncio.wait_for(asyncio.shield(future), timeout=30.0)
        except asyncio.TimeoutError:
            print(f"[Monitor {self.chat_id}] _enqueue timeout uid={user_id}")
            return None
        except Exception as e:
            print(f"[Monitor {self.chat_id}] _enqueue error uid={user_id}: {e}")
            return None

    async def _check_and_save_impl(self, user_id: int) -> "bool | None":
        """
        Implementasi inti check_and_save — hanya dipanggil oleh _bio_worker.
        Sudah dijamin tidak burst karena dijalankan satu per satu oleh worker.
        """
        now = time.time()
        bio_text = await self._fetch_bio(user_id)
        if bio_text is None:
            return None

        has_link = bool(LINK_PATTERN.search(bio_text))
        self._last_checked[user_id] = now

        try:
            from core.ns_bio_guard import check_admin_bio_text
            admin_bio_ok = await check_admin_bio_text(self.chat_id, user_id, bio_text)
        except Exception as e:
            print(f"[Monitor {self.chat_id}] gagal cek admin_bio_ok uid={user_id}: {e}")
            admin_bio_ok = None

        old_doc      = await bio_col.find_one({"chat_id": self.chat_id, "user_id": user_id})
        old_has_link = old_doc.get("has_link") if old_doc else None
        updated_at   = (
            now
            if old_has_link != has_link
            else (old_doc.get("updated_at", now) if old_doc else now)
        )
        expires_at = _make_expires_at()

        await bio_col.update_one(
            {"chat_id": self.chat_id, "user_id": user_id},
            {"$set": {
                "chat_id":      self.chat_id,
                "user_id":      user_id,
                "has_link":     has_link,
                "bio":          bio_text[:500],
                "checked_at":   now,
                "updated_at":   updated_at,
                "expires_at":   expires_at,
                "admin_bio_ok": admin_bio_ok,
            }},
            upsert=True,
        )

        if old_has_link != has_link:
            status = "ADA LINK" if has_link else "HAPUS LINK"
            print(f"[Monitor {self.chat_id}] uid={user_id} → {status} | bio: {bio_text[:80]!r}")

        return has_link

    async def check_and_save(
        self, user_id: int, force: bool = False
    ) -> "bool | None":
        """
        Cek bio user, simpan ke bio_profiles dengan chat_id grup ini.

        Setiap kali data disimpan/diperbarui, field expires_at diset ulang
        ke (sekarang + BIO_TTL_SECS). MongoDB TTL index akan hapus dokumen
        otomatis setelah waktu tersebut.

        Return: True (ada link) | False (tidak) | None (gagal fetch)
        Throttle: skip jika belum BIO_RECHECK_SECS sejak cek terakhir,
                  kecuali force=True.

        RATE-LIMIT SAFE: request ke Telegram API dilakukan melalui _bio_worker
        (antrian per-instance) dengan jeda _BIO_QUEUE_DELAY antar request.
        Tidak langsung hit API → tidak burst saat banyak grup ramai bersamaan.
        """
        now = time.time()

        if not force:
            last = self._last_checked.get(user_id, 0)
            if now - last < BIO_RECHECK_SECS:
                # Kembalikan data dari DB tanpa hit API
                doc = await bio_col.find_one(
                    {"chat_id": self.chat_id, "user_id": user_id}
                )
                return doc.get("has_link", False) if doc else None

        # Lewat antrian → _bio_worker proses satu per satu + jeda
        return await self._enqueue_bio_check(user_id)

    async def check_and_save_vc(self, user_id: int) -> bool | None:
        """
        Paksa re-check bio saat user NAIK KE VOICE CHAT.
        Cache khusus VC: VC_JOIN_RECHECK_SECS (default 60 detik).

        BUG 2 FIX: Jika user tidak dikenal bot (tidak ada di DB) meski dalam
        throttle → bypass throttle, paksa fetch fresh dari Telegram API.
        Memastikan user yang baru pertama kali naik VC tetap di-scan.

        BUG 3 FIX (timestamp): Jika fetch gagal (result=None), timestamp
        tidak disimpan → scan berikutnya langsung retry tanpa tunggu 60 detik.
        """
        now      = time.time()
        last_vc  = self._last_vc_checked.get(user_id, 0)
        last_gen = self._last_checked.get(user_id, 0)
        last_any = max(last_vc, last_gen)

        if now - last_any < VC_JOIN_RECHECK_SECS:
            doc = await bio_col.find_one(
                {"chat_id": self.chat_id, "user_id": user_id}
            )
            if doc is not None:
                # Data fresh ada di DB → pakai langsung
                return doc.get("has_link", False)
            # BUG 2 FIX: Tidak ada di DB meski dalam throttle → bypass, fetch fresh
            # User belum pernah typing → bot belum kenal → harus paksa cek

        result = await self.check_and_save(user_id, force=True)
        # BUG 3 FIX: Simpan timestamp HANYA jika fetch berhasil (result is not None)
        # Jika gagal → tidak simpan timestamp → scan berikutnya langsung retry
        if result is not None:
            self._last_vc_checked[user_id] = now
            self._last_checked[user_id]    = now
        print(
            f"[Monitor {self.chat_id}] VC-join uid={user_id} "
            f"→ bio fresh, has_link={result}"
        )
        return result

    async def check_and_save_typing(self, user_id: int) -> bool | None:
        """
        Re-check bio saat user TYPING di grup.

        Throttle ketat (TYPING_RECHECK_SECS, default 300 detik = 5 menit).
        Jika sudah dicek dalam 5 menit → kembalikan data DB.
        Jika lebih dari 5 menit → fetch fresh dari Telegram API.
        """
        now          = time.time()
        last_typing  = self._last_typing_checked.get(user_id, 0)
        last_general = self._last_checked.get(user_id, 0)
        last_any     = max(last_typing, last_general)

        if now - last_any < TYPING_RECHECK_SECS:
            doc = await bio_col.find_one(
                {"chat_id": self.chat_id, "user_id": user_id}
            )
            return doc.get("has_link", False) if doc else None

        self._last_typing_checked[user_id] = now
        self._last_checked[user_id]        = now
        result = await self.check_and_save(user_id, force=True)
        print(
            f"[Monitor {self.chat_id}] Typing uid={user_id} "
            f"→ bio fresh, has_link={result}"
        )
        return result

    def _register_handlers(self) -> None:
        """Daftarkan handler Pyrogram ke client instance ini."""
        chat_id = self.chat_id
        monitor = self

        # ── User KIRIM PESAN di grup → cek bio (throttle BIO_RECHECK_SECS) ────
        @self.client.on_message(filters.chat(chat_id) & filters.group)
        async def _on_message(client: Client, message: Message):
            user = message.from_user
            if user is None or user.is_bot:
                return
            await monitor.check_and_save(user.id, force=False)

        # ── User JOIN grup → cek bio (force) ─────────────────────────────────
        @self.client.on_chat_member_updated()
        async def _on_join(client: Client, upd: ChatMemberUpdated):
            if upd.chat.id != chat_id:
                return
            if upd.new_chat_member is None:
                return
            user = upd.new_chat_member.user
            if user is None or user.is_bot:
                return
            print(
                f"[Monitor {chat_id}] User {user.id} join "
                "→ cek bio (force)"
            )
            await monitor.check_and_save(user.id, force=True)

        # ── Perubahan profil user & TYPING → raw_update handler ──────────────
        @self.client.on_raw_update()
        async def _on_profile_or_typing(client, update, users, chats):
            try:
                # ── Skenario TYPING ──────────────────────────────────────────
                if isinstance(update, raw_types.UpdateUserTyping):
                    user_id = getattr(update, "user_id", None)
                    if user_id and isinstance(user_id, int) and user_id > 0:
                        # Proses user yang sudah dikenal di grup ini,
                        # ATAU langsung check_and_save (tidak ada data = fresh check)
                        await monitor.check_and_save_typing(user_id)
                    return

                # ── Skenario PERUBAHAN PROFIL ─────────────────────────────────
                user_id = None
                if isinstance(update, raw_types.UpdateUserName):
                    user_id = getattr(update, "user_id", None)
                else:
                    type_name = type(update).__name__
                    if "Photo" in type_name or "Profile" in type_name:
                        user_id = getattr(update, "user_id", None)

                if user_id and isinstance(user_id, int) and user_id > 0:
                    known = await bio_col.find_one(
                        {"chat_id": chat_id, "user_id": user_id}
                    )
                    if known:
                        print(
                            f"[Monitor {chat_id}] Profil uid={user_id} "
                            "berubah → re-check"
                        )
                        await monitor.check_and_save(user_id, force=True)
            except Exception as e:
                print(f"[Monitor {chat_id}] raw_update error: {e}")

        self._raw_handler_registered = True


# ══════════════════════════════════════════════════════════════════════════════
# MANAJER INSTANCE — LOAD / RELOAD / STOP
# ══════════════════════════════════════════════════════════════════════════════

async def _load_instances_from_db() -> None:
    """
    Baca semua grup Security OS aktif dari DB.
    Untuk tiap grup yang punya monitor_token → spawn MonitorInstance.
    Dipanggil saat startup.
    """
    # Pastikan TTL index sudah ada sebelum instance mulai menulis
    await _ensure_ttl_index()

    async for doc in sec_col.find({"monitor_token": {"$exists": True, "$ne": ""}}):
        chat_id = doc.get("chat_id") or doc.get("_id")
        token   = doc.get("monitor_token", "")
        bot_id  = doc.get("monitor_bot_id", 0)
        if not chat_id or not token:
            continue
        async with _instances_lock:
            if chat_id not in _active_instances:
                await _spawn_instance(chat_id, token, bot_id)


async def _spawn_instance(chat_id: int, token: str, bot_id: int) -> bool:
    """
    Buat dan start MonitorInstance baru.
    Return True jika berhasil.
    """
    instance = MonitorInstance(chat_id, token, bot_id)
    ok = await instance.start()
    if ok:
        _active_instances[chat_id] = instance
    return ok


async def _stop_instance(chat_id: int) -> None:
    """Stop dan hapus MonitorInstance untuk grup ini."""
    instance = _active_instances.pop(chat_id, None)
    if instance:
        await instance.stop()


async def reload_monitor_instances() -> None:
    """
    Reload semua instance dari DB.
    Stop instance yang token-nya sudah dihapus,
    spawn instance baru untuk grup yang belum punya instance.
    """
    await _ensure_ttl_index()

    db_chat_ids: set[int] = set()
    async for doc in sec_col.find({"monitor_token": {"$exists": True, "$ne": ""}}):
        chat_id = doc.get("chat_id") or doc.get("_id")
        token   = doc.get("monitor_token", "")
        bot_id  = doc.get("monitor_bot_id", 0)
        if not chat_id or not token:
            continue
        db_chat_ids.add(chat_id)
        async with _instances_lock:
            if chat_id not in _active_instances:
                await _spawn_instance(chat_id, token, bot_id)

    # Stop instance yang sudah tidak ada di DB
    stale = set(_active_instances.keys()) - db_chat_ids
    for chat_id in stale:
        async with _instances_lock:
            await _stop_instance(chat_id)


async def spawn_monitor_for_group(chat_id: int, token: str, bot_id: int) -> bool:
    """
    Spawn MonitorInstance untuk grup baru (dipanggil saat admin setup token).
    Stop instance lama jika ada (token mungkin diganti).
    """
    await _ensure_ttl_index()
    async with _instances_lock:
        if chat_id in _active_instances:
            await _stop_instance(chat_id)
        return await _spawn_instance(chat_id, token, bot_id)


async def stop_monitor_for_group(chat_id: int) -> None:
    """Stop MonitorInstance untuk grup ini (dipanggil saat Security OS dinonaktifkan)."""
    async with _instances_lock:
        await _stop_instance(chat_id)


def get_active_instance_count() -> int:
    return len(_active_instances)


def get_active_chat_ids() -> list[int]:
    return list(_active_instances.keys())


async def save_all_sessions() -> None:
    """
    Backup session SEMUA monitor instance yang sedang aktif ke MongoDB.
    Dipanggil:
      - Secara periodik (lihat _periodic_session_backup di bawah)
      - Saat graceful shutdown proses utama (dipanggil dari antigcast.py)
        agar peer cache yang ditemui sejak start tidak hilang saat redeploy.
    """
    for instance in list(_active_instances.values()):
        try:
            await instance._save_session()
        except Exception as e:
            print(f"[Monitor {instance.chat_id}] ⚠️  Gagal backup session: {e}")


async def _periodic_session_backup(interval_secs: int = 20 * 60) -> None:
    """
    Backup session semua monitor aktif setiap interval_secs (default 20 menit).
    Sama seperti mekanisme di bot utama — peer baru yang ditemui monitor selama
    berjalan (member baru yang masuk grup, dll) ikut terbawa ke redeploy berikutnya.
    """
    while True:
        await asyncio.sleep(interval_secs)
        await save_all_sessions()
        print(f"[Monitor] 🔄 Periodic backup session selesai ({len(_active_instances)} instance).")


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API — dipanggil dari bio.py / video_call.py
# ══════════════════════════════════════════════════════════════════════════════

async def force_check_user(chat_id: int, user_id: int) -> bool | None:
    """
    Paksa re-check bio user via MonitorInstance aktif untuk grup ini.

    Dipanggil oleh bio.py saat:
      - Data belum ada di DB (None) → cek fresh
      - Bot utama terima pesan dari user yang belum dikenal bot pemantau

    Alur:
      1. Bot utama deteksi pesan dari user X di grup A
      2. Query bio_profiles {chat_id: A, user_id: X} → None (belum ada)
      3. Bot utama panggil force_check_user(A, X)
      4. MonitorInstance grup A fetch bio user X dari Telegram API
      5. Simpan ke bio_profiles dengan expires_at = sekarang + 5 menit
      6. Return has_link → bot utama hapus pesan jika True

    Return:
      True  → ada link di bio
      False → tidak ada link
      None  → instance tidak aktif atau gagal fetch
    """
    instance = _active_instances.get(chat_id)
    if instance is None:
        return None
    try:
        return await instance.check_and_save(user_id, force=True)
    except Exception as e:
        print(f"[MonitorQuery] force_check_user chat={chat_id} uid={user_id}: {e}")
        return None


async def force_check_vc_join(chat_id: int, user_id: int) -> bool | None:
    """
    Paksa re-check bio user saat NAIK KE VOICE CHAT.
    Cache khusus VC (VC_JOIN_RECHECK_SECS = 60 detik default).

    Dipanggil dari video_call.py → saat user join VC.
    """
    instance = _active_instances.get(chat_id)
    if instance is None:
        return None
    try:
        return await instance.check_and_save_vc(user_id)
    except Exception as e:
        print(f"[MonitorQuery] force_check_vc_join chat={chat_id} uid={user_id}: {e}")
        return None


async def query_bio(chat_id: int, user_id: int) -> bool | None:
    """
    Baca hasil cek bio dari DB untuk pasangan (chat_id, user_id).
    Data ini ditulis oleh MonitorInstance grup yang bersangkutan.

    Karena data ber-TTL (5 menit), dokumen yang sudah expired otomatis
    tidak ada di DB → return None → bot utama akan trigger force_check_user.

    Return:
      True  → ada link di bio
      False → tidak ada link di bio
      None  → data belum ada atau sudah expired → perlu force_check_user
    """
    try:
        doc = await bio_col.find_one(
            {"chat_id": chat_id, "user_id": user_id}
        )
    except Exception as e:
        print(
            f"[MonitorQuery] Gagal query bio "
            f"chat={chat_id} uid={user_id}: {e}"
        )
        return None

    if not doc:
        return None

    return doc.get("has_link", False)


async def query_admin_bio_ok(chat_id: int, user_id: int) -> bool | None:
    """
    Baca hasil cek "Bio Admin Wajib" (NewsCore) dari DB untuk pasangan
    (chat_id, user_id). Ditulis bersamaan dengan has_link oleh
    MonitorInstance.check_and_save() — lihat field admin_bio_ok.

    Return:
      True  → user adalah admin NewsCore aktif & bio memenuhi teks wajib
      False → user adalah admin NewsCore aktif & bio TIDAK memenuhi teks wajib
              → bot utama harus panggil core.ns_bio_guard.enforce_admin_bio()
      None  → bukan admin NewsCore / data belum ada — tidak perlu tindakan
    """
    try:
        doc = await bio_col.find_one(
            {"chat_id": chat_id, "user_id": user_id}
        )
    except Exception as e:
        print(
            f"[MonitorQuery] Gagal query admin_bio_ok "
            f"chat={chat_id} uid={user_id}: {e}"
        )
        return None

    if not doc:
        return None

    return doc.get("admin_bio_ok")


# ══════════════════════════════════════════════════════════════════════════════
# STARTUP — ENTRY POINT (jalankan sebagai proses terpisah)
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    """
    Entry point jika monitor_bot_reference.py dijalankan langsung (standalone).
    Dalam deployment normal, file ini di-import oleh antigcast.py.
    """
    from database import setup_db
    await setup_db()
    await _load_instances_from_db()
    print(f"[Monitor] {get_active_instance_count()} instance aktif.")
    await idle()


if __name__ == "__main__":
    asyncio.run(main())
