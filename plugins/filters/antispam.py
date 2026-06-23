"""
plugins/filters/antispam.py
────────────────────────────
Filter utama pesan grup:
  1. Regex global & lokal  (Owner Regex — TANPA pengaruh Whitelist Nexus)
  2. External mention
  3. Link detector
  4. Anti duplikasi lokal (per user per grup) — DIOPTIMALKAN VIA FAST-PATH RAM
  5. Anti duplikasi global (anti-gcast lintas grup) — PROTEKSI MASSAL ANTI-CLONE

ARSITEKTUR QUEUE (v2):
  Handler (group=2) HANYA menjalankan fast-path murah:
    • is_message_handled()  — in-memory, <1 μs
    • is_admin()            — DB query ringan (biasanya cache)
    • free_col.find_one()   — DB query ringan
    • Fast-Path RAM Flood Check — Proteksi Kebal Bom Spam & Userbot Massal
    • content kosong / command — string check

  Setelah fast-path lolos → pesan dimasukkan ke detection_queue
  (core/antispam_queue.py) untuk diproses satu per satu oleh
  antispam_detection_worker() yang berjalan sebagai background task.

  Keuntungan:
    • Tidak ada burst Telegram API (mention check, gcast) saat ramai
    • Koordinasi FloodWait lintas worker (delete / moderation / log)
    • Mayoritas pesan (admin, free_col) dilewati TANPA masuk queue

SISTEM MUTE ESKALASI (terpusat di core/punishment.py):
  • 10 pelanggaran spam APAPUN berturut-turut (per user per grup) → mute 5 menit
  • Setiap pelanggaran berikutnya (tanpa pesan bersih) → durasi 2× lipat
  • Pesan bersih (lolos semua filter, group=10) → reset hitungan + level hukuman
  • Berlaku untuk SEMUA jenis spam: regex, mention, link, duplikat, gcast, bio, nexus

PASSIVE LEARNING (v3.1):
  • Penghapusan via regex global/lokal → force_learn=True ke AI (konfirmasi pasti spam)
  • Passive learning dilakukan fire-and-forget agar tidak memperlambat filter utama

PINTU BERURUTAN:
  Setiap kali filter ini memutuskan hapus pesan → mark_message_handled(cid, mid)
  dipanggil agar filter berikutnya (nexus group=5) tidak memproses ulang.
"""

import os
import re
import time
import asyncio
import hashlib
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.enums import MessageEntityType, ParseMode
from pyrogram.errors import UserNotParticipant, PeerIdInvalid, RPCError
from rapidfuzz import fuzz

LOG_CHANNEL = int(os.environ.get("LOG_CHANNEL", 0))

from database import (
    messages_db, regex_db, get_config, is_admin, db,
    delete_queue, GLOBAL_EXPIRY, TZ_WIB, auto_delete_reply,
    mark_message_handled, is_message_handled,
    get_local_mute, reset_local_mute,
    insert_group_action_log,
    has_warned_user, mark_warned_user,
)
from core.regex_utils import simplify, remove_mentions_for_regex, match_with_leet
from core.punishment import check_and_punish
from core.group_notify import send_group_notice
from plugins.nexus.engine import pipeline_pembersihan

group_regex_db = db["regex_per_group"]
free_col       = db["free_per_group"]

# ── 1. Cache Per-User (Bom Spam dari 1 Akun Tunggal) ──────────────────────────
# Struktur: { chat_id: { user_id: (hash_konten, ts_terakhir, hitungan_duplikat) } }
_local_flood_cache: dict[int, dict[int, tuple[str, float, int]]] = {}
_FLOOD_WINDOW   = 5.0  # Jeda toleransi waktu antar pesan berulang (detik)
_MAX_DUPLICATE  = 2    # Batas duplikat sebelum eksekusi instan di RAM

# ── 2. Cache Lintas-User (Serangan Massal Banyak Akun Kloning / Userbot) ──────
# Struktur: { chat_id: { hash_konten: [list_timestamp_pesan_masuk] } }
_global_text_tracker: dict[int, dict[str, list[float]]] = {}
# Struktur: { chat_id: { hash_konten: timestamp_karantina_sampai } }
_global_text_blacklist: dict[int, dict[str, float]] = {}

_MASS_BURST_WINDOW = 1.5  # Rentang waktu kritis serangan (detik)
_MASS_BURST_LIMIT  = 3    # Batas maksimal teks sama masuk dalam rentang kritis tersebut
_LOCK_DURATION     = 10.0 # Durasi teks dikarantina di RAM jika terbukti diserang (detik)

# ── Cache regex ───────────────────────────────────────────────────────────────
_regex_cache:     list  = []
_regex_cache_ts:  float = 0.0
_local_regex_cache: dict[int, tuple[list, float]] = {}
REGEX_TTL = 300

_URL_ENTITY_TYPES = {MessageEntityType.URL, MessageEntityType.TEXT_LINK}


def _has_url_entity(message) -> bool:
    entities = list(message.entities or []) + list(message.caption_entities or [])
    return any(e.type in _URL_ENTITY_TYPES for e in entities)


async def _get_global_patterns():
    """Return list of (compiled_pattern, raw_display_str) untuk regex global owner."""
    global _regex_cache, _regex_cache_ts
    now = time.monotonic()
    if now - _regex_cache_ts < REGEX_TTL:
        return _regex_cache
    patterns = []
    async for doc in regex_db.find({"pattern": {"$exists": True}}):
        try:
            raw = doc.get("raw") or doc.get("pattern", "")
            patterns.append((re.compile(doc["pattern"], re.IGNORECASE), raw))
        except Exception:
            pass
    _regex_cache = patterns
    _regex_cache_ts = now
    return _regex_cache


async def _get_local_patterns(chat_id: int):
    """Return list of (compiled_pattern, raw_display_str) untuk regex lokal grup."""
    now = time.monotonic()
    hit = _local_regex_cache.get(chat_id)
    if hit and (now - hit[1]) < REGEX_TTL:
        return hit[0]
    patterns = []
    async for doc in group_regex_db.find({"chat_id": chat_id}):
        try:
            raw = doc.get("raw") or doc.get("pattern", "")
            patterns.append((re.compile(doc["pattern"], re.IGNORECASE), raw))
        except Exception:
            pass
    _local_regex_cache[chat_id] = (patterns, now)
    return patterns


def invalidate_local_regex_cache(chat_id: int) -> None:
    """Hapus cache pattern lokal agar filter baru/terhapus langsung aktif."""
    _local_regex_cache.pop(chat_id, None)


async def _is_external_mention(client: Client, message) -> bool:
    if not message.entities:
        return False
    content = message.text or message.caption or ""
    cid = message.chat.id
    for entity in message.entities:
        target = None
        if entity.type == MessageEntityType.MENTION:
            target = content[entity.offset:entity.offset + entity.length].lstrip("@").lower()
        elif entity.type == MessageEntityType.TEXT_MENTION and getattr(entity, "user", None):
            target = entity.user.id
        elif entity.type in (MessageEntityType.URL, MessageEntityType.TEXT_LINK):
            url = (content[entity.offset:entity.offset + entity.length]
                   if entity.type == MessageEntityType.URL else entity.url)
            if url.startswith("tg://user?id="):
                try:
                    target = int(url.split("=")[1])
                except Exception:
                    pass
        if target:
            if isinstance(target, str) and target in ["botfather", "telegram"]:
                continue
            try:
                await client.get_chat_member(cid, target)
            except (UserNotParticipant, PeerIdInvalid, RPCError):
                return True
    return False


# ── Passive learning helper — fire-and-forget ─────────────────────────────────

def _trigger_passive_learn_spam(text: str, confidence: float = 1.0) -> None:
    """
    Trigger passive learning sebagai spam secara fire-and-forget.
    Selalu menggunakan force_learn=True karena berasal dari regex (konfirmasi pasti spam).
    Tidak menunggu hasil — jangan panggil await di sini.
    """
    try:
        from nexus.ai_core import nexus_ai_passive_observe
        asyncio.create_task(
            nexus_ai_passive_observe(text, True, confidence, force_learn=True)
        )
    except Exception:
        pass  # Non-fatal — passive learning opsional


# ─────────────────────────────────────────────────────────────────────────────
#  Main filter (group=2) — FAST-PATH ONLY, lalu enqueue ke detection_queue
# ─────────────────────────────────────────────────────────────────────────────
@Client.on_message(filters.group & ~filters.service, group=2)
async def main_antispam_filter(client, message):
    """
    Handler pesan grup. HANYA menjalankan pemeriksaan murah (fast-path).
    Deteksi berat (regex, mention, gcast) didelegasikan ke
    antispam_detection_worker() via detection_queue.
    """
    if not message.from_user:
        return
    cid, uid, mid = message.chat.id, message.from_user.id, message.id

    # ── Fast-path: semua check murah, tidak perlu masuk queue ─────────────
    if is_message_handled(cid, mid):
        return

    if await is_admin(client, cid, uid):
        return

    if await free_col.find_one({"user_id": uid, "chat_id": cid}):
        return

    content = (message.text or message.caption or "").strip()
    if not content or content.startswith("/"):
        return

    # Buat hash MD5 dari isi teks pesan
    content_hash = hashlib.md5(content.encode("utf-8", errors="ignore")).hexdigest()
    now_ts = time.time()

    # ── PROTEKSI A: Karantina RAM Sementara (Serangan Massal Banyak Akun) ──────
    if cid in _global_text_blacklist and content_hash in _global_text_blacklist[cid]:
        # Jika masa berlaku karantina teks belum habis -> HAPUS INSTAN
        if now_ts < _global_text_blacklist[cid][content_hash]:
            mark_message_handled(cid, mid)
            asyncio.create_task(message.delete())
            return
        else:
            # Lepas karantina jika waktu hukuman teks sudah selesai
            _global_text_blacklist[cid].pop(content_hash, None)

    # ── PROTEKSI B: Deteksi Serangan Massal Banyak Akun Kloning (Lintas User) ──
    if cid not in _global_text_tracker:
        _global_text_tracker[cid] = {}
    
    if content_hash not in _global_text_tracker[cid]:
        _global_text_tracker[cid][content_hash] = []
        
    _global_text_tracker[cid][content_hash].append(now_ts)
    
    # Bersihkan log waktu lama yang berada di luar batas jendela kritis
    _global_text_tracker[cid][content_hash] = [
        ts for ts in _global_text_tracker[cid][content_hash] 
        if (now_ts - ts) <= _MASS_BURST_WINDOW
    ]
    
    # Jika teks yang sama dikirim massal melewati batas toleransi manusia wajar
    if len(_global_text_tracker[cid][content_hash]) >= _MASS_BURST_LIMIT:
        if cid not in _global_text_blacklist:
            _global_text_blacklist[cid] = {}
        
        # Kunci teks tersebut di RAM selama 10 detik ke depan
        _global_text_blacklist[cid][content_hash] = now_ts + _LOCK_DURATION
        
        mark_message_handled(cid, mid)
        asyncio.create_task(message.delete())
        return

    # ── PROTEKSI C: Deteksi Duplikasi Tunggal Per-User ────────────────────────
    if cid not in _local_flood_cache:
        _local_flood_cache[cid] = {}

    user_flood_data = _local_flood_cache[cid].get(uid)

    if user_flood_data:
        last_hash, last_time, duplicate_count = user_flood_data
        
        # Jika pesan sama persis dikirim berulang oleh user yang sama dalam jendela waktu
        if last_hash == content_hash and (now_ts - last_time) < _FLOOD_WINDOW:
            duplicate_count += 1
            _local_flood_cache[cid][uid] = (content_hash, now_ts, duplicate_count)
            
            if duplicate_count >= _MAX_DUPLICATE:
                mark_message_handled(cid, mid)
                asyncio.create_task(message.delete())
                return  
        else:
            _local_flood_cache[cid][uid] = (content_hash, now_ts, 1)
    else:
        _local_flood_cache[cid][uid] = (content_hash, now_ts, 1)

    # ── Enqueue ke detection_queue ─────────────────────────────────────────
    # Worker akan menjalankan seluruh logika deteksi spam secara berurutan.
    from core.antispam_queue import enqueue_for_detection
    await enqueue_for_detection(client, message)


async def _gcast_punish_other_group(
    client,
    chat_id: int,
    user_id: int,
    konten: str,
) -> None:
    """
    Hitung punishment gcast untuk user di grup lain (bukan grup pendeteksi).
    Dipanggil hanya jika grup tersebut aktif global (global=True).
    Menggunakan increment langsung tanpa objek message penuh karena
    kita tidak memiliki message object untuk grup lain.
    """
    from database import (
        get_local_mute, increment_local_spam, apply_local_mute,
        revert_failed_local_mute, insert_group_action_log,
    )
    from core.punishment import SPAM_MUTE_THRESHOLD
    from core.moderation_queue import queue_mute
    import time as _time
    now_ts = _time.time()
    mute_rec = await get_local_mute(chat_id, user_id)
    if mute_rec.get("muted_until", 0.0) > now_ts:
        return
    updated = await increment_local_spam(chat_id, user_id)
    consec  = updated.get("consec_spam", 1)
    if consec < SPAM_MUTE_THRESHOLD:
        return
    duration_secs, level_before = await apply_local_mute(chat_id, user_id)
    duration_min = duration_secs // 60

    async def _on_done(success: bool):
        if not success:
            await revert_failed_local_mute(chat_id, user_id, level_before)
            return
        try:
            await insert_group_action_log(
                chat_id, "MUTE",
                f"Mute {duration_min} menit – anti-gcast global 10× berturut-turut",
                user_id, str(user_id), konten,
            )
        except Exception:
            pass

    await queue_mute(chat_id, user_id, duration_secs, on_done=_on_done)


# ─────────────────────────────────────────────────────────────────────────────
#  group=10 — Tracker pesan bersih
#  Berjalan SETELAH semua filter (CAS=-1, bio=1, antispam=2, nexus=5).
#  Jika pesan tidak ditangani oleh filter manapun → reset hitungan spam.
# ─────────────────────────────────────────────────────────────────────────────
@Client.on_message(filters.group & ~filters.service, group=10)
async def _clean_message_tracker(client, message):
    """Reset hitungan spam saat pesan lolos semua filter (pesan bersih)."""
    if not message.from_user or message.from_user.is_bot:
        return
    cid = message.chat.id
    mid = message.id
    uid = message.from_user.id

    if not is_message_handled(cid, mid):
        asyncio.create_task(_reset_mute_async(cid, uid))


async def _reset_mute_async(chat_id: int, user_id: int) -> None:
    """Reset hitungan spam dan level hukuman untuk user yang kirim pesan bersih."""
    try:
        await reset_local_mute(chat_id, user_id)
    except Exception:
        pass
