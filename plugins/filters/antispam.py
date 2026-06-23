"""
plugins/filters/antispam.py
────────────────────────────
Filter utama pesan grup:
  1. Regex global & lokal  (Owner Regex — TANPA pengaruh Whitelist Nexus)
  2. External mention
  3. Link detector
  4. Anti duplikasi lokal (per user per grup) — DIOPTIMALKAN VIA FAST-PATH RAM
  5. Anti duplikasi global (anti-gcast lintas grup)

ARSITEKTUR QUEUE (v2):
  Handler (group=2) HANYA menjalankan fast-path murah:
    • is_message_handled()  — in-memory, <1 μs
    • is_admin()            — DB query ringan (biasanya cache)
    • free_col.find_one()   — DB query ringan
    • Fast-Path Local Duplicate Check — In-Memory RAM Cache (Penyembuh Lag Flood)
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

# ── In-Memory Cache untuk Kebal Serangan Bom Duplikasi Lokal ──────────────────
# Struktur: { chat_id: { user_id: (hash_konten, ts_terakhir, hitungan_duplikat) } }
_local_flood_cache: dict[int, dict[int, tuple[str, float, int]]] = {}
_FLOOD_WINDOW   = 5.0  # Toleransi waktu antar pesan berulang (detik)
_MAX_DUPLICATE  = 2    # Batas toleransi duplikat sebelum eksekusi instan di RAM

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
    cid, uid, mid = message.chat.id, message.from_user.id, message.id[span_2](start_span)[span_2](end_span)

    # ── Fast-path: semua check murah, tidak perlu masuk queue ─────────────
    if is_message_handled(cid, mid):[span_3](start_span)[span_3](end_span)
        return[span_4](start_span)[span_4](end_span)

    if await is_admin(client, cid, uid):[span_5](start_span)[span_5](end_span)
        return[span_6](start_span)[span_6](end_span)

    if await free_col.find_one({"user_id": uid, "chat_id": cid}):[span_7](start_span)[span_7](end_span)
        return[span_8](start_span)[span_8](end_span)

    content = (message.text or message.caption or "").strip()[span_9](start_span)[span_9](end_span)
    if not content or content.startswith("/"):[span_10](start_span)[span_10](end_span)
        return[span_11](start_span)[span_11](end_span)

    # ── Optimasi Fast-Path RAM: Deteksi & Eksekusi Duplikasi Instan ──────────
    content_hash = hashlib.md5(content.encode("utf-8", errors="ignore")).hexdigest()
    now_ts = time.time()

    if cid not in _local_flood_cache:
        _local_flood_cache[cid] = {}

    user_flood_data = _local_flood_cache[cid].get(uid)

    if user_flood_data:
        last_hash, last_time, duplicate_count = user_flood_data
        
        # Jika teks pesan sama persis dan masuk dalam jeda jendela flood window
        if last_hash == content_hash and (now_ts - last_time) < _FLOOD_WINDOW:
            duplicate_count += 1
            _local_flood_cache[cid][uid] = (content_hash, now_ts, duplicate_count)
            
            # Jika menembak ambang batas duplikat berturut-turut
            if duplicate_count >= _MAX_DUPLICATE:
                # Kunci pesan agar tidak disentuh group filter di bawahnya (seperti nexus)
                mark_message_handled(cid, mid)[span_12](start_span)[span_12](end_span)
                
                # Hapus seketika via fire-and-forget (bypass antrean berat agar tidak lag)
                asyncio.create_task(message.delete())
                return  # Potong komparasi di sini, amankan queue utama
        else:
            # Konten berbeda atau jeda waktu aman -> reset hitungan duplikat ke 1
            _local_flood_cache[cid][uid] = (content_hash, now_ts, 1)
    else:
        # Perekaman data pesan pertama user di RAM grup ini
        _local_flood_cache[cid][uid] = (content_hash, now_ts, 1)

    # ── Enqueue ke detection_queue ─────────────────────────────────────────
    # Worker akan menjalankan seluruh logika deteksi spam secara berurutan.
    from core.antispam_queue import enqueue_for_detection[span_13](start_span)[span_13](end_span)
    await enqueue_for_detection(client, message)[span_14](start_span)[span_14](end_span)


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
    )[span_15](start_span)[span_15](end_span)
    from core.punishment import SPAM_MUTE_THRESHOLD[span_16](start_span)[span_16](end_span)
    from core.moderation_queue import queue_mute[span_17](start_span)[span_17](end_span)
    import time as _time[span_18](start_span)[span_18](end_span)
    now_ts = _time.time()[span_19](start_span)[span_19](end_span)
    mute_rec = await get_local_mute(chat_id, user_id)[span_20](start_span)[span_20](end_span)
    if mute_rec.get("muted_until", 0.0) > now_ts:[span_21](start_span)[span_21](end_span)
        return[span_22](start_span)[span_22](end_span)
    updated = await increment_local_spam(chat_id, user_id)[span_23](start_span)[span_23](end_span)
    consec  = updated.get("consec_spam", 1)[span_24](start_span)[span_24](end_span)
    if consec < SPAM_MUTE_THRESHOLD:[span_25](start_span)[span_25](end_span)
        return[span_26](start_span)[span_26](end_span)
    duration_secs, level_before = await apply_local_mute(chat_id, user_id)[span_27](start_span)[span_27](end_span)
    duration_min = duration_secs // 60[span_28](start_span)[span_28](end_span)

    async def _on_done(success: bool):
        if not success:[span_29](start_span)[span_29](end_span)
            await revert_failed_local_mute(chat_id, user_id, level_before)[span_30](start_span)[span_30](end_span)
            return[span_31](start_span)[span_31](end_span)
        try:
            await insert_group_action_log([span_32](start_span)[span_32](end_span)
                chat_id, "MUTE",[span_33](start_span)[span_33](end_span)
                f"Mute {duration_min} menit – anti-gcast global 10× berturut-turut",[span_34](start_span)[span_34](end_span)
                user_id, str(user_id), konten,[span_35](start_span)[span_35](end_span)
            )[span_36](start_span)[span_36](end_span)
        except Exception:
            pass[span_37](start_span)[span_37](end_span)

    await queue_mute(chat_id, user_id, duration_secs, on_done=_on_done)[span_38](start_span)[span_38](end_span)


# ─────────────────────────────────────────────────────────────────────────────
#  group=10 — Tracker pesan bersih
#  Berjalan SETELAH semua filter (CAS=-1, bio=1, antispam=2, nexus=5).
#  Jika pesan tidak ditangani oleh filter manapun → reset hitungan spam.
# ─────────────────────────────────────────────────────────────────────────────
@Client.on_message(filters.group & ~filters.service, group=10)
async def _clean_message_tracker(client, message):
    """Reset hitungan spam saat pesan lolos semua filter (pesan bersih)."""
    if not message.from_user or message.from_user.is_bot:[span_39](start_span)[span_39](end_span)
        return[span_40](start_span)[span_40](end_span)
    cid = message.chat.id[span_41](start_span)[span_41](end_span)
    mid = message.id[span_42](start_span)[span_42](end_span)
    uid = message.from_user.id[span_43](start_span)[span_43](end_span)

    if not is_message_handled(cid, mid):[span_44](start_span)[span_44](end_span)
        asyncio.create_task(_reset_mute_async(cid, uid))[span_45](start_span)[span_45](end_span)


async def _reset_mute_async(chat_id: int, user_id: int) -> None:
    """Reset hitungan spam dan level hukuman untuk user yang kirim pesan bersih."""
    try:
        await reset_local_mute(chat_id, user_id)[span_46](start_span)[span_46](end_span)
    except Exception:
        pass[span_47](start_span)[span_47](end_span)
