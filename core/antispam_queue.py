"""
core/antispam_queue.py
───────────────────────────────────────────────────────────────────────────────
Worker antrian deteksi spam untuk antispam.py.

MASALAH YANG DISELESAIKAN:
  antispam.py memproses 1 bot utama untuk SEMUA grup. Berbeda dengan bio.py
  yang punya 1 bot pemantau per grup, antispam.py langsung tembak logika
  deteksi (DB query, external mention check via Telegram API, global gcast
  check) untuk SETIAP pesan yang masuk dari seluruh grup secara bersamaan.

  Tanpa queue:
    • Jika 10 grup ramai bersamaan → 10+ coroutine jalankan _is_external_mention
      (Telegram API call) serentak → potensi FloodWait / lag
    • MongoDB query get_config, messages_db.find, bio_col.find_one dipanggil
      paralel penuh tanpa throttle
    • check_and_punish + insert_group_action_log tembak beruntun, bisa
      tabrakan dengan moderation_worker_loop dan log_flush_worker_loop

SOLUSI (3 LAPISAN):
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  LAPISAN 1 — Fast-path in-handler (tetap di antispam.py)               │
  │  • is_message_handled() → return cepat, tanpa queue                    │
  │  • is_admin() → return cepat, tanpa queue                              │
  │  • free_col.find_one() → return cepat, tanpa queue                     │
  │  Semua pemeriksaan MURAH ini tetap berjalan langsung di handler         │
  │  sehingga tidak menambah latensi untuk mayoritas pesan yang dilewati.   │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  LAPISAN 2 — detection_queue (SATU item per pesan)                     │
  │  • Setelah fast-path lolos, pesan dimasukkan ke detection_queue         │
  │  • antispam_detection_worker() proses SATU per SATU                    │
  │  • Di dalam worker: regex check, mention check, link check,             │
  │    dup lokal, dup global — semua jalan berurutan, tidak paralel         │
  │  • Koordinasi FloodWait dengan worker lain via                          │
  │    set_global_flood_backoff / wait_global_flood_backoff                 │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  LAPISAN 3 — Action queue (sudah ada)                                  │
  │  • delete_queue (database.py) → delete_worker                          │
  │  • moderation_queue (core/moderation_queue.py) → moderation_worker_loop│
  │  • log_flush_worker_loop (plugins/commands/log.py)                     │
  │  Worker ini tidak berubah — antispam_queue hanya memastikan input ke   │
  │  queue tersebut tidak burst.                                            │
  └─────────────────────────────────────────────────────────────────────────┘

INTEGRASI DENGAN WORKER LAIN:
  • Sebelum _is_external_mention (Telegram API call) → wait_global_flood_backoff()
  • Saat FloodWait dari mention check → set_global_flood_backoff()
  • antispam_detection_worker tidur DETECT_INTER_DELAY antar item
    → memberi nafas ke delete_worker dan moderation_worker_loop

PARAMETER TUNING (via .env):
  ANTISPAM_QUEUE_MAXSIZE  default 500   — max item di antrian sebelum drop
  ANTISPAM_DETECT_DELAY   default 0.05  — jeda antar deteksi (detik)
  ANTISPAM_MENTION_TIMEOUT default 8.0  — timeout Telegram API per mention
"""

import asyncio
import os
import time
from typing import TYPE_CHECKING

from pyrogram.errors import FloodWait
from database import set_global_flood_backoff, wait_global_flood_backoff

if TYPE_CHECKING:
    from pyrogram import Client
    from pyrogram.types import Message

# ── Tuning ───────────────────────────────────────────────────────────────────
_MAXSIZE          = int(os.environ.get("ANTISPAM_QUEUE_MAXSIZE",  500))
_DETECT_DELAY     = float(os.environ.get("ANTISPAM_DETECT_DELAY",  0.05))
_MENTION_TIMEOUT  = float(os.environ.get("ANTISPAM_MENTION_TIMEOUT", 8.0))

# ── Queue utama ───────────────────────────────────────────────────────────────
# Item: (client, message) — message sudah lolos fast-path di handler
detection_queue: asyncio.Queue = asyncio.Queue(maxsize=_MAXSIZE)

# ── Statistik ringan (debug) ──────────────────────────────────────────────────
_stat_enqueued   = 0
_stat_processed  = 0
_stat_dropped    = 0


def get_detection_queue_stats() -> dict:
    """Return statistik queue untuk diagnostik owner."""
    return {
        "enqueued":  _stat_enqueued,
        "processed": _stat_processed,
        "dropped":   _stat_dropped,
        "qsize":     detection_queue.qsize(),
    }


async def enqueue_for_detection(client: "Client", message: "Message") -> bool:
    """
    Masukkan pesan ke detection_queue untuk diproses worker.

    Return True jika berhasil, False jika queue penuh (pesan di-drop).
    Drop adalah pilihan lebih aman daripada memblokir handler utama —
    jika queue penuh berarti bot sedang sangat sibuk; lewatkan saja
    daripada menumpuk memory tak terbatas.
    """
    global _stat_enqueued, _stat_dropped
    try:
        detection_queue.put_nowait((client, message))
        _stat_enqueued += 1
        return True
    except asyncio.QueueFull:
        _stat_dropped += 1
        print(
            f"[antispam_queue] ⚠️  Queue penuh ({_MAXSIZE}) — "
            f"pesan mid={message.id} cid={message.chat.id} di-drop"
        )
        return False


async def antispam_detection_worker(client: "Client") -> None:
    """
    Worker tunggal yang memproses semua deteksi spam satu per satu.

    Dipanggil sekali sebagai background task dari antigcast.py SETELAH
    app.start() — mengikuti pola delete_worker / moderation_worker_loop.

    Setiap item adalah (client, message) yang sudah lolos fast-path:
      • Bukan admin
      • Bukan free_col
      • Content tidak kosong / command

    Worker ini menjalankan seluruh logika deteksi dari antispam.py
    (regex, mention, link, dup lokal, dup global) secara berurutan,
    satu pesan pada satu waktu, sehingga tidak ada burst API.
    """
    global _stat_processed

    # Tunggu client benar-benar terkoneksi
    for _ in range(60):
        if getattr(client, "is_connected", False):
            break
        await asyncio.sleep(1.0)

    print("[antispam_queue] ✅ Worker deteksi antispam siap.", flush=True)

    while True:
        try:
            item = await detection_queue.get()
        except asyncio.CancelledError:
            break

        try:
            _client, _message = item
            await _process_detection(_client, _message)
            _stat_processed += 1
        except asyncio.CancelledError:
            detection_queue.task_done()
            break
        except Exception as e:
            print(f"[antispam_queue] ❌ Error proses pesan: {e}")
        finally:
            detection_queue.task_done()

        # Jeda ringan antar deteksi — beri nafas ke worker lain
        await asyncio.sleep(_DETECT_DELAY)


async def _process_detection(client: "Client", message: "Message") -> None:
    """
    Inti logika deteksi spam. Dipanggil dari worker, bukan langsung dari handler.

    Urutan pemeriksaan sama persis dengan antispam.py versi lama,
    tapi sekarang berjalan SATU PER SATU lewat worker, bukan paralel.
    """
    import hashlib
    import re
    import time as _time
    from datetime import datetime

    from pyrogram.enums import ParseMode
    from database import (
        messages_db, get_config, delete_queue, TZ_WIB, auto_delete_reply,
        mark_message_handled, is_message_handled,
        get_local_mute,
        insert_group_action_log,
        has_warned_user, mark_warned_user,
        GLOBAL_EXPIRY,
    )
    from core.regex_utils import simplify, remove_mentions_for_regex, match_with_leet
    from core.punishment import check_and_punish
    from core.group_notify import send_group_notice
    from plugins.nexus.engine import pipeline_pembersihan

    # Import helper lokal dari antispam.py
    from plugins.filters.antispam import (
        _get_global_patterns,
        _get_local_patterns,
        _is_external_mention,
        _has_url_entity,
        _trigger_passive_learn_spam,
    )

    cid = message.chat.id
    uid = message.from_user.id
    mid = message.id

    # Re-cek: mungkin sudah ditangani filter lain (bio=1 misalnya) saat
    # menunggu di queue
    if is_message_handled(cid, mid):
        return

    content = (message.text or message.caption or "").strip()
    if not content or content.startswith("/"):
        return

    is_short         = (1 <= len(content) <= 3) or content.isdigit()
    cfg              = await get_config(cid)
    now_ts           = _time.time()
    now_dt           = datetime.now(TZ_WIB)
    norm             = simplify(content)
    regex_safe       = remove_mentions_for_regex(message)
    teks_super_clean = pipeline_pembersihan(content)

    # ── 1. Regex global (Owner Regex) ─────────────────────────────────────
    for pat, raw in await _get_global_patterns():
        if match_with_leet(pat, regex_safe) or (teks_super_clean and pat.search(teks_super_clean)):
            mark_message_handled(cid, mid)
            await delete_queue.put((cid, [mid]))
            asyncio.create_task(insert_group_action_log(
                cid, "HAPUS", f"Filter kata global – {raw[:60]}",
                uid, message.from_user.first_name or str(uid), content[:100],
            ))
            asyncio.create_task(check_and_punish(client, message, "filter kata global", content[:100]))
            _trigger_passive_learn_spam(content, confidence=1.0)
            return

    # ── 2. Regex lokal (Group Filter) ─────────────────────────────────────
    for pat, raw in await _get_local_patterns(cid):
        if match_with_leet(pat, regex_safe) or (teks_super_clean and pat.search(teks_super_clean)):
            mark_message_handled(cid, mid)
            await delete_queue.put((cid, [mid]))
            asyncio.create_task(insert_group_action_log(
                cid, "HAPUS", f"Filter kata grup – {raw[:60]}",
                uid, message.from_user.first_name or str(uid), content[:100],
            ))
            asyncio.create_task(check_and_punish(client, message, "filter kata grup", content[:100]))
            _trigger_passive_learn_spam(content, confidence=1.0)
            return

    # ── 3. External mention (Telegram API call — koordinasi FloodWait) ────
    if cfg.get("anti_mention", True):
        await wait_global_flood_backoff()
        try:
            is_ext = await asyncio.wait_for(
                _is_external_mention(client, message),
                timeout=_MENTION_TIMEOUT,
            )
        except asyncio.TimeoutError:
            is_ext = False
            print(f"[antispam_queue] ⚠️  Timeout mention check mid={mid} cid={cid}")
        except FloodWait as fw:
            set_global_flood_backoff(fw.value)
            is_ext = False
            print(f"[antispam_queue] ⚠️  FloodWait {fw.value}s saat mention check, skip")
        except Exception as e:
            is_ext = False
            print(f"[antispam_queue] ⚠️  Error mention check: {e}")

        if is_ext:
            mark_message_handled(cid, mid)
            await delete_queue.put((cid, [mid]))
            asyncio.create_task(insert_group_action_log(
                cid, "HAPUS", "Mention pengguna luar grup",
                uid, message.from_user.first_name or str(uid), content[:100],
            ))
            asyncio.create_task(check_and_punish(client, message, "mention pengguna luar", content[:100]))
            return

    # ── 3.5 Link detector ─────────────────────────────────────────────────
    if _has_url_entity(message):
        mark_message_handled(cid, mid)
        await delete_queue.put((cid, [mid]))
        asyncio.create_task(insert_group_action_log(
            cid, "HAPUS", "Link terdeteksi dalam pesan",
            uid, message.from_user.first_name or str(uid), content[:100],
        ))
        asyncio.create_task(check_and_punish(client, message, "link dalam pesan", content[:100]))
        return

    # ── 4. Anti duplikasi lokal ───────────────────────────────────────────
    if cfg.get("local") is True and not message.via_bot and not is_short:

        mute_rec = await get_local_mute(cid, uid)
        if mute_rec.get("muted_until", 0.0) > now_ts:
            mark_message_handled(cid, mid)
            await delete_queue.put((cid, [mid]))
            return

        spam_limit = max(1, min(5, int(cfg.get("local_spam_limit", 1))))

        matched_old = None
        from rapidfuzz import fuzz
        async for old in messages_db.find(
            {"chat_id": cid, "user_id": uid, "type": "local_track"}
        ).sort("time", -1).limit(spam_limit):
            old_norm = old.get("norm_txt", "")
            if not old_norm:
                continue
            if fuzz.ratio(norm, old_norm) >= 90:
                if (now_ts - old["time"]) < cfg["expiry"]:
                    matched_old = old
                    break

        if matched_old is not None:
            mark_message_handled(cid, mid)
            await delete_queue.put((cid, [matched_old["msg_id"], mid]))
            asyncio.create_task(insert_group_action_log(
                cid, "HAPUS", "Pesan duplikat berulang",
                uid, message.from_user.first_name or str(uid), content[:100],
            ))
            asyncio.create_task(check_and_punish(
                client, message, "spam duplikat lokal", content[:100]
            ))

            if not await has_warned_user(cid, uid, "dup"):
                msg_warn = await send_group_notice(
                    client, cid,
                    f"{message.from_user.mention} jangan kirim pesan yang sama",
                    notice_kind="warn_dup",
                    parse_mode=ParseMode.HTML,
                    reply_to_message_id=mid,
                )
                if msg_warn is not None:
                    asyncio.create_task(auto_delete_reply([msg_warn], delay=5))
                asyncio.create_task(mark_warned_user(cid, uid, "dup"))

            await messages_db.delete_one({"_id": matched_old["_id"]})
            new_id = f"loc_{cid}_{uid}_{hashlib.md5(content.encode()).hexdigest()}_{int(now_ts*1000)}"
            await messages_db.insert_one({
                "_id": new_id, "time": now_ts, "msg_id": mid,
                "chat_id": cid, "user_id": uid, "norm_txt": norm,
                "type": "local_track", "createdAt": now_dt,
            })
            return

        # Pesan bersih lokal → simpan ke DB
        new_id = f"loc_{cid}_{uid}_{mid}_{int(now_ts * 1000)}"
        await messages_db.insert_one({
            "_id": new_id, "time": now_ts, "msg_id": mid,
            "chat_id": cid, "user_id": uid, "norm_txt": norm,
            "type": "local_track", "createdAt": now_dt,
        })
        all_docs = [d async for d in messages_db.find(
            {"chat_id": cid, "user_id": uid, "type": "local_track"}
        ).sort("time", -1)]
        if len(all_docs) > spam_limit:
            old_ids = [d["_id"] for d in all_docs[spam_limit:]]
            await messages_db.delete_many({"_id": {"$in": old_ids}})

    # ── 5. Anti duplikasi global (gcast) ──────────────────────────────────
    if cfg.get("global") is True and not is_short:
        from plugins.filters.antispam import _gcast_punish_other_group

        content_hash = hashlib.md5(content.encode()).hexdigest()
        global_key   = f"glob_{uid}_{content_hash}"
        existing     = await messages_db.find_one({"_id": global_key})

        if existing and (now_ts - existing["time"]) < GLOBAL_EXPIRY:
            locs = existing.get("locations", [])
            locs = [loc for loc in locs if loc[0] != cid]
            locs.append([cid, mid])
            await messages_db.update_one(
                {"_id": global_key},
                {"$set": {"locations": locs, "time": now_ts, "createdAt": now_dt}},
            )

            unique_chats = {loc[0] for loc in locs}
            if len(unique_chats) > 1:
                n_chats = len(unique_chats)
                for loc_cid, loc_mid in locs:
                    t_cfg = await get_config(loc_cid)
                    if t_cfg.get("global") is True:
                        mark_message_handled(loc_cid, loc_mid)
                        await delete_queue.put((loc_cid, [loc_mid]))
                        asyncio.create_task(insert_group_action_log(
                            loc_cid, "HAPUS",
                            f"Anti-duplikat gcast global – dikirim ke {n_chats} grup sekaligus",
                            uid, message.from_user.first_name or str(uid), content[:100],
                        ))
                        if loc_cid == cid:
                            asyncio.create_task(check_and_punish(
                                client, message, "anti-gcast global", content[:100]
                            ))
                        else:
                            asyncio.create_task(_gcast_punish_other_group(
                                client, loc_cid, uid, content[:100]
                            ))
        else:
            await messages_db.update_one(
                {"_id": global_key},
                {"$set": {
                    "time": now_ts, "createdAt": now_dt,
                    "locations": [[cid, mid]],
                }},
                upsert=True,
            )
