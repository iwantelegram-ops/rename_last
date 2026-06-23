"""
core/group_notify.py
─────────────────────
Pengaman FloodWait untuk notifikasi otomatis ke GRUP (bukan LOG_CHANNEL).

LATAR BELAKANG:
LOG_CHANNEL rawan flood karena semua grup numpuk kirim ke 1 peer yang sama
(sudah ditangani via batching queue di plugins/commands/log.py).

Notifikasi ke grup (warn spam, mute, dll) jauh lebih aman karena tujuannya
beda-beda per grup — flood limit Telegram itu per-peer, jadi grup A dan
grup B tidak saling numpuk floodwait-nya.

NAMUN tetap ada 1 skenario nyata: RAID SERENTAK — banyak user BERBEDA
ngirim spam bersamaan di GRUP YANG SAMA. Tiap user baru trigger 1 notif
(warn/mute) ke peer grup yang sama dalam hitungan detik → bisa kena
FloodWait di grup itu juga.

SOLUSI DI SINI (bukan batching seperti LOG_CHANNEL):
  1. send_group_notice() — wrapper send_message dengan auto-retry SEKALI
     jika kena FloodWait singkat (<= MAX_AUTO_WAIT detik), supaya notif
     penting tidak hilang begitu saja. FloodWait yang lebih lama langsung
     di-drop (skip) — tidak menunda alur bot dengan menunggu lama.
  2. Cooldown ringan PER GRUP PER JENIS NOTIFIKASI: jika sudah ada notif
     sejenis terkirim ke grup itu dalam beberapa detik terakhir, notif baru
     yang sejenis di-skip (bukan mengantri) — mencegah spam notifikasi saat
     raid besar tanpa menunda respons individual yang masih dalam ambang wajar.

Ini sengaja TIDAK pakai queue/batch seperti LOG_CHANNEL karena notif ke grup
butuh tampil cepat (real-time warning), bukan dirapel.
"""

import time
import asyncio

from pyrogram.errors import FloodWait
from database import set_global_flood_backoff, wait_global_flood_backoff

# Cooldown per (chat_id, notice_kind) — mencegah notif sejenis membanjiri 1 grup
_NOTICE_COOLDOWN_SECONDS = 2.5
_MAX_AUTO_WAIT_SECONDS   = 5   # FloodWait di atas ini langsung di-skip, tidak ditunggu

_last_notice_ts: dict[tuple[int, str], float] = {}
_cooldown_lock = asyncio.Lock()


async def _allowed_by_cooldown(chat_id: int, notice_kind: str) -> bool:
    """True jika notif jenis ini boleh dikirim ke grup ini sekarang (belum cooldown)."""
    key = (chat_id, notice_kind)
    now = time.monotonic()
    async with _cooldown_lock:
        last = _last_notice_ts.get(key, 0.0)
        if now - last < _NOTICE_COOLDOWN_SECONDS:
            return False
        _last_notice_ts[key] = now
        return True


async def send_group_notice(client, chat_id: int, text: str, notice_kind: str, **kwargs):
    """
    Kirim notifikasi otomatis ke grup dengan pengaman flood.

    notice_kind: label pendek pembeda jenis notif (mis. "warn_dup", "mute"),
                 dipakai untuk cooldown per grup — bukan untuk membedakan isi pesan.

    Return Message jika terkirim, None jika di-skip (cooldown atau FloodWait lama).
    Tidak pernah raise — aman dipanggil dari fire-and-forget task.

    KOORDINASI LINTAS WORKER: cek global flood backoff sebelum send —
    mundur jika delete_worker / moderation_worker / log_worker baru kena FloodWait.
    """
    if not await _allowed_by_cooldown(chat_id, notice_kind):
        return None

    # Mundur jika worker lain baru saja kena FloodWait
    await wait_global_flood_backoff()

    try:
        return await client.send_message(chat_id, text, **kwargs)
    except FloodWait as e:
        set_global_flood_backoff(e.value)   # beritahu worker lain
        if e.value <= _MAX_AUTO_WAIT_SECONDS:
            await asyncio.sleep(e.value)
            try:
                return await client.send_message(chat_id, text, **kwargs)
            except Exception as e2:
                print(f"[group_notify] retry gagal chat={chat_id}: {e2}")
                return None
        else:
            print(f"[group_notify] FloodWait {e.value}s terlalu lama, notif di-skip "
                  f"(chat={chat_id}, kind={notice_kind}).")
            return None
    except Exception as e:
        print(f"[group_notify] gagal kirim notif chat={chat_id}: {e}")
        return None
