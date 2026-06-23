"""
core/punishment.py
──────────────────
Sistem Hukuman Terpusat — berlaku untuk SEMUA jenis pelanggaran spam.

CARA KERJA:
  • Setiap deteksi spam (apapun jenisnya) memanggil check_and_punish().
  • Setelah 10 pelanggaran berturut-turut di grup yang sama → mute 5 menit.
  • Jika user masih spam setelah muted lagi → durasi 2× lipat (10, 20, 40, ... menit).
  • Setelah mute habis: hitungan spam TETAP di angka 10 (tidak direset ke 0),
    sehingga 1 pelanggaran berikutnya langsung memicu mute level berikutnya.
    Restart bot (Termux mati/hidup) tidak mereset hitungan karena data tersimpan
    persisten di database.
  • Pesan bersih (lolos semua filter, group=10) → reset hitungan + level hukuman.
  • Berlaku per user per grup — tidak campur antar grup.
  • Gcast: hanya grup yang mengaktifkan global detection yang menghitung punishment.

API Publik:
  check_and_punish(client, message, spam_type, konten) → bool
    Tambah hitungan. Terapkan mute jika ambang tercapai.
    Return True jika mute diterapkan, False jika belum/sudah muted.
"""

import os
import asyncio
import time
from datetime import datetime, timedelta, timezone

from pyrogram.enums import ParseMode

from database import (
    get_local_mute, increment_local_spam, apply_local_mute,
    revert_failed_local_mute, auto_delete_reply, insert_group_action_log, TZ_WIB,
)
from core.group_notify import send_group_notice
from core.moderation_queue import queue_mute

LOG_CHANNEL         = int(os.environ.get("LOG_CHANNEL", 0))
SPAM_MUTE_THRESHOLD = 10   # Jumlah pelanggaran sebelum mute diterapkan


async def check_and_punish(
    client,
    message,
    spam_type: str,
    konten: str = "",
) -> bool:
    """
    Dipanggil oleh setiap filter setelah mendeteksi spam.
    Menambah hitungan pelanggaran berturut-turut per user per grup.
    Jika mencapai ambang (10) → antrikan mute (lihat core/moderation_queue.py
    — aksi mute dieksekusi oleh worker terpisah, BUKAN langsung di sini, agar
    banyak mute yang terjadi bersamaan saat raid tidak ditembak serentak ke
    Telegram API dan memicu FloodWait).

    Return True jika mute BERHASIL DIANTRIKAN (bukan berarti sudah dieksekusi
    — eksekusi & notifikasi terjadi async di moderation_worker_loop).
    False jika belum mencapai ambang atau masih dalam masa mute aktif.
    """
    cid    = message.chat.id
    uid    = message.from_user.id
    now_ts = time.time()

    mute_rec = await get_local_mute(cid, uid)

    # Jika user masih dalam masa mute → jangan tambah hitungan / mute lagi
    if mute_rec.get("muted_until", 0.0) > now_ts:
        return False

    updated = await increment_local_spam(cid, uid)
    consec  = updated.get("consec_spam", 1)

    if consec < SPAM_MUTE_THRESHOLD:
        return False

    # Ambang tercapai → terapkan mute eskalasi
    duration_secs, level_before = await apply_local_mute(cid, uid)
    duration_min                = duration_secs // 60

    async def _on_mute_done(success: bool):
        if not success:
            # FIXED: muted_until sudah ditulis oleh apply_local_mute() di atas
            # SEBELUM tahu hasil eksekusi API. Jika API gagal (bot bukan admin,
            # kehilangan izin restrict, dll), state mute palsu itu HARUS
            # dirollback — supaya pesan user berikutnya tidak terus-menerus
            # dihapus berdasarkan status mute yang sebenarnya tidak pernah
            # terjadi di Telegram.
            await revert_failed_local_mute(cid, uid, level_before)
            # Peringatkan admin/owner lewat LOG_CHANNEL — sebelumnya kegagalan
            # mute diam-diam saja tanpa sinyal apapun.
            asyncio.create_task(_log_mute_failed(client, message, spam_type))
            return

        # Beri tahu grup (pesan singkat, hapus 10 detik)
        notif = await send_group_notice(
            client, cid,
            f"{message.from_user.mention} di-mute {duration_min} menit "
            f"karena {spam_type} berulang.",
            notice_kind="mute",
            parse_mode=ParseMode.HTML,
        )
        if notif is not None:
            asyncio.create_task(auto_delete_reply([notif], delay=10))

        # Log ke channel + per-grup action log (non-blocking)
        asyncio.create_task(_log_mute(
            client, message, duration_min, cid, uid, spam_type, konten
        ))

    await queue_mute(cid, uid, duration_secs, on_done=_on_mute_done)
    return True


async def _log_mute_failed(client, message, spam_type: str) -> None:
    """
    Peringatkan owner/admin via LOG_CHANNEL saat eksekusi mute API gagal
    (biasanya karena bot bukan admin grup atau kehilangan izin restrict).
    Tanpa ini, kegagalan mute tidak pernah terlihat oleh siapa pun.
    """
    from plugins.commands.log import _send_log

    uid          = message.from_user.id
    cid          = message.chat.id
    user_mention = f"<a href='tg://user?id={uid}'>{message.from_user.first_name}</a>"
    waktu        = datetime.now(TZ_WIB).strftime("%d/%m/%Y %H:%M:%S WIB")

    log_text = (
        "<b>❖ ANTI-SPAM — MUTE GAGAL ❖</b>\n"
        "⚠️ <b>Bot Tidak Bisa Mute User</b>\n"
        "<blockquote>"
        f"◈ <b>User:</b> {user_mention} (<code>{uid}</code>)\n"
        f"◈ <b>Grup:</b> {message.chat.title} (<code>{cid}</code>)\n"
        f"◈ <b>Waktu:</b> {waktu}\n"
        f"◈ <b>Alasan Pelanggaran:</b> {spam_type} — 10× berturut-turut\n"
        f"◈ <b>Kemungkinan Sebab:</b> Bot bukan admin / tidak punya izin "
        f"restrict member di grup ini.\n\n"
        f"<i>Pesan user TIDAK dianggap dalam masa mute — silakan cek izin admin bot di grup ini.</i>"
        "</blockquote>"
    )
    await _send_log(client, log_text)


async def _log_mute(
    client,
    message,
    duration_min: int,
    cid: int,
    uid: int,
    spam_type: str,
    konten: str,
) -> None:
    """Log aksi mute ke group action log dan LOG_CHANNEL."""
    user_name = message.from_user.first_name or str(uid)

    try:
        await insert_group_action_log(
            cid, "MUTE",
            f"Mute {duration_min} menit – {spam_type} 10× berturut-turut",
            uid, user_name, konten,
        )
    except Exception:
        pass

    if not LOG_CHANNEL:
        return

    waktu        = datetime.now(TZ_WIB).strftime("%d/%m/%Y %H:%M:%S WIB")
    user_mention = f"<a href='tg://user?id={uid}'>{user_name}</a>"

    log_text = (
        "<b>❖ ANTI-SPAM — MUTE DITERAPKAN ❖</b>\n"
        "🔇 <b>User Di-Mute Otomatis</b>\n"
        "<blockquote>"
        f"◈ <b>User:</b> {user_mention} (<code>{uid}</code>)\n"
        f"◈ <b>Grup:</b> {message.chat.title} (<code>{cid}</code>)\n"
        f"◈ <b>Waktu:</b> {waktu}\n"
        f"◈ <b>Durasi:</b> {duration_min} menit\n"
        f"◈ <b>Alasan:</b> {spam_type} — 10× berturut-turut\n\n"
        f"<b>Konten:</b> <code>{konten[:300]}</code>"
        "</blockquote>"
    )
    from plugins.commands.log import _send_log
    await _send_log(client, log_text)
