"""
core/moderation_queue.py
─────────────────────────
Worker antrian untuk aksi moderasi: MUTE, UNMUTE, BAN.

LATAR BELAKANG:
Tidak seperti delete_messages (bisa digabung banyak message_id jadi 1 API
call), restrict_chat_member (mute/unmute) dan ban_chat_member WAJIB 1 API
call per user — tidak bisa dirapel jadi 1 panggilan untuk banyak user.

Jika banyak aksi ini terjadi BERSAMAAN (misal raid: 10 user kena mute
otomatis hampir di waktu yang sama, atau CAS mem-ban beberapa spammer
beruntun), tembakan beruntun ke admin API (restrict/ban) berisiko kena
FloodWait — beda dari notifikasi pesan, ini operasi admin yang biasanya
limit-nya lebih ketat.

SOLUSI:
Semua pemanggil tidak lagi memanggil client.ban_chat_member /
restrict_chat_member langsung. Sebagai gantinya mereka panggil
queue_mute() / queue_unmute() / queue_ban() — aksi masuk ke antrian dan
dieksekusi SATU PER SATU oleh moderation_worker_loop() dengan jeda kecil
(MOD_ACTION_DELAY) antar aksi, sehingga tidak pernah ada 2 aksi moderasi
ditembak dalam waktu yang sama persis.

Jika 1 aksi kena FloodWait, worker tidur sesuai durasi lalu retry sekali —
tidak menjatuhkan aksi tersebut maupun aksi lain yang masih di antrian.

KOORDINASI LINTAS WORKER:
Saat kena FloodWait, worker memanggil set_global_flood_backoff() dari
database.py — memberi tahu delete_worker dan log_flush_worker untuk mundur
selama durasi yang sama. Sebaliknya, sebelum eksekusi aksi, worker ini
memanggil wait_global_flood_backoff() untuk mengalah jika worker lain sudah
lebih dulu kena FloodWait.
"""

import asyncio
from datetime import datetime, timedelta, timezone

from pyrogram.errors import ChatAdminRequired, UserAdminInvalid, FloodWait
from pyrogram.types import ChatPermissions
from database import set_global_flood_backoff, wait_global_flood_backoff

MOD_ACTION_DELAY = 0.8  # detik jeda antar aksi moderasi (mute/unmute/ban)
MAX_AUTO_WAIT     = 10   # FloodWait di atas ini tidak ditunggu, langsung skip

moderation_queue: asyncio.Queue = asyncio.Queue()


async def queue_mute(chat_id: int, user_id: int, duration_seconds: int, on_done=None) -> None:
    """Antrikan aksi mute. on_done(success: bool) dipanggil setelah eksekusi (opsional)."""
    await moderation_queue.put(("mute", chat_id, user_id, duration_seconds, on_done))


async def queue_unmute(chat_id: int, user_id: int, on_done=None) -> None:
    """Antrikan aksi unmute (buka kembali izin kirim pesan)."""
    await moderation_queue.put(("unmute", chat_id, user_id, None, on_done))


async def queue_ban(chat_id: int, user_id: int, on_done=None) -> None:
    """Antrikan aksi ban permanen."""
    await moderation_queue.put(("ban", chat_id, user_id, None, on_done))


_UNRESTRICTED_PERMISSIONS = ChatPermissions(
    can_send_messages=True,
    can_send_media_messages=True,
    can_send_polls=True,
    can_send_other_messages=True,
    can_add_web_page_previews=True,
    can_change_info=False,
    can_invite_users=True,
    can_pin_messages=False,
)

_MUTED_PERMISSIONS = ChatPermissions(
    can_send_messages=False,
    can_send_media_messages=False,
    can_send_polls=False,
    can_send_other_messages=False,
    can_add_web_page_previews=False,
    can_change_info=False,
    can_invite_users=False,
    can_pin_messages=False,
)


async def _run_once(client, kind: str, chat_id: int, user_id: int, extra) -> None:
    if kind == "mute":
        until_dt = datetime.now(timezone.utc) + timedelta(seconds=extra)
        await client.restrict_chat_member(chat_id, user_id, _MUTED_PERMISSIONS, until_date=until_dt)
    elif kind == "unmute":
        await client.restrict_chat_member(chat_id, user_id, _UNRESTRICTED_PERMISSIONS)
    elif kind == "ban":
        await client.ban_chat_member(chat_id, user_id)


async def _execute_action(client, kind: str, chat_id: int, user_id: int, extra) -> bool:
    """Jalankan 1 aksi moderasi. Return True jika berhasil."""
    # Tunggu jika worker lain (delete/log) sudah lebih dulu kena FloodWait
    await wait_global_flood_backoff()
    try:
        await _run_once(client, kind, chat_id, user_id, extra)
        return True
    except (ChatAdminRequired, UserAdminInvalid):
        return False
    except FloodWait as e:
        # Catat ke global backoff — worker lain akan ikut mundur
        set_global_flood_backoff(e.value)
        if e.value <= MAX_AUTO_WAIT:
            await asyncio.sleep(e.value)
            try:
                await _run_once(client, kind, chat_id, user_id, extra)
                return True
            except Exception as e2:
                print(f"[moderation_queue] retry {kind} gagal chat={chat_id} user={user_id}: {e2}")
                return False
        else:
            print(f"[moderation_queue] FloodWait {e.value}s terlalu lama untuk {kind} "
                  f"(chat={chat_id}, user={user_id}) — di-skip.")
            return False
    except Exception as e:
        print(f"[moderation_queue] gagal {kind} chat={chat_id} user={user_id}: {e}")
        return False


async def moderation_worker_loop(client) -> None:
    """
    Worker tunggal yang mengeksekusi aksi moderasi (mute/unmute/ban) satu per
    satu dengan jeda MOD_ACTION_DELAY antar aksi — mencegah tembakan beruntun
    ke Telegram admin API saat banyak pelanggaran/raid terjadi bersamaan.

    Dijalankan sekali sebagai background task (lihat antigcast.py), mengikuti
    pola delete_worker / panel_write_worker yang sudah ada.
    """
    # Tunggu sampai client benar-benar terkoneksi
    for _ in range(60):
        if getattr(client, "is_connected", False):
            break
        await asyncio.sleep(1.0)

    while True:
        try:
            kind, chat_id, user_id, extra, on_done = await moderation_queue.get()
        except asyncio.CancelledError:
            break

        try:
            success = await _execute_action(client, kind, chat_id, user_id, extra)
            if on_done is not None:
                try:
                    res = on_done(success)
                    if asyncio.iscoroutine(res):
                        await res
                except Exception as e:
                    print(f"[moderation_queue] on_done callback error: {e}")
        finally:
            moderation_queue.task_done()

        # Jeda antar aksi — inti dari pengaman flood di sini
        await asyncio.sleep(MOD_ACTION_DELAY)
