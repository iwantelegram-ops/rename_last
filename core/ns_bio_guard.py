"""
core/ns_bio_guard.py
────────────────────
Penegakan "Bio Admin Wajib" — fitur NewsCore.

KONSEP:
  Owner grup bisa set sebuah teks wajib (via panel NewsCore → Bio Admin
  Wajib). Admin yang DIANGKAT OTOMATIS oleh bot via NewsCore wajib memiliki
  teks itu di bio profil Telegram mereka (boleh ada teks lain juga, asal
  teks wajib itu ada sebagai substring).

  Jika admin NewsCore tidak memenuhi syarat ini saat bio mereka dicek
  (lewat typing di grup ATAU saat userbot bertemu mereka di voice chat —
  alur cek bio yang sudah ada, TIDAK ada loop berkala tambahan) →
  bot utama akan UNADMIN mereka. Pesan tidak dihapus, mic VC tidak dimute.
  Hanya berlaku untuk admin yang diangkat bot (newscore_admins), bukan
  admin manual/owner asli grup.

  Field bio_admin_text KOSONG (dan bio_admin_required masih True, status
  default) di suatu grup = dianggap "wajib tapi mustahil dipenuhi" →
  SEMUA admin NewsCore grup itu akan di-unadmin sampai owner mengisi
  teksnya. Ini adalah keputusan desain yang disengaja.

  Owner/admin (dengan hak "Ubah Info Grup") bisa menekan tombol
  "Kosongkan" di panel untuk mematikan syarat ini sepenuhnya
  (bio_admin_required=False) — saat itu admin NewsCore TIDAK diwajibkan
  punya teks apapun di bio, boleh kosong sekalipun.

DIPANGGIL DARI:
  - plugins/filters/bio.py        (setelah bio_filter / typing handler bot utama
                                    membaca hasil cek bio dari bot pemantau)
  - video_call.py                 (setelah userbot/bot pemantau cek bio user di VC)

CATATAN ARSITEKTUR:
  monitor_bot_reference.py (bot pemantau) HANYA menulis hasil teks-wajib
  (field admin_bio_ok) ke collection bio_profiles — ia TIDAK melakukan
  unadmin sendiri (bot pemantau tidak punya hak admin di grup).
  Modul inilah yang dipanggil oleh BOT UTAMA (client dengan hak admin)
  untuk benar-benar mengeksekusi unadmin & mengirim log.
"""

from __future__ import annotations

import os
import time
import asyncio
from html import escape as _html_escape
from datetime import datetime

from pyrogram.types import ChatPrivileges
from pyrogram.enums import ParseMode

from database import (
    ns_get_config, ns_get_current_admins, ns_remove_admin,
    insert_group_action_log, TZ_WIB,
)

LOG_CHANNEL = int(os.environ.get("LOG_CHANNEL", 0))

# Throttle ringan agar 1 user+grup tidak diproses berkali-kali dalam waktu
# singkat (mis. beberapa event typing beruntun sebelum unadmin pertama
# selesai diproses). Bukan pengganti TTL bio_profiles, hanya pengaman lokal.
_processing: set[tuple[int, int]] = set()
_recent_unadmin: dict[tuple[int, int], float] = {}
_UNADMIN_COOLDOWN = 30.0  # detik


def _text_ok(bio_text: str | None, required_text: str) -> bool:
    """
    True jika teks wajib ditemukan di bio (substring, case-insensitive,
    whitespace di kedua sisi diabaikan).

    Teks wajib kosong → SELALU False (sengaja — lihat docstring modul).
    """
    required = (required_text or "").strip()
    if not required:
        return False
    if not bio_text:
        return False
    return required.lower() in bio_text.lower()


async def check_admin_bio_text(chat_id: int, user_id: int, bio_text: str | None) -> bool | None:
    """
    Dipanggil oleh bot pemantau (monitor_bot_reference.py) setelah fetch bio.

    Return:
      True/False → user ini ADALAH admin NewsCore aktif di grup ini,
                   ini adalah hasil cek teks wajib (patuh / tidak patuh).
      None       → user ini BUKAN admin NewsCore aktif (tidak relevan,
                   tidak perlu disimpan/ditindaklanjuti).
    """
    try:
        ns_cfg = await ns_get_config(chat_id)
        if not ns_cfg.get("enabled"):
            return None

        ns_admins = await ns_get_current_admins(chat_id)
        if user_id not in {a["user_id"] for a in ns_admins}:
            return None

        # Syarat dikosongkan secara sengaja (tombol "Kosongkan") → admin
        # NewsCore TIDAK diwajibkan apapun di bio. Selalu dianggap patuh.
        if not ns_cfg.get("bio_admin_required", True):
            return True

        required_text = ns_cfg.get("bio_admin_text", "")
        return _text_ok(bio_text, required_text)
    except Exception as e:
        print(f"[NS-BioGuard] check_admin_bio_text error: {e}")
        return None


async def enforce_admin_bio(client, chat_id: int, user_id: int, admin_bio_ok) -> None:
    """
    Dipanggil oleh BOT UTAMA setelah membaca hasil admin_bio_ok dari
    bio_profiles (ditulis bot pemantau). Jika admin_bio_ok adalah False
    secara eksplisit (bukan None) → unadmin + log.

    admin_bio_ok:
      True  → patuh, tidak ada tindakan.
      False → TIDAK patuh, eksekusi unadmin.
      None  → tidak relevan / data belum ada, tidak ada tindakan.
    """
    if admin_bio_ok is not False:
        return

    key = (chat_id, user_id)
    if key in _processing:
        return

    now = time.time()
    last = _recent_unadmin.get(key, 0)
    if now - last < _UNADMIN_COOLDOWN:
        return

    _processing.add(key)
    try:
        # Re-verify: masih admin NewsCore aktif? (hindari race condition,
        # mis. sudah di-unadmin oleh proses lain / reset NewsCore terjadi
        # tepat di waktu yang sama)
        ns_admins = await ns_get_current_admins(chat_id)
        if user_id not in {a["user_id"] for a in ns_admins}:
            return

        ns_cfg        = await ns_get_config(chat_id)
        required_text = (ns_cfg.get("bio_admin_text") or "").strip()
        # CATATAN: fungsi ini hanya dipanggil saat admin_bio_ok eksplisit False.
        # Jika owner sudah menekan "Kosongkan" (bio_admin_required=False),
        # check_admin_bio_text() SELALU return True → fungsi ini tidak pernah
        # tereksekusi untuk kasus tersebut. Jadi di titik ini, required_text
        # kosong HANYA berarti "belum pernah diisi sama sekali" (default awal).

        # Ambil nama & info user untuk log
        user_name = str(user_id)
        try:
            u = await client.get_users(user_id)
            user_name = u.first_name or user_name
        except Exception:
            pass

        try:
            chat_title = str(chat_id)
            try:
                chat = await client.get_chat(chat_id)
                chat_title = chat.title or chat_title
            except Exception:
                pass

            await client.promote_chat_member(
                chat_id=chat_id,
                user_id=user_id,
                privileges=ChatPrivileges(can_manage_chat=False),
            )
        except Exception as e:
            print(f"[NS-BioGuard] gagal unadmin uid={user_id} chat={chat_id}: {e}")
            return

        # Hapus dari daftar admin NewsCore aktif
        await ns_remove_admin(chat_id, user_id)
        _recent_unadmin[key] = now

        reason = (
            "Bio admin wajib belum diisi oleh owner grup (teks kosong)"
            if not required_text
            else "Teks wajib tidak ditemukan di bio profil admin"
        )
        user_mention = f"<a href='tg://user?id={user_id}'>{_html_escape(user_name)}</a>"

        # ── Log ke group_action_log (Log Aktivitas panel grup) ─────────────
        try:
            await insert_group_action_log(
                chat_id, "UNADMIN",
                "Bio Admin Wajib — NewsCore",
                user_id,
                user_name,
                reason,
            )
        except Exception:
            pass

        # ── Log ke LOG_CHANNEL ──────────────────────────────────────────────
        if LOG_CHANNEL:
            waktu       = datetime.now(TZ_WIB).strftime("%d/%m/%Y %H:%M:%S WIB")
            req_snippet = _html_escape(required_text) if required_text else "<i>(belum diisi owner)</i>"
            log_text = (
                "<b>❖ BIO ADMIN WAJIB — NEWSCORE ❖</b>\n"
                "👮 <b>Admin Di-unadmin Otomatis</b>\n"
                "<blockquote>"
                f"◈ <b>User:</b> {user_mention} (<code>{user_id}</code>)\n"
                f"◈ <b>Grup:</b> {_html_escape(chat_title)} (<code>{chat_id}</code>)\n"
                f"◈ <b>Waktu:</b> {waktu}\n"
                f"◈ <b>Teks wajib:</b> <code>{req_snippet}</code>\n"
                f"◈ <b>Alasan:</b> {reason}"
                "</blockquote>"
            )
            try:
                await client.send_message(
                    LOG_CHANNEL, log_text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            except Exception as e:
                print(f"[NS-BioGuard] gagal kirim log channel: {e}")

        # ── Notifikasi singkat di grup (opsional, tanpa hapus pesan/mute) ──
        try:
            await client.send_message(
                chat_id,
                "🔻 <b>Admin diturunkan otomatis</b>\n"
                f"{user_mention} kehilangan status admin karena bio profil "
                "tidak memenuhi syarat teks wajib NewsCore.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

        print(f"[NS-BioGuard] uid={user_id} chat={chat_id} → UNADMIN ({reason})")
    finally:
        _processing.discard(key)
