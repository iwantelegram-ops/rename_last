"""
plugins/commands/newscore.py
────────────────────────────
Sistem Skor Keaktifan & Admin Otomatis (NewsCore).

Fitur:
  • Track setiap pesan member (non-admin) → tambah skor di MongoDB
  • Background worker → cek waktu reset, angkat admin otomatis
  • /ns_score  — lihat leaderboard grup (admin only)
  • /ns_reset  — paksa reset sekarang (owner only, dev/test)
"""

import asyncio
from datetime import datetime
from html import escape as _html_escape

from pyrogram import Client, filters
from pyrogram.types import Message, ChatPrivileges, ChatMemberUpdated
from pyrogram.enums import ParseMode, ChatMemberStatus
from pyrogram.errors import FloodWait

from database import (
    ns_get_config, ns_update, ns_calc_next_reset,
    ns_track_message, ns_get_leaderboard, ns_reset_scores,
    ns_get_current_admins, ns_set_current_admins,
    ns_get_active_user_count, ns_flush_score_buffer,
    ns_remove_score, invalidate_ns_admins_cache,
    HARI_MAP_NS, is_admin, TZ_WIB, delete_queue,
)
from plugins.ui.handlers_fsm import _truncate_to_utf16_limit
from core.member_tag import set_chat_member_tag

import os
_OWNER_ID = int(os.environ.get("OWNER_ID", 0))


# ─────────────────────────────────────────────────────────────────────────────
#  TRACK PESAN MEMBER (non-admin only)
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_message(filters.group & ~filters.service & ~filters.bot, group=15)
async def ns_track(client, message: Message):
    """
    Hitung skor hanya jika:
    - Pengirim bukan bot
    - Pengirim bukan admin/owner grup, KECUALI admin yang diangkat oleh
      bot ini melalui NewsCore periode sebelumnya (NS admin aktif)
    - Pesan bukan command
    - Pesan TIDAK dihapus oleh worker spam (antispam/bio/cas)
    """
    try:
        if not message.from_user or message.from_user.is_bot:
            return
        if message.text and message.text.startswith("/"):
            return

        chat_id = message.chat.id
        user_id = message.from_user.id

        cfg = await ns_get_config(chat_id)
        if not cfg.get("enabled"):
            return

        # Cek apakah user adalah admin di grup
        if await is_admin(client, chat_id, user_id):
            # Izinkan hanya jika dia adalah NS admin (diangkat bot via NewsCore)
            # Admin lain (manual/owner) tetap di-skip
            ns_admins = await ns_get_current_admins(chat_id)
            ns_admin_ids = {a["user_id"] for a in ns_admins}
            if user_id not in ns_admin_ids:
                return

        # Beri jeda kecil agar antispam/bio/cas sempat mark_message_handled
        await asyncio.sleep(0.35)

        # Jika sudah di-mark oleh worker penghapus → skip, tidak dihitung
        from database import is_message_handled
        if is_message_handled(chat_id, message.id):
            return

        await ns_track_message(
            chat_id=chat_id,
            user_id=user_id,
            user_name=message.from_user.first_name or "User",
        )
    except Exception as e:
        print(f"[NewsCore] track handler error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  LEADERBOARD COMMAND  /ns_score
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("ns_score") & filters.group, group=20)
async def cmd_ns_score(client, message: Message):
    try:
        chat_id = message.chat.id
        uid     = message.from_user.id if message.from_user else 0
        if not await is_admin(client, chat_id, uid):
            return

        cfg = await ns_get_config(chat_id)
        if not cfg.get("enabled"):
            rep = await message.reply_text(
                "⚠️ <b>NewsCore</b> belum diaktifkan di grup ini.\n"
                "Aktifkan via <b>⚙️ Kelola Grup → 🏆 NewsCore</b>.",
                parse_mode=ParseMode.HTML,
            )
            asyncio.create_task(_auto_del([message, rep], 10))
            return

        # Flush buffer dulu agar skor yang belum di-DB ikut tampil
        await ns_flush_score_buffer()
        top = await ns_get_leaderboard(chat_id, 10)
        total_aktif = await ns_get_active_user_count(chat_id)
        if not top:
            rep = await message.reply_text(
                "📭 Belum ada data keaktifan periode ini.",
                parse_mode=ParseMode.HTML,
            )
            asyncio.create_task(_auto_del([message, rep], 10))
            return

        lines = "".join(
            f"{i}. <b>{m['user_name']}</b> — <code>{m['score']}</code> poin\n"
            for i, m in enumerate(top, 1)
        )

        next_r = cfg.get("next_reset")
        next_str = ""
        if next_r:
            try:
                next_str = f"\n📅 Reset berikutnya: <code>{datetime.fromisoformat(next_r).strftime('%d %b %Y %H:%M')}</code> WIB"
            except Exception:
                pass

        rep = await message.reply_text(
            f"🏆 <b>PAPAN SKOR KEAKTIFAN</b>\n"
            f"<code>Grup: {chat_id}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👥 <b>Total user aktif periode ini:</b> <code>{total_aktif}</code>\n\n"
            f"{lines}"
            f"{next_str}",
            parse_mode=ParseMode.HTML,
        )
        asyncio.create_task(_auto_del([message, rep], 30))
    except Exception as e:
        print(f"[NewsCore] /ns_score error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  FORCE RESET COMMAND  /ns_reset  (owner only)
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("ns_reset") & filters.group, group=20)
async def cmd_ns_reset(client, message: Message):
    try:
        uid = message.from_user.id if message.from_user else 0
        if uid != _OWNER_ID:
            return
        await message.reply_text("⏳ Memulai simulasi reset NewsCore…")
        await ns_do_reset(client, message.chat.id)
    except Exception as e:
        print(f"[NewsCore] /ns_reset error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  CORE RESET JOB
# ─────────────────────────────────────────────────────────────────────────────

# Jeda (detik) antar setChatMemberTag agar tidak FloodWait.
# Juga dipakai antar promote_chat_member di ns_do_reset.
_NS_ACTION_DELAY = float(os.environ.get("NS_ACTION_DELAY", 0.5))


async def _apply_auto_title_member(
    client, chat_id: int, cfg: dict, admin_ids: set,
    *, base_delay: float = 0.0
) -> str:
    """
    Pasang tag otomatis ke member NON-admin berdasar rank leaderboard typing
    NewsCore, sesuai kelompok 5-rank per nama yang diisi owner (maks 10 nama
    -> cover rank 1-50). Dipanggil dari ns_do_reset(), terpisah dari logika
    pengangkatan admin di atas.

    admin_ids: kumpulan user_id yang BARU diangkat admin periode ini.
               Mereka di-exclude dari pemberian tag member karena:
               1. Mereka adalah admin (setChatMemberTag hanya untuk non-admin).
               2. Mereka sudah dapat custom_title via set_administrator_title.
               Juga termasuk NS admin LAMA yang masih admin (dari ns_get_current_admins)
               agar tidak salah pasang tag ke admin yang tidak tercabut.

    base_delay: offset delay awal (detik) untuk stagger antar grup saat reset
               berjalan bersamaan — cegah semua grup hit API di waktu sama.

    Returns ringkasan singkat (string) untuk disisipkan ke pengumuman reset,
    atau "" jika fitur tidak aktif / tidak ada nama diisi / tidak ada member
    yang memenuhi syarat.
    """
    if not cfg.get("auto_title_enabled", False):
        return ""

    names = [n for n in cfg.get("auto_title_names", []) if n and n.strip()]
    if not names:
        return ""

    # Butuh leaderboard sampai cover seluruh kelompok nama yang diisi
    # (maks 10 nama x 5 rank = 50), supaya rank terakhir tetap dapat tag
    # walau owner mengisi semua 10 slot.
    pool_size  = len(names) * 5
    full_board = await ns_get_leaderboard(chat_id, pool_size + len(admin_ids) + 10)

    # Saring member yang baru jadi admin periode ini DAN admin NS lama
    # (admin_ids sudah mencakup keduanya karena disiapkan di ns_do_reset).
    # Admin tidak boleh dapat tag member — Telegram API akan menolak.
    candidates = [w for w in full_board if w["user_id"] not in admin_ids][:pool_size]

    if not candidates:
        return ""

    ok_count, fail_count = 0, 0
    fail_samples = []

    # Jeda awal (stagger antar grup)
    if base_delay > 0:
        await asyncio.sleep(base_delay)

    for idx, w in enumerate(candidates):
        group_idx = idx // 5  # 0 = rank 1-5, 1 = rank 6-10, dst
        if group_idx >= len(names):
            break
        tag = _truncate_to_utf16_limit(names[group_idx], 16)
        uid = w["user_id"]

        # Jeda antar member untuk menghindari FloodWait setChatMemberTag
        if idx > 0:
            await asyncio.sleep(_NS_ACTION_DELAY)

        success, reason = await set_chat_member_tag(chat_id, uid, tag)
        if success:
            ok_count += 1
        else:
            fail_count += 1
            if len(fail_samples) < 3:
                fail_samples.append(f"{w.get('user_name', uid)}: {reason}")
            print(f"[NewsCore][AutoTitle] gagal uid={uid} tag={tag!r}: {reason}")

    if ok_count == 0 and fail_count == 0:
        return ""

    summary = f"\n\n🏷️ <b>Auto Title Member:</b> <code>{ok_count}</code> member ditandai otomatis."
    if fail_count:
        summary += (
            f"\n⚠️ <code>{fail_count}</code> gagal — kemungkinan bot belum "
            f"punya hak <code>can_manage_tags</code>."
        )
    return summary


# Semaphore global: batasi berapa grup yang diproses reset bersamaan.
# Default 2 → maks 2 grup reset paralel; sisanya antri.
# Cegah semua grup yg jadwal resetnya sama persis langsung memborbardir API.
_ns_reset_semaphore = asyncio.Semaphore(int(os.environ.get("NS_RESET_CONCURRENCY", 2)))

# Stagger offset per grup (detik) — diset saat checker_loop menemukan
# beberapa grup yang waktu resetnya sudah lewat di iterasi yang sama.
# {chat_id: offset_detik}
_ns_reset_stagger: dict[int, float] = {}


async def ns_do_reset(client, chat_id: int):
    """
    Angkat admin berdasarkan skor tertinggi, lalu reset semua skor.

    RATE-LIMIT SAFE:
    - Semaphore _ns_reset_semaphore membatasi reset paralel antar grup.
    - _NS_ACTION_DELAY jeda antar promote_chat_member / setChatMemberTag.
    - Auto Title Member exclude NS admin baru DAN admin NS lama (semua admin
      aktif saat ini) agar tidak mencoba pasang tag ke user yang admin.
    - base_delay (stagger) dipakai untuk offset auto title antar grup.
    """
    stagger = _ns_reset_stagger.pop(chat_id, 0.0)
    if stagger > 0:
        await asyncio.sleep(stagger)

    async with _ns_reset_semaphore:
        await _ns_do_reset_impl(client, chat_id)


async def _ns_do_reset_impl(client, chat_id: int):
    """Implementasi inti reset — hanya dipanggil via ns_do_reset (sudah ada semaphore)."""
    try:
        # Ambil config terbaru dari DB (bukan cache lama)
        cfg         = await ns_get_config(chat_id)
        max_admins  = cfg.get("max_admins", 1)
        p           = cfg.get("privileges", {})
        admin_title = (cfg.get("admin_title") or "").strip()

        # Flush buffer sebelum ambil leaderboard → skor terbaru masuk DB
        await ns_flush_score_buffer()
        top = await ns_get_leaderboard(chat_id, max_admins)

        # Ambil daftar admin NS lama (sebelum periode ini)
        old_admins     = await ns_get_current_admins(chat_id)
        old_admin_ids  = {a["user_id"] for a in old_admins}
        new_ids        = {m["user_id"] for m in top}

        # Copot admin lama yang tidak masuk top baru (+ jeda antar copot)
        for i, old in enumerate(old_admins):
            if old["user_id"] not in new_ids:
                if i > 0:
                    await asyncio.sleep(_NS_ACTION_DELAY)
                try:
                    await client.promote_chat_member(
                        chat_id=chat_id, user_id=old["user_id"],
                        privileges=ChatPrivileges(can_manage_chat=False),
                    )
                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 1)
                    try:
                        await client.promote_chat_member(
                            chat_id=chat_id, user_id=old["user_id"],
                            privileges=ChatPrivileges(can_manage_chat=False),
                        )
                    except Exception:
                        pass
                except Exception:
                    pass

        ann = "📢 <b>PERGANTIAN ADMIN NEWSCORE PERIODE BARU!</b> 📢\n\n"
        new_admin_docs = []

        if top:
            ann += f"🏆 <b>Top {len(top)} member teraktif:</b>\n\n"
            for idx, w in enumerate(top, 1):
                uid   = w["user_id"]
                uname = w["user_name"]

                # Jeda antar promosi admin (idx > 0 berarti bukan yang pertama)
                if idx > 1:
                    await asyncio.sleep(_NS_ACTION_DELAY)

                # Retry sekali jika kena FloodWait
                for _attempt in range(2):
                    try:
                        await client.promote_chat_member(
                            chat_id=chat_id, user_id=uid,
                            privileges=ChatPrivileges(
                                can_manage_chat=True,
                                can_delete_messages=p.get("can_delete_messages", True),
                                can_restrict_members=p.get("can_restrict_members", True),
                                can_invite_users=p.get("can_invite_users", True),
                                can_pin_messages=p.get("can_pin_messages", True),
                                can_manage_video_chats=p.get("can_manage_video_chats", False),
                            ),
                        )
                        title_ok = False
                        title    = admin_title if admin_title else f"Top Member {idx} 👑"
                        title    = _truncate_to_utf16_limit(title, 16)

                        # Jeda singkat sebelum set_administrator_title
                        # (Telegram butuh waktu catat status admin baru)
                        await asyncio.sleep(0.8)

                        for _title_attempt in range(3):
                            try:
                                await client.set_administrator_title(
                                    chat_id, uid, title
                                )
                                title_ok = True
                                break
                            except FloodWait as fw_title:
                                await asyncio.sleep(fw_title.value + 1)
                                continue
                            except Exception as e_title:
                                print(f"[NewsCore] set_custom_title gagal uid={uid} attempt={_title_attempt+1}: {e_title}")
                                await asyncio.sleep(1.5)
                                continue
                        if not title_ok:
                            print(f"[NewsCore] set_custom_title MENYERAH uid={uid} title={title!r}")
                        new_admin_docs.append({"chat_id": chat_id, "user_id": uid, "user_name": uname})
                        title_note = "" if title_ok else " (⚠️ titel gagal dipasang)"
                        ann += f"{idx}. <a href='tg://user?id={uid}'>{uname}</a> — <code>{w['score']}</code> poin{title_note}\n"
                        break
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 1)
                        continue
                    except Exception as e:
                        print(f"[NewsCore] promote error uid={uid}: {e}")
                        ann += f"{idx}. <b>{uname}</b> (⚠️ gagal dipromosikan)\n"
                        break
                else:
                    print(f"[NewsCore] promote uid={uid} gagal setelah retry FloodWait")
                    ann += f"{idx}. <b>{uname}</b> (⚠️ gagal dipromosikan — FloodWait)\n"
        else:
            ann += "Tidak ada aktivitas periode ini. Posisi admin tetap. 🏝️"

        # Syarat bio admin wajib
        if top:
            bio_admin_text     = (cfg.get("bio_admin_text") or "").strip()
            bio_admin_required = cfg.get("bio_admin_required", True)
            if bio_admin_required and bio_admin_text:
                ann += (
                    f"\n\n📝 <b>Wajib!</b> Admin di atas harus mencantumkan "
                    f"teks berikut di bio Telegram:\n"
                    f"<code>{_html_escape(bio_admin_text)}</code>\n"
                    f"<i>Bio tidak sesuai → otomatis di-unadmin.</i>"
                )
            elif bio_admin_required and not bio_admin_text:
                ann += (
                    f"\n\n⚠️ <b>Perhatian:</b> Syarat bio admin wajib aktif "
                    f"tapi teksnya belum diatur owner — admin di atas berisiko "
                    f"di-unadmin otomatis sampai diatur."
                )

        # Auto Title Member: exclude admin NS baru (new_ids) DAN admin NS lama
        # (old_admin_ids) yang belum dicabut — total admin aktif tidak boleh
        # dapat tag member (Telegram tolak setChatMemberTag pada admin).
        all_excluded = new_ids | old_admin_ids
        auto_title_summary = await _apply_auto_title_member(
            client, chat_id, cfg, all_excluded, base_delay=0.0
        )
        ann += auto_title_summary

        await ns_set_current_admins(chat_id, new_admin_docs)

        # Hitung next_reset dari config terbaru
        cfg_fresh = await ns_get_config(chat_id)
        new_next  = ns_calc_next_reset(cfg_fresh)
        await ns_update(chat_id, {"next_reset": new_next})

        ann += (
            f"\n\n🔄 <i>Poin direset ke 0!</i>\n"
            f"📅 Reset berikutnya: <code>{datetime.fromisoformat(new_next).strftime('%d %b %Y %H:%M')}</code> WIB"
        )

        try:
            await client.send_message(chat_id=chat_id, text=ann, parse_mode=ParseMode.HTML)
        except Exception as e:
            print(f"[NewsCore] send announcement error: {e}")

        # Reset skor SETELAH pengumuman dikirim (ns_reset_scores flush buffer lagi)
        await ns_reset_scores(chat_id)

    except Exception as e:
        print(f"[NewsCore] ns_do_reset error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  BACKGROUND TIME-CHECKER LOOP
# ─────────────────────────────────────────────────────────────────────────────

_checker_running = False


async def newscore_checker_loop(client):
    global _checker_running
    if _checker_running:
        return
    _checker_running = True
    print("[NewsCore] Time-checker loop started.")
    while True:
        try:
            from database import newscore_cfg_db
            all_cfgs = await newscore_cfg_db.find({"enabled": True}).to_list(length=200)
            now = datetime.now(TZ_WIB)

            # Kumpulkan semua grup yang waktunya reset di iterasi ini
            due_groups = []
            for cfg in all_cfgs:
                cid      = cfg.get("chat_id")
                next_str = cfg.get("next_reset")
                if cid and next_str:
                    try:
                        target = datetime.fromisoformat(next_str)
                        if target.tzinfo is None:
                            target = target.replace(tzinfo=TZ_WIB)
                        if now >= target:
                            due_groups.append(cid)
                    except Exception as e:
                        print(f"[NewsCore] checker parse error cid={cid}: {e}")

            if due_groups:
                # Stagger: grup pertama langsung, berikutnya dapat offset
                # _NS_ACTION_DELAY * 10 per grup → cegah flood serentak.
                stagger_unit = _NS_ACTION_DELAY * 10
                for i, cid in enumerate(due_groups):
                    offset = i * stagger_unit
                    if offset > 0:
                        _ns_reset_stagger[cid] = offset
                    print(f"[NewsCore] Reset terjadwal grup {cid} (stagger {offset:.1f}s)")
                    # Fire-and-forget: reset berjalan paralel tapi dibatasi
                    # semaphore _ns_reset_semaphore (maks NS_RESET_CONCURRENCY grup)
                    asyncio.create_task(ns_do_reset(client, cid))

        except Exception as e:
            print(f"[NewsCore] checker error: {e}")
        await asyncio.sleep(30)


# ─────────────────────────────────────────────────────────────────────────────
#  Helper
# ─────────────────────────────────────────────────────────────────────────────

async def _auto_del(msgs: list, delay: int):
    """
    Hapus pesan setelah `delay` detik via delete_queue (bukan loop direct delete).
    Pesan dikelompokkan per chat_id sehingga worker dapat mengirim
    1 delete_messages(cid, [...]) per chat — aman dari burst API.
    """
    await asyncio.sleep(delay)
    grouped: dict[int, list[int]] = {}
    for m in msgs:
        try:
            grouped.setdefault(m.chat.id, []).append(m.id)
        except Exception:
            pass
    for cid, mids in grouped.items():
        try:
            await delete_queue.put((cid, mids))
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
#  DETEKSI ADMIN PAKSA — hapus dari count NewsCore
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_chat_member_updated(group=16)
async def ns_watch_forced_admin(client: Client, update: ChatMemberUpdated):
    """
    Deteksi member yang di-adminkan PAKSA oleh owner/admin lain (bukan via
    NewsCore). Jika terdeteksi, hapus skor mereka dari newscore_stats agar:
    - Bot tidak mencoba meng-adminkan mereka lagi di periode berikutnya
      (mereka sudah admin, promote_chat_member akan gagal atau konflik hak)
    - Leaderboard tidak memasukkan mereka sebagai kandidat

    Logika deteksi "admin paksa":
      old_status = member biasa (MEMBER / RESTRICTED)
      new_status = ADMINISTRATOR
      user_id TIDAK ADA di daftar NS admin aktif (ns_get_current_admins)

    Jika user sudah ada di daftar NS admin → berarti ini adalah pengangkatan
    yang dilakukan oleh NewsCore sendiri → SKIP, jangan hapus skornya.

    group=16 → jalan setelah ns_track (group=15), tidak ada konflik.
    """
    try:
        if not update.new_chat_member or not update.old_chat_member:
            return

        new_status = update.new_chat_member.status
        old_status = update.old_chat_member.status

        # Hanya peduli: member biasa → admin
        was_admin = old_status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
        now_admin = new_status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)

        if was_admin or not now_admin:
            return  # bukan promosi baru → skip

        user = update.new_chat_member.user
        if not user or user.is_bot:
            return

        chat_id = update.chat.id
        user_id = user.id

        cfg = await ns_get_config(chat_id)
        if not cfg.get("enabled"):
            return

        # Cek apakah ini pengangkatan oleh NewsCore (ada di daftar NS admin)
        ns_admins    = await ns_get_current_admins(chat_id)
        ns_admin_ids = {a["user_id"] for a in ns_admins}

        if user_id in ns_admin_ids:
            # Diangkat oleh NewsCore sendiri → jangan hapus skor
            return

        # Admin paksa dari luar NewsCore → hapus dari count
        await ns_remove_score(chat_id, user_id)
        print(
            f"[NewsCore] uid={user_id} di-adminkan paksa di chat={chat_id} "
            f"(bukan via NewsCore) → skor dihapus dari count"
        )

    except Exception as e:
        print(f"[NewsCore] ns_watch_forced_admin error: {e}")

