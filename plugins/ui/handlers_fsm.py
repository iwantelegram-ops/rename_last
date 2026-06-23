"""
plugins/ui/handlers_fsm.py
──────────────────────────
Menangkap input teks di DM setelah user masuk mode FSM:
  - Input pola regex baru
  - Input ID untuk whitelist CAS
  - Input ID untuk Member VIP
  - Perintah /batal

Bug fix:
  - Setiap handler pop state + cancel task sebelum proses
  - Validasi ketat sebelum edit message (msg_id mungkin stale)
  - Semua Exception tertangkap, tidak ada yang menyebabkan crash
  - /batal selalu clear semua FSM sekaligus
"""

import asyncio
import re
import unicodedata
from html import escape as _html_escape
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.enums import ParseMode
from pyrogram.errors import MessageNotModified, MessageIdInvalid, FloodWait

from database import db, invalidate_count_cache, update_config as _db_update_config
from plugins.ui.pages import (
    page_regex_list, page_regex_tutorial,
    page_whitelist_text, page_free_list,
    page_cas_panel, page_bio_panel,
)
from plugins.ui.fsm_state import (
    pending_regex_state, pending_free_state, pending_wl_state,
    pending_bio_vip_state,
    clear_all_fsm, _cancel_task,
)
from core.regex_utils import _build_group_interlock, generate_kandidat_mutasi_liar, pipeline_pembersihan
import admin_session as _adm_sess

group_regex_db = db["regex_per_group"]
whitelist_col  = db["whitelist_per_group"]
free_col       = db["free_per_group"]


async def _safe_edit_id(client, chat_id, msg_id, text, keyboard=None):
    """
    Edit pesan via ID. Gagal silent jika pesan sudah tidak relevan.
    FloodWait: retry 1x setelah tunggu (maks 10 detik) — FSM panel bisa
    diklik berurutan cepat oleh admin yang sama.
    """
    async def _do_edit():
        await client.edit_message_text(
            chat_id=chat_id,
            message_id=msg_id,
            text=text,
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML,
        )
    try:
        await _do_edit()
    except (MessageNotModified, MessageIdInvalid):
        pass
    except FloodWait as fw:
        wait = min(fw.value, 10)
        await asyncio.sleep(wait)
        try:
            await _do_edit()
        except Exception as e2:
            print(f"[_safe_edit_id] retry gagal: {e2}")
    except Exception as e:
        print(f"[_safe_edit_id] {e}")


def _split_graphemes(s: str) -> list:
    """
    Pecah string jadi grapheme cluster sederhana: tiap base character beserta
    semua combining mark (diakritik) yang menyertainya dihitung sebagai SATU
    unit visual. Dipakai untuk menampilkan "jumlah karakter terlihat" yang
    akurat untuk font unik (mis. ᴠͥɪͣᴘͫ) yang sebenarnya terdiri dari huruf
    dasar + combining mark Unicode (bukan 1 codepoint per huruf).

    Tidak butuh library eksternal (`regex`) — cukup unicodedata.combining().
    """
    clusters = []
    cur = ""
    for ch in s:
        if unicodedata.combining(ch) and cur:
            cur += ch
        else:
            if cur:
                clusters.append(cur)
            cur = ch
    if cur:
        clusters.append(cur)
    return clusters


def _utf16_units(s: str) -> int:
    """
    Hitung panjang string dalam UTF-16 code units — ini satuan yang
    sebenarnya dipakai Telegram untuk membatasi custom title admin (maks 16).
    Karakter di luar BMP (banyak dipakai di font unik seperti Mathematical
    Bold 𝐕𝐈𝐏, Fraktur, dll) makan 2 code unit per karakter meski cuma
    1 codepoint Python — jadi tidak bisa diukur dengan len() biasa.
    """
    return len(s.encode("utf-16-le")) // 2


def _truncate_to_utf16_limit(s: str, limit: int) -> str:
    """
    Potong string ke batas UTF-16 `limit` tanpa merusak grapheme cluster —
    tidak akan memotong base character lepas dari combining mark-nya.
    """
    out, total = "", 0
    for c in _split_graphemes(s):
        c_len = _utf16_units(c)
        if total + c_len > limit:
            break
        out += c
        total += c_len
    return out


# ─────────────────────────────────────────────────────────────────────────────
#  Tangkap semua teks di DM (non-command) → routing ke FSM aktif
# ─────────────────────────────────────────────────────────────────────────────
@Client.on_message(filters.private & filters.text & ~filters.command(["start", "batal", "antigcast"]))
async def handle_fsm_input(client, message: Message):
    user_id = message.from_user.id

    regex_state = pending_regex_state.get(user_id)
    if regex_state:
        await _handle_regex_input(client, message, user_id, regex_state)
        return

    free_state = pending_free_state.get(user_id)
    if free_state:
        await _handle_free_input(client, message, user_id, free_state)
        return

    wl_state = pending_wl_state.get(user_id)
    if wl_state:
        await _handle_wl_input(client, message, user_id, wl_state)
        return

    bio_vip_st = pending_bio_vip_state.get(user_id)
    if bio_vip_st:
        await _handle_bio_vip_input(client, message, user_id, bio_vip_st)
        return

    # ── NewsCore FSM ──────────────────────────────────────────────────────────
    from plugins.ui.handlers_dm import _ns_fsm
    if user_id in _ns_fsm:
        await _handle_ns_fsm_input(client, message, user_id)
        return


# ─────────────────────────────────────────────────────────────────────────────
#  Handler regex FSM
# ─────────────────────────────────────────────────────────────────────────────
async def _handle_regex_input(client, message: Message, user_id: int, state: dict):
    # Simpan raw_asli dengan kapital utuh — JANGAN .lower() di sini
    # karena kapital dari owner dipakai sebagai penanda posisi wajib di generator
    raw_asli = unicodedata.normalize("NFKC", message.text.strip())
    raw      = raw_asli.lower()  # versi lowercase hanya untuk _build_group_interlock
    chat_id  = state["chat_id"]
    msg_id   = state["msg_id"]

    _cancel_task(pending_regex_state.pop(user_id, None))

    try:
        pola, kata_list = _build_group_interlock(raw)
        re.compile(pola)
    except (ValueError, re.error) as e:
        err = await message.reply(
            f"❌ <b>ERROR</b>\n\n"
            f"Input tidak dikenali:\n<code>{raw}</code>\n"
            f"<b>Keterangan:</b> <code>{e}</code>\n\n"
            f"<i>Contoh: <code>togel</code> atau <code>jual | akun</code></i>",
            parse_mode=ParseMode.HTML,
        )
        await asyncio.sleep(6)
        try:
            await err.delete()
            await message.delete()
        except Exception:
            pass
        return

    raw_display = " | ".join(kata_list) if kata_list else raw

    # Pisahkan kata dari raw_asli agar kapital tetap terjaga
    # raw_asli bisa "bAkSo | lonTOng" → split | → ["bAkSo", "lonTOng"]
    kata_asli_list = [k.strip() for k in raw_asli.split("|") if k.strip()]

    mutasi_map: dict = {}
    for i, kata in enumerate(kata_list):
        # Ambil versi asli (dengan kapital) jika tersedia
        kata_dengan_kapital = kata_asli_list[i] if i < len(kata_asli_list) else kata
        # Bersihkan simbol tapi JANGAN lowercase — kapital harus sampai ke generator
        import re as _re
        kata_bersih_asli = _re.sub(r"\(?[×xX]\d+\)?", "", kata_dengan_kapital)
        kata_bersih_asli = _re.sub(r"[^\w]", "", kata_bersih_asli).strip().split()[0] if kata_bersih_asli.strip() else ""
        if kata_bersih_asli:
            mutasi_map[kata] = generate_kandidat_mutasi_liar(kata_bersih_asli)

    await group_regex_db.update_one(
        {"chat_id": chat_id, "pattern": pola},
        {"$set": {
            "chat_id":   chat_id,
            "pattern":   pola,
            "pola":      pola,
            "raw":       raw_display,
            "kata_list": kata_list,
            "mutasi":    mutasi_map,
        }},
        upsert=True,
    )

    try:
        from plugins.filters.antispam import invalidate_local_regex_cache
        invalidate_local_regex_cache(chat_id)
    except Exception:
        pass
    invalidate_count_cache(chat_id)  # refresh jumlah filter di panel

    text, keyboard = await page_regex_list(chat_id, 1)
    kata_str = " + ".join(f"<code>{k}</code>" for k in kata_list) if kata_list else f"<code>{raw}</code>"
    header = (
        f"✅ <b>Filter Kata Berhasil Ditambahkan!</b>\n"
        f"◈ <b>Kata Kunci:</b> {kata_str}\n"
        f"◈ <b>Deteksi mutasi otomatis aktif</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    )
    await _safe_edit_id(client, message.chat.id, msg_id, header + text, keyboard)

    try:
        await message.delete()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
#  Handler free/VIP FSM
# ─────────────────────────────────────────────────────────────────────────────
async def _handle_free_input(client, message: Message, user_id: int, state: dict):
    raw = message.text.strip()
    if not raw.isdigit():
        return

    target_id = int(raw)
    chat_id   = state["chat_id"]
    msg_id    = state["msg_id"]

    _cancel_task(pending_free_state.pop(user_id, None))

    await free_col.update_one(
        {"user_id": target_id, "chat_id": chat_id},
        {"$set": {"user_id": target_id, "chat_id": chat_id}},
        upsert=True,
    )
    # Invalidasi cache VIP agar /unmutemic langsung mengenali status VIP baru.
    try:
        from video_call import invalidate_vip_cache
        invalidate_vip_cache(chat_id, target_id)
    except ImportError:
        pass
    invalidate_count_cache(chat_id)  # refresh jumlah VIP di panel

    text, keyboard = await page_free_list(chat_id)
    header = (
        f"✅ <b>User <code>{target_id}</code> berhasil dijadikan Member VIP!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    )
    await _safe_edit_id(client, message.chat.id, msg_id, header + text, keyboard)

    try:
        await message.delete()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
#  Handler whitelist CAS FSM
# ─────────────────────────────────────────────────────────────────────────────
async def _handle_wl_input(client, message: Message, user_id: int, state: dict):
    raw = message.text.strip()
    if not raw.lstrip("-").isdigit():
        err = await message.reply(
            "❌ <b>ID TIDAK VALID</b>\n\n"
            "System hanya menerima angka numerik Telegram.\n"
            "Contoh valid: <code>123456789</code>",
            parse_mode=ParseMode.HTML,
        )
        await asyncio.sleep(5)
        try:
            await err.delete()
            await message.delete()
        except Exception:
            pass
        return

    target_id = int(raw)
    action    = state["action"]
    chat_id   = state["chat_id"]
    msg_id    = state["msg_id"]

    _cancel_task(pending_wl_state.pop(user_id, None))

    if action == "wl":
        await whitelist_col.update_one(
            {"user_id": target_id, "chat_id": chat_id},
            {"$set": {"status": "whitelisted"}},
            upsert=True,
        )
        result_text = (
            f"✅ <b>Otorisasi Whitelist Diterima!</b>\n"
            f"◈ <b>User ID:</b> <code>{target_id}</code> telah dikecualikan."
        )
    else:
        res = await whitelist_col.delete_one({"user_id": target_id, "chat_id": chat_id})
        result_text = (
            f"🗑️ <b>Whitelist Berhasil Dicabut!</b>\n"
            f"◈ <b>User ID:</b> <code>{target_id}</code> akan kembali dipantau."
        ) if res.deleted_count else (
            f"❌ <b>Data Tidak Ditemukan!</b>\n"
            f"ID <code>{target_id}</code> tidak terdaftar di sistem pengecualian."
        )

    wl_text = await page_whitelist_text(chat_id)
    await _safe_edit_id(
        client, message.chat.id, msg_id,
        f"{result_text}\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n{wl_text}",
        InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙  Kembali ke CAS Panel", callback_data=f"cas_panel_{chat_id}")]
        ]),
    )

    try:
        await message.delete()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
#  /batal — batalkan FSM aktif manapun
# ─────────────────────────────────────────────────────────────────────────────
@Client.on_message(filters.command("batal") & filters.private)
async def cancel_fsm(client, message: Message):
    user_id = message.from_user.id

    regex_state = pending_regex_state.get(user_id)
    free_state  = pending_free_state.get(user_id)
    wl_state    = pending_wl_state.get(user_id)

    clear_all_fsm(user_id)

    if regex_state:
        chat_id = regex_state["chat_id"]
        msg_id  = regex_state["msg_id"]
        text, keyboard = await page_regex_tutorial(chat_id)
        await _safe_edit_id(
            client, message.chat.id, msg_id,
            "✅ <b>Operasi Dibatalkan.</b>\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n" + text,
            keyboard
        )

    elif free_state:
        chat_id = free_state["chat_id"]
        msg_id  = free_state["msg_id"]
        text, keyboard = await page_free_list(chat_id)
        await _safe_edit_id(
            client, message.chat.id, msg_id,
            "✅ <b>Operasi Dibatalkan.</b>\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n" + text,
            keyboard
        )

    elif wl_state:
        chat_id = wl_state["chat_id"]
        msg_id  = wl_state["msg_id"]
        text, keyboard = await page_cas_panel(chat_id)
        await _safe_edit_id(
            client, message.chat.id, msg_id,
            "✅ <b>Operasi Dibatalkan.</b>\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n" + text,
            keyboard
        )

    else:
        res = await message.reply(
            "ℹ️ <b>Sistem:</b> Tidak ada sesi operasi aktif.",
            parse_mode=ParseMode.HTML
        )
        await asyncio.sleep(3)
        try:
            await res.delete()
        except Exception:
            pass

    try:
        await message.delete()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
#  Handler Bio VIP text FSM
# ─────────────────────────────────────────────────────────────────────────────
async def _handle_bio_vip_input(client, message: Message, user_id: int, state: dict):
    """
    Terima input teks VIP bio dari admin.
    Teks ini disimpan ke config grup — jika ditemukan di bio user (case-insensitive),
    user dianggap VIP dan bebas dari seluruh pengecekan bot di grup tersebut.
    """
    raw     = message.text.strip()
    chat_id = state["chat_id"]
    msg_id  = state["msg_id"]

    _cancel_task(pending_bio_vip_state.pop(user_id, None))

    # Validasi minimal
    if not raw or len(raw) > 200:
        err = await message.reply(
            "❌ <b>Teks tidak valid.</b>\n\n"
            "Teks harus antara 1–200 karakter.\n"
            "<i>Coba lagi dari Bio Panel.</i>",
            parse_mode=ParseMode.HTML,
        )
        await asyncio.sleep(5)
        try:
            await err.delete()
            await message.delete()
        except Exception:
            pass
        return

    # Simpan ke DB (langsung, bukan optimistic — agar cache config_cache ikut reset)
    await _db_update_config(chat_id, "bio_vip_text", raw)

    text, keyboard = await page_bio_panel(chat_id)
    from html import escape as _esc
    header = (
        f"✅ <b>Teks VIP Bio berhasil disimpan!</b>\n"
        f"<code>{_esc(raw[:80])}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    )
    await _safe_edit_id(client, message.chat.id, msg_id, header + text, keyboard)

    try:
        await message.delete()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
#  NewsCore FSM Handler
# ─────────────────────────────────────────────────────────────────────────────
async def _handle_ns_fsm_input(client, message: Message, user_id: int):
    """
    2-langkah input untuk mode day/date:
      step 1 (ns_step1_day)  → ketik N hari  (angka bebas ≥ 1)
      step 1 (ns_step1_date) → ketik tanggal  (1–30)
      step 2 (ns_input_time) → ketik HH:MM   (berlaku semua mode)
    Weekday langsung masuk step 2 setelah pilih hari via tombol.

    Perilaku:
      - Pesan user langsung dihapus (tampilan bersih)
      - Antar step: edit pesan bot in-place (bukan reply baru)
      - Selesai: edit pesan bot → konfirmasi, lalu otomatis ke ns_panel
    """
    from plugins.ui.handlers_dm import _ns_fsm
    from database import ns_update, ns_get_config, ns_calc_next_reset
    from datetime import datetime

    state   = _ns_fsm[user_id]
    chat_id = state["chat_id"]
    action  = state["action"]
    msg_id  = state.get("msg_id")   # ID pesan bot yang akan di-edit
    text    = message.text.strip()

    # Hapus pesan user segera agar chat tetap bersih
    try:
        await message.delete()
    except Exception:
        pass

    # Validasi hak akses terpusat (defense in depth): seluruh sub-menu
    # NewsCore — termasuk semua input FSM-nya — hanya untuk admin dengan
    # hak "Ubah Info Grup" (atau owner). Pintu masuk callback-nya sendiri
    # sudah dikunci di handlers_dm.py, tapi dicek ulang di sini juga
    # supaya tidak ada state FSM basi/sisa yang bisa dieksploitasi.
    if not await _adm_sess.has_change_info_privilege(client, user_id, chat_id):
        _ns_fsm.pop(user_id, None)
        if msg_id:
            await _safe_edit_id(
                client, message.chat.id, msg_id,
                "<b>❖ AKSES DITOLAK ❖</b>\n\n"
                "⛔ <b>Pengaturan NewsCore</b> hanya bisa diakses oleh admin "
                "dengan hak <b>'Ubah Info Grup'</b>.\n\n"
                "<i>Minta admin lain yang memiliki hak tersebut, atau owner "
                "grup, untuk mengatur fitur ini.</i>",
                InlineKeyboardMarkup([[InlineKeyboardButton("🔙  Kembali", callback_data=f"manage_{chat_id}")]]),
            )
        return

    try:
        # ── BIO ADMIN WAJIB: simpan teks literal apa adanya (BUKAN regex,
        # tidak lewat pipeline_pembersihan/regex_utils — murni substring match
        # case-insensitive, lihat core/ns_bio_guard.py) ───────────────────────
        if action == "ns_input_bioadmin_text":
            if not text:
                await _safe_edit_id(
                    client, message.chat.id, msg_id,
                    "📝 <b>BIO ADMIN WAJIB — Ketik Teks Baru</b>\n\n"
                    "❌ Teks tidak boleh kosong.\n"
                    "Ketik teks yang wajib ada di bio admin NewsCore.\n\n"
                    "<i>Ketik /batal untuk membatalkan.</i>",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Batal", callback_data=f"ns_bioadmin_{chat_id}")]]),
                )
                return
            if len(text) > 200:
                await _safe_edit_id(
                    client, message.chat.id, msg_id,
                    "📝 <b>BIO ADMIN WAJIB — Ketik Teks Baru</b>\n\n"
                    "❌ Teks terlalu panjang (maks 200 karakter).\n\n"
                    "<i>Ketik /batal untuk membatalkan.</i>",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Batal", callback_data=f"ns_bioadmin_{chat_id}")]]),
                )
                return

            await ns_update(chat_id, {"bio_admin_text": text, "bio_admin_required": True})
            _ns_fsm.pop(user_id, None)

            await _safe_edit_id(
                client, message.chat.id, msg_id,
                "✅ <b>Teks wajib disimpan!</b>\n\n"
                f"<code>{_html_escape(text)}</code>\n\n"
                "<i>Kembali ke panel…</i>",
            )
            await asyncio.sleep(1.5)
            from plugins.ui.pages import page_newscore_bioadmin
            text_panel, keyboard_panel = await page_newscore_bioadmin(chat_id)
            await _safe_edit_id(client, message.chat.id, msg_id, text_panel, keyboard_panel)
            return

        # ── TITEL ADMIN: titel custom (maks 16 UTF-16 code units, batas asli
        # Telegram untuk custom title — BUKAN 16 huruf biasa) yang dipasang
        # ke admin yang diangkat NewsCore tiap periode reset, lihat
        # ns_do_reset() di newscore.py. Mendukung font unik/Unicode style
        # (combining mark, karakter di luar BMP seperti Mathematical Bold)
        # lewat _utf16_units() dan _split_graphemes() di atas ──────────────
        if action == "ns_input_admintitle_text":
            if not text:
                await _safe_edit_id(
                    client, message.chat.id, msg_id,
                    "🎖️ <b>TITEL ADMIN — Ketik Teks Baru</b>\n\n"
                    "❌ Teks tidak boleh kosong.\n"
                    "Ketik titel yang akan dipasang ke admin NewsCore.\n\n"
                    "<i>Maksimal 16 karakter (font unik/Unicode didukung).</i>\n\n"
                    "<i>Ketik /batal untuk membatalkan.</i>",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Batal", callback_data=f"ns_admintitle_{chat_id}")]]),
                )
                return

            units = _utf16_units(text)
            if units > 16:
                graphemes  = _split_graphemes(text)
                suggestion = _truncate_to_utf16_limit(text, 16)
                await _safe_edit_id(
                    client, message.chat.id, msg_id,
                    "🎖️ <b>TITEL ADMIN — Ketik Teks Baru</b>\n\n"
                    f"❌ Teks terlalu panjang untuk Telegram "
                    f"(<code>{units}</code>/16 — Telegram menghitung font unik/Unicode "
                    f"per code unit, bukan per huruf terlihat).\n"
                    f"   Jumlah karakter terlihat: <code>{len(graphemes)}</code>\n\n"
                    f"💡 Versi yang pas batas:\n<code>{_html_escape(suggestion)}</code>\n\n"
                    "Ketik ulang teks yang lebih pendek, atau kirim ulang "
                    "<i>persis</i> teks di atas untuk memakainya.\n\n"
                    "<i>Ketik /batal untuk membatalkan.</i>",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Batal", callback_data=f"ns_admintitle_{chat_id}")]]),
                )
                return

            await ns_update(chat_id, {"admin_title": text})
            _ns_fsm.pop(user_id, None)

            await _safe_edit_id(
                client, message.chat.id, msg_id,
                "✅ <b>Titel admin disimpan!</b>\n\n"
                f"<code>{_html_escape(text)}</code>\n\n"
                "<i>Kembali ke panel…</i>",
            )
            await asyncio.sleep(1.5)
            from plugins.ui.pages import page_newscore_admintitle
            text_panel, keyboard_panel = await page_newscore_admintitle(chat_id)
            await _safe_edit_id(client, message.chat.id, msg_id, text_panel, keyboard_panel)
            return

        # ── AUTO TITLE MEMBER: 10 nama berurutan dipisah spasi, dipakai
        # sebagai tag per kelompok 5-rank leaderboard typing NewsCore.
        # Dipasang via Bot API setChatMemberTag (lihat core/member_tag.py),
        # bukan set_administrator_title (itu khusus admin, bukan member) ──
        if action == "ns_input_autotitle_names":
            if not text:
                await _safe_edit_id(
                    client, message.chat.id, msg_id,
                    "🏷️ <b>AUTO TITLE MEMBER — Ketik 10 Nama</b>\n\n"
                    "❌ Teks tidak boleh kosong.\n"
                    "Ketik 10 nama berurutan, dipisahkan spasi.\n\n"
                    "<i>Ketik /batal untuk membatalkan.</i>",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Batal", callback_data=f"ns_autotitle_{chat_id}")]]),
                )
                return

            names = text.split()

            if len(names) > 10:
                await _safe_edit_id(
                    client, message.chat.id, msg_id,
                    "🏷️ <b>AUTO TITLE MEMBER — Ketik 10 Nama</b>\n\n"
                    f"❌ Terlalu banyak nama (<code>{len(names)}</code>/10 maksimal).\n"
                    "Maksimal <b>10 nama</b>, dipisahkan spasi.\n\n"
                    "<i>Ketik ulang, atau /batal untuk membatalkan.</i>",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Batal", callback_data=f"ns_autotitle_{chat_id}")]]),
                )
                return

            # Validasi panjang per-nama: batas sama dengan custom title
            # Telegram (16 UTF-16 code unit), karena dipasang sebagai tag.
            too_long = [(i, n, _utf16_units(n)) for i, n in enumerate(names, 1) if _utf16_units(n) > 16]
            if too_long:
                detail = "\n".join(
                    f"   Nama ke-{i}: <code>{_html_escape(n)}</code> ({units}/16)"
                    for i, n, units in too_long
                )
                await _safe_edit_id(
                    client, message.chat.id, msg_id,
                    "🏷️ <b>AUTO TITLE MEMBER — Ketik 10 Nama</b>\n\n"
                    f"❌ Ada nama yang lebih dari 16 karakter:\n{detail}\n\n"
                    "Setiap nama maksimal <b>16 karakter</b> (batas tag Telegram).\n\n"
                    "<i>Ketik ulang semua 10 nama, atau /batal untuk membatalkan.</i>",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Batal", callback_data=f"ns_autotitle_{chat_id}")]]),
                )
                return

            await ns_update(chat_id, {"auto_title_names": names})
            _ns_fsm.pop(user_id, None)

            preview = "\n".join(
                f"   Rank {idx*5+1}-{idx*5+5}  →  <code>{_html_escape(n)}</code>"
                for idx, n in enumerate(names)
            )
            await _safe_edit_id(
                client, message.chat.id, msg_id,
                "✅ <b>Auto Title Member disimpan!</b>\n\n"
                f"{preview}\n\n"
                "<i>Kembali ke panel…</i>",
            )
            await asyncio.sleep(1.8)
            from plugins.ui.pages import page_newscore_autotitle
            text_panel, keyboard_panel = await page_newscore_autotitle(chat_id)
            await _safe_edit_id(client, message.chat.id, msg_id, text_panel, keyboard_panel)
            return

        # ── LANGKAH 1A: input N hari ─────────────────────────────────────────
        if action == "ns_step1_day":
            if not text.isdigit() or int(text) < 1:
                # Edit pesan bot dengan pesan error + prompt ulang
                await _safe_edit_id(
                    client, message.chat.id, msg_id,
                    "📆 <b>LANGKAH 1/2 — Jumlah Hari</b>\n\n"
                    "❌ Harus angka bulat positif (minimal 1).\n"
                    "Contoh: <code>7</code>  (reset setiap 7 hari)\n\n"
                    "<i>Angka bebas, minimal 1.</i>",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Batal", callback_data=f"ns_panel_{chat_id}")]]),
                )
                return

            val1 = int(text)
            await ns_update(chat_id, {"reset_days": val1})
            _ns_fsm[user_id] = {"chat_id": chat_id, "action": "ns_input_time", "step": 2, "val1": val1, "msg_id": msg_id}
            await _safe_edit_id(
                client, message.chat.id, msg_id,
                f"⏰ <b>LANGKAH 2/2 — Jam Reset</b>\n\n"
                f"✅ Jumlah hari: <code>{val1}</code>\n\n"
                "Ketik jam dan menit dalam format <code>HH:MM</code>.\n"
                "Contoh: <code>23:59</code>",
                InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Batal", callback_data=f"ns_panel_{chat_id}")]]),
            )
            return

        # ── LANGKAH 1B: input tanggal ────────────────────────────────────────
        elif action == "ns_step1_date":
            if not text.isdigit() or not (1 <= int(text) <= 30):
                await _safe_edit_id(
                    client, message.chat.id, msg_id,
                    "📅 <b>LANGKAH 1/2 — Tanggal Reset</b>\n\n"
                    "❌ Tanggal harus angka 1 — 30.\n"
                    "Contoh: <code>1</code>  (reset setiap tgl 1)\n\n"
                    "<i>Harus angka 1 — 30.</i>",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Batal", callback_data=f"ns_panel_{chat_id}")]]),
                )
                return

            val1 = int(text)
            await ns_update(chat_id, {"reset_date": val1})
            _ns_fsm[user_id] = {"chat_id": chat_id, "action": "ns_input_time", "step": 2, "val1": val1, "msg_id": msg_id}
            await _safe_edit_id(
                client, message.chat.id, msg_id,
                f"⏰ <b>LANGKAH 2/2 — Jam Reset</b>\n\n"
                f"✅ Tanggal reset: <code>{val1}</code>\n\n"
                "Ketik jam dan menit dalam format <code>HH:MM</code>.\n"
                "Contoh: <code>23:59</code>",
                InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Batal", callback_data=f"ns_panel_{chat_id}")]]),
            )
            return

        # ── LANGKAH 2: input HH:MM ────────────────────────────────────────────
        elif action == "ns_input_time":
            parts = text.split(":")
            if len(parts) != 2 or not all(p.isdigit() for p in parts):
                await _safe_edit_id(
                    client, message.chat.id, msg_id,
                    "⏰ <b>Ketik jam reset NewsCore:</b>\n\n"
                    "❌ Format salah. Harus <code>HH:MM</code>.\n"
                    "Contoh: <code>23:59</code>",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Batal", callback_data=f"ns_panel_{chat_id}")]]),
                )
                return

            hour, minute = int(parts[0]), int(parts[1])
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                await _safe_edit_id(
                    client, message.chat.id, msg_id,
                    "⏰ <b>Ketik jam reset NewsCore:</b>\n\n"
                    "❌ Jam harus 0–23, menit harus 0–59.\n"
                    "Contoh: <code>23:59</code>",
                    InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Batal", callback_data=f"ns_panel_{chat_id}")]]),
                )
                return

            await ns_update(chat_id, {"reset_hour": hour, "reset_minute": minute})

        else:
            _ns_fsm.pop(user_id, None)
            return

        # ── Selesai: hitung next_reset, tampilkan konfirmasi sebentar, lalu kembali ke panel ──
        _ns_fsm.pop(user_id, None)
        cfg      = await ns_get_config(chat_id)
        new_next = ns_calc_next_reset(cfg)
        await ns_update(chat_id, {"next_reset": new_next})

        # Tampilkan konfirmasi singkat di pesan bot
        await _safe_edit_id(
            client, message.chat.id, msg_id,
            "✅ <b>Konfigurasi NewsCore disimpan!</b>\n\n"
            f"📅 Reset berikutnya: <code>{datetime.fromisoformat(new_next).strftime('%d %b %Y %H:%M')}</code> WIB\n\n"
            "<i>Kembali ke panel…</i>",
        )

        # Jeda singkat lalu otomatis kembali ke sub-menu NewsCore
        await asyncio.sleep(1.5)
        from plugins.ui.pages import page_newscore
        text_panel, keyboard_panel = await page_newscore(chat_id)
        await _safe_edit_id(client, message.chat.id, msg_id, text_panel, keyboard_panel)

    except ValueError:
        _ns_fsm.pop(user_id, None)
        await _safe_edit_id(
            client, message.chat.id, msg_id,
            "❌ Input tidak valid, pastikan angka semua.",
        )
    except Exception as e:
        _ns_fsm.pop(user_id, None)
        print(f"[ns_fsm_input] {e}")
