"""
plugins/ui/pages.py
───────────────────
Semua fungsi pembuat konten halaman (teks + keyboard).
Tidak ada handler di sini — murni "data layer" untuk UI.

Dipanggil oleh:
  - plugins/ui/handlers_dm.py   (callback & /start)
  - plugins/ui/handlers_fsm.py  (setelah FSM selesai)

FIXED: page_group_log sekarang return HTML murni (bukan marker [BQ]) sehingga
  cb_grp_log bisa menggunakan safe_edit biasa tanpa raw API.
  Ini memperbaiki crash collapsed=True pada Pyrogram 2.0.106.
"""

import os
from html import escape as _html_escape
from datetime import datetime, timezone
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from database import get_config, db, get_group_action_log_page, TZ_WIB as _TZ_WIB, get_bot_config, get_regex_count, get_free_count, invalidate_count_cache
from video_call import security_os_get_status, is_userbot_ready

_OWNER_ID        = int(os.environ.get("OWNER_ID", 0))
_CHANNEL_OWNER   = int(os.environ.get("CHANNEL_OWNER", 0))
_PANDUAN_OS      = os.environ.get("PANDUAN_OS", "").strip()


group_regex_db = db["regex_per_group"]
free_col       = db["free_per_group"]
whitelist_col  = db["whitelist_per_group"]

TOTAL_GUIDE_PAGES = 10


# ─────────────────────────────────────────────────────────────────────────────
#  Halaman Utama — Menu Utama
# ─────────────────────────────────────────────────────────────────────────────
async def _fetch_owner_line(client) -> str:
    parts = []
    try:
        if _OWNER_ID:
            owner = await client.get_users(_OWNER_ID)
            name  = owner.first_name + (f" {owner.last_name}" if owner.last_name else "")
            parts.append(f'👤 By <a href="tg://user?id={_OWNER_ID}">{name}</a>')
    except Exception:
        pass

    if _CHANNEL_OWNER:
        ch_title = ch_link = None
        try:
            # Coba resolve langsung (berhasil jika sesi sudah kenal channel)
            ch      = await client.get_chat(_CHANNEL_OWNER)
            ch_title = ch.title or "Channel"
            ch_uname = getattr(ch, "username", None) or ""
            ch_link  = f"https://t.me/{ch_uname}" if ch_uname else None
        except Exception:
            # Sesi baru belum kenal peer → baca dari cache DB yang disimpan saat startup
            try:
                ch_title = await get_bot_config("channel_owner_title")
                ch_uname = await get_bot_config("channel_owner_username") or ""
                if not ch_title and ch_uname:
                    # Coba resolve via @username dari DB
                    try:
                        ch2 = await client.get_chat(f"@{ch_uname}")
                        ch_title = ch2.title or ch_uname
                    except Exception:
                        ch_title = ch_uname
                ch_link = f"https://t.me/{ch_uname}" if ch_uname else None
            except Exception:
                pass

        if ch_title:
            if ch_link:
                parts.append(f'📢 <a href="{ch_link}">{ch_title}</a>')
            else:
                parts.append(f'📢 {ch_title}')

    return "  ·  ".join(parts) if parts else ""


async def page_start(client):
    me      = await client.get_me()
    add_url = f"t.me/{me.username}?startgroup=true&admin=delete_messages+ban_users"

    owner_line = await _fetch_owner_line(client)
    footer = f"\n<code>{'─' * 26}</code>\n{owner_line}" if owner_line else ""

    text = (
        "🛡️ <b>ANTIGCAST</b>\n"
        "<i>Anti-Spam Engine Cerdas · Powered by Nexus AI</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Sistem pertahanan otomatis untuk grup Telegram.\n"
        "Belajar dari setiap laporan spam dan membangun pertahanan\n"
        "baru secara otomatis setiap tengah malam.\n\n"
        "<b>⚡ 7 LAPIS PERLINDUNGAN:</b>\n"
        "◈ <b>Anti-Spam Lokal</b> — hapus pesan duplikat berulang\n"
        "◈ <b>Anti-GCast</b> — blokir broadcast massal lintas grup\n"
        "◈ <b>Filter Kata AI</b> — regex mutasi otomatis per kata kunci\n"
        "◈ <b>CAS Global</b> — auto-ban 200.000+ spammer terverifikasi\n"
        "◈ <b>Bio Link Detector</b> — filter user dengan link di bio\n"
        "◈ <b>Security OS</b> — pantau voice chat &amp; mute mic otomatis\n"
        "◈ <b>Nexus AI Engine</b> — rebuild pola tiap pukul 00:00 WIB\n\n"
        "🔇 <b>SISTEM HUKUMAN MUTE:</b>\n"
        "<i>10 pelanggaran spam berturut-turut → mute otomatis (berlipat)</i>\n\n"
        "🤖 <b>BOT PEMANTAU:</b>\n"
        "<i>Bot terpisah untuk cek bio profil user secara independen.\n"
        "Diperlukan untuk Bio Link Detector &amp; Security OS.</i>\n\n"
        "<i>Pilih grup dari <b>⚙️ Kelola Grup</b> untuk mulai mengatur.</i>"
        f"{footer}"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕  Pasang di Grup Saya", url=add_url)],
        [
            InlineKeyboardButton("⚙️  Kelola Grup",  callback_data="admin_menu"),
            InlineKeyboardButton("📖  Panduan",       callback_data="guide_1"),
        ],
        [InlineKeyboardButton("🤖  Nexus AI Panel",  callback_data="nx_home")],
    ])
    return text, keyboard


# ─────────────────────────────────────────────────────────────────────────────
#  Panduan Multi-Halaman (9 Halaman · Next/Prev)
# ─────────────────────────────────────────────────────────────────────────────

_GUIDE_CONTENT = {

    1: (
        "📖 <b>PANDUAN GLOBAL SPAM</b>  <code>[1/{t}]</code>\n"
        "<i>Apa Itu Bot Ini?</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Bot ini adalah <b>sistem keamanan otomatis</b> untuk grup Telegram.\n"
        "Dirancang membasmi spam, promosi liar, dan akun berbahaya — tanpa repot manual.\n\n"
        "<b>🛡️ MODUL PERLINDUNGAN:</b>\n\n"
        "🔁 <b>Anti-Spam Lokal</b>\n"
        "   Hapus pesan duplikat berulang dari satu user.\n\n"
        "🌐 <b>Anti-GCast Global</b>\n"
        "   Blokir pesan broadcast yang disebar ke banyak grup.\n\n"
        "🔤 <b>Filter Kata (Regex)</b>\n"
        "   Larang kata/kalimat promosi spesifik secara akurat.\n\n"
        "🛡️ <b>CAS Protection</b>\n"
        "   Auto-ban dari database 200.000+ spammer terverifikasi.\n\n"
        "🔍 <b>Bio Link Detector</b>\n"
        "   Filter user yang menyimpan link di profil bio mereka.\n\n"
        "🤖 <b>Nexus AI Engine</b>\n"
        "   AI yang belajar dari laporan spam dan merakit pola pertahanan\n"
        "   otomatis setiap hari pukul 00:00 WIB.\n\n"
        "🔇 <b>Sistem Mute Eskalasi</b>\n"
        "   10 pelanggaran spam berturut-turut → mute otomatis.\n"
        "   Berlaku untuk SEMUA jenis spam."
    ),

    2: (
        "📖 <b>PANDUAN GLOBAL SPAM</b>  <code>[2/{t}]</code>\n"
        "<i>Cara Pasang & Mulai</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>✦ 4 LANGKAH AKTIVASI:</b>\n\n"
        "<b>① Tambah ke Grup</b>\n"
        "   Tekan <b>「 ➕ Pasang di Grup Saya 」</b> di menu utama,\n"
        "   lalu pilih grup tujuan.\n\n"
        "<b>② Berikan Akses Admin</b>\n"
        "   Bot butuh 2 izin untuk bekerja optimal:\n"
        "   ◈ <code>Hapus Pesan</code> — agar bisa eksekusi spam\n"
        "   ◈ <code>Batasi Anggota</code> — untuk mute otomatis & CAS auto-ban\n\n"
        "<b>③ Cek Status</b>\n"
        "   Ketik <code>/status</code> di grup untuk melihat semua modul.\n\n"
        "<b>④ Atur via Panel</b>\n"
        "   Ketik <code>/antigcast</code> di grup → bot kirim panel\n"
        "   kontrol lengkap ke DM kamu.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<b>⚡ Semua filter aktif otomatis.</b>\n"
        "<i>Kamu tinggal menyesuaikan sesuai kebutuhan grup.</i>"
    ),

    3: (
        "📖 <b>PANDUAN GLOBAL SPAM</b>  <code>[3/{t}]</code>\n"
        "<i>Perintah Pengaturan Grup</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<i>👮 Khusus Admin Grup · Ketik langsung di grup</i>\n\n"
        "<b>✦ TOGGLE ON / OFF:</b>\n\n"
        "<code>/setlocal on</code>  atau  <code>off</code>\n"
        "   Anti-Spam Lokal — hapus pesan duplikat berulang.\n\n"
        "<code>/setglobal on</code>  atau  <code>off</code>\n"
        "   Anti-GCast Global — blokir broadcast massal.\n\n"
        "<code>/setbio on</code>  atau  <code>off</code>\n"
        "   Bio Link Detector — filter user dengan link di bio.\n\n"
        "<b>✦ KONFIGURASI LANJUTAN:</b>\n\n"
        "<code>/setwaktu [menit]</code>\n"
        "   Durasi bot mengingat pesan spam.\n"
        "   Contoh: <code>/setwaktu 30</code> → ingat selama 30 menit.\n\n"
        "<code>/status</code>\n"
        "   Dashboard status semua modul di grup ini.\n\n"
        "<code>/antigcast</code>\n"
        "   Kirim panel kontrol lengkap ke DM kamu.\n"
        "   <i>(Lebih canggih dari semua command di atas)</i>"
    ),

    4: (
        "📖 <b>PANDUAN GLOBAL SPAM</b>  <code>[4/{t}]</code>\n"
        "<i>Perintah /spam — Fitur Inti Nexus AI</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<i>👮 Khusus Admin Grup</i>\n\n"
        "<b>✦ APA ITU /spam?</b>\n\n"
        "Perintah <code>/spam</code> adalah cara kamu melaporkan pesan berbahaya\n"
        "ke <b>otak AI (Nexus Engine)</b>.\n\n"
        "Setiap laporan dianalisis dan diubah menjadi <b>pola pertahanan\n"
        "otomatis</b> yang berlaku di semua grup pengguna bot ini.\n\n"
        "<b>✦ CARA PAKAI (3 Langkah):</b>\n\n"
        "<b>①</b> Temukan pesan spam di grup.\n"
        "<b>②</b> Tekan lama → pilih <b>Balas (Reply)</b>.\n"
        "<b>③</b> Kirim: <code>/spam</code>\n\n"
        "Bot akan otomatis:\n"
        "◈ Hapus pesan spam dari grup\n"
        "◈ Simpan kontennya ke database Nexus AI\n"
        "◈ Proses pada siklus tengah malam (00:00 WIB)\n\n"
        "<b>✦ KENAPA PENTING?</b>\n\n"
        "Semakin banyak laporan, semakin cerdas AI membangun pola.\n"
        "Ini kontribusi nyata melindungi komunitas Telegram secara kolektif.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<b>⚠️ Wajib pakai reply.</b>\n"
        "<i>/spam tanpa reply tidak akan diproses.</i>"
    ),

    5: (
        "📖 <b>PANDUAN GLOBAL SPAM</b>  <code>[5/{t}]</code>\n"
        "<i>Filter Kata Khusus Grup</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<i>👮 Khusus Admin Grup</i>\n\n"
        "<b>✦ PERINTAH DASAR:</b>\n\n"
        "<code>/addgroupregex [kata]</code>\n"
        "   Tambah kata yang dilarang di grup ini.\n\n"
        "<code>/delgroupregex [kata]</code>\n"
        "   Hapus kata dari daftar filter.\n\n"
        "<code>/listgroupregex</code>\n"
        "   Lihat semua kata yang sedang diblokir.\n\n"
        "<b>✦ FORMAT INPUT:</b>\n\n"
        "Pisahkan kata dengan <code>|</code> — semua kata HARUS hadir\n"
        "sekaligus dalam pesan agar filter aktif (AND semantics).\n\n"
        "<b>Blokir 1 kata (dengan deteksi mutasi otomatis):</b>\n"
        "<code>/addgroupregex togel</code>\n"
        "   <i>→ deteksi: togel, t0g3l, togg3l, dll.</i>\n\n"
        "<b>Blokir jika ada 'jual' DAN 'akun' sekaligus:</b>\n"
        "<code>/addgroupregex jual | akun</code>\n\n"
        "<b>Blokir jika ada tiga kata sekaligus:</b>\n"
        "<code>/addgroupregex promo | slot | link</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<b>⚠️ Tanda | = AND, bukan OR.</b>\n"
        "<i>Setiap kata diproses AI mutasi — variasi huruf & leet terdeteksi otomatis.</i>\n\n"
        "<i>💡 Kelola filter lebih mudah via panel DM: /antigcast</i>"
    ),

    6: (
        "📖 <b>PANDUAN GLOBAL SPAM</b>  <code>[6/{t}]</code>\n"
        "<i>CAS Protection — Anti-Spam Global</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>✦ APA ITU CAS?</b>\n\n"
        "<b>CAS (Combot Anti-SPAM)</b> adalah database global berisi\n"
        "200.000+ akun spammer terverifikasi dari seluruh Telegram.\n\n"
        "Saat user baru masuk grup → bot langsung cek database.\n"
        "Jika terdeteksi → <b>auto-ban otomatis</b>.\n\n"
        "<b>✦ AKTIFKAN / NONAKTIFKAN:</b>\n\n"
        "CAS <b>OFF secara default</b>. Nyalakan via panel:\n"
        "<code>/antigcast</code> → pilih grup → <code>🛡️ CAS</code> →\n"
        "tap tombol status untuk ON/OFF.\n\n"
        "<b>✦ WHITELIST CAS (Pengecualian):</b>\n\n"
        "<code>/wlcas</code> + <i>reply</i> ke pesannya\n"
        "   User dikecualikan dari ban CAS di grup ini.\n\n"
        "<code>/wlcas [ID]</code>\n"
        "   Kecualikan berdasarkan User ID langsung.\n\n"
        "<code>/unwlcas</code> + <i>reply</i>  /  <code>/unwlcas [ID]</code>\n"
        "   Cabut pengecualian CAS.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<b>💡 Whitelist hanya berlaku saat CAS sedang ON.</b>\n"
        "<i>💡 Kelola whitelist CAS via panel DM: /antigcast → Grup → CAS</i>"
    ),

    7: (
        "📖 <b>PANDUAN GLOBAL SPAM</b>  <code>[7/{t}]</code>\n"
        "<i>Member VIP — Bypass Semua Filter</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<i>👮 Khusus Admin Grup</i>\n\n"
        "<b>✦ APA ITU MEMBER VIP?</b>\n\n"
        "User VIP <b>dibebaskan dari semua filter bot</b> di grup tertentu.\n"
        "Cocok untuk trusted member atau yang sering kena false positive.\n\n"
        "<b>✦ PERINTAH:</b>\n\n"
        "<code>/vip</code> + <i>reply</i> ke pesannya\n"
        "   Jadikan user sebagai Member VIP.\n\n"
        "<code>/vip [ID]</code>\n"
        "   Tambahkan berdasarkan User ID.\n\n"
        "<code>/unvip</code> + <i>reply</i>  /  <code>/unvip [ID]</code>\n"
        "   Cabut status VIP.\n\n"
        "<b>✦ EFEK VIP (bebas dari semua ini):</b>\n"
        "◈ Anti-Spam Lokal\n"
        "◈ Anti-GCast Global\n"
        "◈ Filter Kata (Regex)\n"
        "◈ Bio Link Detector\n"
        "◈ CAS Auto-Ban\n"
        "◈ Sistem Mute Eskalasi\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<b>⚠️ Status VIP hanya berlaku di grup tempat ditetapkan.</b>\n"
        "<i>Tidak berlaku lintas grup.</i>"
    ),

    8: (
        "📖 <b>PANDUAN GLOBAL SPAM</b>  <code>[8/{t}]</code>\n"
        "<i>Nexus AI Panel & Perintah Owner</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>✦ NEXUS AI — UNTUK SEMUA USER:</b>\n\n"
        "Panel Nexus AI dapat diakses langsung dari\n"
        "menu utama bot → tombol <b>🤖 Nexus AI Panel</b>.\n\n"
        "<b>✦ PERINTAH KHUSUS OWNER:</b>\n"
        "<i>⚠️ Hanya pemilik bot — tidak untuk admin grup biasa.</i>\n\n"
        "<code>/addregex [kata1|kata2|kata3]</code>\n"
        "   Tambah pola blokir GLOBAL (berlaku di semua grup).\n"
        "   Nexus AI merakit interlock pattern otomatis.\n\n"
        "<code>/delregex [kata]</code>\n"
        "   Hapus pola blokir global berdasarkan kata kunci.\n\n"
        "<code>/delnexus [kalimat atau pola]</code>\n"
        "   Hapus data spesifik dari database Nexus AI.\n\n"
        "<code>/infobot</code>\n"
        "   Tampilkan semua pola blokir global yang aktif.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<b>🔄 SIKLUS NEXUS AI:</b>\n"
        "Setiap hari pukul <b>00:00 WIB</b>, engine AI memproses semua\n"
        "laporan /spam dan merakit pola pertahanan baru otomatis."
    ),

    9: (
        "📖 <b>PANDUAN GLOBAL SPAM</b>  <code>[9/{t}]</code>\n"
        "🔇 <i>Sistem Mute Eskalasi — Hukuman Spam Otomatis</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>✦ APA ITU SISTEM MUTE ESKALASI?</b>\n\n"
        "Hukuman otomatis yang berlaku jika user spam <b>terus-menerus</b>\n"
        "tanpa jeda pesan bersih, apapun jenis spamnya.\n\n"
        "<b>✦ CARA KERJA:</b>\n\n"
        "📊 <b>Hitungan pelanggaran berturut-turut:</b>\n"
        "◈ Setiap pesan yang dihapus oleh bot (filter apapun) → +1 hitungan\n"
        "◈ Satu pesan bersih (lolos semua filter) → hitungan RESET ke 0\n\n"
        "⚠️ <b>Ambang hukuman:</b>\n"
        "◈ Pelanggaran ke-10 berturut-turut → <b>Mute 5 menit</b>\n\n"
        "📈 <b>Eskalasi (jika masih spam setelah dibuka):</b>\n"
        "◈ Pelanggaran ke-10 berikutnya → <b>Mute 10 menit</b>\n"
        "◈ Lanjut lagi → <b>Mute 20 menit</b>\n"
        "◈ Terus berlipat ganda hingga 80 menit, 160 menit, dst.\n\n"
        "<b>✦ BERLAKU UNTUK SEMUA JENIS SPAM:</b>\n"
        "◈ Filter kata global / grup\n"
        "◈ Anti-spam lokal (duplikat)\n"
        "◈ Anti-GCast global\n"
        "◈ Mention pengguna luar\n"
        "◈ Link dalam pesan\n"
        "◈ Bio link detector\n"
        "◈ Nexus AI detection\n\n"
        "<b>✦ PENGECUALIAN:</b>\n"
        "◈ Admin grup: tidak kena mute\n"
        "◈ Member VIP: tidak kena mute\n"
        "◈ Bot tidak punya hak Batasi Anggota: mute gagal, pesan tetap dihapus\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<b>💡 Tip:</b> Pastikan bot punya hak <code>Batasi Anggota</code>\n"
        "<i>agar sistem mute bekerja optimal.</i>"
    ),

    10: (
        "📖 <b>PANDUAN GLOBAL SPAM</b>  <code>[10/{t}]</code>\n"
        "🔐 <i>Security OS — Pantau Voice Chat Otomatis</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>✦ APA ITU SECURITY OS?</b>\n\n"
        "Security OS mengawasi <b>obrolan suara (voice chat)</b> grup secara real-time.\n"
        "Saat user naik ke VC, sistem memeriksa keanggotaan &amp; bio profil mereka.\n\n"
        "<b>✦ CARA KERJA:</b>\n\n"
        "◈ User <b>non-member</b> naik ke VC → mic <b>di-mute otomatis</b>\n"
        "◈ Bio mengandung link → mic <b>di-mute otomatis</b> + peringatan\n"
        "◈ Pemantauan berjalan 24/7 selama ada aktivitas voice chat\n\n"
        "<b>✦ 3 SYARAT AKTIVASI:</b>\n\n"
        "① <b>Userbot</b> — akun Telegram biasa (bukan bot) sebagai 'mata' di VC\n"
        "   Set <code>USERBOT_PHONE</code> di .env, login via OTP saat pertama kali\n\n"
        "② <b>Bot Pemantau</b> — bot token terpisah untuk cek bio profil user\n"
        "   Buat via @BotFather, pasang melalui panel Security OS tiap grup\n\n"
        "③ <b>Bot Pemantau di Grup</b> — tambahkan manual ke grup, jadikan admin\n\n"
        "<b>✦ KONFIGURASI .env TERKAIT:</b>\n\n"
        "<code>USERBOT_PHONE</code>          — nomor HP akun userbot\n"
        "<code>BOT_TOKEN_MONITOR</code>      — token bot pemantau (berbeda dari BOT_TOKEN)\n"
        "<code>LOG_OS</code>                 — channel log khusus aktivitas Security OS\n"
        "<code>SCAN_INTERVAL_MINUTES</code>  — interval scan bio (default: 30 menit)\n"
        "<code>BIO_RECHECK_SECS</code>       — jeda re-check bio user sama (default: 10 menit)\n"
        "<code>BIO_TTL_SECS</code>           — TTL data bio di database (default: 5 menit)\n\n"
        "<b>✦ CATATAN PENTING:</b>\n"
        "◈ 1 bot pemantau hanya untuk <b>1 grup</b>\n"
        "◈ Userbot tidak perlu jadi admin di grup (kecuali untuk mute mic di VC)\n"
        "◈ Jika user mempriv bio → bot pemantau tidak lihat link → tidak di-mute\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<b>💡 Tip:</b> Tekan tombol <b>📖 Panduan Install</b> di panel Security OS\n"
        "<i>untuk panduan lengkap instalasi &amp; setup userbot.</i>"
    ),
}


def page_guide(page_num: int):
    p    = max(1, min(page_num, TOTAL_GUIDE_PAGES))
    text = _GUIDE_CONTENT[p].format(t=TOTAL_GUIDE_PAGES)

    nav = []
    if p > 1:
        nav.append(InlineKeyboardButton("⏪ Prev", callback_data=f"guide_{p - 1}"))
    nav.append(InlineKeyboardButton(f"· {p}/{TOTAL_GUIDE_PAGES} ·", callback_data="noop"))
    if p < TOTAL_GUIDE_PAGES:
        nav.append(InlineKeyboardButton("Next ⏩", callback_data=f"guide_{p + 1}"))

    keyboard = InlineKeyboardMarkup([
        nav,
        [InlineKeyboardButton("🔙  Menu Utama", callback_data="start")],
    ])
    return text, keyboard


# ─────────────────────────────────────────────────────────────────────────────
#  Halaman — Dasbor Kelola Grup
# ─────────────────────────────────────────────────────────────────────────────
async def page_manage(chat_id: int):
    cfg = await get_config(chat_id)

    def flag(key): return "🟢 ON" if cfg[key] else "🔴 OFF"
    def icon(key): return "✅" if cfg[key] else "❌"

    waktu       = cfg["expiry"] // 60
    regex_count = await get_regex_count(chat_id)
    free_count  = await get_free_count(chat_id)

    # Ambil status Security OS
    sec_doc    = await security_os_get_status(chat_id)
    sec_on     = sec_doc.get("enabled", False)
    sec_flag   = "🟢 ON" if sec_on else "🔴 OFF"
    sec_icon   = "✅" if sec_on else "❌"
    ub_ready   = is_userbot_ready()
    ub_hint    = "" if ub_ready else " ⚠️"

    # Ambil status NewsCore
    from database import ns_get_config as _ns_cfg
    ns_cfg  = await _ns_cfg(chat_id)
    ns_on   = ns_cfg.get("enabled", False)
    ns_flag = "🟢 ON" if ns_on else "🔴 OFF"
    ns_icon = "✅" if ns_on else "❌"

    text = (
        f"⚙️ <b>CONTROL PANEL</b>\n"
        f"<code>Grup: {chat_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{icon('local')} <b>Anti-Spam Lokal</b>  —  <code>{flag('local')}</code>\n"
        f"<i>   Hapus pesan duplikat berulang dari 1 user.</i>\n\n"
        f"{icon('global')} <b>Anti-GCast Global</b>  —  <code>{flag('global')}</code>\n"
        f"<i>   Deteksi & hapus pesan broadcast lintas grup.</i>\n\n"
        f"{icon('bio_check')} <b>Bio Link Detector</b>  —  <code>{flag('bio_check')}</code>\n"
        f"<i>   Filter user yang menyimpan link di bio profil.</i>\n\n"
        f"{icon('anti_mention')} <b>Anti Mention Non-Member</b>  —  <code>{flag('anti_mention')}</code>\n"
        f"<i>   Hapus pesan yang mention user di luar grup.</i>\n\n"
        f"{icon('cas')} <b>CAS Anti-Spam Global</b>  —  <code>{flag('cas')}</code>\n"
        f"<i>   Auto-ban user di database 200.000+ spammer terverifikasi.</i>\n\n"
        f"⏱️ <b>Durasi Memori Spam</b>  —  <code>{waktu} menit</code>\n"
        f"<i>   Bot mengingat pesan selama durasi ini.</i>\n\n"
        f"🔤 <b>Filter Kata Khusus</b>  —  <code>{regex_count} aktif</code>\n"
        f"<i>   Blokir promosi spesifik (contoh: 'jual followers').</i>\n\n"
        f"👑 <b>Member VIP</b>  —  <code>{free_count} user</code>\n"
        f"<i>   User yang dibebaskan dari semua filter bot.</i>\n\n"
        f"🔇 <b>Mute Eskalasi</b>  —  <code>🟢 AKTIF</code>\n"
        f"<i>   10 spam berturut-turut → mute otomatis (berlipat).</i>\n\n"
        f"{sec_icon} <b>Security OS</b>  —  <code>{sec_flag}</code>{ub_hint}\n"
        f"<i>   Mute mic user non-member &amp; bio-link di obrolan suara via userbot.</i>\n\n"
        f"{ns_icon} <b>NewsCore</b>  —  <code>{ns_flag}</code>\n"
        f"<i>   Angkat admin otomatis dari member teraktif secara berkala.</i>\n\n"
        f"<i>Tap tombol di bawah untuk ubah pengaturan secara instan.</i>"
    )
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"🔁 Anti-Spam Lokal ›",        callback_data=f"local_panel_{chat_id}"),
            InlineKeyboardButton(f"🌐 GCast: {flag('global')}",  callback_data=f"tgl_global_{chat_id}"),
        ],
        [
            InlineKeyboardButton(f"🔍 Bio: {flag('bio_check')}", callback_data=f"tgl_bio_check_{chat_id}"),
            InlineKeyboardButton(f"🚫 Mention: {flag('anti_mention')}", callback_data=f"tgl_anti_mention_{chat_id}"),
        ],
        [
            InlineKeyboardButton(f"🔤 Filter ({regex_count})", callback_data=f"rgxpanel_{chat_id}"),
            InlineKeyboardButton(f"👑 VIP ({free_count})",     callback_data=f"freelist_{chat_id}"),
        ],
        [
            InlineKeyboardButton(f"🛡️ CAS: {flag('cas')}", callback_data=f"cas_panel_{chat_id}"),
            InlineKeyboardButton("📋 Log Aktivitas", callback_data=f"grp_log_{chat_id}_1"),
        ],
        [
            InlineKeyboardButton(f"🔐 Security OS: {sec_flag}", callback_data=f"secos_panel_{chat_id}"),
        ],
        [
            InlineKeyboardButton(f"🏆 NewsCore: {ns_flag}", callback_data=f"ns_panel_{chat_id}"),
        ],
        [InlineKeyboardButton("🔙  Daftar Grup", callback_data="admin_menu")],
    ])
    return text, keyboard


# ─────────────────────────────────────────────────────────────────────────────
#  Halaman — Sub-Panel Anti-Spam Lokal
# ─────────────────────────────────────────────────────────────────────────────
async def page_local_panel(chat_id: int):
    cfg = await get_config(chat_id)

    def flag(key): return "🟢 ON" if cfg[key] else "🔴 OFF"
    def icon(key): return "✅" if cfg[key] else "❌"

    waktu      = cfg["expiry"] // 60
    spam_limit = max(1, min(5, int(cfg.get("local_spam_limit", 1))))

    text = (
        f"🔁 <b>ANTI-SPAM LOKAL</b>\n"
        f"<code>Grup: {chat_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{icon('local')} <b>Status</b>  —  <code>{flag('local')}</code>\n"
        f"<i>   Hapus pesan duplikat berulang dari 1 user.</i>\n\n"
        f"⏱️ <b>Durasi Memori Spam</b>  —  <code>{waktu} menit</code>\n"
        f"<i>   Bot mengingat pesan selama durasi ini.</i>\n\n"
        f"📋 <b>Jumlah Pesan Diingat</b>  —  <code>{spam_limit} pesan</code>\n"
        f"<i>   Bot membandingkan pesan baru dengan {spam_limit} pesan terakhir user.\n"
        f"   Semakin besar → lebih ketat (lebih jauh ke belakang dicek).</i>\n\n"
        f"<i>Tap tombol di bawah untuk ubah pengaturan.</i>"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🔁 {flag('local')} — Tap untuk ubah", callback_data=f"tgl_local_{chat_id}")],
        [
            InlineKeyboardButton(f"⏱ {waktu}mnt", callback_data="noop"),
            InlineKeyboardButton("➖", callback_data=f"time_dec_{chat_id}"),
            InlineKeyboardButton("➕", callback_data=f"time_inc_{chat_id}"),
        ],
        [
            InlineKeyboardButton(f"📋 {spam_limit} pesan", callback_data="noop"),
            InlineKeyboardButton("➖", callback_data=f"spmlimit_dec_{chat_id}"),
            InlineKeyboardButton("➕", callback_data=f"spmlimit_inc_{chat_id}"),
        ],
        [InlineKeyboardButton("🔙  Kembali ke Panel Grup", callback_data=f"manage_{chat_id}")],
    ])
    return text, keyboard


# ─────────────────────────────────────────────────────────────────────────────
#  Halaman — Tutorial & List Regex
# ─────────────────────────────────────────────────────────────────────────────
async def page_regex_tutorial(chat_id: int):
    text = (
        f"🔤 <b>FILTER KATA KHUSUS GRUP</b>\n"
        f"<code>Grup: {chat_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Blokir pesan yang mengandung kombinasi kata tertentu.\n"
        f"Setiap kata diproses AI mutasi — mendeteksi variasi huruf & leetspeak otomatis.\n\n"
        f"<b>✦ FORMAT INPUT:</b>\n\n"
        f"Pisahkan kata dengan <code> | </code> (tanda pipa)\n"
        f"Semua kata <b>HARUS hadir sekaligus</b> dalam satu pesan\n\n"
        f"<b>📌 1 kata (deteksi mutasi otomatis):</b>\n"
        f"<code>togel</code>\n"
        f"<i>→ mendeteksi: togel, t0g3l, togg3l, t0gel, dll.</i>\n\n"
        f"<b>📌 2 kata — AND (harus ada keduanya):</b>\n"
        f"<code>jual | akun</code>\n"
        f"<i>→ hanya hapus jika ada 'jual' DAN 'akun' bersamaan</i>\n\n"
        f"<b>📌 3 kata — AND (semua wajib ada):</b>\n"
        f"<code>promo | slot | link</code>\n\n"
        f"<b>📌 4 huruf — kapital (huruf wajib ada dalam 1 kata):</b>\n"
        f"<code>boToL | miNYaK</code>\n"
        f"<i>→ maka: huruf b,t dan l wajib ada dlm satu kata di teks di grup, dan m,n,y,k wajib ada dalam target kata minyak di grup. dll.</i>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>⚠️ PENTING:</b> Tanda <code>|</code> = AND (bukan OR)\n"
        f"Semua kata wajib ada bersamaan agar pesan dihapus.\n\n"
        f"<i>Tekan tombol Tambah Filter, lalu ketik kata/kombinasinya.</i>"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋  Lihat Filter Tersimpan", callback_data=f"rgxlist_{chat_id}")],
        [InlineKeyboardButton("➕  Tambah Filter Baru",     callback_data=f"rgxadd_{chat_id}")],
        [InlineKeyboardButton("🔙  Kembali ke Panel Grup",  callback_data=f"manage_{chat_id}")],
    ])
    return text, keyboard


async def page_regex_list(chat_id: int, page: int = 1):
    from core.regex_utils import generate_kandidat_mutasi_liar, pipeline_pembersihan

    LIMIT  = 5
    offset = (page - 1) * LIMIT
    total  = await get_regex_count(chat_id)
    docs   = [doc async for doc in group_regex_db.find({"chat_id": chat_id}).sort("_id", -1).skip(offset).limit(LIMIT)]
    total_pages = max(1, (total + LIMIT - 1) // LIMIT)

    if docs:
        body        = ""
        del_buttons = []
        for local_i, doc in enumerate(docs):
            global_idx = offset + local_i
            raw        = doc.get("raw", "—")
            pola_full  = doc.get("pola", doc.get("pattern", ""))
            kata_list  = doc.get("kata_list", [])
            mutasi_map = doc.get("mutasi", {})

            if not kata_list and raw != "—":
                kata_list = [k.strip() for k in raw.split("|") if k.strip()]

            body += f"🔑 <b>[LOKAL-{global_idx + 1}]</b>\n"
            body += "📝 <b>Koleksi Asli:</b> " + ", ".join(f"<code>{k}</code>" for k in kata_list) + "\n"

            if mutasi_map:
                body += "🔍 <b>Probabilitas Lolos Mutasi (≥50%):</b>\n"
                for kata in kata_list:
                    mutasi = mutasi_map.get(kata, [])
                    if mutasi:
                        preview = "|".join(mutasi[:3])
                        body += f"• <code>{kata}</code> ➔ <code>{preview}</code>{'...' if len(mutasi) > 3 else ''}\n"
            elif kata_list:
                body += "🔍 <b>Probabilitas Lolos Mutasi (≥50%):</b>\n"
                for kata in kata_list:
                    kata_c = pipeline_pembersihan(kata)
                    if kata_c:
                        mutasi = generate_kandidat_mutasi_liar(kata_c.split()[0])
                        preview = "|".join(mutasi[:3])
                        body += f"• <code>{kata}</code> ➔ <code>{preview}</code>{'...' if len(mutasi) > 3 else ''}\n"

            if pola_full:
                short_pola = pola_full[:80] + ("..." if len(pola_full) > 80 else "")
                body += f"💥 <b>Full Interlock:</b>\n<code>{short_pola}</code>\n"

            body += "──────────────────────────\n"

            doc_id = str(doc["_id"])
            del_buttons.append([InlineKeyboardButton(
                f"🗑  Hapus: {raw[:35]}",
                callback_data=f"rgxdel_{chat_id}_{doc_id}"
            )])

        content = (
            f"⚡ <b>Aktif: {total} pola</b>  ·  Hal {page}/{total_pages}\n\n"
            f"{body}"
            f"<i>Tap 🗑 di bawah untuk hapus filter secara instan.</i>"
        )
    else:
        content     = (
            "📭 <b>Belum ada filter kata.</b>\n\n"
            "<i>Tambahkan kata terlarang dengan tombol di bawah.</i>"
        )
        del_buttons = []

    text = (
        f"🔤 <b>FILTER KATA LOKAL</b>\n"
        f"<code>Grup: {chat_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{content}\n"
    )

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("⏪ Sebelumnya", callback_data=f"rgxlist_{chat_id}_{page - 1}"))
    if (offset + LIMIT) < total:
        nav.append(InlineKeyboardButton("Selanjutnya ⏩", callback_data=f"rgxlist_{chat_id}_{page + 1}"))

    keyboard_rows = del_buttons.copy()
    if nav:
        keyboard_rows.append(nav)
    keyboard_rows += [
        [InlineKeyboardButton("➕  Tambah Filter Baru",      callback_data=f"rgxadd_{chat_id}")],
        [InlineKeyboardButton("🔙  Kembali ke Panduan Regex", callback_data=f"rgxpanel_{chat_id}")],
    ]
    return text, InlineKeyboardMarkup(keyboard_rows)


# ─────────────────────────────────────────────────────────────────────────────
#  Halaman — CAS Panel
# ─────────────────────────────────────────────────────────────────────────────
async def page_whitelist_text(chat_id: int) -> str:
    ids = [str(doc["user_id"]) async for doc in whitelist_col.find({"chat_id": chat_id})]
    if not ids:
        return (
            "🛡️ <b>WHITELIST CAS</b>\n"
            f"<code>Grup: {chat_id}</code>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "📭 <b>Daftar pengecualian masih kosong.</b>\n\n"
            "<i>User di whitelist kebal terhadap ban otomatis CAS,\n"
            "meskipun namanya ada di database global.</i>"
        )
    lines = "\n".join(f"  ◈ <code>{i}</code>" for i in ids)
    return (
        f"🛡️ <b>WHITELIST CAS</b>\n"
        f"<code>Grup: {chat_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⚡ <b>Total dikecualikan:</b> <code>{len(ids)} user</code>\n\n"
        f"{lines}\n\n"
        f"<i>User-user di atas terbebas dari deteksi CAS.</i>"
    )


async def page_cas_panel(chat_id: int):
    cfg      = await get_config(chat_id)
    cas_on   = cfg.get("cas", False)
    cas_flag = "🟢 ON" if cas_on else "🔴 OFF"
    cas_icon = "✅" if cas_on else "❌"
    ids      = [str(doc["user_id"]) async for doc in whitelist_col.find({"chat_id": chat_id})]
    wl_count = len(ids)
    text = (
        f"🛡️ <b>CAS PROTECTION</b>\n"
        f"<code>Grup: {chat_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>CAS (Combot Anti-SPAM)</b> adalah database global berisi\n"
        f"200.000+ akun spammer terverifikasi dari seluruh Telegram.\n\n"
        f"Saat user baru masuk → bot langsung cek database.\n"
        f"Jika terdeteksi → <b>auto-ban otomatis</b>.\n\n"
        f"{cas_icon} <b>Status CAS:</b>  —  <code>{cas_flag}</code>\n\n"
        f"<b>📋 WHITELIST CAS:</b>\n"
        f"User di whitelist akan <b>kebal</b> dari ban CAS meskipun\n"
        f"namanya tercatat di database global.\n\n"
        f"⚡ <b>Total whitelist:</b> <code>{wl_count} user</code>\n\n"
        f"<i>Pilih operasi di bawah ini.</i>"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🛡️ CAS: {cas_flag} — Tap untuk ubah", callback_data=f"tgl_cas_{chat_id}")],
        [InlineKeyboardButton("✅  Tambah Whitelist CAS",   callback_data=f"wl_cas_{chat_id}")],
        [InlineKeyboardButton("❌  Hapus Whitelist CAS",    callback_data=f"unwl_cas_{chat_id}")],
        [InlineKeyboardButton("📋  Lihat Daftar Whitelist", callback_data=f"view_wl_{chat_id}")],
        [InlineKeyboardButton("🔙  Kembali ke Panel Grup",  callback_data=f"manage_{chat_id}")],
    ])
    return text, keyboard


# ─────────────────────────────────────────────────────────────────────────────
#  Halaman — Free/VIP User List
# ─────────────────────────────────────────────────────────────────────────────
async def page_free_list(chat_id: int):
    docs = [doc async for doc in free_col.find({"chat_id": chat_id})]
    if docs:
        lines = "\n".join(f"  ◈ <code>{doc['user_id']}</code>" for doc in docs)
        body = (
            f"⚡ <b>Total:</b> <code>{len(docs)} user</code>\n\n"
            f"{lines}\n\n"
            f"<i>User ini bebas dari semua filter bot di grup ini.</i>"
        )
        del_buttons = [
            [InlineKeyboardButton(
                f"🗑  Unvip: {doc['user_id']}",
                callback_data=f"freedel_{chat_id}_{doc['user_id']}"
            )]
            for doc in docs
        ]
    else:
        body = (
            "📭 <b>Belum ada Member VIP.</b>\n\n"
            "<i>Tambahkan user trusted dengan tombol di bawah.</i>"
        )
        del_buttons = []

    text = (
        f"👑 <b>MEMBER VIP</b>\n"
        f"<code>Grup: {chat_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{body}\n"
    )
    keyboard_rows = del_buttons + [
        [InlineKeyboardButton("➕  Tambah Member VIP",     callback_data=f"freeadd_{chat_id}")],
        [InlineKeyboardButton("🔙  Kembali ke Panel Grup", callback_data=f"manage_{chat_id}")],
    ]
    return text, InlineKeyboardMarkup(keyboard_rows)


# ─────────────────────────────────────────────────────────────────────────────
#  Halaman — Log Aktivitas Per Grup
# FIXED: Return HTML murni — cb_grp_log sekarang pakai safe_edit (bukan edit_with_bq)
# Ini memperbaiki crash collapsed=True di Pyrogram 2.0.106
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_ts(ts: float) -> str:
    try:
        dt = datetime.fromtimestamp(ts, tz=_TZ_WIB)
        return dt.strftime("%H:%M · %d %b %Y")
    except Exception:
        return "—"


async def page_group_log(chat_id: int, page: int = 1):
    """
    Return (text_html, keyboard).
    FIXED: Menggunakan HTML biasa dengan <blockquote> standar.
    Tidak ada lagi marker [BQ] — tidak perlu edit_with_bq (yang crash karena collapsed=True).
    """
    PER_PAGE = 10
    docs, total = await get_group_action_log_page(chat_id, page, PER_PAGE)

    total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    page        = max(1, min(page, total_pages))

    if not docs:
        text = (
            "📋 <b>LOG AKTIVITAS</b>\n"
            f"<code>Grup: {chat_id}</code>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "📭 <b>Belum ada aktivitas tercatat.</b>\n\n"
            "Log muncul saat bot menghapus pesan, mute, atau ban user.\n"
            "<i>Log tersimpan selama 7 hari.</i>"
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙  Kembali ke Panel Grup", callback_data=f"manage_{chat_id}")],
        ])
        return text, keyboard

    _ICON = {"HAPUS": "🗑", "MUTE": "🔇", "BAN": "⛔", "KICK-VC": "🎤", "SECOS": "🔐",
             "MUTE-VC-MIC": "🔇", "UNMUTE-VC-MIC": "🔊"}

    entries = []
    for d in docs:
        icon   = _ICON.get(d.get("aksi", ""), "▸")
        aksi   = d.get("aksi", "?")
        alasan = d.get("alasan", "—")
        nama   = d.get("user_name", "?")
        uid    = d.get("user_id", "?")
        ts_str = _fmt_ts(d.get("ts", 0))
        konten = d.get("konten", "").strip()

        inner = f"👤 {nama} ({uid})\n📌 {alasan}"
        if konten:
            inner += f"\n📨 {konten[:80]}"

        entry = (
            f"{icon} <b>{aksi}</b> · {ts_str}\n"
            f"<blockquote>{inner}</blockquote>"
        )
        entries.append(entry)

    body = "\n\n".join(entries)

    text = (
        "📋 <b>LOG AKTIVITAS</b>\n"
        f"<code>Grup: {chat_id}  ·  Hal {page}/{total_pages}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{body}\n\n"
        f"<i>Menampilkan {len(docs)} dari {total} log (7 hari terakhir).</i>"
    )

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("⏪ Sebelumnya", callback_data=f"grp_log_{chat_id}_{page - 1}"))
    nav.append(InlineKeyboardButton(f"· {page}/{total_pages} ·", callback_data="noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("Berikutnya ⏩", callback_data=f"grp_log_{chat_id}_{page + 1}"))

    keyboard = InlineKeyboardMarkup([
        nav,
        [InlineKeyboardButton("🔙  Kembali ke Panel Grup", callback_data=f"manage_{chat_id}")],
    ])
    return text, keyboard


# ─────────────────────────────────────────────────────────────────────────────
#  Halaman — Security OS Panel
# ─────────────────────────────────────────────────────────────────────────────

async def page_security_os(chat_id: int, client=None):
    """
    Halaman panel Security OS.
    Menampilkan status 3 syarat aktivasi dan tombol aktifkan/nonaktifkan.

    client: opsional — jika diberikan, akan cek keanggotaan bot pemantau di grup
            secara real-time. Jika None, hanya cek status dari DB.
    """
    from video_call import (
        security_os_get_status, is_userbot_ready,
        check_monitor_is_member, _monitor_username_cache,
    )

    sec_doc   = await security_os_get_status(chat_id)
    enabled   = sec_doc.get("enabled", False)
    mon_id    = sec_doc.get("monitor_bot_id", 0)
    has_mon   = bool(mon_id)
    ub_ready  = is_userbot_ready()

    # Cek keanggotaan bot pemantau di grup (real-time jika client tersedia)
    if has_mon and client:
        mon_in_group = await check_monitor_is_member(client, chat_id)
    else:
        mon_in_group = False   # tidak bisa cek tanpa client

    mon_uname = _monitor_username_cache.get(mon_id, f"id:{mon_id}") if mon_id else "—"

    # ── Status label per syarat ───────────────────────────────────────────────
    flag    = "🟢 AKTIF" if enabled else "🔴 NONAKTIF"
    ub_st   = "✅ Online" if ub_ready else "❌ Offline — set USERBOT_PHONE di .env"

    if not has_mon:
        mon_st = "❌ Belum dibuat — tekan Pasang Bot Pemantau"
    elif not mon_in_group:
        mon_st = f"⚠️ @{mon_uname} belum join grup"
    else:
        mon_st = f"✅ @{mon_uname} sudah di grup"

    # Semua syarat terpenuhi?
    all_ready = ub_ready and has_mon and mon_in_group

    text = (
        f"🔐 <b>SECURITY OS</b>\n"
        f"<code>Grup: {chat_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>📌 APA INI?</b>\n"
        f"Security OS mengawasi <b>obrolan suara (voice chat)</b> grup.\n"
        f"Saat user naik ke obrolan suara, userbot memeriksa status keanggotaan &amp; bio profil mereka.\n"
        f"• User <b>non-member</b> → mic <b>di-mute otomatis</b> (terlepas isi bio).\n"
        f"• Bio mengandung link → mic <b>di-mute otomatis</b> dan user mendapat peringatan.\n\n"
        f"<b>📊 STATUS SYARAT AKTIVASI:</b>\n"
        f"  {'✅' if ub_ready else '❌'} Userbot      : <code>{ub_st}</code>\n"
        f"  {'✅' if has_mon else '❌'} Bot Pemantau : <code>{mon_st}</code>\n"
        f"  {'✅' if (has_mon and mon_in_group) else '❌'} Di Grup      : "
        f"<code>{'✅ Sudah jadi anggota' if (has_mon and mon_in_group) else '❌ Belum join — tambahkan manual'}</code>\n\n"
        f"<b>🔐 Security OS  : <code>{flag}</code></b>\n\n"
        f"<b>⚙️ CARA KERJA SINGKAT:</b>\n"
        f"◈ Tiap grup punya bot pemantau <b>masing-masing</b>.\n"
        f"◈ Bot pemantau hanya menjawab di grupnya sendiri.\n"
        f"◈ Jika user mempriv bio untuk grup tertentu → bot pemantau di grup itu\n"
        f"   tidak akan melihat link → mic user <b>tidak di-mute</b> di grup tersebut.\n"
        f"◈ 1 bot pemantau hanya boleh dipakai di <b>1 grup</b>.\n"
    )

    buttons = []

    # Tombol panduan install (hanya jika PANDUAN_OS diisi di .env)
    if _PANDUAN_OS:
        buttons.append([
            InlineKeyboardButton("📖  Panduan Install", url=_PANDUAN_OS)
        ])

    # Tombol pasang/ganti bot pemantau
    label_mon = "🔄  Ganti Bot Pemantau" if has_mon else "🤖  Pasang Bot Pemantau"
    buttons.append([
        InlineKeyboardButton(label_mon, callback_data=f"secos_setmon_{chat_id}")
    ])

    # Tombol aktifkan / nonaktifkan
    if enabled:
        buttons.append([
            InlineKeyboardButton("🔴  Nonaktifkan Security OS", callback_data=f"secos_off_{chat_id}")
        ])
    else:
        # Tombol aktifkan selalu ditampilkan; validasi syarat dilakukan saat diklik
        lbl = "🟢  Aktifkan Security OS" if all_ready else "🟢  Aktifkan (cek syarat dulu)"
        buttons.append([
            InlineKeyboardButton(lbl, callback_data=f"secos_on_{chat_id}")
        ])

    buttons.append([
        InlineKeyboardButton("🔙  Kembali ke Panel Grup", callback_data=f"manage_{chat_id}")
    ])

    keyboard = InlineKeyboardMarkup(buttons)
    return text, keyboard


# ─────────────────────────────────────────────────────────────────────────────
#  Halaman — NewsCore Panel
# ─────────────────────────────────────────────────────────────────────────────

async def page_newscore(chat_id: int):
    from database import ns_get_config, HARI_MAP_NS
    from datetime import datetime

    cfg     = await ns_get_config(chat_id)
    enabled = cfg.get("enabled", False)

    def flag(v): return "🟢 ON" if v else "🔴 OFF"
    def icon(v): return "✅" if v else "❌"

    mode = cfg.get("mode", "day")
    if mode == "day":
        mode_text = f"Setiap <code>{cfg.get('reset_days', 7)}</code> hari"
    elif mode == "date":
        mode_text = f"Setiap tanggal <code>{cfg.get('reset_date', 1)}</code>"
    else:
        mode_text = f"Setiap hari <code>{HARI_MAP_NS.get(cfg.get('reset_weekday', 0))}</code>"

    reset_time = f"{cfg.get('reset_hour', 23):02d}:{cfg.get('reset_minute', 59):02d} WIB"

    next_r   = cfg.get("next_reset")
    next_str = ""
    if next_r and enabled:
        try:
            next_str = f"\n📅 <b>Reset Berikutnya:</b>  <code>{datetime.fromisoformat(next_r).strftime('%d %b %Y %H:%M')}</code> WIB"
        except Exception:
            pass

    privs = cfg.get("privileges", {})
    PLABELS = {
        "can_delete_messages":    "Hapus Pesan",
        "can_restrict_members":   "Mute / Kick",
        "can_invite_users":       "Undang Member",
        "can_pin_messages":       "Pin Pesan",
        "can_manage_video_chats": "Kelola Video Chat",
    }
    priv_lines = "\n".join(
        f"   {'✅' if privs.get(k, False) else '❌'} {label}"
        for k, label in PLABELS.items()
    )

    bio_admin_text     = (cfg.get("bio_admin_text") or "").strip()
    bio_admin_required = cfg.get("bio_admin_required", True)
    if not bio_admin_required:
        bio_admin_line = "➖ <code>Tidak diwajibkan (dikosongkan oleh admin)</code>"
    elif bio_admin_text:
        bio_admin_line = f"✅ <code>{_html_escape(bio_admin_text[:40])}</code>"
    else:
        bio_admin_line = "❌ <code>(belum diisi — semua admin NewsCore akan di-unadmin)</code>"

    admin_title = (cfg.get("admin_title") or "").strip()
    if admin_title:
        admin_title_line = f"✅ <code>{_html_escape(admin_title)}</code>"
    else:
        admin_title_line = "➖ <code>(belum diatur — pakai titel default)</code>"

    auto_title_enabled = cfg.get("auto_title_enabled", False)
    auto_title_names   = [n for n in cfg.get("auto_title_names", []) if n and n.strip()]
    if auto_title_enabled and auto_title_names:
        auto_title_line = f"✅ <code>ON — {len(auto_title_names)} nama diatur</code>"
    elif auto_title_enabled:
        auto_title_line = "⚠️ <code>ON — tapi belum ada nama diisi</code>"
    else:
        auto_title_line = "➖ <code>OFF</code>"

    text = (
        f"🏆 <b>NEWSCORE — Skor Keaktifan & Admin Otomatis</b>\n"
        f"<code>Grup: {chat_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{icon(enabled)} <b>Status NewsCore:</b>  <code>{flag(enabled)}</code>\n\n"
        f"⚙️ <b>Konfigurasi Reset:</b>\n"
        f"   📆 Mode: <code>{mode.upper()}</code>  ({mode_text})\n"
        f"   ⏰ Jam Reset: <code>{reset_time}</code>\n"
        f"   👑 Kuota Admin: <code>Top {cfg.get('max_admins', 1)}</code> teratas\n"
        f"{next_str}\n\n"
        f"🛡️ <b>Hak Akses Admin Baru:</b>\n{priv_lines}\n\n"
        f"📝 <b>Bio Admin Wajib:</b>  {bio_admin_line}\n"
        f"<i>   Admin NewsCore wajib punya teks ini di bio, atau di-unadmin.</i>\n\n"
        f"🎖️ <b>Titel Admin:</b>  {admin_title_line}\n"
        f"<i>   Titel ini dipasang otomatis ke admin yang diangkat NewsCore.</i>\n\n"
        f"🏷️ <b>Auto Title Member:</b>  {auto_title_line}\n"
        f"<i>   Tag otomatis untuk member biasa (non-admin) berdasar rank typing.</i>\n\n"
        f"<i>Ketik /ns_score di grup untuk lihat leaderboard.</i>"
    )

    keyboard_rows = [
        [InlineKeyboardButton(
            f"{'🔴 Matikan' if enabled else '🟢 Aktifkan'} NewsCore",
            callback_data=f"ns_toggle_{chat_id}"
        )],
        [
            InlineKeyboardButton("⚙️ Mode Reset",    callback_data=f"ns_mode_{chat_id}"),
            InlineKeyboardButton("👑 Kuota Admin",   callback_data=f"ns_maxadmin_{chat_id}"),
        ],
        [InlineKeyboardButton("⏰ Jam Reset",        callback_data=f"ns_time_{chat_id}")],
        [InlineKeyboardButton("🛡️ Hak Admin Baru",  callback_data=f"ns_privs_{chat_id}")],
        [InlineKeyboardButton("📝 Bio Admin Wajib",  callback_data=f"ns_bioadmin_{chat_id}")],
        [InlineKeyboardButton("🎖️ Titel Admin",     callback_data=f"ns_admintitle_{chat_id}")],
        [InlineKeyboardButton("🏷️ Auto Title Member", callback_data=f"ns_autotitle_{chat_id}")],
        [InlineKeyboardButton("🔙  Kembali ke Panel", callback_data=f"manage_{chat_id}")],
    ]
    return text, InlineKeyboardMarkup(keyboard_rows)


async def page_newscore_privs(chat_id: int):
    from database import ns_get_config
    cfg   = await ns_get_config(chat_id)
    privs = cfg.get("privileges", {})
    PLABELS = {
        "can_delete_messages":    "Hapus Pesan",
        "can_restrict_members":   "Mute / Kick",
        "can_invite_users":       "Undang Member",
        "can_pin_messages":       "Pin Pesan",
        "can_manage_video_chats": "Kelola Video Chat",
    }
    buttons = [
        [InlineKeyboardButton(
            f"{'🟢' if privs.get(k, False) else '🔴'}  {label}",
            callback_data=f"ns_priv_{k}_{chat_id}"
        )]
        for k, label in PLABELS.items()
    ]
    buttons.append([InlineKeyboardButton("🔙  Kembali", callback_data=f"ns_panel_{chat_id}")])
    text = (
        f"🛡️ <b>HAK AKSES ADMIN BARU — NewsCore</b>\n"
        f"<code>Grup: {chat_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Tap tombol untuk toggle ON/OFF hak akses admin yang akan diangkat otomatis."
    )
    return text, InlineKeyboardMarkup(buttons)


async def page_newscore_bioadmin(chat_id: int):
    """
    Halaman pengaturan "Bio Admin Wajib" — sub-menu panel NewsCore.

    Admin yang diangkat otomatis oleh NewsCore wajib memiliki teks ini
    di bio Telegram mereka (boleh ada teks lain juga). Jika tidak ada
    saat bio mereka dicek (lewat typing di grup / bertemu userbot di VC)
    → bot utama akan unadmin mereka otomatis.

    Owner/admin (dengan hak Ubah Info Grup) bisa menekan "Kosongkan" agar
    syarat ini dimatikan sepenuhnya — admin NewsCore boleh punya bio apa
    saja, termasuk kosong.
    """
    from database import ns_get_config
    cfg                = await ns_get_config(chat_id)
    bio_admin_text     = (cfg.get("bio_admin_text") or "").strip()
    bio_admin_required = cfg.get("bio_admin_required", True)

    if not bio_admin_required:
        status_block = (
            "➖ <b>Tidak diwajibkan.</b>\n"
            "Syarat ini sudah dikosongkan — admin NewsCore boleh punya bio "
            "apa saja (termasuk kosong), tidak akan di-unadmin karena bio."
        )
    elif bio_admin_text:
        status_block = f"✅ <b>Teks aktif saat ini:</b>\n<code>{_html_escape(bio_admin_text)}</code>"
    else:
        status_block = (
            "❌ <b>Belum diisi.</b>\n"
            "Selama kosong, <u>SEMUA admin NewsCore akan di-unadmin</u> "
            "begitu bio mereka dicek — karena syarat dianggap wajib namun "
            "tidak mungkin terpenuhi tanpa teks.\n\n"
            "<i>Tekan 'Kosongkan' jika ingin syarat ini dimatikan sepenuhnya.</i>"
        )

    text = (
        f"📝 <b>BIO ADMIN WAJIB — NewsCore</b>\n"
        f"<code>Grup: {chat_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>📌 APA INI?</b>\n"
        f"Admin yang <b>diangkat otomatis oleh NewsCore</b> wajib mencantumkan "
        f"teks tertentu di bio profil Telegram mereka (boleh ada teks lain juga, "
        f"asal teks wajib ini ikut tercantum).\n\n"
        f"Jika saat dicek (lewat ketikan di grup atau saat bertemu userbot di "
        f"obrolan suara) admin tidak memenuhi syarat ini → bot utama akan "
        f"<b>melepas status admin</b> mereka secara otomatis, lalu mengirim log "
        f"ke channel log &amp; Log Aktivitas grup.\n\n"
        f"<i>Pesan mereka TIDAK dihapus dan mic VC TIDAK dimute — hanya status "
        f"admin yang dicabut. Ini hanya berlaku untuk admin yang diangkat oleh "
        f"NewsCore, bukan admin manual/owner asli grup.</i>\n\n"
        f"<i>Hanya admin grup dengan hak 'Ubah Info Grup' (atau owner) yang "
        f"bisa mengatur fitur ini.</i>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{status_block}"
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "✏️  Ubah Teks Wajib" if bio_admin_text else "✏️  Isi Teks Wajib",
            callback_data=f"ns_bioadmin_set_{chat_id}"
        )],
        [InlineKeyboardButton("🔙  Kembali ke NewsCore", callback_data=f"ns_panel_{chat_id}")],
    ])
    return text, keyboard


async def page_newscore_admintitle(chat_id: int):
    """
    Halaman pengaturan "Titel Admin" — sub-menu panel NewsCore.

    Titel ini (maks 16 UTF-16 code unit — batas asli Telegram untuk custom
    title; setara ±16 huruf biasa, tapi font unik/Unicode style yang pakai
    combining mark atau karakter di luar BMP bisa makan lebih dari 1 unit
    per huruf terlihat) akan dipasang otomatis ke setiap admin yang
    diangkat NewsCore setiap kali periode reset score berjalan, lewat
    set_administrator_title. Jika kosong, sistem memakai
    titel default bawaan ("Top Member N 👑").
    """
    from database import ns_get_config
    cfg         = await ns_get_config(chat_id)
    admin_title = (cfg.get("admin_title") or "").strip()

    if admin_title:
        status_block = f"✅ <b>Titel aktif saat ini:</b>\n<code>{_html_escape(admin_title)}</code>"
    else:
        status_block = (
            "➖ <b>Belum diatur.</b>\n"
            "Selama kosong, sistem akan memakai titel default bawaan "
            "(<code>Top Member N 👑</code>) saat mengangkat admin NewsCore."
        )

    text = (
        f"🎖️ <b>TITEL ADMIN — NewsCore</b>\n"
        f"<code>Grup: {chat_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>📌 APA INI?</b>\n"
        f"Titel (custom title) yang akan dipasang otomatis ke admin yang "
        f"<b>diangkat NewsCore</b> setiap kali periode reset score berjalan.\n\n"
        f"<i>Maksimal 16 karakter — ini batas asli Telegram untuk custom "
        f"title admin. Font unik/Unicode style (mis. 𝐕𝐈𝐏, ᴠɪᴘ) didukung, "
        f"tapi beberapa gaya font memakan lebih dari 1 unit per huruf "
        f"terlihat, jadi muatannya bisa lebih pendek dari perkiraan.</i>\n\n"
        f"<i>Hanya admin grup dengan hak 'Ubah Info Grup' (atau owner) yang "
        f"bisa mengatur fitur ini.</i>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{status_block}"
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "✏️  Ubah Titel" if admin_title else "✏️  Isi Titel",
            callback_data=f"ns_admintitle_set_{chat_id}"
        )],
        [InlineKeyboardButton("🔙  Kembali ke NewsCore", callback_data=f"ns_panel_{chat_id}")],
    ])
    return text, keyboard


async def page_newscore_autotitle(chat_id: int):
    """
    Halaman pengaturan "Auto Title Member" — sub-menu panel NewsCore.

    Memasang tag (BUKAN custom title admin — beda mekanisme) ke member
    NON-admin secara otomatis berdasar rank leaderboard typing NewsCore,
    tiap kali periode reset berjalan (bareng ns_do_reset()).

    Mapping rank -> nama: rank 1-5 pakai nama urutan ke-1, rank 6-10 pakai
    nama urutan ke-2, dst, hingga maksimal 10 nama (rank 1-50). Dipasang
    via Bot API setChatMemberTag — lihat core/member_tag.py untuk detail
    kenapa ini lewat HTTP request manual, bukan method Pyrogram langsung.
    """
    from database import ns_get_config
    cfg     = await ns_get_config(chat_id)
    enabled = cfg.get("auto_title_enabled", False)
    names   = [n for n in cfg.get("auto_title_names", []) if n and n.strip()]

    if names:
        preview_lines = []
        for idx, name in enumerate(names):
            lo = idx * 5 + 1
            hi = lo + 4
            preview_lines.append(f"   Rank {lo}-{hi}  →  <code>{_html_escape(name)}</code>")
        names_block = "\n".join(preview_lines)
        status_block = f"✅ <b>{len(names)} nama diatur:</b>\n{names_block}"
    else:
        status_block = (
            "➖ <b>Belum ada nama diisi.</b>\n"
            "Fitur tidak akan memasang tag apapun sampai nama diisi, "
            "meski statusnya Aktif."
        )

    text = (
        f"🏷️ <b>AUTO TITLE MEMBER — NewsCore</b>\n"
        f"<code>Grup: {chat_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>📌 APA INI?</b>\n"
        f"Tag otomatis untuk <b>member biasa (non-admin)</b> berdasar rank "
        f"leaderboard typing NewsCore, dipasang tiap periode reset.\n\n"
        f"<b>📊 CARA KERJA:</b>\n"
        f"Tiap kelompok <b>5 rank teratas</b> dapat 1 nama. Rank 1-5 pakai "
        f"nama pertama yang kamu isi, rank 6-10 pakai nama kedua, dan "
        f"seterusnya — hingga maksimal <b>10 nama</b> (cover rank 1-50).\n\n"
        f"<i>Member di luar 50 rank teratas, atau di luar jumlah nama yang "
        f"diisi, tidak mendapat tag apapun.</i>\n\n"
        f"<i>Bot harus admin grup dengan hak 'Kelola Tag Member' "
        f"(can_manage_tags) agar fitur ini bisa berjalan.</i>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{'🟢' if enabled else '🔴'} <b>Status:</b> <code>{'AKTIF' if enabled else 'NONAKTIF'}</code>\n\n"
        f"{status_block}"
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            f"{'🔴 Nonaktifkan' if enabled else '🟢 Aktifkan'}",
            callback_data=f"ns_autotitle_toggle_{chat_id}"
        )],
        [InlineKeyboardButton(
            "✏️  Ubah Custom Title" if names else "✏️  Isi Custom Title",
            callback_data=f"ns_autotitle_set_{chat_id}"
        )],
        [InlineKeyboardButton("🔙  Kembali ke NewsCore", callback_data=f"ns_panel_{chat_id}")],
    ])
    return text, keyboard
