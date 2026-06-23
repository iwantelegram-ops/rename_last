"""
plugins/ui/handler_otp.py
────────────────────────────────────────────────────────────────────────────
Handler permanen untuk menangkap /otp <kode> dari OWNER BOT (owner utama,
BUKAN owner/admin grup) saat proses "Ganti Userbot" (Security OS) berjalan.

KENAPA FILE INI ADA (FIX TOTAL bug: /otp diam total, tidak ada respon
apapun di bot maupun di log):

Sebelumnya handler /otp dipasang secara MANUAL lewat fungsi
register_otp_handler(bot) yang dipanggil dari dalam start_userbot() atau
change_userbot() — keduanya berjalan sebagai coroutine/task SETELAH
app.start(). ada celah waktu nyata di mana bot sudah aktif menerima
update dari Telegram tapi register_otp_handler() belum sempat tereksekusi
(misalnya owner langsung klik "Ganti Userbot" tepat setelah redeploy).
Kalau /otp dikirim tepat di celah itu, tidak ada handler yang
mendengarkan sama sekali — diam total, tidak ada log apapun.

Pyrogram plugin auto-load (root="plugins" di Client(...)) MEMINDAI dan
MENDAFTARKAN semua handler di folder ini SAAT Client() pertama dibuat —
jauh SEBELUM app.start() dipanggil sama sekali. Dengan menaruh handler
/otp di sini, handler ini PASTI sudah aktif sejak detik pertama bot
hidup, tidak bergantung pada start_userbot(), change_userbot(), atau
task scheduling apapun.

FORMAT yang diterima: "/otp 12345" (perintah, spasi, lalu kode).
Bukan "/12345" — owner harus mengetik literal kata "otp".
"""

import os
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode

OWNER_ID = int(os.environ.get("OWNER_ID", 0))


@Client.on_message(
    filters.private & filters.user(OWNER_ID) & filters.command("otp"),
    group=-100,  # paling awal — dicek sebelum semua handler DM lain manapun
)
async def catch_otp_command(client: Client, message: Message):
    """
    Tangkap /otp <kode> dari OWNER BOT secara DM ke bot utama (bot biasa).

    filters.command("otp") oleh Pyrogram mengisi message.command sebagai
    list: ["otp", "<kode>"] — ini lebih andal daripada parsing manual
    message.text, karena tetap benar walau ada spasi ganda, mention bot
    (/otp@namabot 12345), dsb.
    """
    print(
        f"[UB-OTP] 📩 /otp masuk dari user_id={message.from_user.id} "
        f"(OWNER_ID={OWNER_ID})"
    )

    if len(message.command) < 2 or not message.command[1].strip():
        await message.reply(
            "❌ Format salah. Gunakan: <code>/otp 12345</code>",
            parse_mode=ParseMode.HTML,
        )
        message.stop_propagation()
        return

    otp_code = message.command[1].strip()

    # Import lazy untuk hindari circular import (video_call mengimpor banyak
    # hal dari plugins secara tidak langsung lewat antigcast.py).
    from video_call import receive_otp_from_bot, _otp_is_waiting

    if _otp_is_waiting():
        receive_otp_from_bot(otp_code)
        await message.reply(
            f"✅ <b>OTP diterima:</b> <code>{otp_code}</code>\n"
            "Mencoba login userbot...",
            parse_mode=ParseMode.HTML,
        )
    else:
        # Tidak ada proses login userbot yang sedang menunggu OTP saat ini.
        # Tetap simpan nilainya (buffer) — kalau proses login baru saja
        # mulai sepersekian detik setelah ini dan belum sempat membuat
        # _otp_event, _prompt_owner() akan membaca buffer ini langsung.
        receive_otp_from_bot(otp_code)
        await message.reply(
            "⚠️ Bot tidak sedang menunggu OTP saat ini.\n"
            "Kode tetap disimpan sementara — jika proses Ganti Userbot "
            "baru dimulai dalam beberapa detik, kode ini akan otomatis "
            "terpakai. Jika tidak, mulai ulang proses Ganti Userbot lalu "
            "kirim ulang /otp.",
            parse_mode=ParseMode.HTML,
        )

    # /otp sudah ditangani sepenuhnya di sini — jangan lanjutkan ke handler
    # FSM/group lain manapun, supaya tidak ada double-handling.
    message.stop_propagation()
