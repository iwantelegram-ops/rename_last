"""
core/member_tag.py
───────────────────
Helper untuk fitur "Auto Title Member" — memasang tag (custom title) ke
member NON-admin via method Bot API `setChatMemberTag` (Bot API 9.5+,
rilis Maret 2026).

PENTING — kenapa ini lewat HTTP request manual, bukan Pyrogram langsung:
`setChatMemberTag` adalah method Bot API resmi, tapi versi Pyrogram yang
dipakai project ini (2.0.106) dirilis SEBELUM method ini ada, sehingga
belum diekspos sebagai method bound (`client.set_chat_member_tag(...)`
tidak tersedia). Daripada menunggu Pyrogram di-update atau migrasi ke
fork, kita panggil endpoint REST Bot API langsung — ini selalu jalan
karena tidak tergantung dukungan library Python apapun, hanya butuh
BOT_TOKEN yang valid.

Syarat dari sisi Telegram (di luar kendali kode ini):
- Bot pemanggil HARUS admin di grup target.
- Bot HARUS punya hak admin `can_manage_tags` untuk mengatur tag member lain.
- Target HARUS member biasa atau restricted (BUKAN admin — admin pakai
  custom_title/set_administrator_title, mekanisme yang berbeda).
"""

import os
import httpx

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
_API_BASE = "https://api.telegram.org/bot{token}/{method}"


async def set_chat_member_tag(chat_id: int, user_id: int, tag: str) -> tuple[bool, str]:
    """
    Pasang/hapus tag member via Bot API `setChatMemberTag`.

    Pass tag="" untuk menghapus tag (sesuai dokumentasi resmi: string kosong
    menghapus tag yang sudah ada).

    Returns:
        (True, "") jika berhasil.
        (False, alasan) jika gagal — alasan diambil dari field 'description'
        respons Telegram apa adanya, supaya pesan error asli (mis.
        "Bad Request: CHAT_ADMIN_REQUIRED" atau info hak yang kurang)
        tidak hilang dan bisa di-log/ditampilkan ke owner.
    """
    if not BOT_TOKEN:
        return False, "BOT_TOKEN tidak terdeteksi di environment"

    url = _API_BASE.format(token=BOT_TOKEN, method="setChatMemberTag")
    payload = {"chat_id": chat_id, "user_id": user_id, "tag": tag}

    try:
        async with httpx.AsyncClient(timeout=15) as hc:
            resp = await hc.post(url, json=payload)
        data = resp.json()
    except Exception as e:
        return False, f"HTTP error: {e}"

    if data.get("ok"):
        return True, ""
    return False, data.get("description", "Unknown error dari Telegram")


def split_rank_groups(names: list, group_size: int = 5) -> list:
    """
    Pecah leaderboard jadi kelompok rank sesuai jumlah nama yang tersedia.

    Contoh dengan names=['juara1','juara2'] dan group_size=5:
      rank 1-5   -> 'juara1'
      rank 6-10  -> 'juara2'
      rank 11+   -> tidak dapat tag (di luar kelompok yang tersedia)

    Returns list nama TANPA index — dipakai sebagai referensi util murni;
    pemetaan rank->nama yang sebenarnya dilakukan di newscore.py karena
    butuh konteks leaderboard (urutan member, bukan cuma nama).
    """
    return [n for n in names if n and n.strip()]
