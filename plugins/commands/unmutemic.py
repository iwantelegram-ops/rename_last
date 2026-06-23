"""
plugins/commands/unmutemic.py
──────────────────────────────
Perintah /unmutemic — minta inspeksi dadakan untuk membuka mute mic.

FLOW MEMBER BIASA:
  1. User kirim /unmutemic di grup
  2. Hapus pesan perintah segera
  3. Cek cooldown join-VC per grup (lapis 1) — lihat "COOLDOWN TIGA LAPIS"
  4. Cek cooldown antar-eksekusi per grup, 5 detik (lapis 2)
  5. Cek anti-spam (cooldown 5 menit per user per grup — lapis 3)
  6. Cek apakah user pernah di-mute userbot (vc_muted_by_ub)
     → Tidak ada di daftar → abaikan (skip)
  7. Cek Security OS aktif untuk grup ini
  8. Cek bio user via bot pemantau (fresh check)
     Kondisi A: masih terdeteksi link di bio (has_link=True)
        → abaikan perintah, userbot TIDAK perlu join VC grup ini.
     Kondisi B: tidak terdeteksi link — bio bersih/kosong (has_link=False)
        ATAU bio diprivasi/tidak ada respon apapun (has_link=None)
        → userbot dipaksa join VC grup ini untuk unmute.
  9. (Kondisi B saja) Invalidasi cache member, antri scan VC ke worker

FLOW MEMBER VIP:
  1–7. Sama seperti member biasa
  8. SKIP cek bio (VIP bebas dari aturan bio link) — userbot dipaksa naik VC
     dan memastikan mic VIP unmuted di grup ini
  9. Invalidasi cache member, antri scan VC ke worker
     Userbot cek: apakah VIP ada di VC? → unmute mic langsung

CATATAN ARSITEKTUR:
  - Inspeksi dadakan SELALU lewat _enqueue_vc_scan (bukan langsung _vc_scan_and_enforce)
    agar tidak bentrok dengan siklus 30 menit atau follow-up recheck.
  - Worker queue di video_call.py yang mengatur eksekusi berurutan dan jeda antar grup.
  - Lock inspeksi (_vc_inspection_lock) TIDAK dipakai di sini — sudah diurus worker.

FIX (cache VIP basi):
  _is_vip_user() di video_call.py punya cache 3 menit. Jika user baru saja
  dijadikan VIP (via /vip atau tombol UI) lalu langsung kirim /unmutemic
  dalam window 3 menit tersebut, cache lama bisa membuat user terdeteksi
  "bukan VIP" sehingga jatuh ke jalur cek-bio biasa — kalau bio masih ada
  link, command berhenti tanpa pernah antri scan VC (userbot tidak naik).
  Perbaikan: command /vip dan /unvip (serta tombol UI-nya) sekarang memanggil
  video_call.invalidate_vip_cache(chat_id, user_id) setiap kali status VIP
  berubah, agar /unmutemic langsung membaca status VIP terbaru tanpa delay.

FIX (bug unpacking has_link):
  _query_bio_from_db() mengembalikan tuple (has_link, monitor_unavailable).
  Kode sebelumnya melakukan `has_link = await _query_bio_from_db(...)` —
  menyimpan tuple utuh ke has_link, bukan elemen pertamanya. Akibatnya
  `has_link is True` TIDAK PERNAH True (karena nilainya tuple, bukan literal
  True), jadi Kondisi A (bio masih ada link → abaikan) tidak pernah berjalan
  — command selalu lanjut antri scan VC meski bio user masih ada link.
  Perbaikan: unpack tuple dengan benar →
  `has_link, monitor_unavailable = await _query_bio_from_db(...)`.

COOLDOWN TIGA LAPIS (per grup join-VC → per grup antar-eksekusi → per user):
  Lapis 1 — Cooldown JOIN VC (per grup, berlaku untuk SEMUA user di grup itu):
    Saat userbot baru naik VC grupA (_vc_join_last_ts[grupA] di-set oleh
    _vc_scan_and_enforce di video_call.py), data bio hasil scan tersebut
    baru "matang"/valid setelah BIO_TTL_SECS detik (cache bio bot pemantau +
    userbot, lihat _BIO_CACHE_TTL). Maka /unmutemic di grupA HARUS ditolak
    selama: now < waktu_join_vc_grupA + BIO_TTL_SECS + 5 (buffer 5 detik).
    Cooldown ini per grup — siapapun yang memanggil /unmutemic di grupA
    selama window ini akan kena tolak yang sama, bukan per user.
    Jika grupA belum pernah tercatat join VC sama sekali (_vc_join_last_ts
    tidak punya entri), maka tidak ada cooldown lapis ini (lanjut ke lapis 2).

  Lapis 2 — Cooldown antar-eksekusi per grup, 5 detik (anti double-klik
    beramai-ramai), SETELAH lapis 1 lewat:
    Begitu lapis 1 lewat, permintaan PERTAMA yang lolos di grup itu akan
    men-set _group_exec_cooldown[cid] = now. Permintaan lain di grup yang
    SAMA (dari user manapun, termasuk user yang sama) ditolak diam-diam
    selama 5 detik berikutnya. Ini mencegah banyak user yang bersamaan
    nge-spam /unmutemic begitu cooldown grup baru lewat, supaya tidak
    semuanya lolos lapis 3 sekaligus dan membebani worker scan VC.

  Lapis 3 — Cooldown anti-spam (per chat_id + user_id), 5 menit, SETELAH
    lapis 1 & 2 lewat:
    Kembali ke perilaku lama: tiap user punya cooldown sendiri 5 menit
    per grup (_COOLDOWN_SECS).
"""

import asyncio
import time

from pyrogram import Client, filters
from pyrogram.types import Message

from database import db

# ── Cooldown anti-spam: 5 menit per (chat_id, user_id) — lapis 3 ──────────
_unmutemic_cooldown: dict[tuple[int, int], float] = {}
_COOLDOWN_SECS = 300   # 5 menit

# ── Cooldown join-VC: per grup — lapis 1 (memblokir SEMUA user di grup) ───
# Window ini menunggu data bio hasil scan join-VC terakhir "matang" sesuai
# BIO_TTL_SECS (+ buffer 5 detik) sebelum /unmutemic diizinkan diproses lagi
# di grup tersebut. Nilai ini dibaca dari video_call._BIO_CACHE_TTL (yang
# sudah ikut env BIO_TTL_SECS) agar selalu konsisten satu sumber kebenaran.
_JOIN_COOLDOWN_BUFFER_SECS = 5

# ── Cooldown antar-eksekusi per grup — lapis 2 (anti double-klik ramai) ───
# Begitu satu permintaan lolos lapis 1 di grup ini, user LAIN (atau user
# yang sama) harus tunggu _GROUP_EXEC_COOLDOWN_SECS detik sebelum permintaan
# berikutnya di grup ini diproses lagi — terlepas siapa usernya.
_group_exec_cooldown: dict[int, float] = {}   # {chat_id: time.time()}
_GROUP_EXEC_COOLDOWN_SECS = 5   # 5 detik


@Client.on_message(filters.command("unmutemic") & filters.group)
async def cmd_unmutemic(client: Client, message: Message):
    """
    Perintah /unmutemic di grup.
    Siapapun bisa pakai (untuk diri sendiri) — tidak perlu admin.
    """
    cid = message.chat.id
    uid = message.from_user.id if message.from_user else None
    if not uid:
        try:
            await message.delete()
        except Exception:
            pass
        return

    # ── Hapus pesan perintah segera ─────────────────────────────────────────
    try:
        await message.delete()
    except Exception:
        pass

    # ── Import dari video_call ───────────────────────────────────────────────
    try:
        from video_call import (
            _ub_muted_this_user,
            _query_bio_from_db,
            _is_vip_user,
            _enqueue_vc_scan,
            is_userbot_ready,
            _sec_os_get,
            _member_cache,
            _vc_join_last_ts,
            _BIO_CACHE_TTL,
        )
    except ImportError as _e:
        print(f"[UnmuteMic] Import error dari video_call: {_e}")
        return

    # ── Lapis 1: cooldown JOIN VC — per grup, blokir SEMUA user ─────────────
    # Selama belum lewat (waktu_join_vc_grupA + BIO_TTL_SECS + 5s), command
    # ditolak diam-diam untuk siapapun di grup ini. Jika grup ini belum
    # pernah tercatat join VC, tidak ada cooldown lapis ini.
    now_mono = time.monotonic()
    join_ts = _vc_join_last_ts.get(cid)
    if join_ts is not None:
        join_cooldown_until = join_ts + _BIO_CACHE_TTL + _JOIN_COOLDOWN_BUFFER_SECS
        if now_mono < join_cooldown_until:
            remaining = join_cooldown_until - now_mono
            print(
                f"[UnmuteMic] grup={cid}: masih cooldown join-VC "
                f"({remaining:.1f}s lagi) → abaikan."
            )
            return

    # ── Lapis 2: cooldown antar-eksekusi per grup, 5 detik ──────────────────
    # Anti double-klik beramai-ramai: begitu lapis 1 baru lewat, banyak user
    # berbeda bisa kirim /unmutemic hampir bersamaan. Permintaan pertama yang
    # lolos di grup ini men-set jeda 5 detik untuk grup tersebut — siapapun
    # (user manapun) yang kirim lagi dalam jeda itu ditolak diam-diam.
    now = time.time()
    last_group_exec = _group_exec_cooldown.get(cid, 0.0)
    if now - last_group_exec < _GROUP_EXEC_COOLDOWN_SECS:
        return   # masih jeda antar-eksekusi grup → abaikan diam-diam

    # Set jeda grup sebelum proses agar permintaan user lain yang masuk
    # bersamaan juga langsung ditolak (cegah race antar user berbeda)
    _group_exec_cooldown[cid] = now

    # ── Lapis 3: cooldown anti-spam per (chat_id, user_id) ──────────────────
    last_used = _unmutemic_cooldown.get((cid, uid), 0.0)
    if now - last_used < _COOLDOWN_SECS:
        _group_exec_cooldown.pop(cid, None)   # kembalikan jeda grup, bukan dipakai
        return   # masih cooldown → abaikan diam-diam

    # Set cooldown sebelum proses agar spam saat proses berjalan juga ditolak
    _unmutemic_cooldown[(cid, uid)] = now

    # ── Cek apakah user pernah di-mute userbot ──────────────────────────────
    was_muted = await _ub_muted_this_user(cid, uid)
    if not was_muted:
        # User tidak ada di daftar muted userbot → abaikan, kembalikan cooldown
        _unmutemic_cooldown.pop((cid, uid), None)
        _group_exec_cooldown.pop(cid, None)
        return

    # ── Cek apakah Security OS aktif untuk grup ini ──────────────────────────
    sec_doc = await _sec_os_get(cid)
    if not sec_doc.get("enabled"):
        _unmutemic_cooldown.pop((cid, uid), None)
        _group_exec_cooldown.pop(cid, None)
        return

    # ── Cek userbot siap ─────────────────────────────────────────────────────
    if not is_userbot_ready():
        _unmutemic_cooldown.pop((cid, uid), None)
        _group_exec_cooldown.pop(cid, None)
        return

    # ── Cek apakah user adalah Member VIP grup ini ──────────────────────────
    is_vip = await _is_vip_user(cid, uid)

    if is_vip:
        # ── VIP: skip cek bio, langsung antri scan ──────────────────────────
        # Userbot akan naik VC dan unmute mic VIP tanpa memedulikan bio link.
        # _vc_scan_and_enforce akan menemukan user ini muted + _ub_muted_this_user=True
        # + _is_vip_user=True → unmute mic langsung.
        print(
            f"[UnmuteMic] uid={uid} grup={cid}: VIP → skip cek bio, antri scan VC."
        )
        _member_cache.pop((cid, uid), None)
        _enqueue_vc_scan(cid)
        return

    # ── Member biasa: cek bio via bot pemantau (fresh) ──────────────────────
    has_link, monitor_unavailable = await _query_bio_from_db(cid, uid)

    if has_link is True:
        # Kondisi A (spek): masih terdeteksi link di bio → abaikan perintah,
        # userbot tidak perlu join VC grup ini.
        print(f"[UnmuteMic] uid={uid} grup={cid}: bio masih ada link → abaikan.")
        return

    if has_link is None and not monitor_unavailable:
        # Golongan 1: user TIDAK DIKENALI SAMA SEKALI oleh bot pemantau
        # (bukan soal privasi/kosong — peer gagal di-resolve total).
        # Hasil akhir di siklus scan VC akan tetap MUTE untuk golongan ini,
        # jadi memaksa join VC di sini hanya buang-buang resource → abaikan.
        print(f"[UnmuteMic] uid={uid} grup={cid}: tidak dikenali bot pemantau → abaikan.")
        return

    # Kondisi B (spek): has_link=False — bio bersih/kosong/diprivasi (golongan 2),
    # ATAU monitor_unavailable (bot pemantau belum terdaftar) → tidak terdeteksi
    # link → userbot dipaksa join VC grup ini untuk unmute.
    # Invalidasi cache member agar non-member yang sudah join bisa dikenali ulang
    _member_cache.pop((cid, uid), None)

    # ── Antri scan VC ke worker ──────────────────────────────────────────────
    # Worker yang mengatur giliran — tidak bentrok dengan siklus 30 menit.
    print(
        f"[UnmuteMic] uid={uid} grup={cid}: tidak terdeteksi link "
        f"(has_link={has_link}, monitor_unavailable={monitor_unavailable}) "
        f"→ antri scan VC."
    )
    _enqueue_vc_scan(cid)
