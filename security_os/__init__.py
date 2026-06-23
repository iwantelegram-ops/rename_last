"""
Folder ini berisi modul-modul terkait userbot Security OS:
  - video_call.py            : userbot Pyrogram (mute/unmute VC, deteksi bio-link)
  - monitor_bot_reference.py : manajer bot pemantau per-grup (cek bio member)
  - admin_session.py         : token sesi sementara untuk panel admin di DM

Modul-modul ini sengaja TETAP diimpor secara flat di seluruh proyek
(contoh: `from video_call import ...`, `import admin_session`) — bukan
sebagai `from security_os.video_call import ...` — agar tidak perlu mengubah
puluhan baris import yang sudah ada di plugins/. Folder ini ditambahkan
ke sys.path saat startup (lihat antigcast.py, baris dekat bagian atas),
sehingga Python tetap menemukan ketiga modul ini seolah berada di root.
"""

