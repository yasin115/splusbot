"""
group_registry_server.py
-------------------------------------------------------------------
سرور مرکزیِ «رجیستری گروه‌ها» - کاملاً جدا و مستقل از panel_server.py.
کارش فقط اینه: برای هر گروه (chat_id)، لیستِ نمونه‌های رباتِ همین مجموعه (بر
اساس آیدیِ عددیِ اکانت‌شون - BOT_ID) که الان توی اون گروه حضور دارن رو نگه
داره، تا هیچ‌وقت دو نمونه هم‌زمان توی یک گروه نمونن.

فقط سه اندپوینت داره:
  POST /api/check     - فقط می‌خونه: چه کسانی الان توی این گروه رجیستر شده‌ن
                         (برای پیش‌چک قبل از عضو شدن، بدون ثبت چیزی).
  POST /api/announce  - یک ربات اعلام می‌کنه «من الان توی این گروهم»
                         (ثبت/به‌روزرسانی) و در جواب، کل لیستِ حاضرینِ همون
                         گروه رو می‌گیره تا خودش تصمیم بگیره بمونه یا لفت بده.
  POST /api/leave     - یک ربات اعلام می‌کنه «از این گروه لفت دادم» (رزرو آزاد
                         می‌شه).

نصب و اجرا (یک‌بار، روی هاست/سروری که در دسترسِ همه‌ی نمونه‌های ربات باشه -
مثلاً همون‌جایی که panel_server.py هم روشه):
    pip install flask
    python group_registry_server.py

پیش‌فرض روی پورت 8788 بالا میاد؛ دقیقاً همون پورتی که توی config.py هر نمونه‌ی
ربات، GROUP_REGISTRY_API_URL بهش اشاره می‌کنه (http://<آیدی‌سرور>:8788/api).
اگه پشتِ فایروال اجرا می‌کنی، مطمئن شو پورت 8788 برای اتصالِ بیرونی بازه.

⚠️ REGISTRY_SECRET رو حتماً به یک رشته‌ی تصادفیِ بلند و متفاوت از PANEL_SECRET
عوض کن، و دقیقاً همون رشته رو توی config.py همه‌ی نمونه‌های ربات
(GROUP_REGISTRY_SECRET) هم بذار - وگرنه همه‌ی درخواست‌ها با خطای 401 رد می‌شن.
"""

import logging
import os
import sqlite3
import threading
import time
import traceback

from flask import Flask, request, jsonify

# هر خطای پیش‌بینی‌نشده (۵۰۰) کاملاً با traceback توی همین فایل لاگ می‌شه، چون
# قبلاً هیچ اثری از خودِ خطا جایی ثبت نمی‌شد و فقط توی لاگِ ربات‌ها «پاسخ 500
# داد» دیده می‌شد - که برای پیدا کردن علتِ واقعی (دیسک پر، دیتابیس قفل، پرمیشن و...)
# کافی نبود.
logging.basicConfig(
    filename=os.path.join(os.path.dirname(os.path.abspath(__file__)), "group_registry_errors.log"),
    level=logging.ERROR,
    format="%(asctime)s %(message)s",
)

# ============================================================
# تنظیمات
# ============================================================
PORT = 8790

# باید دقیقاً همینی باشه که توی config.py همه‌ی نمونه‌های ربات (متغیر
# GROUP_REGISTRY_SECRET) گذاشتی.
REGISTRY_SECRET = "him"

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "group_registry.db")

# اگه یک ربات بیشتر از این‌قدر ساعت هیچ /announce ای نفرسته (مثلاً پروسه‌ش
# کرش کرده و هیچ‌وقت /leave رو صدا نزده)، رزروش خودکار منقضی می‌شه تا یک
# اکانتِ واقعاً غیرفعال/خاموش، برای همیشه جلوی بقیه رو نگیره.
STALE_AFTER_HOURS = 72

app = Flask(__name__)
_db_lock = threading.Lock()


def _get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    # WAL باعث می‌شه چند پروسه/ریکوئستِ هم‌زمان (مثلاً اگه این سرور هم مثل ربات
    # اصلی زیر Passenger با چند worker بالا بیاد) به‌جای گرفتنِ خطای «database is
    # locked»، منتظر بمونن. busy_timeout هم یک محافظِ اضافه‌ست.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def _init_db():
    with _db_lock, _get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS group_presence (
                chat_id INTEGER NOT NULL,
                account_id INTEGER NOT NULL,
                bot_label TEXT,
                group_name TEXT,
                project TEXT,
                last_seen REAL NOT NULL,
                PRIMARY KEY (chat_id, account_id)
            )
            """
        )
        conn.commit()


_init_db()


_last_prune = 0.0


def _prune_stale_if_due(conn):
    """قبلاً این DELETE روی *هر* درخواستِ /check و /announce اجرا می‌شد - یعنی
    اندپوینتِ /check که قرار بود فقط-خواندنی باشه، هم نویسنده بود. این فشارِ
    نوشتنِ اضافه (خصوصاً وقتی چند ربات پشتِ‌سرهم برای صدها گروه announce می‌زنن)
    احتمالِ قفل‌شدنِ SQLite رو بیشتر می‌کرد. الان فقط هر یک ساعت یک‌بار واقعاً
    prune می‌کنیم."""
    global _last_prune
    now = time.time()
    if now - _last_prune < 3600:
        return
    cutoff = now - STALE_AFTER_HOURS * 3600
    conn.execute("DELETE FROM group_presence WHERE last_seen < ?", (cutoff,))
    _last_prune = now


def _roster(conn, chat_id):
    rows = conn.execute(
        "SELECT account_id, bot_label, group_name, project FROM group_presence WHERE chat_id = ?",
        (chat_id,),
    ).fetchall()
    return [
        {
            "account_id": row["account_id"],
            "bot_label": row["bot_label"],
            "group_name": row["group_name"],
            "project": row["project"],
        }
        for row in rows
    ]


def _authorized(data):
    return isinstance(data, dict) and data.get("secret") == REGISTRY_SECRET


@app.route("/api/check", methods=["POST"])
def api_check():
    data = request.get_json(silent=True) or {}
    if not _authorized(data):
        return jsonify({"error": "unauthorized"}), 401
    chat_id = data.get("chat_id")
    if chat_id is None:
        return jsonify({"error": "chat_id required"}), 400
    with _db_lock, _get_conn() as conn:
        _prune_stale_if_due(conn)
        conn.commit()
        members = _roster(conn, chat_id)
    return jsonify({"chat_id": chat_id, "members": members})


@app.route("/api/announce", methods=["POST"])
def api_announce():
    data = request.get_json(silent=True) or {}
    if not _authorized(data):
        return jsonify({"error": "unauthorized"}), 401
    chat_id = data.get("chat_id")
    account_id = data.get("account_id")
    if chat_id is None or account_id is None:
        return jsonify({"error": "chat_id and account_id required"}), 400
    with _db_lock, _get_conn() as conn:
        _prune_stale_if_due(conn)
        conn.execute(
            """
            INSERT INTO group_presence (chat_id, account_id, bot_label, group_name, project, last_seen)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id, account_id) DO UPDATE SET
                bot_label=excluded.bot_label,
                group_name=excluded.group_name,
                project=excluded.project,
                last_seen=excluded.last_seen
            """,
            (
                chat_id,
                account_id,
                data.get("bot_label"),
                data.get("group_name"),
                data.get("project"),
                time.time(),
            ),
        )
        conn.commit()
        members = _roster(conn, chat_id)
    return jsonify({"chat_id": chat_id, "members": members})


@app.route("/api/leave", methods=["POST"])
def api_leave():
    data = request.get_json(silent=True) or {}
    if not _authorized(data):
        return jsonify({"error": "unauthorized"}), 401
    chat_id = data.get("chat_id")
    account_id = data.get("account_id")
    if chat_id is None or account_id is None:
        return jsonify({"error": "chat_id and account_id required"}), 400
    with _db_lock, _get_conn() as conn:
        conn.execute(
            "DELETE FROM group_presence WHERE chat_id = ? AND account_id = ?",
            (chat_id, account_id),
        )
        conn.commit()
    return jsonify({"ok": True})


@app.errorhandler(Exception)
def _handle_unexpected_error(exc):
    """قبلاً هر ۵۰۰ بدون هیچ اثری از خودِ traceback برمی‌گشت - نه توی کنسول، نه
    جایی دیگه - و فقط توی لاگِ ربات‌ها «پاسخ 500 داد» دیده می‌شد. الان traceback
    کامل توی group_registry_errors.log ذخیره می‌شه تا علتِ واقعی (قفل دیتابیس،
    دیسک پر، پرمیشن و...) مشخص باشه."""
    logging.error("Unhandled exception in %s:\n%s", request.path, traceback.format_exc())
    return jsonify({"error": "internal_error"}), 500


if __name__ == "__main__":
    print(f"[group_registry_server] در حال اجرا روی پورت {PORT} - دیتابیس: {DB_PATH}")
    app.run(host="0.0.0.0", port=PORT)
