import sqlite3
import threading
import queue
import re
import logging
import os
import json
import time
from datetime import datetime, timedelta
from config import DB_PATH, SUPER_OWNER_IDS

_write_logger = logging.getLogger("database.writer")

# تشخیص فحش با کتابخونه‌ی persian-text-guard (pip install persian-text-guard).
# مزیت نسبت به لیست/regex قبلی:
#  - کلمات به‌صورت «کلمه‌ی کامل» چک می‌شن، نه زیررشته؛ پس جمله‌هایی مثل
#    «کسی نیست کس بخونه» / «کسی اینجا هست؟» / «کسب و کار» / «کلاس» دیگه اشتباهاً
#    حذف نمی‌شن (قبلاً «کس» داخل «کسی» پیدا می‌شد).
#  - ترفندهای دورزدن رو می‌خونه: «ک.ی.ر»، «ک ی ر»، «کیییر»، «k1r»، حروف عربی/فارسی
#    شبیه هم، کاراکترهای نامرئی و ...
# فیلتر فقط یک‌بار موقع import ساخته می‌شه (immutable و thread-safe).
try:
    from persian_text_guard import ProfanityFilter, WordList
    _PROFANITY_FILTER = ProfanityFilter(WordList.persian_default())
except Exception as _ptg_err:  # کتابخونه نصب نیست → fallback امن (کلمه‌ی کامل)
    _PROFANITY_FILTER = None
    logging.getLogger("database").warning(
        "persian-text-guard در دسترس نیست (%s)؛ از fallback ساده استفاده می‌شه. "
        "نصب: pip install persian-text-guard", _ptg_err)

# fallback (فقط اگه کتابخونه نصب نبود): تطبیق «کلمه‌ی کامل» تا «کسی» گرفته نشه.
_BAD_WORDS = (
    "فحش", "کس", "کیر", "جنده", "پدر سگ", "مادر جنده", "حروم",
    "لعنت", "کص", "کونی", "خار", "گه", "کون", "کصکش",
)
_BAD_WORDS_PATTERN = re.compile(
    r"(?<![\w])(?:" + "|".join(re.escape(w) for w in _BAD_WORDS) + r")(?![\w])"
)


class _PersistentConnProxy:
    """پروکسی روی کانکشن دائمی sqlite.
    کد قدیمی بعد از هر کوئری conn.close() صدا می‌زد که باعث می‌شد به ازای
    هر کوئری یک اتصال جدید به فایل دیتابیس باز و بسته شود (بسیار کند و زیر بار
    زیاد باعث قفل‌شدن دیتابیس و کند شدن/هنگ کردن کل ربات می‌شد).
    این پروکسی close() را نادیده می‌گیرد تا همان کانکشن دائمی/مشترک دوباره
    استفاده شود، بدون این‌که لازم باشد صدها فراخوانی conn.close() در کل
    پروژه تغییر کنند."""

    def __init__(self, real_conn):
        self._real_conn = real_conn

    def close(self):
        pass  # عمداً هیچ کاری نمی‌کند؛ کانکشن دائمی است و نباید بسته شود

    def __getattr__(self, name):
        return getattr(self._real_conn, name)


class Database:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path

        # ==================== چک سلامت + بازیابی خودکار در startup ====================
        # قبلاً اگه فایل دیتابیس هر دلیلی خراب/corrupt می‌شد (دیسک پر شده، قطع
        # برق/kill شدن پروسه وسط نوشتن، یا فایل‌سیستم هاست که قفل‌گذاری WAL رو
        # درست پشتیبانی نمی‌کنه)، همون sqlite3.DatabaseError("database disk
        # image is malformed") مستقیم از دل __init__ بیرون می‌زد و کل ربات حتی
        # قبل از بالا اومدن، با یک traceback خام می‌مرد - بدون هیچ تلاشی برای
        # جبران. الان: قبل از باز کردن کانکشن اصلی، یک PRAGMA integrity_check
        # سریع می‌زنیم؛ اگه خراب بود، اول دنبال آخرین بکاپ سالم (پایین‌تر همین
        # فایل، تابع _rotate_backup) می‌گردیم و خودکار جایگزینش می‌کنیم؛ اگه
        # بکاپی هم نبود، به‌جای کرش خام، یک پیام فارسیِ قابل‌فهم با دستورات دقیق
        # بازیابی دستی چاپ می‌کنیم و تمیز خارج می‌شیم.
        self._ensure_db_integrity_or_recover()

        # یک کانکشن دائمی برای کل عمر برنامه به‌جای باز/بسته کردن مکرر آن.
        # WAL باعث می‌شود خواندن و نوشتن هم‌زمان بدون قفل‌شدن دیتابیس ممکن باشد
        # و busy_timeout از خطای "database is locked" جلوگیری می‌کند.
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        # سقف کش داخلی SQLite را محدود می‌کنیم تا خود موتور دیتابیس هم با
        # گذشت زمان بی‌دلیل RAM بیشتری نگیرد. این فقط کش اجرایی SQLite است و
        # هیچ داده‌ای را از دیتابیس حذف نمی‌کند.
        self._conn.execute("PRAGMA cache_size=-8192")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        # سقف کش داخلی SQLite را محدود می‌کنیم تا خود موتور دیتابیس هم با
        # گذشت زمان بی‌دلیل RAM بیشتری نگیرد. این فقط کش اجرایی SQLite است و
        # هیچ داده‌ای را از دیتابیس حذف نمی‌کند.
        self._conn.execute("PRAGMA cache_size=-8192")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        # کش درون‌حافظه‌ای سخنگو: قبلاً get_speaker_data و حالت سخنگو روی *هر* پیام
        # (حتی پیام‌های عادی که قرار نیست دستوری باشند) یک کوئری SQLite می‌زدند.
        # زیر بار زیاد همین کوئری‌های اضافه باعث کند شدن/تأخیر در پاسخ سخنگو می‌شد.
        # این کش دقیقاً همان دیتای دیتابیس را نگه می‌دارد و روی هر add/remove/clear/set
        # هم‌زمان با دیتابیس به‌روزرسانی می‌شود، پس هیچ‌وقت دیتای قدیمی نمی‌دهد.
        self._speaker_data_cache = {}   # chat_id -> {word: response}
        self._speaker_mode_cache = {}   # chat_id -> mode
        self._bio_mode_cache = {}       # chat_id -> mode بیو ("off" / "warn" / "lock")

        # ==================== کش‌های درون‌حافظه‌ای برای مسیرهای پرتکرار ====================
        # چک‌های زیر (روشن بودن ربات، قفل‌ها، ادمین بودن، مالک گروه، اسم اختصاصی ربات)
        # روی *هر* پیام در *هر* گروه انجام می‌شن. با نزدیک ۱۰۰۰ گروه فعال، هر پیام قبلاً
        # چندین کوئری SQLite جداگانه می‌زد (هرکدوم هرچند سریع، رویهم زیر بار زیاد باعث
        # کند شدن پاسخ‌گویی ربات می‌شد). این کش‌ها دقیقاً همون دیتای دیتابیس رو نگه
        # می‌دارن و روی هر set/add/remove هم‌زمان با دیتابیس آپدیت می‌شن، پس هیچ‌وقت
        # دیتای قدیمی/ناسازگار برنمی‌گردونن.
        self._bot_enabled_cache = None      # None یعنی هنوز لود نشده
        self._admin_ids_cache = None        # set(user_id) جدول admins (سراسری) - لود تنبل
        self._group_admins_cache = {}       # chat_id -> set(user_id)
        self._group_special_admins_cache = {}  # chat_id -> set(user_id) - مقام «ادمین ویژه»
        self._group_special_cache = {}      # chat_id -> set(user_id) - مقام «ویژه»
        self._group_exempt_cache = {}       # chat_id -> set(user_id) - مقام «معاف»
        self._group_owner_cache = {}        # chat_id -> owner_id یا None
        self._lock_cache = {}               # (chat_id, lock_type) -> bool
        self._lock_action_cache = {}        # (chat_id, lock_type) -> "warn"/"mute"/"ban"
        self._bot_name_cache = {}           # chat_id -> name یا None
        self._known_groups_cache = set()    # chat_id هایی که قبلاً در جدول groups ثبت شدن

        # کش کاربران نادیده‌گرفته‌شده و تنظیمات Auto Mod: این دوتا برخلاف بقیه‌ی
        # چک‌های بالا، قبلاً کش نشده بودن و روی *هر* پیام (توی هزاران گروه) مستقیم
        # به SQLite می‌رفتن. اضافه کردنشون به همون الگوی کش موجود (لود تنبل +
        # آپدیت هم‌زمان روی هر تغییر) این دو کوئری رو هم از مسیر داغ پیام حذف می‌کنه.
        self._ignored_users_cache = None    # set(user_id) - لود تنبل
        self._auto_mod_cache = {}           # chat_id -> dict تنظیمات

        # is_banned/is_muted هم دقیقاً همین مشکل رو داشتن: روی *هر* پیامِ *هر*
        # گروه (بدون استثنا) مستقیم دو تا SELECT به SQLite می‌زدن، چون برخلاف
        # بقیه‌ی چک‌ها کش نشده بودن.
        self._banned_cache = {}             # chat_id -> set(user_id)
        self._muted_cache = {}              # chat_id -> set(user_id)
        self._global_muted_cache = None     # set(user_id) - لود تنبل (محرومیت سراسری)
        self._warn_count_cache = {}         # user_id -> شمارنده‌ی اخطار سراسری (warns)
        self._user_warning_cache = {}       # (chat_id, user_id) -> شمارنده‌ی اخطار مخصوص گروه
        self._media_filter_cache = {}       # chat_id -> {media_key: {type, fp, action}} (فیلتر رسانه)
        self._random_name_enabled_cache = {}  # chat_id -> bool
        self._random_name_cache = {}          # (chat_id, user_id) -> random alias
        self._timer_lock = threading.RLock()  # عملیات جدول AUTOS را Thread-Safe نگه می‌دارد.

        # ==================== نویسنده‌ی پس‌زمینه (Background Writer) ====================
        # تا اینجا هر نوشتن روی دیتابیس (بن، سکوت، لقب، قفل، هرچی) مستقیم توی
        # همون تسکِ async که پیام رو پردازش می‌کرد conn.execute()+conn.commit()
        # صدا می‌زد. چون asyncio تک‌رشته‌ایه، دقیقاً همون لحظه‌ی commit (که
        # می‌تونه چند میلی‌ثانیه یا زیر بار دیسک/قفل حتی بیشتر طول بکشه) کل
        # ربات - یعنی همه‌ی چند هزار گروه دیگه - عملاً منتظر می‌موند.
        # اینجا یک ترد جدا (مستقل از event loop) با کانکشن مستقل خودش، و یک صف
        # (queue.Queue که خودش thread-safe است) ساخته می‌شه: توابع نوشتن به‌جای
        # commit مستقیم، کوئری رو توی این صف می‌ذارن (که خودش لحظه‌ای برمی‌گرده -
        # فقط یک put درون‌حافظه‌ست) و بلافاصله به کد async برمی‌گردن؛ ترد
        # پس‌زمینه کوئری‌ها رو یکی‌یکی واقعاً روی دیسک می‌نویسه، کاملاً جدا از
        # event loop اصلی. برای این‌که هیچ تناقضی توی خوندن‌ها پیش نیاد، همه‌ی
        # کش‌های درون‌حافظه‌ی بالا (بن/سکوت/قفل/ادمین و...) همچنان سنکرون و
        # هم‌زمان با هر add/remove آپدیت می‌شن؛ فقط خودِ نوشتن فیزیکی روی فایل
        # دیتابیس به تعویق می‌افته، نه وضعیتی که بقیه‌ی کد بلافاصله ازش می‌خونه.
        self._write_queue = queue.Queue()
        self._writer_db_lock = threading.Lock()
        self._writer_conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._writer_conn.execute("PRAGMA journal_mode=WAL")
        self._writer_conn.execute("PRAGMA synchronous=NORMAL")
        self._writer_conn.execute("PRAGMA busy_timeout=5000")
        self._writer_conn.execute("PRAGMA cache_size=-8192")
        self._writer_conn.execute("PRAGMA temp_store=MEMORY")
        self._writer_conn.execute("PRAGMA cache_size=-8192")
        self._writer_conn.execute("PRAGMA temp_store=MEMORY")
        self._writer_thread = threading.Thread(
            target=self._writer_loop, name="db-writer", daemon=True
        )
        self._writer_thread.start()

        self._init_db()

    def _get_connection(self):
        return _PersistentConnProxy(self._conn)

    # ==================== سلامت دیتابیس + بکاپ خودکار ====================
    def _backup_dir(self):
        d = os.path.join(os.path.dirname(os.path.abspath(self.db_path)) or ".", "db_backups")
        os.makedirs(d, exist_ok=True)
        return d

    def _ensure_db_integrity_or_recover(self):
        """قبل از باز کردن کانکشن اصلی صدا زده می‌شود. اگه فایل دیتابیس اصلاً
        وجود نداشت (اولین اجرا) کاری نمی‌کند. اگه وجود داشت ولی corrupt بود:
        ۱) اول دنبال جدیدترین بکاپ سالم توی db_backups/ می‌گردد و اگه پیدا شد،
           خودکار جایگزینش می‌کند (فایل خراب رو با پسوند .corrupted_<زمان>
           نگه می‌داره، پاک نمی‌کنه).
        ۲) اگه هیچ بکاپی نبود، به‌جای crash خام، یک پیام فارسیِ قابل‌فهم با
           دستورات دقیق بازیابی دستی (dump-based، چون .recover روی همه‌ی
           build های sqlite3 در دسترس نیست) چاپ می‌کند و سپس os._exit(1) -
           تمیزتر از یک traceback نامفهوم وسط ایمپورت main.py."""
        if not os.path.exists(self.db_path):
            return  # اولین اجرا - فایل تازه ساخته می‌شه، چیزی برای چک نیست

        def _is_healthy(path):
            try:
                test_conn = sqlite3.connect(path)
                row = test_conn.execute("PRAGMA integrity_check;").fetchone()
                test_conn.close()
                return bool(row) and row[0] == "ok"
            except Exception:
                return False

        if _is_healthy(self.db_path):
            return

        _write_logger.error("⛔ فایل دیتابیس %s خراب (corrupt) تشخیص داده شد.", self.db_path)

        # دنبال جدیدترین بکاپ سالم بگرد
        backup_dir = self._backup_dir()
        try:
            candidates = sorted(
                (f for f in os.listdir(backup_dir) if f.endswith(".db")),
                reverse=True,
            )
        except Exception:
            candidates = []

        for fname in candidates:
            candidate_path = os.path.join(backup_dir, fname)
            if _is_healthy(candidate_path):
                corrupted_backup_name = self.db_path + f".corrupted_{int(datetime.now().timestamp())}"
                try:
                    os.replace(self.db_path, corrupted_backup_name)
                    import shutil
                    shutil.copy2(candidate_path, self.db_path)
                    _write_logger.error(
                        "✅ خودکار از بکاپ سالم (%s) بازیابی شد. فایل خراب برای بررسی توی %s نگه داشته شد. "
                        "توجه: هر تغییری بین لحظه‌ی آخرین بکاپ و لحظه‌ی خراب‌شدن از دست رفته.",
                        fname, corrupted_backup_name,
                    )
                    return
                except Exception as e:
                    _write_logger.error("بازیابی خودکار از بکاپ شکست خورد: %s", e)
                    break  # به پیام دستی زیر برو

        # هیچ بکاپ سالمی پیدا نشد - دستورالعمل بازیابی دستی + خروج تمیز
        msg = (
            "\n" + "=" * 70 + "\n"
            "⛔ فایل دیتابیس خراب (corrupt) است و هیچ بکاپ سالمی هم پیدا نشد.\n"
            f"   مسیر: {self.db_path}\n\n"
            "برای بازیابی دستی (توی SSH سرور، توی همین پوشه):\n\n"
            f"  cp {os.path.basename(self.db_path)} {os.path.basename(self.db_path)}.corrupted_backup\n"
            f"  sqlite3 {os.path.basename(self.db_path)} \".dump\" > dump.sql 2> dump_errors.log\n"
            f"  sqlite3 {os.path.basename(self.db_path)}.recovered < dump.sql\n"
            f"  sqlite3 {os.path.basename(self.db_path)}.recovered \"PRAGMA integrity_check;\"\n\n"
            "اگه آخرین دستور 'ok' داد:\n"
            f"  mv {os.path.basename(self.db_path)} {os.path.basename(self.db_path)}.broken\n"
            f"  mv {os.path.basename(self.db_path)}.recovered {os.path.basename(self.db_path)}\n\n"
            "dump_errors.log رو هم چک کن - هر خطی که توش هست یعنی اون بخش از "
            "دیتا (مثلاً یک ردیف یا جدول خاص) قابل‌بازیابی نبوده و توی نسخه‌ی "
            "بازیابی‌شده نیست. بعد از این، دوباره ربات رو اجرا کن.\n"
            + "=" * 70 + "\n"
        )
        _write_logger.error(msg)
        print(msg)
        os._exit(1)

    def backup_now(self, keep=5):
        """یک بکاپ سالم و امن (با API رسمی backup خود sqlite - نه کپی خام فایل،
        که ممکنه وسط نوشتن نیمه‌کاره کپی بشه) توی db_backups/ می‌سازه و فقط
        `keep` تای آخر رو نگه می‌داره. از runtime_maintenance_loop دوره‌ای صدا
        زده می‌شه؛ خروجیش دقیقاً همون فایلیه که _ensure_db_integrity_or_recover
        در صورت خراب‌شدن دیتابیس اصلی، خودکار ازش استفاده می‌کنه."""
        backup_dir = self._backup_dir()
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        target = os.path.join(backup_dir, f"backup_{stamp}.db")
        try:
            dest_conn = sqlite3.connect(target)
            with dest_conn:
                self._conn.backup(dest_conn)
            dest_conn.close()
        except Exception as e:
            _write_logger.error("بکاپ خودکار دیتابیس شکست خورد: %s", e)
            return False

        try:
            existing = sorted(
                f for f in os.listdir(backup_dir)
                if f.startswith("backup_") and f.endswith(".db")
            )
            for old in existing[:-keep]:
                try:
                    os.remove(os.path.join(backup_dir, old))
                except Exception:
                    pass
        except Exception:
            pass
        return True

    def _writer_loop(self):
        """نویسنده‌ی پس‌زمینه را به‌صورت دسته‌ای اجرا می‌کند.
        قبلاً هر عملیات یک commit جدا داشت؛ زیر بار زیاد این یعنی تعداد بسیار
        زیادی sync روی دیسک. حالا تا 100 عملیات نزدیک به هم در یک تراکنش جمع
        می‌شوند، بدون اینکه کدهای قابلیت‌ها یا منطق دیتابیس تغییر کند."""
        while True:
            sql, params = self._write_queue.get()
            batch = [(sql, params)]
            try:
                # چند عملیات خیلی نزدیک را بدون منتظر ماندن طولانی جمع کن.
                while len(batch) < 100:
                    try:
                        batch.append(self._write_queue.get_nowait())
                    except queue.Empty:
                        break

                with self._writer_db_lock:
                    self._writer_conn.execute("BEGIN")
                    for item_sql, item_params in batch:
                        self._writer_conn.execute(item_sql, item_params)
                    self._writer_conn.commit()
            except Exception as e:
                try:
                    self._writer_conn.rollback()
                except Exception:
                    pass
                _write_logger.error(
                    "خطا در نوشتن دسته‌ای روی دیتابیس: %s | تعداد=%s | SQL=%s",
                    e, len(batch), sql
                )
            finally:
                # برای queue.join() در نگهداری دوره‌ای لازم است همه‌ی آیتم‌های
                # برداشته‌شده علامت انجام‌شده بخورند.
                for _ in batch:
                    self._write_queue.task_done()

    def _enqueue_write(self, sql, params):
        """نسخه‌ی غیرمسدودکننده‌ی execute+commit: بلافاصله برمی‌گرده (فقط یک
        put روی صف درون‌حافظه‌ست) و commit واقعی توی ترد پس‌زمینه انجام می‌شه.
        توابعی که به rowcount یا نتیجه‌ی فوری کوئری نیاز دارن از این استفاده
        نمی‌کنن و همچنان از _get_connection مستقیم/سنکرون استفاده می‌کنن."""
        self._write_queue.put((sql, params))

    def _init_db(self):
        conn = self._get_connection()
        c = conn.cursor()
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS owners (
                user_id INTEGER PRIMARY KEY
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS group_admins (
                chat_id INTEGER,
                user_id INTEGER,
                PRIMARY KEY (chat_id, user_id)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS group_special_admins (
                chat_id INTEGER,
                user_id INTEGER,
                PRIMARY KEY (chat_id, user_id)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS group_special (
                chat_id INTEGER,
                user_id INTEGER,
                PRIMARY KEY (chat_id, user_id)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS group_exempt (
                chat_id INTEGER,
                user_id INTEGER,
                PRIMARY KEY (chat_id, user_id)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS bans (
                user_id INTEGER PRIMARY KEY,
                reason TEXT,
                timestamp TEXT
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS mutes (
                user_id INTEGER PRIMARY KEY,
                until TEXT,
                reason TEXT
            )
        ''')
        # لیست بن/سکوت مخصوص هر گروه (جدا از هم؛ پاکسازی هر گروه فقط همون گروه رو پاک می‌کنه)
        c.execute('''
            CREATE TABLE IF NOT EXISTS group_bans (
                chat_id INTEGER,
                user_id INTEGER,
                reason TEXT,
                timestamp TEXT,
                banned_by INTEGER,
                PRIMARY KEY (chat_id, user_id)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS group_mutes (
                chat_id INTEGER,
                user_id INTEGER,
                until TEXT,
                reason TEXT,
                muted_by INTEGER,
                PRIMARY KEY (chat_id, user_id)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS warns (
                user_id INTEGER PRIMARY KEY,
                count INTEGER DEFAULT 0
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS word_filters (
                chat_id INTEGER,
                word TEXT,
                action TEXT,
                PRIMARY KEY (chat_id, word)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS media_filters (
                chat_id INTEGER,
                media_key TEXT,
                media_type TEXT,
                fingerprint TEXT DEFAULT '',
                action TEXT DEFAULT 'warn',
                PRIMARY KEY (chat_id, media_key)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS locks (
                chat_id INTEGER,
                lock_type TEXT,
                enabled INTEGER DEFAULT 0,
                action TEXT DEFAULT 'warn',
                PRIMARY KEY (chat_id, lock_type)
            )
        ''')
        # روی دیتابیس‌های قدیمی که این جدول رو بدون ستون action ساخته بودن.
        try:
            c.execute("ALTER TABLE locks ADD COLUMN action TEXT DEFAULT 'warn'")
        except Exception:
            pass
        c.execute('''
            CREATE TABLE IF NOT EXISTS bot_status (
                enabled INTEGER DEFAULT 1
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS stats (
                chat_id INTEGER PRIMARY KEY,
                total_messages INTEGER DEFAULT 0,
                last_updated TEXT
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS anti_spam (
                enabled INTEGER DEFAULT 0,
                max_messages INTEGER DEFAULT 5,
                time_window INTEGER DEFAULT 10
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id INTEGER PRIMARY KEY,
                display_name TEXT,
                title TEXT,
                custom_name TEXT,
                updated_at TEXT
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS user_titles (
                chat_id INTEGER,
                user_id INTEGER,
                title TEXT,
                updated_at TEXT,
                PRIMARY KEY (chat_id, user_id)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS user_stats (
                user_id INTEGER,
                chat_id INTEGER,
                message_count INTEGER DEFAULT 0,
                last_message TEXT,
                PRIMARY KEY (user_id, chat_id)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS user_ranks (
                user_id INTEGER,
                chat_id INTEGER,
                rank TEXT,
                assigned_at TEXT,
                PRIMARY KEY (user_id, chat_id)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS groups (
                group_id INTEGER PRIMARY KEY,
                group_name TEXT,
                owner_id INTEGER,
                joined_at TEXT
            )
        ''')
        # کشِ تعدادِ اعضا - فقط توسطِ رفرشِ دوره‌ای/کم‌فرکانسِ خودِ ربات (نه با
        # درخواستِ زنده‌ی پنل) به‌روز می‌شه؛ برای همینه که پنل هیچ‌وقت مستقیم
        # client.get_entity صدا نمی‌زنه و نمی‌تونه باعثِ هنگ بشه (نگاه کنید به
        # main.py -> بخشِ «بهینه‌سازی سرعت» و _get_member_count).
        try:
            c.execute("ALTER TABLE groups ADD COLUMN member_count INTEGER")
        except Exception:
            pass
        try:
            c.execute("ALTER TABLE groups ADD COLUMN member_count_updated_at REAL")
        except Exception:
            pass

        # ==================== تایمر / زمان‌بندی دستورات (AUTOS) ====================
        # هر رکورد یک زمان‌بندی مستقل برای یک گروه است. دستورات به‌صورت JSON ذخیره
        # می‌شوند تا با ری‌استارت پروسه و بدون وابستگی به RAM قابل بازیابی باشند.
        c.execute('''
            CREATE TABLE IF NOT EXISTS autos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                commands TEXT NOT NULL,
                creator_id INTEGER NOT NULL,
                created_at REAL NOT NULL,
                last_run REAL,
                next_run REAL NOT NULL,
                interval_seconds INTEGER,
                run_hour INTEGER,
                run_minute INTEGER,
                is_loop INTEGER NOT NULL DEFAULT 1,
                loop_count INTEGER NOT NULL DEFAULT 0,
                loop_count_use INTEGER NOT NULL DEFAULT 0,
                last_daily_date TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                last_error TEXT
            )
        ''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_autos_due ON autos(status, next_run)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_autos_chat ON autos(chat_id, id)')

        # گروه‌هایی که ربات با فرستادن لینک (خودکار) بهشون وارد شده. بعد از ۴۸ ساعت
        # فعالیت گروه چک می‌شه؛ اگه کمتر از ۱۰۰ پیام رد و بدل شده باشه، ربات خودکار خارج می‌شه.
        c.execute('''
            CREATE TABLE IF NOT EXISTS auto_joined_groups (
                group_id INTEGER PRIMARY KEY,
                group_name TEXT,
                joined_at TEXT,
                baseline_messages INTEGER DEFAULT 0,
                checked INTEGER DEFAULT 0
            )
        ''')

        # سقف تعداد لینک‌هایی که هر کاربر توی پیوی می‌تونه بفرسته تا ربات باهاش عضو
        # گروه بشه (نفر سوم به بعد رد می‌شه). فقط لینک‌هایی که واقعاً منجر به عضویت
        # موفق تو یک گروه (نه کانال) شدن شمرده می‌شن.
        c.execute('''
            CREATE TABLE IF NOT EXISTS user_join_link_counts (
                user_id INTEGER PRIMARY KEY,
                count INTEGER DEFAULT 0
            )
        ''')

        # روزشمار جداگانه‌ی هر گروه: از لحظه‌ای که مالک گروه با نوشتن «فعال» ربات رو
        # روشن می‌کنه، اینجا ثبت می‌شه. وقتی این روزشمار به ۳۰ روز برسه، یک‌بار (فقط
        # یک‌بار، به لطف notified_30) به پی‌وی هر دو سازنده‌ی اصلی خبر داده می‌شه که
        # فلان گپ ۳۰ روزه فعاله.
        c.execute('''
            CREATE TABLE IF NOT EXISTS group_activation (
                chat_id INTEGER PRIMARY KEY,
                group_name TEXT,
                activated_at TEXT,
                notified_30 INTEGER DEFAULT 0
            )
        ''')
        
        # ==================== جداول جدید ====================
        c.execute('''
            CREATE TABLE IF NOT EXISTS welcome_settings (
                chat_id INTEGER PRIMARY KEY,
                enabled INTEGER DEFAULT 1,
                message TEXT
            )
        ''')
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS goodbye_settings (
                chat_id INTEGER PRIMARY KEY,
                enabled INTEGER DEFAULT 1,
                message TEXT
            )
        ''')
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS auto_mod_settings (
                chat_id INTEGER PRIMARY KEY,
                spam_enabled INTEGER DEFAULT 1,
                ad_enabled INTEGER DEFAULT 1,
                badword_enabled INTEGER DEFAULT 1,
                warn_limit INTEGER DEFAULT 3,
                mute_duration INTEGER DEFAULT 300,
                silent_mode INTEGER DEFAULT 0
            )
        ''')
        # روی دیتابیس‌های قدیمی‌تر (که از قبل این جدول رو بدون ستون silent_mode
        # ساخته بودن) CREATE TABLE IF NOT EXISTS بالا هیچ اثری نداره، پس اینجا با
        # ALTER TABLE ستون رو اضافه می‌کنیم؛ اگه از قبل وجود داشته باشه (دیتابیس
        # تازه‌ساز) خطاش رو نادیده می‌گیریم.
        try:
            c.execute("ALTER TABLE auto_mod_settings ADD COLUMN silent_mode INTEGER DEFAULT 0")
        except Exception:
            pass
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS group_members_seen (
                chat_id INTEGER,
                user_id INTEGER,
                PRIMARY KEY (chat_id, user_id)
            )
        ''')

        # فعالیتِ روزانه‌ی هر کاربر توی هر گروه - برای رتبه‌بندیِ «برترین‌های
        # امروز/دیروز/این‌هفته/این‌ماه» توی پنلِ مرکزی. جدا از user_stats
        # (که فقط مجموعِ کلِ پیام‌ها رو نگه می‌داره و هیچ بعدِ زمانی نداره).
        # تاریخ به‌صورتِ رشته‌ی YYYY-MM-DD (به‌وقتِ ایران) ذخیره می‌شه.
        c.execute('''
            CREATE TABLE IF NOT EXISTS user_activity_daily (
                chat_id INTEGER,
                user_id INTEGER,
                date TEXT,
                message_count INTEGER DEFAULT 0,
                PRIMARY KEY (chat_id, user_id, date)
            )
        ''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_activity_chat_date ON user_activity_daily(chat_id, date)')

        # تعداد پیام‌های هر گروه به تفکیک ساعت (برای چکِ «۴۸ ساعتِ اخیر» به‌صورت لغزنده).
        # hour_ts = ابتدای همون ساعت به‌صورت epoch (ثانیه).
        c.execute('''
            CREATE TABLE IF NOT EXISTS group_hourly_activity (
                chat_id INTEGER,
                hour_ts INTEGER,
                message_count INTEGER DEFAULT 0,
                PRIMARY KEY (chat_id, hour_ts)
            )
        ''')

        c.execute('''
            CREATE TABLE IF NOT EXISTS speaker_data (
                chat_id INTEGER,
                word TEXT,
                response TEXT,
                PRIMARY KEY (chat_id, word)
            )
        ''')

        # حالت سخنگو (مؤدب/بی‌ادب/کم‌حرف/پرحرف/خاموش) مخصوص هر گروه. قبلاً این حالت با یک
        # متغیر global در main.py نگه‌داری می‌شد که یعنی وقتی ادمین یک گروه حالت را
        # عوض می‌کرد (مثلاً "حالت خاموش")، سخنگو در *همه‌ی* گروه‌های دیگر هم خاموش
        # می‌شد. همین دقیقاً همان چیزی بود که باعث می‌شد گاهی سخنگو بدون دلیل مشخصی
        # در یک گروه جواب ندهد در حالی که بقیه‌ی دستورات کار می‌کردند.
        c.execute('''
            CREATE TABLE IF NOT EXISTS speaker_settings (
                chat_id INTEGER PRIMARY KEY,
                mode TEXT DEFAULT 'polite'
            )
        ''')

        # حالت بررسی «بیو» عضو تازه‌وارد، مخصوص هر گروه:
        # off  -> کاری نداره (پیش‌فرض)
        # warn -> اگه بیوی عضو تازه‌وارد لینک داشته باشه فقط اخطار می‌ده
        # lock -> اگه بیوی عضو تازه‌وارد لینک داشته باشه بند (بن) می‌شه
        c.execute('''
            CREATE TABLE IF NOT EXISTS bio_mode_settings (
                chat_id INTEGER PRIMARY KEY,
                mode TEXT DEFAULT 'off'
            )
        ''')

        c.execute('''
            CREATE TABLE IF NOT EXISTS user_warnings (
                user_id INTEGER,
                chat_id INTEGER,
                count INTEGER DEFAULT 0,
                reason TEXT,
                timestamp TEXT,
                PRIMARY KEY (user_id, chat_id)
            )
        ''')
        
        c.execute('''
            CREATE TABLE IF NOT EXISTS bot_names (
                chat_id INTEGER PRIMARY KEY,
                name TEXT
            )
        ''')

        # ==================== انتخاب اسم تصادفی ====================
        c.execute('''
            CREATE TABLE IF NOT EXISTS random_names (
                chat_id INTEGER,
                user_id INTEGER,
                name TEXT NOT NULL,
                updated_at TEXT,
                PRIMARY KEY (chat_id, user_id)
            )
        ''')

        # ==================== «اصل» هر کاربر (مخصوص همون گروه) ====================
        # «اصل» یک متن آزاد (بیوگرافی کوتاه، مثلاً «یاسین ۲۳ تبریز») مخصوص یک کاربر
        # خاص در یک گروه خاصه، نه یک مقدار واحد برای کل گروه. با ریپلای روی پیامی که
        # همین متن رو داره و نوشتن «ثبت اصل»، متن همون پیام به‌عنوان «اصل» فرستنده‌ی
        # پیام ثبت می‌شه. از اون به بعد، با ریپلای روی پیام همون کاربر و نوشتن «اصل»،
        # همین متن نشون داده می‌شه. هر کاربر در هر گروه فقط یک «اصل» داره؛ ثبت جدید
        # قبلی رو جایگزین می‌کنه.
        c.execute('''
            CREATE TABLE IF NOT EXISTS user_asl (
                chat_id INTEGER,
                user_id INTEGER,
                asl_text TEXT,
                set_by INTEGER,
                updated_at TEXT,
                PRIMARY KEY (chat_id, user_id)
            )
        ''')

        # نادید گرفتن کاربر (فقط سازنده‌ی اصلی، از پی‌وی ربات): برخلاف bans/mutes،
        # این جدول هیچ محدودیتی روی کاربر اعمال نمی‌کنه (بن/سکوت نمی‌شه)، فقط
        # مشخص می‌کنه که ربات دیگه نباید به پیام‌های این کاربر (نه در پی‌وی، نه در
        # هیچ گروهی) هیچ پاسخی بده.
        c.execute('''
            CREATE TABLE IF NOT EXISTS ignored_users (
                user_id INTEGER PRIMARY KEY,
                added_by INTEGER,
                timestamp TEXT
            )
        ''')

        conn.commit()
        conn.close()
        self._init_defaults()
    
    def _init_defaults(self):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO bot_status (enabled) VALUES (1)")
        c.execute("INSERT OR IGNORE INTO anti_spam (enabled) VALUES (0)")
        c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('link_limit', '3')")
        conn.commit()
        conn.close()
    
    def get_setting(self, key, default=None):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = c.fetchone()
        conn.close()
        return row[0] if row else default
    
    def set_setting(self, key, value):
        self._enqueue_write(
"INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value))
        )
    
    def add_super_owner(self, user_id):
        """آیدی جدید رو به جدول owners اضافه می‌کنه (علاوه بر آیدی‌های ثابت داخل config.py)."""
        self._enqueue_write(
"INSERT OR IGNORE INTO owners (user_id) VALUES (?)", (user_id,)
        )

    def is_super_owner(self, user_id):
        """چک می‌کنه آیدی داخل لیست SUPER_OWNER_IDS (config.py) یا جدول owners (اضافه‌شده در ران‌تایم) باشه."""
        try:
            user_id = int(user_id)
        except (TypeError, ValueError):
            return False
        if user_id in SUPER_OWNER_IDS:
            return True
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT 1 FROM owners WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        conn.close()
        return row is not None

    def get_super_owners(self):
        """لیست همه‌ی سازنده‌ها: هم آیدی‌های ثابت config.py و هم آیدی‌های اضافه‌شده در دیتابیس."""
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT user_id FROM owners")
        db_owners = [row[0] for row in c.fetchall()]
        conn.close()
        return list(dict.fromkeys(list(SUPER_OWNER_IDS) + db_owners))

    def get_super_owner(self):
        """برای سازگاری با کدهای قدیمی؛ اولین سازنده رو برمی‌گردونه. ترجیحاً از get_super_owners استفاده کن."""
        owners = self.get_super_owners()
        return owners[0] if owners else None

    def remove_super_owner(self, user_id):
        """فقط سازنده‌های اضافه‌شده در ران‌تایم قابل حذفن؛ آیدی‌های ثابت داخل config.py حذف نمی‌شن."""
        self._enqueue_write(
"DELETE FROM owners WHERE user_id = ?", (user_id,)
        )
    
    def is_bot_enabled(self):
        if self._bot_enabled_cache is not None:
            return self._bot_enabled_cache
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT enabled FROM bot_status LIMIT 1")
        row = c.fetchone()
        conn.close()
        result = bool(row[0]) if row else True
        self._bot_enabled_cache = result
        return result
    
    def set_bot_enabled(self, enabled):
        self._bot_enabled_cache = bool(enabled)
        self._enqueue_write("UPDATE bot_status SET enabled = ?", (1 if enabled else 0,))
    
    def is_owner(self, user_id):
        return self.is_super_owner(user_id)
    
    def add_owner(self, user_id):
        self.add_super_owner(user_id)
    
    def remove_owner(self, user_id):
        self.remove_super_owner(user_id)
    
    def get_owners(self):
        return self.get_super_owners()
    
    def _load_admin_ids_cache(self):
        if self._admin_ids_cache is None:
            conn = self._get_connection()
            c = conn.cursor()
            c.execute("SELECT user_id FROM admins")
            self._admin_ids_cache = {row[0] for row in c.fetchall()}
            conn.close()
        return self._admin_ids_cache

    def is_admin(self, user_id):
        return user_id in self._load_admin_ids_cache()
    
    def add_admin(self, user_id):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (user_id,))
        conn.commit()
        conn.close()
        self._load_admin_ids_cache().add(user_id)
    
    def remove_admin(self, user_id):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
        conn.commit()
        conn.close()
        self._load_admin_ids_cache().discard(user_id)
    
    def get_admins(self):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT user_id FROM admins")
        rows = c.fetchall()
        conn.close()
        return [row[0] for row in rows]
    
    def get_seen_members(self, chat_id):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT user_id FROM group_members_seen WHERE chat_id = ?", (chat_id,))
        rows = c.fetchall()
        conn.close()
        return set(row[0] for row in rows)

    def add_seen_member(self, chat_id, user_id):
        self._enqueue_write(
"INSERT OR IGNORE INTO group_members_seen (chat_id, user_id) VALUES (?, ?)", (chat_id, user_id)
        )

    def remove_seen_member(self, chat_id, user_id):
        self._enqueue_write(
"DELETE FROM group_members_seen WHERE chat_id = ? AND user_id = ?", (chat_id, user_id)
        )

    def set_seen_members(self, chat_id, user_ids):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("DELETE FROM group_members_seen WHERE chat_id = ?", (chat_id,))
        c.executemany(
            "INSERT OR IGNORE INTO group_members_seen (chat_id, user_id) VALUES (?, ?)",
            [(chat_id, uid) for uid in user_ids]
        )
        conn.commit()
        conn.close()

    def _load_group_admins_cache(self, chat_id):
        if chat_id not in self._group_admins_cache:
            conn = self._get_connection()
            c = conn.cursor()
            c.execute("SELECT user_id FROM group_admins WHERE chat_id = ?", (chat_id,))
            self._group_admins_cache[chat_id] = {row[0] for row in c.fetchall()}
            conn.close()
        return self._group_admins_cache[chat_id]

    def is_group_admin(self, chat_id, user_id):
        return user_id in self._load_group_admins_cache(chat_id)

    def add_group_admin(self, chat_id, user_id):
        self._load_group_admins_cache(chat_id).add(user_id)
        self._enqueue_write(
            "INSERT OR IGNORE INTO group_admins (chat_id, user_id) VALUES (?, ?)",
            (chat_id, user_id)
        )

    def remove_group_admin(self, chat_id, user_id):
        self._load_group_admins_cache(chat_id).discard(user_id)
        self._enqueue_write(
            "DELETE FROM group_admins WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id)
        )

    def get_group_admins(self, chat_id):
        return list(self._load_group_admins_cache(chat_id))

    def remove_all_group_admins(self, chat_id):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("DELETE FROM group_admins WHERE chat_id = ?", (chat_id,))
        conn.commit()
        conn.close()
        self._group_admins_cache[chat_id] = set()

    # ==================== مقام‌بندی: معاف / ویژه / ادمین‌ویژه (مخصوص هر گروه) ====================
    # ترتیب مقام‌ها از پایین به بالا: عادی < معاف < ویژه < ادمین < ادمین‌ویژه < مالک گروه < سازنده.
    # «معاف» و «ویژه» هیچ دستور مدیریتی ندارن، فقط از قفل‌ها/فیلترها/Auto Mod آزادن
    # (هر کاری دلشون خواست می‌تونن بکنن). «ادمین‌ویژه» یک پله بالاتر از ادمین عادیه:
    # همه‌ی دستورهای ادمین رو داره، به‌علاوه اینکه ادمین‌های عادی نمی‌تونن روش
    # دستور بن/سکوت/اخطار بزنن (به get_user_rank / can_moderate نگاه کنید).

    def _load_group_special_admins_cache(self, chat_id):
        if chat_id not in self._group_special_admins_cache:
            conn = self._get_connection()
            c = conn.cursor()
            c.execute("SELECT user_id FROM group_special_admins WHERE chat_id = ?", (chat_id,))
            self._group_special_admins_cache[chat_id] = {row[0] for row in c.fetchall()}
            conn.close()
        return self._group_special_admins_cache[chat_id]

    def is_group_special_admin(self, chat_id, user_id):
        return user_id in self._load_group_special_admins_cache(chat_id)

    def add_group_special_admin(self, chat_id, user_id):
        self._load_group_special_admins_cache(chat_id).add(user_id)
        self._enqueue_write(
            "INSERT OR IGNORE INTO group_special_admins (chat_id, user_id) VALUES (?, ?)",
            (chat_id, user_id)
        )

    def remove_group_special_admin(self, chat_id, user_id):
        self._load_group_special_admins_cache(chat_id).discard(user_id)
        self._enqueue_write(
            "DELETE FROM group_special_admins WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id)
        )

    def get_group_special_admins(self, chat_id):
        return list(self._load_group_special_admins_cache(chat_id))

    def _load_group_special_cache(self, chat_id):
        if chat_id not in self._group_special_cache:
            conn = self._get_connection()
            c = conn.cursor()
            c.execute("SELECT user_id FROM group_special WHERE chat_id = ?", (chat_id,))
            self._group_special_cache[chat_id] = {row[0] for row in c.fetchall()}
            conn.close()
        return self._group_special_cache[chat_id]

    def is_group_special(self, chat_id, user_id):
        return user_id in self._load_group_special_cache(chat_id)

    def add_group_special(self, chat_id, user_id):
        self._load_group_special_cache(chat_id).add(user_id)
        self._enqueue_write(
            "INSERT OR IGNORE INTO group_special (chat_id, user_id) VALUES (?, ?)",
            (chat_id, user_id)
        )

    def remove_group_special(self, chat_id, user_id):
        self._load_group_special_cache(chat_id).discard(user_id)
        self._enqueue_write(
            "DELETE FROM group_special WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id)
        )

    def get_group_special(self, chat_id):
        return list(self._load_group_special_cache(chat_id))

    def _load_group_exempt_cache(self, chat_id):
        if chat_id not in self._group_exempt_cache:
            conn = self._get_connection()
            c = conn.cursor()
            c.execute("SELECT user_id FROM group_exempt WHERE chat_id = ?", (chat_id,))
            self._group_exempt_cache[chat_id] = {row[0] for row in c.fetchall()}
            conn.close()
        return self._group_exempt_cache[chat_id]

    def is_group_exempt(self, chat_id, user_id):
        return user_id in self._load_group_exempt_cache(chat_id)

    def add_group_exempt(self, chat_id, user_id):
        self._load_group_exempt_cache(chat_id).add(user_id)
        self._enqueue_write(
            "INSERT OR IGNORE INTO group_exempt (chat_id, user_id) VALUES (?, ?)",
            (chat_id, user_id)
        )

    def remove_group_exempt(self, chat_id, user_id):
        self._load_group_exempt_cache(chat_id).discard(user_id)
        self._enqueue_write(
            "DELETE FROM group_exempt WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id)
        )

    def get_group_exempt(self, chat_id):
        return list(self._load_group_exempt_cache(chat_id))

    # مقدار عددی مقام: 0=عادی 1=معاف 2=(قدیمی/غیرفعال) 3=ویژه 4=ادمین 5=مالک‌گروه 6=سازنده
    #
    # ==== مقام‌بندی فعلی (به‌روزشده) ====
    # عادی < معاف < ویژه < ادمین < مالک گروه < سازنده
    # - معاف: فقط از قفل‌ها/فیلترها/Auto Mod آزاده، هیچ دستور مدیریتی نداره.
    # - ویژه (قبلاً «ادمین ربات»/جدول group_admins): همه‌ی دستورهای مدیریتی
    #   (بن/سکوت/اخطار/پاک و...) رو داره، به‌جز سه مورد: نمی‌تونه پاکسازی
    #   (پاک‌کردن دسته‌ای پیام) بزنه، نمی‌تونه کسی رو معاف کنه، و نمی‌تونه کسی
    #   رو ویژه کنه (این دو مورد آخر هم عملاً چون «تنظیم ویژه»/«تنظیم معاف»
    #   محدودن، طبیعتاً رعایت می‌شه — نگاه کن به is_only_special_tier در main.py).
    # - ادمین (قبلاً «ادمین‌ویژه»/جدول group_special_admins): همه‌ی دستورهای
    #   ادمین رو داره، مقامش از ویژه بالاتره و ویژه نمی‌تونه روش کاری بکنه.
    # - RANK_SPECIAL (رتبه‌ی ۲، جدول قدیمی group_special) دیگه از هیچ دستوری
    #   ست نمی‌شه و عملاً منسوخ شده؛ فقط برای سازگاری با داده‌ی قدیمی نگه
    #   داشته شده.
    RANK_NORMAL = 0
    RANK_EXEMPT = 1
    RANK_SPECIAL = 2       # منسوخ - دیگه هیچ دستوری این رتبه رو ست نمی‌کنه
    RANK_ADMIN = 3         # = «ویژه» جدید (جدول group_admins)
    RANK_SPECIAL_ADMIN = 4  # = «ادمین» جدید (جدول group_special_admins)
    RANK_GROUP_OWNER = 5
    RANK_SUPER_OWNER = 6

    RANK_LABELS = {
        0: "👤 عضو عادی",
        1: "🕊 معاف",
        2: "👤 عضو عادی",
        3: "🌟 ویژه",
        4: "🎖 ادمین",
        5: "👑 مالک گروه",
        6: "🌠 سازنده",
    }

    def get_mod_rank(self, chat_id, user_id):
        """مقام کاربر توی مقام‌بندی مدیریتی ربات رو به‌صورت عدد (0 تا 6، طبق RANK_*
        بالا) برمی‌گردونه. توجه: این با get_user_rank (رتبه‌ی متنی دلخواهی که با
        دستور جداگانه‌ی «تنظیم رتبه» ثبت می‌شه) کاملاً فرق داره و ربطی بهش نداره."""
        if self.is_super_owner(user_id):
            return self.RANK_SUPER_OWNER
        if self.is_group_owner(chat_id, user_id):
            return self.RANK_GROUP_OWNER
        if self.is_group_special_admin(chat_id, user_id):
            return self.RANK_SPECIAL_ADMIN
        if self.is_admin(user_id) or self.is_group_admin(chat_id, user_id):
            return self.RANK_ADMIN
        if self.is_group_special(chat_id, user_id):
            return self.RANK_SPECIAL
        if self.is_group_exempt(chat_id, user_id):
            return self.RANK_EXEMPT
        return self.RANK_NORMAL

    def get_mod_rank_label(self, chat_id, user_id):
        return self.RANK_LABELS.get(self.get_mod_rank(chat_id, user_id), "👤 عضو عادی")

    def can_moderate(self, chat_id, actor_id, target_id):
        """چک می‌کنه actor مجاز باشه روی target دستور مدیریتی (بن/سکوت/اخطار و...)
        بزنه: فقط وقتی مقام actor از مقام target واقعاً بالاتر باشه (یا actor سازنده باشه)."""
        if self.is_super_owner(actor_id):
            return True
        return self.get_mod_rank(chat_id, actor_id) > self.get_mod_rank(chat_id, target_id)

    # ==================== بن/سکوت مخصوص هر گروه ====================
    def ban_user(self, chat_id, user_id, reason="", banned_by=None):
        self._enqueue_write(
            "INSERT OR REPLACE INTO group_bans (chat_id, user_id, reason, timestamp, banned_by) VALUES (?, ?, ?, ?, ?)",
            (chat_id, user_id, reason, datetime.now().isoformat(), banned_by)
        )
        self._load_banned_cache(chat_id).add(user_id)

    def unban_user(self, chat_id, user_id):
        self._enqueue_write(
            "DELETE FROM group_bans WHERE chat_id = ? AND user_id = ?", (chat_id, user_id)
        )
        self._load_banned_cache(chat_id).discard(user_id)

    def _load_banned_cache(self, chat_id):
        if chat_id not in self._banned_cache:
            conn = self._get_connection()
            c = conn.cursor()
            c.execute("SELECT user_id FROM group_bans WHERE chat_id = ?", (chat_id,))
            self._banned_cache[chat_id] = {row[0] for row in c.fetchall()}
            conn.close()
        return self._banned_cache[chat_id]

    def is_banned(self, chat_id, user_id):
        # این تابع روی *هر* پیامِ *هر* گروه صدا زده می‌شه (نگاه کنید به
        # message_handler)؛ قبلاً بدون کش، مستقیم یک SELECT به SQLite می‌زد.
        # الان مثل بقیه‌ی چک‌های پرتکرار، لود تنبل + کش درون‌حافظه‌ست.
        return user_id in self._load_banned_cache(chat_id)

    def get_banned_users(self, chat_id):
        """لیست بن‌شده‌های همین گروه؛ گروه‌های دیگه دست نمی‌خوره."""
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT user_id, reason, timestamp FROM group_bans WHERE chat_id = ?", (chat_id,))
        rows = c.fetchall()
        conn.close()
        return [{"user_id": row[0], "reason": row[1], "timestamp": row[2]} for row in rows]

    def clear_banned_users(self, chat_id):
        """پاکسازی کامل لیست بن فقط همین گروه (بن سایر گروه‌ها دست نمی‌خوره). تعداد پاک‌شده رو برمی‌گردونه."""
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM group_bans WHERE chat_id = ?", (chat_id,))
        count = c.fetchone()[0]
        c.execute("DELETE FROM group_bans WHERE chat_id = ?", (chat_id,))
        conn.commit()
        conn.close()
        self._banned_cache[chat_id] = set()
        return count

    def mute_user(self, chat_id, user_id, until=None, reason="", muted_by=None):
        self._enqueue_write(
            "INSERT OR REPLACE INTO group_mutes (chat_id, user_id, until, reason, muted_by) VALUES (?, ?, ?, ?, ?)",
            (chat_id, user_id, until or "permanent", reason, muted_by)
        )
        self._load_muted_cache(chat_id).add(user_id)

    def unmute_user(self, chat_id, user_id):
        self._enqueue_write(
            "DELETE FROM group_mutes WHERE chat_id = ? AND user_id = ?", (chat_id, user_id)
        )
        self._load_muted_cache(chat_id).discard(user_id)

    def _load_muted_cache(self, chat_id):
        if chat_id not in self._muted_cache:
            conn = self._get_connection()
            c = conn.cursor()
            c.execute("SELECT user_id FROM group_mutes WHERE chat_id = ?", (chat_id,))
            self._muted_cache[chat_id] = {row[0] for row in c.fetchall()}
            conn.close()
        return self._muted_cache[chat_id]

    def is_muted(self, chat_id, user_id):
        # مثل is_banned: روی هر پیام صدا زده می‌شه، الان کش شده.
        return user_id in self._load_muted_cache(chat_id)

    def get_muted_users(self, chat_id):
        """لیست سکوت‌شده‌های همین گروه؛ گروه‌های دیگه دست نمی‌خوره."""
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT user_id, reason, until FROM group_mutes WHERE chat_id = ?", (chat_id,))
        rows = c.fetchall()
        conn.close()
        return [{"user_id": row[0], "reason": row[1], "until": row[2]} for row in rows]

    # ==================== محرومیت/سکوت سراسری (فقط سازنده‌ی اصلی) ====================
    # برخلاف mute_user/group_mutes که مخصوص یک گروه است، این‌ها روی جدول
    # سراسری mutes کار می‌کنند و صرف‌نظر از این‌که کاربر در یک گروه خاص
    # ادمین یا مالک باشد، در همه‌ی گروه‌ها اعمال می‌شوند.
    def global_mute_user(self, user_id, reason=""):
        self._enqueue_write(
            "INSERT OR REPLACE INTO mutes (user_id, until, reason) VALUES (?, ?, ?)",
            (user_id, "permanent", reason)
        )
        self._load_global_muted_cache().add(user_id)

    def global_unmute_user(self, user_id):
        # قبلاً اینجا rowcount واقعیِ DELETE سنکرون چک می‌شد که این خودش یک نوع
        # race condition ایجاد می‌کرد: چون global_mute_user نوشتنش رو به صف
        # پس‌زمینه می‌سپاره (ممکنه هنوز واقعاً commit نشده باشه)، یک DELETE
        # سنکرونِ بلافاصله بعدش می‌تونست هیچ ردیفی پیدا نکنه و اشتباهاً False
        # برگردونه. چون کش درون‌حافظه همیشه فوری و دقیقه، همون رو منبع حقیقت
        # قرار می‌دیم؛ خودِ DELETE هم مثل بقیه از صف پس‌زمینه رد می‌شه.
        removed = user_id in self._load_global_muted_cache()
        self._load_global_muted_cache().discard(user_id)
        self._enqueue_write("DELETE FROM mutes WHERE user_id = ?", (user_id,))
        return removed

    def _load_global_muted_cache(self):
        if self._global_muted_cache is None:
            conn = self._get_connection()
            c = conn.cursor()
            c.execute("SELECT user_id FROM mutes")
            self._global_muted_cache = {row[0] for row in c.fetchall()}
            conn.close()
        return self._global_muted_cache

    def is_globally_muted(self, user_id):
        # این هم مثل is_banned/is_muted روی *هر* پیامِ *هر* گروه صدا زده می‌شه
        # (نگاه کنید به message_handler)؛ الان کش شده.
        return user_id in self._load_global_muted_cache()

    def get_globally_muted_users(self):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT user_id, reason, until FROM mutes")
        rows = c.fetchall()
        conn.close()
        return [{"user_id": row[0], "reason": row[1], "until": row[2]} for row in rows]

    def clear_muted_users(self, chat_id):
        """پاکسازی کامل لیست سکوت فقط همین گروه (سکوت سایر گروه‌ها دست نمی‌خوره). تعداد پاک‌شده رو برمی‌گردونه."""
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM group_mutes WHERE chat_id = ?", (chat_id,))
        count = c.fetchone()[0]
        c.execute("DELETE FROM group_mutes WHERE chat_id = ?", (chat_id,))
        conn.commit()
        conn.close()
        self._muted_cache[chat_id] = set()
        return count

    # ==================== نادید گرفتن (فقط سازنده‌ی اصلی) ====================
    # کاملاً جدا از بن/سکوت: کاربر هیچ محدودیتی روی ارسال پیامش نمی‌گیره، فقط
    # خود ربات دیگه بهش پاسخی نمی‌ده (نه پی‌وی، نه هیچ گروهی).
    def _load_ignored_users_cache(self):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT user_id FROM ignored_users")
        rows = c.fetchall()
        conn.close()
        self._ignored_users_cache = {row[0] for row in rows}

    def add_ignored_user(self, user_id, added_by=None):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO ignored_users (user_id, added_by, timestamp) VALUES (?, ?, ?)",
            (user_id, added_by, datetime.now().isoformat())
        )
        conn.commit()
        conn.close()
        if self._ignored_users_cache is None:
            self._load_ignored_users_cache()
        else:
            self._ignored_users_cache.add(user_id)

    def remove_ignored_user(self, user_id):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("DELETE FROM ignored_users WHERE user_id = ?", (user_id,))
        removed = c.rowcount > 0
        conn.commit()
        conn.close()
        if self._ignored_users_cache is None:
            self._load_ignored_users_cache()
        else:
            self._ignored_users_cache.discard(user_id)
        return removed

    def is_user_ignored(self, user_id):
        # قبلاً این تابع روی *هر* پیام (توی هر گروه) مستقیم یک SELECT به SQLite
        # می‌زد. چون لیست نادیده‌گرفته‌شده‌ها معمولاً خیلی کوچیکه و به‌ندرت تغییر
        # می‌کنه، یک‌بار توی حافظه لود می‌شه و از اون به بعد فقط یک لوکاپ روی
        # دیکشنری پایتونه؛ روی هر add/remove هم‌زمان با دیتابیس آپدیت می‌شه.
        if self._ignored_users_cache is None:
            self._load_ignored_users_cache()
        return user_id in self._ignored_users_cache

    def get_ignored_users(self):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT user_id, added_by, timestamp FROM ignored_users ORDER BY timestamp DESC")
        rows = c.fetchall()
        conn.close()
        return [{"user_id": row[0], "added_by": row[1], "timestamp": row[2]} for row in rows]

    def add_warn(self, user_id):
        # قبلاً: INSERT/UPDATE + commit سنکرون، بعد بلافاصله یک SELECT دیگه برای
        # خوندن شمارنده‌ی به‌روزشده - یعنی دو رفت‌وبرگشت سنکرون به دیتابیس روی
        # هر تخلف. چون همیشه دقیقاً می‌دونیم مقدار قبلی چند بوده (خودمون یکی
        # بهش اضافه می‌کنیم)، دیگه نیازی به SELECT دوباره نیست؛ فقط کش رو
        # افزایش می‌دیم و نوشتن واقعی روی دیسک رو به ترد پس‌زمینه می‌سپاریم.
        current = self.get_warn_count(user_id) + 1
        self._warn_count_cache[user_id] = current
        self._enqueue_write(
            "INSERT INTO warns (user_id, count) VALUES (?, 1) ON CONFLICT(user_id) DO UPDATE SET count = count + 1",
            (user_id,)
        )
        return current
    
    def remove_warn(self, user_id):
        self._enqueue_write(
            "DELETE FROM warns WHERE user_id = ?", (user_id,)
        )
        self._warn_count_cache[user_id] = 0
    
    def get_warn_count(self, user_id):
        if user_id in self._warn_count_cache:
            return self._warn_count_cache[user_id]
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT count FROM warns WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        conn.close()
        result = row[0] if row else 0
        self._warn_count_cache[user_id] = result
        return result
    
    def get_warn_limit(self):
        limit = self.get_setting("warn_limit", "3")
        return int(limit)
    
    def set_warn_limit(self, limit):
        self.set_setting("warn_limit", str(limit))
    
    def add_word_filter(self, chat_id, word, action):
        self._enqueue_write(
"INSERT OR REPLACE INTO word_filters (chat_id, word, action) VALUES (?, ?, ?)", (chat_id, word.lower(), action)
        )
    
    def remove_word_filter(self, chat_id, word):
        """کلمه فیلترشده را حذف می‌کند و True/False برمی‌گرداند تا مشخص شود
        کلمه واقعاً در لیست فیلترها وجود داشته یا نه."""
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("DELETE FROM word_filters WHERE chat_id = ? AND word = ?", (chat_id, word.lower()))
        removed = c.rowcount > 0
        conn.commit()
        conn.close()
        return removed
    
    def get_word_filters(self, chat_id):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT word, action FROM word_filters WHERE chat_id = ?", (chat_id,))
        rows = c.fetchall()
        conn.close()
        return {row[0]: row[1] for row in rows}
    
    def clear_word_filters(self, chat_id):
        self._enqueue_write(
"DELETE FROM word_filters WHERE chat_id = ?", (chat_id,)
        )

    # ==================== فیلتر رسانه (گیف/ویدیو/آهنگ/استیکر/عکس/فایل) ====================
    # هر رسانه با «شناسه‌ی فایل روی سرور» (document.id / photo.id) ثبت می‌شه؛ این شناسه
    # برای یک فایل مشخص، حتی وقتی دست‌به‌دست/فوروارد می‌شه ثابته. برای فایل‌هایی که دوباره
    # آپلود شدن (شناسه‌ی جدید) یک «اثر انگشت» (حجم دقیق + نوع + مدت + ابعاد) هم نگه می‌داریم.
    # کل لیست هر گروه توی RAM کش می‌شه و تغییرات همزمان روی کش اعمال می‌شن، پس چک کردن
    # روی هر پیام رسانه‌ای هیچ کوئری دیتابیسی نداره.
    def _load_media_filters(self, chat_id):
        cached = self._media_filter_cache.get(chat_id)
        if cached is not None:
            return cached
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "SELECT media_key, media_type, fingerprint, action FROM media_filters WHERE chat_id = ?",
            (chat_id,),
        )
        rows = c.fetchall()
        conn.close()
        data = {r[0]: {"type": r[1], "fp": r[2] or "", "action": r[3] or "warn"} for r in rows}
        self._media_filter_cache[chat_id] = data
        return data

    def add_media_filter(self, chat_id, media_key, media_type, fingerprint="", action="warn"):
        data = self._load_media_filters(chat_id)
        data[media_key] = {"type": media_type, "fp": fingerprint or "", "action": action or "warn"}
        self._enqueue_write(
            "INSERT OR REPLACE INTO media_filters (chat_id, media_key, media_type, fingerprint, action) VALUES (?, ?, ?, ?, ?)",
            (chat_id, media_key, media_type, fingerprint or "", action or "warn"),
        )

    def find_media_filter(self, chat_id, media_key, fingerprint=""):
        """اگه این رسانه فیلتر شده باشه دیکشنری {type, fp, action} و در غیر این صورت None."""
        data = self._media_filter_cache.get(chat_id)
        if data is None:
            data = self._load_media_filters(chat_id)
        if not data:
            return None
        hit = data.get(media_key)
        if hit:
            return hit
        if fingerprint:
            for item in data.values():
                if item["fp"] and item["fp"] == fingerprint:
                    return item
        return None

    def remove_media_filter(self, chat_id, media_key, fingerprint=""):
        """همه‌ی ورودی‌هایی که با این شناسه (یا اثر انگشت) می‌خونن حذف می‌شن؛ True/False."""
        data = self._load_media_filters(chat_id)
        to_remove = [
            k for k, item in data.items()
            if k == media_key or (fingerprint and item["fp"] and item["fp"] == fingerprint)
        ]
        for k in to_remove:
            data.pop(k, None)
            self._enqueue_write(
                "DELETE FROM media_filters WHERE chat_id = ? AND media_key = ?", (chat_id, k)
            )
        return bool(to_remove)

    def get_media_filters(self, chat_id):
        return dict(self._load_media_filters(chat_id))

    def clear_media_filters(self, chat_id):
        self._load_media_filters(chat_id).clear()
        self._enqueue_write("DELETE FROM media_filters WHERE chat_id = ?", (chat_id,))
    
    def set_lock(self, chat_id, lock_type, enabled, action=None):
        key = (chat_id, lock_type)
        if action is None:
            action = self._lock_action_cache.get(key)
            if action is None:
                action = self.get_lock_action(chat_id, lock_type)
        action = action or "warn"
        self._lock_cache[key] = bool(enabled)
        self._lock_action_cache[key] = action
        self._enqueue_write(
            "INSERT OR REPLACE INTO locks (chat_id, lock_type, enabled, action) VALUES (?, ?, ?, ?)",
            (chat_id, lock_type, 1 if enabled else 0, action)
        )

    def set_lock_action(self, chat_id, lock_type, action):
        key = (chat_id, lock_type)
        enabled = self._lock_cache.get(key)
        if enabled is None:
            enabled = self.is_locked(chat_id, lock_type)
        self._lock_cache[key] = bool(enabled)
        self._lock_action_cache[key] = action
        self._enqueue_write(
            "INSERT OR REPLACE INTO locks (chat_id, lock_type, enabled, action) VALUES (?, ?, ?, ?)",
            (chat_id, lock_type, 1 if enabled else 0, action)
        )

    def get_lock_action(self, chat_id, lock_type):
        """عمل قفل رو برمی‌گردونه: "warn" (پیش‌فرض - اخطار و بعد از رسیدن به سقف
        اخطار بن)، "mute" (بلافاصله همون بار اول سکوت)، یا "ban" (بلافاصله همون
        بار اول بن)."""
        key = (chat_id, lock_type)
        if key in self._lock_action_cache:
            return self._lock_action_cache[key]
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT action FROM locks WHERE chat_id = ? AND lock_type = ?", (chat_id, lock_type))
        row = c.fetchone()
        conn.close()
        result = row[0] if row and row[0] else "warn"
        self._lock_action_cache[key] = result
        return result

    def is_locked(self, chat_id, lock_type):
        key = (chat_id, lock_type)
        if key in self._lock_cache:
            return self._lock_cache[key]
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT enabled FROM locks WHERE chat_id = ? AND lock_type = ?", (chat_id, lock_type))
        row = c.fetchone()
        conn.close()
        result = bool(row[0]) if row else False
        self._lock_cache[key] = result
        return result
    
    def get_active_locks(self, chat_id):
        """قفل‌های فعال یک گروه را مستقیم از SQLite برمی‌گرداند.

        برای نمایش «محدودیت» عمداً از کوئری مستقیم استفاده می‌شود تا این صفحه
        به‌خاطر کش قدیمی، قفل فعال را اشتباه «هیچ موردی» نشان ندهد.
        خروجی: {lock_type: action}.
        """
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "SELECT lock_type, COALESCE(action, 'warn') FROM locks WHERE chat_id = ? AND enabled = 1",
            (chat_id,)
        )
        rows = c.fetchall()
        conn.close()
        result = {str(row[0]): (row[1] or 'warn') for row in rows}
        for lock_type, action in result.items():
            self._lock_cache[(chat_id, lock_type)] = True
            self._lock_action_cache[(chat_id, lock_type)] = action
        return result

    # جدول‌هایی که با ستونِ chat_id به یک گروه ارجاع می‌دن، و جدول‌هایی که با
    # group_id (فقط groups/auto_joined_groups) - برای مهاجرتِ آیدیِ خرابِ
    # قدیمی (ببینید get_unmarked_group_ids/remap_group_id پایین‌تر).
    _CHAT_ID_TABLES = [
        "group_admins", "group_special_admins", "group_special", "group_exempt",
        "group_bans", "group_mutes", "word_filters", "media_filters", "group_hourly_activity", "locks", "stats",
        "user_titles", "user_stats", "user_ranks", "group_activation",
        "welcome_settings", "goodbye_settings", "auto_mod_settings",
        "group_members_seen", "user_activity_daily", "speaker_data",
        "speaker_settings", "bio_mode_settings", "user_warnings", "bot_names",
        "random_names", "user_asl",
    ]
    _GROUP_ID_TABLES = ["groups", "auto_joined_groups"]

    def get_unmarked_group_ids(self):
        """آیدی‌های «بدون مارک» (مثبت) که به‌جای آیدیِ درستِ یک گروه (که همیشه
        منفیه - چه گروهِ عادی چه سوپرگروه/کانال) ثبت شدن. این نشونه‌ی یک باگِ
        قدیمی در مسیرِ «جوین با لینک» بود (استفاده از entity.id خام به‌جای
        آیدیِ مارک‌دار) که باعث می‌شد قفل‌ها/تنظیماتِ اون گروه هیچ‌وقت موقعِ
        بررسیِ پیام‌های واقعی (که با آیدیِ درست/مارک‌دار میان) پیدا نشن - یعنی
        «ثبت می‌شد ولی اعمال نمی‌شد». هر مقدارِ مثبت اینجا قطعاً همون باگه."""
        ids = set()
        conn = self._get_connection()
        c = conn.cursor()
        for table in self._GROUP_ID_TABLES:
            try:
                c.execute(f"SELECT DISTINCT group_id FROM {table} WHERE group_id > 0")
                ids.update(r[0] for r in c.fetchall())
            except Exception:
                pass
        for table in self._CHAT_ID_TABLES:
            try:
                c.execute(f"SELECT DISTINCT chat_id FROM {table} WHERE chat_id > 0")
                ids.update(r[0] for r in c.fetchall())
            except Exception:
                pass
        conn.close()
        return sorted(ids)

    def remap_group_id(self, old_id, new_id):
        """همه‌ی ردیف‌های مربوط به یک گروه رو از آیدیِ اشتباه (قدیمی/بدون‌مارک)
        به آیدیِ درستِ مارک‌دار منتقل می‌کنه. هر جدول با UPDATE OR IGNORE
        جداگانه - اگه توی جدولی گروهِ مقصد از قبل رکورد داشته باشه (تداخلِ
        کلیدِ اصلی)، فقط همون ردیف رد می‌شه، نه کلِ مهاجرت."""
        if old_id == new_id:
            return
        conn = self._get_connection()
        c = conn.cursor()
        for table in self._GROUP_ID_TABLES:
            try:
                c.execute(f"UPDATE OR IGNORE {table} SET group_id = ? WHERE group_id = ?", (new_id, old_id))
            except Exception:
                pass
        for table in self._CHAT_ID_TABLES:
            try:
                c.execute(f"UPDATE OR IGNORE {table} SET chat_id = ? WHERE chat_id = ?", (new_id, old_id))
            except Exception:
                pass
        conn.commit()
        conn.close()
        # کش‌های درون‌حافظه‌ای قفل‌ها ممکنه هنوز آیدیِ قدیمی رو داشته باشن؛
        # منتقلشون می‌کنیم تا خواندنِ بعدی مستقیماً درست باشه.
        for key in list(self._lock_cache.keys()):
            if key[0] == old_id:
                self._lock_cache[(new_id, key[1])] = self._lock_cache.pop(key)
        for key in list(self._lock_action_cache.keys()):
            if key[0] == old_id:
                self._lock_action_cache[(new_id, key[1])] = self._lock_action_cache.pop(key)

    def get_anti_spam_settings(self):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT enabled, max_messages, time_window FROM anti_spam LIMIT 1")
        row = c.fetchone()
        conn.close()
        if row:
            return {"enabled": bool(row[0]), "max_messages": row[1], "time_window": row[2]}
        return {"enabled": False, "max_messages": 5, "time_window": 10}
    
    def set_anti_spam(self, enabled, max_messages=5, time_window=10):
        self._enqueue_write(
            "UPDATE anti_spam SET enabled = ?, max_messages = ?, time_window = ?",
            (1 if enabled else 0, max_messages, time_window)
        )
    
    def update_stats(self, chat_id, count=1):
        self._enqueue_write(
            "INSERT INTO stats (chat_id, total_messages, last_updated) VALUES (?, ?, ?) "
            "ON CONFLICT(chat_id) DO UPDATE SET total_messages = total_messages + ?, last_updated = ?",
            (chat_id, count, datetime.now().isoformat(), count, datetime.now().isoformat())
        )

    def bulk_update_stats(self, chat_batch, user_batch):
        """نسخه‌ی دسته‌جمعی/تراکنشی update_stats + update_user_stats، مخصوص
        flush_stats_loop. قبلاً این حلقه به ازای *هر* گروه و *هر* کاربر یک
        conn.commit() جدا صدا می‌زد؛ یعنی با ۲-۳ هزار گروه فعال (و به‌مراتب کاربر
        بیشتر)، هر ۲۰ ثانیه یک‌بار هزاران commit سنکرون پشت سر هم روی همون تک‌ترد
        event loop اجرا می‌شد - دقیقاً در همون بازه کل ربات (همه‌ی گروه‌ها، همه‌ی
        دستورات) عملاً فریز می‌شد. اینجا همه‌ی نوشتن‌ها توی یک تراکنش با فقط
        یک commit در پایان انجام می‌شن که این فریزهای دوره‌ای رو از بین می‌بره.
        """
        if not chat_batch and not user_batch:
            return
        conn = self._get_connection()
        c = conn.cursor()
        now = datetime.now().isoformat()
        try:
            c.execute("BEGIN")
            for chat_id, count in chat_batch.items():
                c.execute(
                    "INSERT INTO stats (chat_id, total_messages, last_updated) VALUES (?, ?, ?) "
                    "ON CONFLICT(chat_id) DO UPDATE SET total_messages = total_messages + ?, last_updated = ?",
                    (chat_id, count, now, count, now)
                )
            for (user_id, chat_id), count in user_batch.items():
                c.execute(
                    "INSERT INTO user_stats (user_id, chat_id, message_count, last_message) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(user_id, chat_id) DO UPDATE SET message_count = message_count + ?, last_message = ?",
                    (user_id, chat_id, count, now, count, now)
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def bulk_update_daily_activity(self, daily_batch):
        """نسخه‌ی دسته‌جمعی‌ی معادلِ bulk_update_stats ولی برای user_activity_daily
        - داخلِ همون flush_stats_loop، هر ۲۰ ثانیه، در کنارِ آمارِ کلی صدا زده
        می‌شه. daily_batch: {(user_id, chat_id, date_str): count}."""
        if not daily_batch:
            return
        conn = self._get_connection()
        c = conn.cursor()
        try:
            c.execute("BEGIN")
            for (user_id, chat_id, date_str), count in daily_batch.items():
                c.execute(
                    "INSERT INTO user_activity_daily (chat_id, user_id, date, message_count) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(chat_id, user_id, date) DO UPDATE SET message_count = message_count + ?",
                    (chat_id, user_id, date_str, count, count)
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def bulk_update_hourly_activity(self, hourly_batch):
        """مثل bulk_update_daily_activity ولی برای group_hourly_activity.
        hourly_batch: {(chat_id, hour_ts): count}."""
        if not hourly_batch:
            return
        conn = self._get_connection()
        c = conn.cursor()
        try:
            c.execute("BEGIN")
            for (chat_id, hour_ts), count in hourly_batch.items():
                c.execute(
                    "INSERT INTO group_hourly_activity (chat_id, hour_ts, message_count) VALUES (?, ?, ?) "
                    "ON CONFLICT(chat_id, hour_ts) DO UPDATE SET message_count = message_count + ?",
                    (chat_id, hour_ts, count, count)
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_recent_message_count(self, chat_id, since_ts):
        """مجموع پیام‌های یک گروه از ساعتِ شاملِ since_ts به بعد (بازه‌ی لغزنده)."""
        since_hour = int(since_ts // 3600) * 3600
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "SELECT COALESCE(SUM(message_count), 0) FROM group_hourly_activity WHERE chat_id = ? AND hour_ts >= ?",
            (chat_id, since_hour)
        )
        row = c.fetchone()
        conn.close()
        return int(row[0] or 0) if row else 0

    def prune_hourly_activity(self, older_than_ts):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("DELETE FROM group_hourly_activity WHERE hour_ts < ?", (int(older_than_ts),))
        conn.commit()
        conn.close()

    def get_top_active(self, chat_id, date_from, date_to, limit=10):
        """رتبه‌بندیِ برترین‌های یک گروه بینِ دو تاریخ (شاملِ خودِ دو سر بازه)،
        بر اساسِ مجموعِ پیام‌ها. خروجی: [(user_id, total_count), ...] نزولی."""
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "SELECT user_id, SUM(message_count) as total FROM user_activity_daily "
            "WHERE chat_id = ? AND date >= ? AND date <= ? "
            "GROUP BY user_id ORDER BY total DESC LIMIT ?",
            (chat_id, date_from, date_to, limit)
        )
        rows = c.fetchall()
        conn.close()
        return [(r[0], r[1] or 0) for r in rows]

    def get_period_total_messages(self, chat_id, date_from, date_to):
        """مجموعِ کلِ پیام‌های یک گروه بینِ دو تاریخ (شاملِ خودِ دو سر بازه) -
        برخلافِ get_top_active که فقط نفراتِ برتر رو می‌ده، این عدد مجموعِ
        همه‌ی کاربرهاست (نه فقط ۱۰ تای اول)، تا وقتی فعالیتِ گروه بینِ افرادِ
        زیادی پخش شده، «آمارِ این بازه» به‌جای مجموعِ لیدربوردِ ناقص، عددِ
        واقعیِ کل رو نشون بده."""
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "SELECT COALESCE(SUM(message_count), 0) FROM user_activity_daily "
            "WHERE chat_id = ? AND date >= ? AND date <= ?",
            (chat_id, date_from, date_to)
        )
        row = c.fetchone()
        conn.close()
        return row[0] if row else 0

    def get_group_joined_at(self, group_id):
        """تاریخِ اولین باری که این گروه ثبت شد (ستونِ joined_at جدولِ groups)؛
        برای تشخیصِ گروه‌های «تازه» (بدونِ داده‌ی دیروز/هفته/ماه) توی گزارشِ
        فعالیت (چه شبانه، چه دستیِ آنی) استفاده می‌شه. اگه پیدا نشد None
        برمی‌گردونه."""
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT joined_at FROM groups WHERE group_id = ?", (group_id,))
        row = c.fetchone()
        conn.close()
        return row[0] if row else None

    def prune_daily_activity(self, keep_days=35):
        """ردیف‌های قدیمی‌تر از keep_days روز رو پاک می‌کنه تا این جدول برای
        هزاران گروه بی‌نهایت بزرگ نشه. تاریخِ مرز بر اساسِ رشته مقایسه می‌شه
        (چون فرمت YYYY-MM-DD به‌ترتیبِ لغوی هم مرتبه)."""
        cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("DELETE FROM user_activity_daily WHERE date < ?", (cutoff,))
        conn.commit()
        conn.close()

    def get_stats(self, chat_id):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT total_messages, last_updated FROM stats WHERE chat_id = ?", (chat_id,))
        row = c.fetchone()
        conn.close()
        return {"total_messages": row[0], "last_updated": row[1]} if row else {"total_messages": 0, "last_updated": None}
    
    def add_group(self, group_id, group_name):
        # اگه قبلاً ثبت شده (کش می‌دونه)، دیگه نیازی به کوئری/نوشتن روی دیتابیس نیست.
        # این تابع روی هر پیام صدا زده می‌شه، پس این کش از هزاران نوشتن اضافه‌ی
        # بی‌فایده (INSERT OR IGNORE که همیشه ignore می‌شه) روی دیتابیس جلوگیری می‌کنه.
        if group_id in self._known_groups_cache:
            return
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "INSERT OR IGNORE INTO groups (group_id, group_name, joined_at) VALUES (?, ?, ?)",
            (group_id, group_name, datetime.now().isoformat())
        )
        conn.commit()
        conn.close()
        self._known_groups_cache.add(group_id)
    
    def get_known_groups(self):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT group_id FROM groups")
        rows = c.fetchall()
        conn.close()
        group_ids = [row[0] for row in rows]
        self._known_groups_cache.update(group_ids)
        return group_ids

    def get_known_groups_with_names(self):
        """مثل get_known_groups ولی اسم هر گروه رو هم برمی‌گردونه (یک کوئری واحد،
        نه یک کوئری جدا به‌ازای هر گروه) - برای گزارش «گروه‌های فعال» به پنل مرکزی
        سازنده استفاده می‌شه تا اونجا فقط شمارش خام نبینی، اسم گروه‌ها رو هم ببینی."""
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT group_id, group_name FROM groups")
        rows = c.fetchall()
        conn.close()
        return [{"group_id": r[0], "group_name": r[1] or ""} for r in rows]
    
    def set_group_owner(self, group_id, owner_id):
        conn = self._get_connection()
        c = conn.cursor()
        
        c.execute("SELECT owner_id FROM groups WHERE group_id = ?", (group_id,))
        row = c.fetchone()
        
        if not row:
            c.execute(
                "INSERT INTO groups (group_id, owner_id, joined_at) VALUES (?, ?, ?)",
                (group_id, owner_id, datetime.now().isoformat())
            )
            conn.commit()
            conn.close()
            self._known_groups_cache.add(group_id)
            self._group_owner_cache[group_id] = owner_id
            return True
        
        if row[0] is not None:
            conn.close()
            self._group_owner_cache[group_id] = row[0]
            return False
        
        c.execute("UPDATE groups SET owner_id = ? WHERE group_id = ?", (owner_id, group_id))
        conn.commit()
        conn.close()
        self._group_owner_cache[group_id] = owner_id
        return True
    
    def get_group_owner(self, group_id):
        if group_id in self._group_owner_cache:
            return self._group_owner_cache[group_id]
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT owner_id FROM groups WHERE group_id = ?", (group_id,))
        row = c.fetchone()
        conn.close()
        result = row[0] if row else None
        self._group_owner_cache[group_id] = result
        return result
    
    def remove_group_owner(self, group_id):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("UPDATE groups SET owner_id = NULL WHERE group_id = ?", (group_id,))
        conn.commit()
        conn.close()
        self._group_owner_cache[group_id] = None
    
    def is_group_owner(self, group_id, user_id):
        owner = self.get_group_owner(group_id)
        return owner == user_id
    
    def get_group_name(self, group_id):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT group_name FROM groups WHERE group_id = ?", (group_id,))
        row = c.fetchone()
        conn.close()
        return row[0] if row else None

    def set_group_name(self, group_id, group_name):
        """برخلافِ add_group (INSERT OR IGNORE که اگه ردیفِ گروه از قبل - حتی
        بدونِ اسم، مثلاً از مسیرِ set_group_owner - ساخته شده باشه، اسمِ تازه
        رو کلاً نادیده می‌گیره)، این متد همیشه اسمِ گروه رو با آخرین مقدارِ
        واقعیِ resolve‌شده به‌روز می‌کنه؛ هرجا اسمِ واقعیِ یک گروه به‌دست میاد
        (تازه‌جوین، چکِ عضویتِ از‌قبل، رفرشِ دوره‌ای) باید همین صدا زده بشه، نه
        add_group، تا پنل هیچ‌وقت روی یک اسمِ خالی/قدیمی گیر نکنه."""
        if not group_name:
            return
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "INSERT INTO groups (group_id, group_name, joined_at) VALUES (?, ?, ?) "
            "ON CONFLICT(group_id) DO UPDATE SET group_name = excluded.group_name",
            (group_id, group_name, datetime.now().isoformat())
        )
        conn.commit()
        conn.close()
        self._known_groups_cache.add(group_id)

    def set_group_member_count(self, group_id, count):
        """فقط از مسیرِ رفرشِ دوره‌ی/کم‌فرکانسِ main.py (نه از پنل) صدا زده
        می‌شه - نگاه کنید به توضیحاتِ ستونِ member_count بالای این فایل."""
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "UPDATE groups SET member_count = ?, member_count_updated_at = ? WHERE group_id = ?",
            (count, time.time(), group_id)
        )
        conn.commit()
        conn.close()

    def get_group_member_count(self, group_id):
        """(count, approx, updated_at) - approx=True یعنی این عدد قدیمی‌تر از
        یک روزه (یا اصلاً نداریمش) و باید به‌عنوانِ تخمین نمایش داده بشه."""
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT member_count, member_count_updated_at FROM groups WHERE group_id = ?", (group_id,))
        row = c.fetchone()
        conn.close()
        if not row or row[0] is None:
            return None, True, None
        updated_at = row[1] or 0
        approx = (time.time() - updated_at) > 86400
        return row[0], approx, updated_at

    def register_auto_joined_group(self, group_id, group_name, baseline_messages):
        """وقتی ربات با لینک وارد یک گروه جدید (بالای ۵۰ عضو) میشه، این گروه رو برای
        چک فعالیت بعد از ۴۸ ساعت ثبت می‌کنه. baseline_messages یعنی تعداد پیام‌های
        شمارش‌شده‌ی گروه (از جدول stats) درست در لحظه‌ی ورود ربات."""
        self._enqueue_write(
            "INSERT INTO auto_joined_groups (group_id, group_name, joined_at, baseline_messages, checked) "
            "VALUES (?, ?, ?, ?, 0) "
            "ON CONFLICT(group_id) DO UPDATE SET group_name = ?, joined_at = ?, baseline_messages = ?, checked = 0",
            (group_id, group_name, datetime.now().isoformat(), baseline_messages,
             group_name, datetime.now().isoformat(), baseline_messages)
        )

    def get_pending_auto_joined_groups(self):
        """گروه‌هایی که هنوز چک ۴۸ ساعته‌شون انجام نشده."""
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "SELECT group_id, group_name, joined_at, baseline_messages FROM auto_joined_groups WHERE checked = 0"
        )
        rows = c.fetchall()
        conn.close()
        return [
            {"group_id": r[0], "group_name": r[1], "joined_at": r[2], "baseline_messages": r[3]}
            for r in rows
        ]

    def get_all_auto_joined_groups(self):
        """همه‌ی گروه‌های عضوشده با لینک (چه چک‌شده چه نه) - برای چکِ لغزنده‌ی فعالیت."""
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT group_id, group_name, joined_at, baseline_messages FROM auto_joined_groups")
        rows = c.fetchall()
        conn.close()
        return [
            {"group_id": r[0], "group_name": r[1], "joined_at": r[2], "baseline_messages": r[3]}
            for r in rows
        ]

    def mark_auto_joined_checked(self, group_id):
        """گروه فعالیت کافی داشته؛ دیگه نیازی به چک دوباره نیست."""
        self._enqueue_write(
"UPDATE auto_joined_groups SET checked = 1 WHERE group_id = ?", (group_id,)
        )

    def remove_auto_joined_group(self, group_id):
        self._enqueue_write(
"DELETE FROM auto_joined_groups WHERE group_id = ?", (group_id,)
        )

    def record_group_activation(self, chat_id, group_name):
        """اولین باری که یک گروه با «فعال» روشن می‌شه، همون لحظه رو به‌عنوان مبدأ
        روزشمار ۳۰ روزه‌اش ثبت می‌کنه. اگه قبلاً ثبت شده بود (مثلاً یک بار خاموش و
        دوباره روشن شده)، تاریخ دست‌نخورده می‌مونه تا روزشمار از اول شروع نشه؛ فقط
        اسم گروه در صورت تغییر به‌روز می‌شه."""
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT chat_id FROM group_activation WHERE chat_id = ?", (chat_id,))
        row = c.fetchone()
        if row:
            c.execute("UPDATE group_activation SET group_name = ? WHERE chat_id = ?", (group_name, chat_id))
        else:
            c.execute(
                "INSERT INTO group_activation (chat_id, group_name, activated_at, notified_30) VALUES (?, ?, ?, 0)",
                (chat_id, group_name, datetime.now().isoformat())
            )
        conn.commit()
        conn.close()

    def get_pending_30day_groups(self):
        """گروه‌هایی که هنوز پیام «۳۰ روز فعالیت» براشون فرستاده نشده."""
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT chat_id, group_name, activated_at FROM group_activation WHERE notified_30 = 0")
        rows = c.fetchall()
        conn.close()
        return [{"chat_id": r[0], "group_name": r[1], "activated_at": r[2]} for r in rows]

    def mark_30day_notified(self, chat_id):
        self._enqueue_write(
"UPDATE group_activation SET notified_30 = 1 WHERE chat_id = ?", (chat_id,)
        )

    def get_all_group_activations(self):
        """همه‌ی گروه‌های فعال‌شده رو (برای نمایش در پنل سازنده - دستور «لیست
        روزشمار») برمی‌گردونه، از قدیمی‌ترین به جدیدترین."""
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT chat_id, group_name, activated_at, notified_30 FROM group_activation ORDER BY activated_at ASC")
        rows = c.fetchall()
        conn.close()
        return [{"chat_id": r[0], "group_name": r[1], "activated_at": r[2], "notified_30": r[3]} for r in rows]

    def add_speaker_response(self, chat_id, word, response):
        self._enqueue_write(
"INSERT OR REPLACE INTO speaker_data (chat_id, word, response) VALUES (?, ?, ?)", (chat_id, word, response)
        )
        # کش را هم‌زمان با دیتابیس آپدیت کن (به‌جای پاک کردن کامل و کوئری دوباره)
        self._speaker_data_cache.setdefault(chat_id, {})[word] = response

    def remove_speaker_response(self, chat_id, word):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("DELETE FROM speaker_data WHERE chat_id = ? AND word = ?", (chat_id, word))
        conn.commit()
        conn.close()
        if chat_id in self._speaker_data_cache:
            self._speaker_data_cache[chat_id].pop(word, None)

    def get_speaker_data(self, chat_id):
        # این تابع روی هر پیام معمولی گروه صدا زده می‌شود (نه فقط دستورها)، پس باید
        # فوق‌العاده سریع باشد. به‌جای زدن یک کوئری SQLite جدید به ازای هر پیام،
        # نتیجه را در حافظه کش می‌کنیم و فقط یک‌بار (تا وقتی چیزی تغییر کند) از
        # دیتابیس می‌خوانیم.
        if chat_id in self._speaker_data_cache:
            return self._speaker_data_cache[chat_id]
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT word, response FROM speaker_data WHERE chat_id = ?", (chat_id,))
        rows = c.fetchall()
        conn.close()
        data = {row[0]: row[1] for row in rows}
        self._speaker_data_cache[chat_id] = data
        return data

    def clear_speaker_data(self, chat_id):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("DELETE FROM speaker_data WHERE chat_id = ?", (chat_id,))
        conn.commit()
        conn.close()
        self._speaker_data_cache[chat_id] = {}

    def get_speaker_mode(self, chat_id):
        """حالت سخنگو مخصوص همین گروه (نه یک متغیر مشترک بین همه‌ی گروه‌ها)."""
        if chat_id in self._speaker_mode_cache:
            return self._speaker_mode_cache[chat_id]
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT mode FROM speaker_settings WHERE chat_id = ?", (chat_id,))
        row = c.fetchone()
        conn.close()
        mode = row[0] if row and row[0] in ("polite", "rude", "short", "long", "off") else "off"
        self._speaker_mode_cache[chat_id] = mode
        return mode

    def set_speaker_mode(self, chat_id, mode):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "INSERT INTO speaker_settings (chat_id, mode) VALUES (?, ?) "
            "ON CONFLICT(chat_id) DO UPDATE SET mode = excluded.mode",
            (chat_id, mode)
        )
        conn.commit()
        conn.close()
        self._speaker_mode_cache[chat_id] = mode

    def get_bio_mode(self, chat_id):
        """حالت بررسی بیوی اعضای تازه‌وارد مخصوص همین گروه (off/warn/lock)."""
        if chat_id in self._bio_mode_cache:
            return self._bio_mode_cache[chat_id]
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT mode FROM bio_mode_settings WHERE chat_id = ?", (chat_id,))
        row = c.fetchone()
        conn.close()
        mode = row[0] if row else "off"
        self._bio_mode_cache[chat_id] = mode
        return mode

    def set_bio_mode(self, chat_id, mode):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "INSERT INTO bio_mode_settings (chat_id, mode) VALUES (?, ?) "
            "ON CONFLICT(chat_id) DO UPDATE SET mode = excluded.mode",
            (chat_id, mode)
        )
        conn.commit()
        conn.close()
        self._bio_mode_cache[chat_id] = mode

    def get_bad_words(self):
        """لیست ثابتِ fallback (فقط برای سازگاری با کد قدیمی)."""
        return _BAD_WORDS

    def find_bad_word(self, text):
        """اولین فحشِ پیدا شده در متن رو برمی‌گردونه (همون متنی که کاربر نوشته)؛
        اگه چیزی نبود None. با persian-text-guard کلمه‌ی کامل چک می‌شه (پس
        «کسی نیست کس بخونه» پاک نمی‌شه) و فحش‌های دورزده‌شده (ک.ی.ر، کیییر و ...)
        هم گرفته می‌شن."""
        if not text:
            return None
        if _PROFANITY_FILTER is not None:
            m = _PROFANITY_FILTER.find_match(text)
            if m is None:
                return None
            return text[m.index:m.index + m.length].strip() or m.word.text
        m = _BAD_WORDS_PATTERN.search(text)
        return m.group(0) if m else None

    def get_user_profile(self, user_id):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT display_name, title, custom_name, updated_at FROM user_profiles WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        conn.close()
        # توجه: ستون title اینجا دیگه استفاده نمی‌شه (لقب حالا مخصوص هر گروهه، نه سراسری).
        # برای گرفتن لقب مخصوص یک گروه از get_user_title(chat_id, user_id) استفاده کن.
        if row:
            return {"display_name": row[0], "title": None, "custom_name": row[2], "updated_at": row[3]}
        return {"display_name": None, "title": None, "custom_name": None, "updated_at": None}
    
    def set_display_name(self, user_id, name):
        # نکته مهم: قبلاً این تابع از "INSERT OR REPLACE" استفاده می‌کرد که وقتی
        # ردیف کاربر از قبل وجود داشت، کل ردیف حذف و دوباره ساخته می‌شد و در نتیجه
        # مقادیر ستون‌های دیگر (مثل title و custom_name) پاک می‌شدند. با ON CONFLICT
        # DO UPDATE فقط همان ستونی که می‌خواهیم عوض می‌شود و بقیه دست‌نخورده می‌مانند.
        self._enqueue_write(
            "INSERT INTO user_profiles (user_id, display_name, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET display_name = excluded.display_name, updated_at = excluded.updated_at",
            (user_id, name, datetime.now().isoformat())
        )
    
    def set_user_title(self, chat_id, user_id, title):
        """لقب کاربر رو مخصوص همین گروه (chat_id) تنظیم می‌کنه (title=None یعنی حذف لقب).
        چون لقب حالا در جدول جداگانه‌ای بر اساس (chat_id, user_id) ذخیره می‌شه، تنظیم لقب
        در یک گروه هیچ تاثیری روی گروه‌های دیگه‌ای که همین کاربر عضوشونه نداره."""
        self._enqueue_write(
            "INSERT INTO user_titles (chat_id, user_id, title, updated_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(chat_id, user_id) DO UPDATE SET title = excluded.title, updated_at = excluded.updated_at",
            (chat_id, user_id, title, datetime.now().isoformat())
        )

    def remove_user_title(self, chat_id, user_id):
        self.set_user_title(chat_id, user_id, None)

    def get_user_title(self, chat_id, user_id):
        """لقب ثبت‌شده‌ی کاربر مخصوص همین گروه رو برمی‌گردونه (یا None اگه نداشته باشه)."""
        if chat_id is None:
            return None
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "SELECT title FROM user_titles WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id)
        )
        row = c.fetchone()
        conn.close()
        return row[0] if row and row[0] else None

    def find_user_by_title(self, chat_id, title_text):
        """یک عضو *همین گروه* رو بر اساس متن دقیق لقبش (که مخصوص همین گروهه) پیدا می‌کنه؛
        برای دستوراتی مثل «اخطار قهرمان» که به‌جای ریپلای/آیدی، متن لقب کاربر نوشته می‌شه."""
        title_text = (title_text or "").strip()
        if not title_text or chat_id is None:
            return None
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "SELECT user_id FROM user_titles "
            "WHERE chat_id = ? AND title IS NOT NULL AND TRIM(title) = ? COLLATE NOCASE "
            "LIMIT 1",
            (chat_id, title_text)
        )
        row = c.fetchone()
        conn.close()
        return row[0] if row else None

    # ==================== «اصل» هر گروه ====================
    def set_user_asl(self, chat_id, user_id, asl_text, set_by=None):
        """متن «اصل» (بیوگرافی) کاربر رو مخصوص همین گروه ثبت/بروزرسانی می‌کنه."""
        self._enqueue_write(
            "INSERT INTO user_asl (chat_id, user_id, asl_text, set_by, updated_at) VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(chat_id, user_id) DO UPDATE SET asl_text = excluded.asl_text, set_by = excluded.set_by, updated_at = excluded.updated_at",
            (chat_id, user_id, asl_text, set_by, datetime.now().isoformat())
        )

    def get_user_asl(self, chat_id, user_id):
        """متن «اصل» ثبت‌شده‌ی این کاربر مخصوص همین گروه رو برمی‌گردونه (یا None)."""
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT asl_text FROM user_asl WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
        row = c.fetchone()
        conn.close()
        return row[0] if row and row[0] else None

    def remove_user_asl(self, chat_id, user_id):
        self._enqueue_write(
"DELETE FROM user_asl WHERE chat_id = ? AND user_id = ?", (chat_id, user_id)
        )

    def set_custom_name(self, user_id, name):
        self._enqueue_write(
            "INSERT INTO user_profiles (user_id, custom_name, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET custom_name = excluded.custom_name, updated_at = excluded.updated_at",
            (user_id, name, datetime.now().isoformat())
        )
    
    def update_user_stats(self, user_id, chat_id, count=1):
        self._enqueue_write(
            "INSERT INTO user_stats (user_id, chat_id, message_count, last_message) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(user_id, chat_id) DO UPDATE SET message_count = message_count + ?, last_message = ?",
            (user_id, chat_id, count, datetime.now().isoformat(), count, datetime.now().isoformat())
        )
    
    def get_user_stats(self, user_id, chat_id):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT message_count, last_message FROM user_stats WHERE user_id = ? AND chat_id = ?", (user_id, chat_id))
        row = c.fetchone()
        conn.close()
        if row:
            return {"message_count": row[0], "last_message": row[1]}
        return {"message_count": 0, "last_message": None}
    
    def set_user_rank(self, user_id, chat_id, rank):
        self._enqueue_write(
            "INSERT OR REPLACE INTO user_ranks (user_id, chat_id, rank, assigned_at) VALUES (?, ?, ?, ?)",
            (user_id, chat_id, rank, datetime.now().isoformat())
        )
    
    def get_user_rank(self, user_id, chat_id):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT rank FROM user_ranks WHERE user_id = ? AND chat_id = ?", (user_id, chat_id))
        row = c.fetchone()
        conn.close()
        return row[0] if row else None
    
    def get_ranked_users(self, chat_id):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT user_id, rank FROM user_ranks WHERE chat_id = ?", (chat_id,))
        rows = c.fetchall()
        conn.close()
        return {row[0]: row[1] for row in rows}
    
    def get_full_user_info(self, user_id, chat_id):
        profile = self.get_user_profile(user_id)
        stats = self.get_user_stats(user_id, chat_id)
        rank = self.get_user_rank(user_id, chat_id)
        is_super_owner = self.is_super_owner(user_id)
        is_group_owner = self.is_group_owner(chat_id, user_id)
        is_admin = self.is_admin(user_id)
        is_banned = self.is_banned(chat_id, user_id)
        is_muted = self.is_muted(chat_id, user_id)
        warns = self.get_warn_count(user_id)
        
        return {
            "display_name": profile["display_name"],
            "title": self.get_user_title(chat_id, user_id),
            "custom_name": profile["custom_name"],
            "message_count": stats["message_count"],
            "last_message": stats["last_message"],
            "rank": rank,
            "is_super_owner": is_super_owner,
            "is_group_owner": is_group_owner,
            "is_admin": is_admin,
            "is_banned": is_banned,
            "is_muted": is_muted,
            "warns": warns
        }

    # ==================== متدهای جدید ====================
    
    # خوشامدگویی
    def get_welcome_settings(self, chat_id):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT enabled, message FROM welcome_settings WHERE chat_id = ?", (chat_id,))
        row = c.fetchone()
        conn.close()
        if row:
            return {"enabled": bool(row[0]), "message": row[1]}
        return {"enabled": True, "message": "👋 خوش اومدی {name} عزیز! به گروه خوش آمدی! 🎉"}

    def set_welcome_settings(self, chat_id, enabled=None, message=None):
        conn = self._get_connection()
        c = conn.cursor()
        current = self.get_welcome_settings(chat_id)
        if enabled is None:
            enabled = current["enabled"]
        if message is None:
            message = current["message"]
        c.execute(
            "INSERT OR REPLACE INTO welcome_settings (chat_id, enabled, message) VALUES (?, ?, ?)",
            (chat_id, 1 if enabled else 0, message)
        )
        conn.commit()
        conn.close()

    # خداحافظی ثابت و غیرقابل تنظیم
    def get_goodbye_settings(self, chat_id):
        return {"enabled": True, "message": "👋 خداحافظ {name} عزیز! امیدواریم دوباره ببینمت! 🌹"}

    def set_goodbye_settings(self, chat_id, enabled=None, message=None):
        # عمداً هیچ تغییری اعمال نمی‌شود؛ خداحافظی ثابت است.
        return None

    def get_auto_mod_settings(self, chat_id):
        # این تابع هم مثل is_locked/is_admin و... روی *هر* پیامی که فیلتر/قفل
        # نداره صدا زده می‌شه (تشخیص اسپم و تبلیغ). قبلاً کش نشده بود و زیر بار
        # هزاران گروه یک SELECT اضافه‌ی سنکرون روی هر پیام می‌زد.
        if chat_id in self._auto_mod_cache:
            return self._auto_mod_cache[chat_id]
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "SELECT spam_enabled, ad_enabled, badword_enabled, warn_limit, mute_duration, silent_mode FROM auto_mod_settings WHERE chat_id = ?",
            (chat_id,)
        )
        row = c.fetchone()
        conn.close()
        if row:
            result = {
                "spam_enabled": bool(row[0]),
                "ad_enabled": bool(row[1]),
                "badword_enabled": bool(row[2]),
                "warn_limit": row[3],
                "mute_duration": row[4],
                "silent_mode": bool(row[5]) if row[5] is not None else False
            }
        else:
            result = {
                "spam_enabled": True,
                "ad_enabled": False,
                "badword_enabled": False,
                "warn_limit": 3,
                "mute_duration": 300,
                "silent_mode": False
            }
        self._auto_mod_cache[chat_id] = result
        return result

    def set_auto_mod_settings(self, chat_id, **kwargs):
        current = self.get_auto_mod_settings(chat_id)
        current = dict(current)
        for key in ["spam_enabled", "ad_enabled", "badword_enabled", "warn_limit", "mute_duration", "silent_mode"]:
            if key in kwargs:
                current[key] = kwargs[key]
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO auto_mod_settings (chat_id, spam_enabled, ad_enabled, badword_enabled, warn_limit, mute_duration, silent_mode) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (chat_id, 1 if current["spam_enabled"] else 0, 1 if current["ad_enabled"] else 0, 1 if current["badword_enabled"] else 0, current["warn_limit"], current["mute_duration"], 1 if current["silent_mode"] else 0)
        )
        conn.commit()
        conn.close()
        self._auto_mod_cache[chat_id] = current
        return current

    def get_user_warnings(self, chat_id, user_id):
        key = (chat_id, user_id)
        if key in self._user_warning_cache:
            return self._user_warning_cache[key]
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT count FROM user_warnings WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
        row = c.fetchone()
        conn.close()
        result = row[0] if row else 0
        self._user_warning_cache[key] = result
        return result

    def add_user_warning(self, chat_id, user_id, reason=""):
        # همون بهینه‌سازی add_warn: چون شمارنده رو خودمون درون‌حافظه دنبال می‌کنیم،
        # دیگه نیازی به SELECT دوباره‌ی سنکرون بعد از commit نیست.
        key = (chat_id, user_id)
        current = self.get_user_warnings(chat_id, user_id) + 1
        self._user_warning_cache[key] = current
        now = datetime.now().isoformat()
        self._enqueue_write(
            "INSERT INTO user_warnings (chat_id, user_id, count, reason, timestamp) VALUES (?, ?, 1, ?, ?) ON CONFLICT(chat_id, user_id) DO UPDATE SET count = count + 1, reason = ?, timestamp = ?",
            (chat_id, user_id, reason, now, reason, now)
        )
        return current

    def remove_user_warning(self, chat_id, user_id):
        """فقط «یک» اخطار کم می‌کنه (نه همه)؛ تعداد باقی‌مونده رو برمی‌گردونه.
        وقتی به صفر برسه ردیف کاملاً پاک می‌شه."""
        key = (chat_id, user_id)
        current = max(0, self.get_user_warnings(chat_id, user_id) - 1)
        self._user_warning_cache[key] = current
        self._enqueue_write(
            "UPDATE user_warnings SET count = MAX(count - 1, 0) WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id),
        )
        if current == 0:
            self._enqueue_write(
                "DELETE FROM user_warnings WHERE chat_id = ? AND user_id = ? AND count <= 0",
                (chat_id, user_id),
            )
        return current

    def reset_user_warnings(self, chat_id, user_id):
        self._enqueue_write(
            "DELETE FROM user_warnings WHERE chat_id = ? AND user_id = ?", (chat_id, user_id)
        )
        self._user_warning_cache[(chat_id, user_id)] = 0

    # ==================== اسم اختصاصی ربات مخصوص هر گروه ====================
    # ==================== انتخاب اسم تصادفی ====================
    def get_random_name_enabled(self, chat_id):
        """وضعیت انتخاب اسم تصادفی برای هر گروه؛ پیش‌فرض روشن است."""
        try:
            chat_id = int(chat_id)
        except (TypeError, ValueError):
            return True
        if chat_id in self._random_name_enabled_cache:
            return self._random_name_enabled_cache[chat_id]
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "SELECT value FROM settings WHERE key = ?",
            (f"random_name_enabled:{chat_id}",),
        )
        row = c.fetchone()
        conn.close()
        enabled = True if row is None else str(row[0]).strip().lower() in ("1", "true", "on", "yes")
        self._random_name_enabled_cache[chat_id] = enabled
        return enabled

    def set_random_name_enabled(self, chat_id, enabled):
        """فعال/غیرفعال کردن انتخاب اسم تصادفی به‌صورت پایدار."""
        try:
            chat_id = int(chat_id)
        except (TypeError, ValueError):
            return
        value = "1" if enabled else "0"
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (f"random_name_enabled:{chat_id}", value),
        )
        conn.commit()
        conn.close()
        self._random_name_enabled_cache[chat_id] = bool(enabled)

    def get_random_name(self, chat_id, user_id):
        """نام فانتزیِ قبلاً انتخاب‌شده برای این کاربر در این گروه."""
        try:
            chat_id, user_id = int(chat_id), int(user_id)
        except (TypeError, ValueError):
            return None
        key = (chat_id, user_id)
        if key in self._random_name_cache:
            return self._random_name_cache[key]
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "SELECT name FROM random_names WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id),
        )
        row = c.fetchone()
        conn.close()
        name = row[0] if row and row[0] else None
        self._random_name_cache[key] = name
        return name

    def set_random_name(self, chat_id, user_id, name):
        """نام فانتزی انتخاب‌شده را برای همان کاربر و همان گروه ذخیره می‌کند."""
        try:
            chat_id, user_id = int(chat_id), int(user_id)
        except (TypeError, ValueError):
            return
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO random_names (chat_id, user_id, name, updated_at) VALUES (?, ?, ?, ?)",
            (chat_id, user_id, str(name), datetime.now().isoformat()),
        )
        conn.commit()
        conn.close()
        self._random_name_cache[(chat_id, user_id)] = str(name)

    def clear_random_name(self, chat_id, user_id):
        """نام فانتزی ذخیره‌شده را پاک می‌کند."""
        try:
            chat_id, user_id = int(chat_id), int(user_id)
        except (TypeError, ValueError):
            return
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("DELETE FROM random_names WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
        conn.commit()
        conn.close()
        self._random_name_cache.pop((chat_id, user_id), None)

    def get_bot_name(self, chat_id):
        if chat_id in self._bot_name_cache:
            return self._bot_name_cache[chat_id]
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT name FROM bot_names WHERE chat_id = ?", (chat_id,))
        row = c.fetchone()
        conn.close()
        result = row[0] if row and row[0] else None
        self._bot_name_cache[chat_id] = result
        return result

    def set_bot_name(self, chat_id, name):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO bot_names (chat_id, name) VALUES (?, ?)", (chat_id, name))
        conn.commit()
        conn.close()
        self._bot_name_cache[chat_id] = name

    def remove_bot_name(self, chat_id):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("DELETE FROM bot_names WHERE chat_id = ?", (chat_id,))
        conn.commit()
        conn.close()
        self._bot_name_cache[chat_id] = None

    # ==================== سقف لینک عضویت هر کاربر (پیوی) ====================
    def get_user_join_link_count(self, user_id):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute("SELECT count FROM user_join_link_counts WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        conn.close()
        return row[0] if row else 0

    def increment_user_join_link_count(self, user_id):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "INSERT INTO user_join_link_counts (user_id, count) VALUES (?, 1) "
            "ON CONFLICT(user_id) DO UPDATE SET count = count + 1",
            (user_id,)
        )
        conn.commit()
        c.execute("SELECT count FROM user_join_link_counts WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        conn.close()
        return row[0] if row else 1


    # ==================== Timer / Scheduler persistence ====================

    @staticmethod
    def _timer_row_to_dict(row):
        """یک رکورد AUTOS را با اعتبارسنجی مستقل به دیکشنری تبدیل می‌کند."""
        (
            timer_id, chat_id, timer_type, commands_json, creator_id, created_at,
            last_run, next_run, interval_seconds, run_hour, run_minute,
            is_loop, loop_count, loop_count_use, last_daily_date, status, last_error
        ) = row

        data_error = None
        try:
            commands = json.loads(commands_json)
            if (
                not isinstance(commands, list)
                or not commands
                or not all(isinstance(x, str) and x.strip() for x in commands)
            ):
                raise ValueError("commands باید یک لیست غیرخالی از رشته‌ها باشد")
            commands = [x.strip() for x in commands]
        except Exception as e:
            commands = []
            data_error = f"داده دستورات تایمر خراب است: {e}"

        try:
            timer_type = str(timer_type)
        except Exception:
            timer_type = "invalid"

        try:
            created_at = float(created_at)
        except Exception:
            created_at = 0.0
            data_error = data_error or "created_at تایمر خراب است"
        try:
            next_run = float(next_run)
        except Exception:
            next_run = 0.0
            data_error = data_error or "next_run تایمر خراب است"
        try:
            interval_seconds = int(interval_seconds) if interval_seconds is not None else None
        except Exception:
            interval_seconds = None
            data_error = data_error or "interval تایمر خراب است"
        try:
            run_hour = int(run_hour) if run_hour is not None else None
            run_minute = int(run_minute) if run_minute is not None else None
        except Exception:
            run_hour = run_minute = None
            data_error = data_error or "ساعت تایمر خراب است"
        try:
            loop_count = int(loop_count or 0)
        except Exception:
            loop_count = 0
            data_error = data_error or "loop_count تایمر خراب است"
        try:
            loop_count_use = int(loop_count_use or 0)
        except Exception:
            loop_count_use = 0
            data_error = data_error or "loop_count_use تایمر خراب است"

        if timer_type not in ("interval", "daily"):
            data_error = data_error or "type نامعتبر است"
        elif timer_type == "interval" and (interval_seconds is None or interval_seconds < 1):
            data_error = data_error or "interval نامعتبر است"
        elif timer_type == "daily" and (
            run_hour is None or run_minute is None
            or not (0 <= run_hour <= 23)
            or not (0 <= run_minute <= 59)
        ):
            data_error = data_error or "ساعت روزانه نامعتبر است"
        elif not bool(is_loop) and loop_count < 1:
            data_error = data_error or "loop_count نامعتبر است"

        if data_error:
            status = "error"
            last_error = data_error

        return {
            "id": int(timer_id),
            "chat_id": int(chat_id),
            "type": timer_type,
            "commands": commands,
            "creator_id": int(creator_id),
            "created_at": float(created_at),
            "last_run": float(last_run) if last_run is not None else None,
            "next_run": float(next_run),
            "interval_seconds": interval_seconds,
            "run_hour": run_hour,
            "run_minute": run_minute,
            "is_loop": bool(is_loop),
            "loop_count": loop_count,
            "loop_count_use": loop_count_use,
            "last_daily_date": last_daily_date,
            "status": status,
            "last_error": last_error,
        }


    def add_timer(
        self, chat_id, timer_type, commands, creator_id, next_run,
        interval_seconds=None, run_hour=None, run_minute=None,
        is_loop=True, loop_count=0, created_at=None
    ):
        """یک تایمر را به‌صورت اتمیک ثبت و id واقعی آن را برمی‌گرداند."""
        if not commands:
            raise ValueError("commands خالی است")
        if timer_type not in ("interval", "daily"):
            raise ValueError("نوع تایمر نامعتبر است")
        if timer_type == "interval" and (not interval_seconds or interval_seconds < 1):
            raise ValueError("interval نامعتبر است")
        if timer_type == "daily" and (
            run_hour is None or run_minute is None or
            not (0 <= int(run_hour) <= 23) or not (0 <= int(run_minute) <= 59)
        ):
            raise ValueError("ساعت روزانه نامعتبر است")
        if not is_loop and int(loop_count) < 1:
            raise ValueError("تعداد تکرار باید حداقل 1 باشد")

        created_at = float(created_at if created_at is not None else __import__("time").time())
        payload = json.dumps([str(x).strip() for x in commands], ensure_ascii=False)
        with self._timer_lock:
            conn = self._get_connection()
            c = conn.cursor()
            c.execute(
                """INSERT INTO autos
                   (chat_id, type, commands, creator_id, created_at, next_run,
                    interval_seconds, run_hour, run_minute, is_loop, loop_count,
                    loop_count_use, status, last_error)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'active', NULL)""",
                (
                    int(chat_id), timer_type, payload, int(creator_id), created_at,
                    float(next_run),
                    int(interval_seconds) if interval_seconds is not None else None,
                    int(run_hour) if run_hour is not None else None,
                    int(run_minute) if run_minute is not None else None,
                    1 if is_loop else 0, int(loop_count or 0),
                ),
            )
            conn.commit()
            return int(c.lastrowid)

    def get_timers(self, chat_id=None):
        """لیست تایمرها را بر اساس id برمی‌گرداند؛ شماره نمایشی در main محاسبه می‌شود."""
        with self._timer_lock:
            conn = self._get_connection()
            c = conn.cursor()
            if chat_id is None:
                c.execute(
                    """SELECT id, chat_id, type, commands, creator_id, created_at,
                              last_run, next_run, interval_seconds, run_hour, run_minute,
                              is_loop, loop_count, loop_count_use, last_daily_date,
                              status, last_error
                       FROM autos ORDER BY id"""
                )
            else:
                c.execute(
                    """SELECT id, chat_id, type, commands, creator_id, created_at,
                              last_run, next_run, interval_seconds, run_hour, run_minute,
                              is_loop, loop_count, loop_count_use, last_daily_date,
                              status, last_error
                       FROM autos WHERE chat_id = ? ORDER BY id""",
                    (int(chat_id),),
                )
            rows = c.fetchall()
            result = []
            for row in rows:
                item = self._timer_row_to_dict(row)
                # داده خراب را از حالت active خارج می‌کنیم تا Scheduler روی آن loop نشود.
                if item["status"] == "error" and row[15] != "error":
                    c.execute(
                        "UPDATE autos SET status = 'error', last_error = ? WHERE id = ?",
                        (item["last_error"], item["id"]),
                    )
                    conn.commit()
                result.append(item)
            return result

    def get_timer(self, timer_id):
        with self._timer_lock:
            conn = self._get_connection()
            c = conn.cursor()
            c.execute(
                """SELECT id, chat_id, type, commands, creator_id, created_at,
                          last_run, next_run, interval_seconds, run_hour, run_minute,
                          is_loop, loop_count, loop_count_use, last_daily_date,
                          status, last_error
                   FROM autos WHERE id = ?""",
                (int(timer_id),),
            )
            row = c.fetchone()
            return self._timer_row_to_dict(row) if row else None

    def get_due_timers(self, now_ts):
        """تمام تایمرهای فعال و رسیده را برمی‌گرداند؛ داده خراب هر تایمر مستقل مدیریت می‌شود."""
        with self._timer_lock:
            conn = self._get_connection()
            c = conn.cursor()
            c.execute(
                """SELECT id, chat_id, type, commands, creator_id, created_at,
                          last_run, next_run, interval_seconds, run_hour, run_minute,
                          is_loop, loop_count, loop_count_use, last_daily_date,
                          status, last_error
                   FROM autos
                   WHERE status = 'active' AND next_run <= ?
                   ORDER BY next_run, id""",
                (float(now_ts),),
            )
            rows = c.fetchall()
            result = []
            for row in rows:
                item = self._timer_row_to_dict(row)
                if item["status"] == "error":
                    c.execute(
                        "UPDATE autos SET status = 'error', last_error = ? WHERE id = ?",
                        (item["last_error"], item["id"]),
                    )
                else:
                    result.append(item)
            conn.commit()
            return result

    def update_timer_after_run(
        self, timer_id, last_run, next_run, loop_count_use,
        last_daily_date=None, last_error=None
    ):
        """وضعیت یک اجرای تایمر را ذخیره می‌کند؛ اگر سقف تکرار تمام شده باشد حذف می‌شود."""
        with self._timer_lock:
            conn = self._get_connection()
            c = conn.cursor()
            c.execute(
                """SELECT is_loop, loop_count FROM autos WHERE id = ?""",
                (int(timer_id),),
            )
            row = c.fetchone()
            if not row:
                return False, False
            is_loop, loop_count = int(row[0]), int(row[1] or 0)
            if not is_loop and int(loop_count_use) >= loop_count:
                c.execute("DELETE FROM autos WHERE id = ?", (int(timer_id),))
                conn.commit()
                return True, True
            c.execute(
                """UPDATE autos
                   SET last_run = ?, next_run = ?, loop_count_use = ?,
                       last_daily_date = ?, status = 'active', last_error = ?
                   WHERE id = ?""",
                (
                    float(last_run), float(next_run), int(loop_count_use),
                    last_daily_date, last_error, int(timer_id)
                ),
            )
            changed = c.rowcount > 0
            conn.commit()
            return changed, False

    def update_timer_error(self, timer_id, next_run, error_text):
        """خطای یک اجرای تایمر را ذخیره می‌کند بدون اینکه Scheduler متوقف شود."""
        with self._timer_lock:
            conn = self._get_connection()
            c = conn.cursor()
            c.execute(
                """UPDATE autos SET next_run = ?, status = 'active', last_error = ?
                   WHERE id = ?""",
                (float(next_run), str(error_text)[:1000], int(timer_id)),
            )
            changed = c.rowcount > 0
            conn.commit()
            return changed

    def delete_timer(self, chat_id, timer_id):
        with self._timer_lock:
            conn = self._get_connection()
            c = conn.cursor()
            c.execute(
                "DELETE FROM autos WHERE id = ? AND chat_id = ?",
                (int(timer_id), int(chat_id)),
            )
            conn.commit()
            return c.rowcount > 0

    def clear_timers(self, chat_id):
        with self._timer_lock:
            conn = self._get_connection()
            c = conn.cursor()
            c.execute("DELETE FROM autos WHERE chat_id = ?", (int(chat_id),))
            count = c.rowcount
            conn.commit()
            return int(count)

    # ==================== نگهداری دوره‌ای بدون حذف اطلاعات ====================

    def clear_runtime_caches(self):
        """فقط کش‌های RAM را خالی می‌کند؛ هیچ جدول/رکوردی حذف نمی‌شود."""
        cache_attrs = (
            "_speaker_data_cache", "_speaker_mode_cache", "_bio_mode_cache",
            "_group_admins_cache", "_group_special_admins_cache",
            "_group_special_cache", "_group_exempt_cache", "_group_owner_cache",
            "_lock_cache", "_lock_action_cache", "_bot_name_cache",
            "_auto_mod_cache", "_banned_cache", "_muted_cache",
            "_warn_count_cache", "_user_warning_cache",
        )
        for attr in cache_attrs:
            cache = getattr(self, attr, None)
            if hasattr(cache, "clear"):
                cache.clear()
        self._bot_enabled_cache = None
        self._admin_ids_cache = None
        self._ignored_users_cache = None
        self._global_muted_cache = None
        self._known_groups_cache.clear()

    def perform_maintenance(self, vacuum_if_large=True):
        """WAL/SQLite را مرتب می‌کند؛ هیچ داده‌ی کاربردی حذف نمی‌شود."""
        try:
            # اجازه بده writer هر چیزی که الان در صف است را تمام کند.
            self._write_queue.join()

            # کانکشن writer فقط توسط همین lock استفاده می‌شود تا VACUUM و
            # commitهای پس‌زمینه هم‌زمان روی یک sqlite connection اجرا نشوند.
            with self._writer_db_lock:
                try:
                    self._writer_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                except Exception as e:
                    _write_logger.warning("WAL checkpoint ناموفق بود: %s", e)

                try:
                    self._writer_conn.execute("PRAGMA optimize")
                except Exception:
                    pass

                if vacuum_if_large:
                    try:
                        db_size = os.path.getsize(self.db_path)
                    except OSError:
                        db_size = 0
                    if db_size >= 256 * 1024 * 1024:
                        _write_logger.info(
                            "نگهداری DB: فایل %.1f MB است؛ VACUUM شروع شد.",
                            db_size / (1024 * 1024)
                        )
                        self._writer_conn.execute("VACUUM")
                        self._writer_conn.commit()
        except Exception as e:
            _write_logger.error("خطا در نگهداری دوره‌ای دیتابیس: %s", e)


db = Database()