import asyncio
import bisect
import contextlib
import itertools
import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

from aiohttp import web, ClientSession, ClientTimeout, TCPConnector

BASE = Path(__file__).resolve().parent
TEMPLATE = BASE / "bot_template"
DATA = BASE / "manager_data"
INSTANCES = DATA / "instances"
CONFIG_PATH = BASE / "config.py"
DATA.mkdir(exist_ok=True)
INSTANCES.mkdir(exist_ok=True)

HOST = "127.0.0.1"
PORT = 8787
LOCK = threading.RLock()
RUNTIME_PROCS = {}
PENDING = {}
CENTRAL_SESSION = None
# فیوچرهایی که منتظر نتیجه‌ی یک دستورِ «تستِ جوینِ لینک» (broadcast_id -> Future)
# هستن؛ api_ack همین که ack مربوط به یک join_test برسه، فیوچر رو resolve
# می‌کنه تا run_join_test دیگه لازم نباشه polling کنه.
JOIN_RESULT_WAITERS = {}

# هر چند ثانیه یک‌بار هر سلف باید گزارش وضعیت بده (باید با مقداری که
# write_instance_config توی PANEL_REPORT_INTERVAL_SECONDS ست می‌کنه یکی باشه).
LOCAL_REPORT_INTERVAL_SECONDS = 15
# اگه با وجود روشن بودن پروسه، بیشتر از این مدت هیچ گزارشی نرسه، یعنی پروسه
# زنده‌ست ولی احتمالاً هنگ کرده (زامبی) - به‌جای سبز نشون دادنش، جداگانه
# مشخص می‌کنیم تا نیازی به چک دستی نباشه.
STALE_AFTER_SECONDS = LOCAL_REPORT_INTERVAL_SECONDS * 4
# اگه از لحظه‌ی استارت این‌قدر گذشته باشه و هنوز حتی یک گزارش هم نرسیده باشه
# (نه فقط قدیمی شده باشه، بلکه اصلاً نیومده باشه)، به‌جای موندن روی 🟡 «در
# حال اتصال» تا ابد، صریح می‌گیم بدون‌پاسخه - این دقیقاً همون سناریوییه که
# ربات خاموش/هنگ شده ولی پنل هنوز هیچی جدید نگرفته که نشونش بده.
STARTUP_GRACE_SECONDS = 90
# قفل جداگانه برای هر سلف - جلوی این رو می‌گیره که دو تا عملیات استارت/استاپ/
# ری‌استارت روی یک سلف هم‌زمان اجرا بشن (مثلاً دو بار پشت‌سرهم زدنِ دکمه)،
# چون همون هم‌زمانی می‌تونه دقیقاً باعث بشه دو تا پروسه برای یک سلف بالا بیاد.
INSTANCE_LOCKS = {}

# ==================== تنظیمات سرعتِ پنلِ کاربری ====================
# هر سلف چند ثانیه بعد از پایانِ هر دستور دوباره از manager دستور می‌گیره (با
# long-poll پایین، وقتی دستوری نیست فقط هر ~۶ ثانیه یک درخواستِ خالی می‌زنه).
# این عدد توی config.py هر سلف نوشته می‌شه و بعد از یک ری‌استارتِ سلف اعمال می‌شه.
LOCAL_BROADCAST_POLL_SECONDS = 0.03
# manager درخواستِ poll سلف رو تا این‌قدر نگه می‌داره تا دستورِ تازه برسه
# (باید کمتر از timeout=10 ثانیه‌ی main.py باشه).
LONGPOLL_SECONDS = 6
# اطلاعاتِ زنده‌ی یک گروه (قفل‌ها/تنظیمات/آمار) این‌قدر ثانیه کش می‌شه؛ ورق‌زدنِ
# بین منوها دیگه هر بار از سلف نمی‌پرسه. بعد از هر تغییر، کش با نتیجه‌ی تازه پر می‌شه.
GROUP_INFO_TTL = 20
GROUP_INFO_CACHE_MAX = 20000
# رتبه‌ی گروه‌ها از یک لیستِ مرتب‌شده‌ی کش‌شده حساب می‌شه، نه با خوندنِ دیتابیسِ همه‌ی سلف‌ها.
RANK_REFRESH_SECONDS = 120
# سقفِ صفِ دستورهای منتظرِ یک سلف؛ بیشتر از این فوراً «شلوغه» می‌گیره
# (به‌جای اینکه کاربر ۲۵ ثانیه معطل بمونه و آخرش timeout بخوره).
MAX_QUEUED_CMDS_PER_SELF = 80
PANEL_CMD_TIMEOUT = 12
JOIN_TIMEOUT = 45   # جوین + چکِ ادمین/مالکِ درخواست‌دهنده
LEAVE_TIMEOUT = 20
# چکِ سبکِ «آیا از قبل عضوه» (بدون جوینِ واقعی) - سریع‌تر از تستِ جوینِ کامله.
MEMBERSHIP_CHECK_TIMEOUT = 8   # سلفِ بی‌پاسخ «عضو نیست» حساب می‌شه؛ تکراری‌بودن رو بعد از جوین رجیستری هم می‌گیره
# حداکثر تعداد سلفی که برای جوینِ یک لینک امتحان می‌شه.
MAX_JOIN_TRIES = 8
# حداکثر آپدیتِ هم‌زمانی که پردازش می‌شه.
MAX_CONCURRENT_UPDATES = 200
# دستورهای تکیِ پنل که سلف هرکدوم رو توی task جدا (هم‌زمان) اجرا می‌کنه؛ بعد از
# تحویل به سلف، این دستورها توی poll بعدی دوباره برنمی‌گردن (وگرنه سلف هر بار
# همون دستورِ در حالِ اجرا رو دوباره می‌گرفت و بقیه‌ی دستورها پشتش گیر می‌کردن).
CONCURRENT_CMD_TYPES = ("join_test", "check_membership", "leave_group", "panel_group_cmd")
# اگه دستورِ تحویل‌داده‌شده تا این‌قدر ثانیه ack نشد (مثلاً جوابِ poll گم شد)
# دوباره تحویل داده می‌شه؛ سلف خودش دستورِ تکراری رو تشخیص می‌ده و دوباره اجرا نمی‌کنه.
REDELIVER_AFTER_SECONDS = 10

CMD_EVENTS = {}       # iid -> asyncio.Event (بیدار کردنِ long-poll سلف)
PENDING_CMDS = {}     # iid -> تعدادِ دستورهای منتظر
JOIN_COOLDOWN = {}    # iid -> تا کِی برای جوین‌ِ جدید استفاده نشه
JOIN_INFLIGHT = {}    # iid -> تعدادِ جوینِ در حالِ انجام
_BG_TASKS = set()


def spawn(coro):
    """یک تسک پس‌زمینه می‌سازه و مرجعش رو نگه می‌داره (تا garbage collect نشه)."""
    task = asyncio.ensure_future(coro)
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)
    return task


def _wake_instance(iid):
    ev = CMD_EVENTS.get(iid)
    if ev is not None:
        ev.set()


def _lock_for(iid):
    lock = INSTANCE_LOCKS.get(iid)
    if lock is None:
        lock = INSTANCE_LOCKS[iid] = asyncio.Lock()
    return lock


def _proc_start_ticks(pid):
    """مهرِ زمانِ شروعِ واقعیِ یک پروسه، از /proc (فقط لینوکس). فقط برای این
    لازمه که اگه یک PID قدیمی بعداً توسط یک پروسه‌ی کاملاً نامرتبط دوباره
    استفاده بشه (PID reuse - بعد از مدت‌ها آپتایم روی یک VPS بعیدم نیست)،
    اشتباهاً به‌جای پروسه‌ی سلفِ خودمون شناخته نشه. اگه در دسترس نباشه (پلتفرم
    دیگه‌ای غیر از لینوکس)، فقط از چکِ زنده‌بودنِ ساده (pid_alive) استفاده
    می‌شه - نه خطرناک‌تر از رفتار قبلیِ کد که اصلاً این چک رو نداشت."""
    try:
        with open(f"/proc/{pid}/stat", "r") as f:
            raw = f.read()
        after_comm = raw.rsplit(")", 1)[-1].split()
        return after_comm[19]  # starttime؛ فیلد ۲۲ کلی، منهای pid/comm/state
    except Exception:
        return None


def pid_alive(pid, expected_start_ticks=None):
    """آیا پروسه‌ای با این PID *واقعاً* زنده‌ست - نه صرفاً از روی حافظه‌ی این
    اجرا از manager.py. این چک لازمه چون بعد از هر بار بالا اومدنِ خودِ
    manager.py (کرش/دیپلوی/ری‌استارتِ سرویس)، RUNTIME_PROCS از صفر ساخته
    می‌شه؛ در حالی که پروسه‌ی سلف‌هایی که قبلاً روشن بودن (اگه واقعاً هنوز زنده
    باشن) به‌صورت orphan به کارشون ادامه می‌دن. اگه این‌ها دیده نشن، پنل
    اشتباهاً «خاموش» نشونشون می‌ده و اولین استارت/ری‌استارتی که کاربر بزنه یک
    پروسه‌ی دومِ موازیِ کنارِ پروسه‌ی زنده‌ی قبلی بالا میاره - همون باگِ «ربات به
    هر پیام دوبار جواب می‌ده»."""
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False
    if expected_start_ticks is not None:
        current = _proc_start_ticks(pid)
        if current is not None and current != expected_start_ticks:
            return False  # همون شماره‌ی PID، ولی یک پروسه‌ی کاملاً دیگه
    return True


class ManagedProcess:
    """هندلِ یکسان برای پروسه‌ی یک سلف - چه یک subprocess.Popen واقعی که همین
    اجرا ساختیمش، چه یک PID «اداپت‌شده» بعد از بالا اومدنِ دوباره‌ی خودِ
    manager.py (پروسه‌ای که هنوز زنده‌ست ولی دیگه فرزندِ مستقیمِ این اجرا
    نیست، پس os.waitpid رویش کار نمی‌کنه). بقیه‌ی کد فقط با poll()/pid/
    terminate()/kill() کار می‌کنه و لازم نیست بدونه پشتِ صحنه کدومه."""

    __slots__ = ("pid", "popen", "start_ticks")

    def __init__(self, pid, popen=None, start_ticks=None):
        self.pid = pid
        self.popen = popen
        self.start_ticks = start_ticks if start_ticks is not None else _proc_start_ticks(pid)

    def poll(self):
        if self.popen is not None:
            return self.popen.poll()
        return None if pid_alive(self.pid, self.start_ticks) else 0

    def terminate(self):
        if self.popen is not None and os.name == "nt":
            self.popen.terminate()
            return
        try:
            os.kill(self.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

    def kill(self):
        if self.popen is not None and os.name == "nt":
            self.popen.kill()
            return
        try:
            os.kill(self.pid, signal.SIGKILL if os.name != "nt" else signal.SIGTERM)
        except ProcessLookupError:
            pass

    @property
    def stdin(self):
        return self.popen.stdin if self.popen is not None else None


DEFAULT_PUBLIC_JOIN_MESSAGE = (
    "🔒 برای استفاده از پنل کاربری، اول باید توی کانال‌های زیر عضو بشی.\n\n"
    "بعد از عضویت روی دکمه‌ی «✅ عضو شدم» بزن."
)

DEFAULT_PRO_PURCHASE_MESSAGE = (
    "💎 نسخه‌ی پرو: بدون صف انتظار، اولویت با سلف‌های اختصاصی و پشتیبانی مستقیم.\n\n"
    "برای خرید یا سوال، از دکمه‌ی زیر پیام بده."
)


def load_config():
    if not CONFIG_PATH.exists():
        raise SystemExit("config.py پیدا نشد. فایل config.py را کنار manager.py قرار دهید.")
    import importlib.util
    spec = importlib.util.spec_from_file_location("central_config", CONFIG_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return {
        "central_bot_token": getattr(mod, "CENTRAL_BOT_TOKEN", ""),
        "owner_ids": getattr(mod, "OWNER_IDS", []),
        "broadcast_delay": getattr(mod, "BROADCAST_DELAY", 1.5),
        "public_join_channels": getattr(mod, "PUBLIC_JOIN_CHANNELS", ["@Soft_Studio", "@soft_Helper"]),
        "public_join_message": getattr(mod, "PUBLIC_JOIN_MESSAGE", DEFAULT_PUBLIC_JOIN_MESSAGE),
        "max_groups_per_self": getattr(mod, "MAX_GROUPS_PER_SELF", 500),
        "pro_contact": getattr(mod, "PRO_PURCHASE_CONTACT", "@soft_Helper"),
        "pro_message": getattr(mod, "PRO_PURCHASE_MESSAGE", DEFAULT_PRO_PURCHASE_MESSAGE),
        "support_contact": getattr(mod, "SUPPORT_CONTACT", "@soft_Helper"),
    }


CFG = load_config()
CENTRAL_TOKEN = str(CFG["central_bot_token"]).strip()
OWNERS = {int(x) for x in CFG["owner_ids"]}
BCAST_DELAY = float(CFG["broadcast_delay"])
# ==================== بخش عمومی: پنل کاربر معمولی ====================
PUBLIC_JOIN_CHANNELS = list(CFG["public_join_channels"])
PUBLIC_JOIN_MESSAGE = str(CFG["public_join_message"])
MAX_GROUPS_PER_SELF = int(CFG["max_groups_per_self"])
PRO_CONTACT = str(CFG["pro_contact"]).strip()
PRO_MESSAGE = str(CFG["pro_message"])
SUPPORT_CONTACT = str(CFG["support_contact"]).strip()


def _contact_url(handle):
    handle = (handle or "").strip()
    if handle.startswith("http://") or handle.startswith("https://"):
        return handle
    return f"https://splus.ir/{handle.lstrip('@')}"
if not CENTRAL_TOKEN or CENTRAL_TOKEN == "CENTRAL_BOT_TOKEN_HERE":
    raise SystemExit("CENTRAL_BOT_TOKEN را در config.py وارد کنید.")
if not OWNERS:
    raise SystemExit("حداقل یک owner id لازم است.")

STATE_FILE = DATA / "state.json"
STATE = {"instances": {}, "broadcasts": {}, "next_broadcast_id": 0}
if STATE_FILE.exists():
    try:
        STATE.update(json.loads(STATE_FILE.read_text(encoding="utf-8")))
    except Exception:
        pass
STATE.setdefault("instances", {})
# broadcasts: دیکشنریِ چندتایی (نه یک اسلاتِ تکی) از دستورهای در حالِ اجرا -
# هر کدوم مستقل، با targets خودش. این چیزیه که موازی‌سازیِ واقعیِ دستورهای
# پنل رو ممکن می‌کنه (نگاه کنید به توضیحاتِ run_remote_command). یک رکوردِ
# قدیمیِ تکی به اسمِ "broadcast" ممکنه از یک state.json قدیمی لود بشه؛ دیگه
# جایی خونده نمی‌شه و بی‌ضرره.
STATE.setdefault("broadcasts", {})
# شناسه‌ی دستورها بر پایه‌ی زمان (میلی‌ثانیه) شروع می‌شه تا بعد از ری‌استارتِ
# manager.py هیچ‌وقت با آخرین دستورِ اجراشده‌ی ثبت‌شده روی دیسکِ سلف‌ها (که
# main.py برای جلوگیری از اجرای دوباره نگه می‌داره) برخورد نکنه.
_BROADCAST_ID_COUNTER = itertools.count(
    max(int(STATE.get("next_broadcast_id", 0)) + 1, int(time.time() * 1000))
)
# یک کلید مشترکِ یکتا برای رجیستریِ سراسریِ گروه‌ها - فقط یک‌بار ساخته می‌شه و
# توی state.json ذخیره می‌مونه، تا با هر ری‌استارتِ manager.py عوض نشه (وگرنه
# سلف‌هایی که از قبل روشن مونده‌ن با کلید قدیمی 401 می‌گیرن).
STATE.setdefault("registry_secret", uuid.uuid4().hex + uuid.uuid4().hex)
# آیدی عددیِ کاربرهای معمولی (غیر از OWNER) که از گیتِ عضویتِ اجباریِ بخشِ
# عمومی رد شدن - تا لازم نباشه هر بار دوباره تاییدِ عضویت رو ازشون بخوایم.
STATE.setdefault("public_verified", [])
# ثبتِ گروه‌های بخشِ عمومی: هر chat_id (به‌صورتِ رشته) به یک رکوردِ
# {uid, iid, group_name, joined_at} وصله. این همون چیزیه که مشخص می‌کنه هر
# گروه برای کدوم کاربر ثبت شده - قبلاً همچین نگاشتی اصلاً نگه‌داری نمی‌شد و
# پنل نمی‌فهمید کدوم گروه مالِ کیه. هر uid فقط می‌تونه یک رکورد (یک گروه)
# داشته باشه.
STATE.setdefault("public_registrations", {})
# حضورِ گروه‌ها دیگه توی state.json (STATE) نگه داشته نمی‌شه - با هزاران گروه،
# یک دیکشنری توی یک فایل JSON که کاملاً از نو روی دیسک نوشته می‌شه (هر
# save_state) هم کند می‌شه هم فایل غول‌آسا. به‌جاش یک SQLite جدا
# (group_registry.db) دقیقاً مثل نسخه‌ی مستقل group_registry_server.py.
if "group_presence" in STATE:
    _LEGACY_GROUP_PRESENCE = STATE.pop("group_presence")
else:
    _LEGACY_GROUP_PRESENCE = None

for inst in STATE["instances"].values():
    inst.setdefault("panel_secret", uuid.uuid4().hex + uuid.uuid4().hex)
    inst.setdefault("groups", {})
    inst.setdefault("status", "stopped")
    inst.setdefault("pid", None)
    inst.setdefault("pid_started_at", None)
    inst.setdefault("started_at", None)
    inst.setdefault("username", None)


# ---- ذخیره‌سازیِ state.json ----
# قبلاً هر save_state() (و هر سلف هر ۱۵ ثانیه یک‌بار از api_report صداش می‌زد)
# کلِ STATE رو - شاملِ لیستِ همه‌ی گروه‌های همه‌ی سلف‌ها - با indent=2 و
# سنکرون روی event loop می‌نوشت. با هزاران گروه هر بار صدها میلی‌ثانیه تا چند
# ثانیه کلِ manager (و پنلِ کاربرا) فریز می‌شد. الان:
#   • لیستِ گروه‌های هر سلف (`groups`) اصلاً روی دیسک نوشته نمی‌شه (هر ۱۵ ثانیه از
#     خودِ سلف دوباره می‌رسه)،
#   • نوشتن debounce شده (حداکثر هر ۲ ثانیه یک‌بار) و توی thread انجام می‌شه،
#   • فقط برای کارهای نادر و مهم (ساخت/حذفِ سلف، ثبتِ گروهِ کاربر) save_state(now=True).
_STATE_DIRTY = False
_STATE_SEQ = 0
_STATE_WRITTEN_SEQ = 0
_STATE_WRITE_LOCK = threading.Lock()


def _state_snapshot():
    """(seq, json_text) - همیشه روی thread اصلی ساخته می‌شه (نه توی thread جدا) تا
    وسطِ ساختنش دیکشنری‌ها دستکاری نشن."""
    global _STATE_SEQ
    with LOCK:
        snap = dict(STATE)
        snap["instances"] = {
            iid: {k: v for k, v in inst.items() if k != "groups"}
            for iid, inst in STATE["instances"].items()
        }
        # فقط پیام‌های همگانیِ owner ماندگار می‌شن؛ دستورهای کوتاه‌عمرِ پنل نه.
        snap["broadcasts"] = {
            k: v for k, v in STATE.get("broadcasts", {}).items()
            if v.get("command_type") == "broadcast"
        }
        _STATE_SEQ += 1
        return _STATE_SEQ, json.dumps(snap, ensure_ascii=False, separators=(",", ":"))


def _write_state_text(seq, text):
    global _STATE_WRITTEN_SEQ
    with _STATE_WRITE_LOCK:
        if seq < _STATE_WRITTEN_SEQ:
            return  # یک snapshotِ جدیدتر قبلاً نوشته شده
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, STATE_FILE)
        _STATE_WRITTEN_SEQ = seq


def save_state(now=False):
    global _STATE_DIRTY
    if now:
        _STATE_DIRTY = False
        _write_state_text(*_state_snapshot())
    else:
        _STATE_DIRTY = True


async def state_flush_loop():
    global _STATE_DIRTY
    while True:
        await asyncio.sleep(2)
        if not _STATE_DIRTY:
            continue
        _STATE_DIRTY = False
        try:
            seq, text = _state_snapshot()
            await asyncio.to_thread(_write_state_text, seq, text)
        except Exception as e:
            _STATE_DIRTY = True
            print("state save error:", e)


save_state(now=True)


def instance_dir(iid):
    return INSTANCES / iid


def safe_name(value):
    value = re.sub(r"[^\w.\-]+", "_", str(value), flags=re.UNICODE).strip("._")
    return value[:40] or uuid.uuid4().hex[:8]


def write_instance_config(iid):
    d = instance_dir(iid)
    inst = STATE["instances"][iid]
    cfg = (TEMPLATE / "config.py").read_text(encoding="utf-8")

    # Replace whole assignment blocks (including multiline lists), not just the
    # first line. This prevents the old SUPER_OWNER_IDS list from being left
    # behind and causing IndentationError in the copied instance config.
    def replace_assignment(text, name, value):
        # Replace the whole assignment safely. A callable replacement prevents
        # re.sub from interpreting backslashes in names/values as regex escapes.
        pattern = rf"(?ms)^[ \t]*{re.escape(name)}[ \t]*=.*?(?=^[ \t]*[A-Za-z_][A-Za-z0-9_]*[ \t]*=|\Z)"
        replacement = f"{name} = {value}\n"
        text, count = re.subn(pattern, lambda _m: replacement, text)
        if count == 0:
            if text and not text.endswith("\n"):
                text += "\n"
            text += replacement
        return text

    values = {
        "SESSION_FILE": '"session.txt"',
        "DB_PATH": '"group_bot.db"',
        "SUPER_OWNER_IDS": repr(sorted(OWNERS)),
        "PANEL_API_URL": f'"http://{HOST}:{PORT}/api"',
        "PANEL_REPORT_INTERVAL_SECONDS": str(LOCAL_REPORT_INTERVAL_SECONDS),
        "PANEL_BROADCAST_POLL_SECONDS": repr(LOCAL_BROADCAST_POLL_SECONDS),
        "PANEL_BROADCAST_DELAY_SECONDS": repr(BCAST_DELAY),
        # رجیستری سراسری گروه‌ها همین‌جا (توی خودِ manager.py، همون API محلی
        # روی 127.0.0.1:8787) پیاده شده - دیگه نیازی به اجرای جداگانه‌ی
        # group_registry_server.py نیست. کلید مشترک هم یک‌بار توی state.json
        # ساخته می‌شه و بین همه‌ی سلف‌ها یکسانه.
        "GROUP_REGISTRY_API_URL": f'"http://{HOST}:{PORT}/api/registry"',
        "GROUP_REGISTRY_SECRET": repr(STATE["registry_secret"]),
        "BOT_DISPLAY_LABEL": repr(inst.get("name", iid)),
    }
    inst.setdefault("panel_secret", uuid.uuid4().hex + uuid.uuid4().hex)
    values["PANEL_SECRET"] = repr(inst["panel_secret"])

    for name, value in values.items():
        cfg = replace_assignment(cfg, name, value)

    (d / "config.py").write_text(cfg, encoding="utf-8")


def create_instance(name):
    iid = uuid.uuid4().hex[:10]
    d = instance_dir(iid)
    d.mkdir(parents=True, exist_ok=False)
    for item in TEMPLATE.iterdir():
        if item.name == "__pycache__":
            continue
        target = d / item.name
        if item.is_dir():
            shutil.copytree(item, target)
        else:
            shutil.copy2(item, target)
    STATE["instances"][iid] = {
        "id": iid,
        "name": safe_name(name) if name else f"Self {len(STATE['instances']) + 1}",
        "status": "stopped",
        "pid": None,
        "pid_started_at": None,
        "started_at": None,
        "panel_bot_id": None,
        "account_id": None,
        "groups": {},
        "group_count": 0,
        "created_at": time.time(),
        "panel_secret": uuid.uuid4().hex + uuid.uuid4().hex,
        "pending_phone": None,
        "last_seen": None,
        "days_active": None,
    }
    write_instance_config(iid)
    save_state(now=True)
    return iid


async def delete_instance(iid):
    """سلف رو کامل حذف می‌کنه: پروسه رو متوقف، پوشه‌ی سشن/دیتابیسش رو پاک، و از
    لیست وضعیت مرکزی هم حذف می‌کنه. برگشت‌پذیر نیست."""
    await stop_instance(iid)
    d = instance_dir(iid)
    try:
        shutil.rmtree(d, ignore_errors=True)
    except Exception:
        pass
    STATE["instances"].pop(iid, None)
    INSTANCE_LOCKS.pop(iid, None)
    save_state(now=True)


def process_for(iid):
    p = RUNTIME_PROCS.get(iid)
    if p and p.poll() is None:
        return p
    return None


def is_running(iid):
    return process_for(iid) is not None


async def _wait_dead(mp, timeout):
    """بدونِ بلاک کردنِ event loop (بر خلافِ Popen.wait قبلی) صبر می‌کنه تا
    پروسه واقعاً بمیره یا زمان تموم بشه. روی هر دو حالت کار می‌کنه: پروسه‌ی
    فرزندِ واقعی و PID اداپت‌شده‌ای که فرزندِ مستقیمِ این اجرا نیست."""
    deadline = time.time() + timeout
    while mp.poll() is None:
        if time.time() >= deadline:
            return False
        await asyncio.sleep(0.2)
    return True


async def _do_stop(iid):
    """پیاده‌سازیِ واقعیِ استاپ - فرض می‌کنه قفلِ سلف از قبل گرفته شده. تا وقتی
    که مطمئن نشیم پروسه واقعاً مرده، وضعیت رو 'stopped' اعلام نمی‌کنیم؛ همین
    یک قانون، کلِ باگِ 'پروسه‌ی قبلی کیل نشده بود' رو حل می‌کنه."""
    inst = STATE["instances"].get(iid)
    mp = RUNTIME_PROCS.get(iid)
    if mp is not None and mp.poll() is None:
        mp.terminate()  # SIGTERM - فرصت برای خروج تمیز (ذخیره‌ی سشن و ...)
        died = await _wait_dead(mp, timeout=10)
        if not died:
            mp.kill()  # SIGKILL - دیگه قابلِ نادیده گرفتن نیست
            died = await _wait_dead(mp, timeout=5)
        if not died:
            # عملاً نباید پیش بیاد، ولی اگه پیش اومد (مثلاً پروسه گیرِ I/O
            # غیرقابل‌وقفه‌ست) به‌جای دروغِ 'متوقف شد'، صادقانه شکست رو
            # برمی‌گردونیم تا caller پروسه‌ی جدیدی کنارش بالا نیاره.
            return False
    RUNTIME_PROCS.pop(iid, None)
    if inst is not None:
        inst["pid"] = None
        inst["pid_started_at"] = None
        inst["status"] = "stopped"
        inst["pending_phone"] = None
        save_state()
    return True


async def stop_instance(iid):
    async with _lock_for(iid):
        return await _do_stop(iid)


async def _do_start(iid):
    """پیاده‌سازیِ واقعیِ استارت - فرض می‌کنه قفلِ سلف از قبل گرفته شده."""
    inst = STATE["instances"][iid]
    d = instance_dir(iid)
    write_instance_config(iid)

    mp = RUNTIME_PROCS.get(iid)
    if mp is not None and mp.poll() is None:
        # از قبل روشنه (چه پروسه‌ی این اجرا، چه یک PID اداپت‌شده‌ی زنده) -
        # دوباره استارت نمی‌زنیم که کنارش یک پروسه‌ی دوم بالا نیاد.
        return mp

    log = open(d / "manager.log", "a", encoding="utf-8", buffering=1)
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    p = subprocess.Popen(
        [sys.executable, "-u", "manager_boot.py"],
        cwd=str(d),
        stdin=subprocess.PIPE,
        stdout=log,
        stderr=subprocess.STDOUT,
        creationflags=flags,
        bufsize=0,
    )
    mp = ManagedProcess(p.pid, popen=p)
    RUNTIME_PROCS[iid] = mp
    inst["pid"] = p.pid
    inst["pid_started_at"] = mp.start_ticks
    inst["status"] = "starting"
    inst["started_at"] = time.time()
    inst["last_seen"] = None
    save_state()
    return mp


async def start_instance(iid):
    async with _lock_for(iid):
        return await _do_start(iid)


async def restart_instance(iid):
    """نقطه‌ی واحدِ ری‌استارت - همه‌جای پنل (دکمه‌ی شیشه‌ای، دستور متنی
    /restartbot، ری‌استارتِ خودکار بعدِ آپدیتِ کد) دقیقاً از همینجا رد می‌شن.
    زیرِ یک قفل: اول واقعاً صبر می‌کنیم پروسه‌ی قبلی کاملاً بمیره (نه فقط
    سیگنال بفرستیم و امیدوار باشیم بعد از یک stop_instance جداگانه)، بعد
    پروسه‌ی جدید رو بالا می‌آریم. اگه استاپ واقعاً موفق نشه، عمداً پروسه‌ی
    جدیدی بالا نمیاریم - دقیقاً همینجا بود که قبلاً دو تا پروسه‌ی هم‌زمان برای
    یک سلف بالا می‌موند و ربات به هر پیام دوبار جواب می‌داد.
    خروجی: (stopped_cleanly: bool, mp: ManagedProcess)"""
    async with _lock_for(iid):
        stopped_cleanly = await _do_stop(iid)
        mp = await _do_start(iid) if stopped_cleanly else process_for(iid)
        return stopped_cleanly, mp


def reconcile_on_boot():
    """فقط یک‌بار، درست بعد از بالا اومدنِ خودِ manager.py صدا زده می‌شه.
    RUNTIME_PROCS فقط تو حافظه‌ست، پس بعد از هر کرش/دیپلوی/ری‌استارتِ سرویسِ
    manager.py از صفر خالی می‌شه - در حالی که پروسه‌ی سلف‌هایی که قبلاً روشن
    بودن (اگه واقعاً هنوز زنده باشن) به‌صورت orphan به کارشون ادامه می‌دن.
    بدونِ این تابع، پنل بلافاصله بعد از بالا اومدن اشتباهاً همه‌شون رو
    'خاموش' نشون می‌ده، و اولین استارت/ری‌استارتی که کاربر بزنه یک پروسه‌ی
    دومِ موازیِ کنارِ پروسه‌ی زنده‌ی قبلی بالا میاره."""
    changed = False
    for iid, inst in STATE["instances"].items():
        pid = inst.get("pid")
        if pid and pid_alive(pid, inst.get("pid_started_at")):
            RUNTIME_PROCS[iid] = ManagedProcess(pid, popen=None, start_ticks=inst.get("pid_started_at"))
            inst.setdefault("started_at", time.time())
            if inst.get("status") not in ("online", "starting"):
                inst["status"] = "starting"
                changed = True
        elif inst.get("pid") is not None or inst.get("status") != "stopped":
            inst["pid"] = None
            inst["pid_started_at"] = None
            inst["status"] = "stopped"
            changed = True
    if changed:
        save_state()


def send_child_input(iid, value):
    p = process_for(iid)
    if not p or not p.stdin:
        return False
    try:
        p.stdin.write((str(value) + "\n").encode("utf-8"))
        p.stdin.flush()
        return True
    except Exception:
        return False


def log_text(iid):
    path = instance_dir(iid) / "manager.log"
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""


def log_tail(iid, n=3500):
    return log_text(iid)[-n:]


def phone_prompt_seen(text):
    return any(x in text for x in (
        "📱 شماره تلفن اکانت ربات",
        "شماره تلفن اکانت ربات",
        "Please enter your phone",
    ))


def otp_prompt_seen(text):
    return any(x in text for x in (
        "🔑 کد تایید (OTP)",
        "کد تایید (OTP)",
        "Please enter the code you received",
    ))


def login_success_seen(text):
    return "✅ لاگین موفق!" in text or "Signed in successfully as " in text


def two_factor_seen(text):
    return "Two-step verification is enabled" in text or "SessionPasswordNeededError" in text


def code_invalid_seen(text):
    return "Invalid code. Please try again." in text


async def wait_until_prompt(iid, kind, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        p = process_for(iid)
        text = log_text(iid)
        if not p:
            return False, "process_stopped"
        if kind == "phone" and phone_prompt_seen(text):
            return True, "phone_prompt"
        if kind == "otp" and otp_prompt_seen(text):
            return True, "otp_prompt"
        await asyncio.sleep(0.5)
    return False, "timeout"


# ============================================================
# کمک‌کننده‌های نمایش (وضعیت آنلاین/آفلاین، تعداد گروه، زمان نسبی)
# ============================================================

def group_count_of(inst):
    """منبع واحد برای تعداد گروه‌های یک سلف - همه‌جا (آمار، لیست، ارسال همگانی)
    دقیقاً از همین یک تابع استفاده می‌کنن تا هیچ‌وقت دو عدد متفاوت نشون داده نشه."""
    gc = inst.get("group_count")
    if isinstance(gc, int) and gc >= 0:
        return gc
    return len(inst.get("groups") or {})


def humanize_ago(ts):
    if not ts:
        return "هرگز"
    d = max(0, time.time() - ts)
    if d < 60:
        return f"{int(d)} ثانیه پیش"
    if d < 3600:
        return f"{int(d // 60)} دقیقه پیش"
    if d < 86400:
        return f"{int(d // 3600)} ساعت پیش"
    return f"{int(d // 86400)} روز پیش"


def display_status(iid, inst):
    """وضعیت آنلاین/آفلاین کاملاً محلی تشخیص داده می‌شه: خودِ manager.py پدر
    پروسه‌ی هر سلف است، پس نیازی نیست برای فهمیدن روشن/خاموش بودن به هیچ پیام
    بیرونی (یا سروری با IP عمومی) متکی باشیم - همینجا (process_for) مستقیماً
    می‌دونیم. گزارش‌های دوره‌ای سلف (روی لوپ‌بک محلی 127.0.0.1) فقط برای
    اطلاعات تکمیلی (تعداد گروه، آیدی اکانت) و تشخیص «وصل واقعی به سروش» به کار
    می‌رن؛ اگه با وجود روشن بودن پروسه گزارشی نرسه، به‌جای سبزِ گمراه‌کننده،
    حالت جدا «بدون پاسخ» نشون داده می‌شه."""
    running = is_running(iid)
    if not running:
        return "🔴", "خاموش"
    st = inst.get("status")
    last_seen = inst.get("last_seen")
    if last_seen:
        if (time.time() - last_seen) > STALE_AFTER_SECONDS:
            return "🟠", "بدون پاسخ (احتمالاً هنگ)"
        if st == "online":
            return "🟢", "روشن و متصل"
    # هنوز حتی یک گزارش هم از این اجرا نرسیده. اگه به‌اندازه‌ی کافی از استارت
    # گذشته باشه (بیشتر از زمانِ عادیِ اتصال/لاگین)، به‌جای موندنِ ابدی روی
    # 🟡، صریح می‌گیم بدون‌پاسخه - این دقیقاً همون حالتیه که ربات خاموش/هنگ
    # شده ولی چون چیزی گزارش نکرده، پنل هنوز چیز جدیدی نگرفته که نشون بده.
    started_at = inst.get("started_at")
    if started_at and (time.time() - started_at) > STARTUP_GRACE_SECONDS:
        return "🟠", "بدون پاسخ (از استارت گزارشی نرسیده)"
    return "🟡", "در حال اتصال..."


# ============================================================
# کیبورد شیشه‌ای (Inline Keyboard)
# ============================================================

def kb(*rows):
    return {"inline_keyboard": [list(r) for r in rows]}


def btn(text, data):
    return {"text": text, "callback_data": data}


CANCEL_KB = kb([btn("❌ لغو", "m:cancel")])


def home_kb():
    return kb([btn("🏠 بازگشت به منو", "m:home")])


async def login_flow(iid, cid, phone):
    await start_instance(iid)

    # main.py شماره را با input() می‌گیرد. روی بعضی سیستم‌ها متن prompt
    # دیر به فایل لاگ flush می‌شود، بنابراین منتظر prompt نمی‌مانیم.
    # stdin پروسه صف دارد و شماره وقتی main.py به input() برسد مصرف می‌شود.
    await asyncio.sleep(1.0)
    if not send_child_input(iid, phone):
        await send_central(CENTRAL_SESSION, cid, "❌ نتوانستم شماره را به پروسه سلف بدهم. پروسه را بررسی کن.", home_kb())
        PENDING.pop(cid, None)
        return

    await send_central(CENTRAL_SESSION, cid, "📨 شماره دریافت شد. دارم منتظر درخواست OTP از خود سلف می‌مانم...", CANCEL_KB)

    ok, reason = await wait_until_prompt(iid, "otp", 180)
    if not ok:
        if not process_for(iid):
            msg = "❌ پروسه سلف متوقف شد و OTP درخواست نشد.\n\n" + (log_tail(iid) or "لاگ خالی است.")
        else:
            msg = "⏱️ هنوز درخواست OTP از سلف دریافت نشد. ممکن است اتصال/API سروش پاسخ نداده باشد.\n\n" + (log_tail(iid) or "لاگ خالی است.")
        await send_central(CENTRAL_SESSION, cid, msg, home_kb())
        PENDING.pop(cid, None)
        return

    PENDING[cid] = {"iid": iid, "stage": "otp"}
    await send_central(CENTRAL_SESSION, cid, "🔑 کد یکبارمصرف ارسال شده. فقط کد عددی را همینجا بفرست:", CANCEL_KB)


async def wait_login_result(iid, cid):
    deadline = time.time() + 180
    while time.time() < deadline:
        text = log_text(iid)
        if login_success_seen(text):
            inst = STATE["instances"][iid]
            inst["status"] = "online"
            inst["pending_phone"] = None
            save_state()
            PENDING.pop(cid, None)
            await send_central(
                CENTRAL_SESSION, cid, "✅ ورود سلف با موفقیت انجام شد و سلف اجرا شد.",
                kb([btn("🤖 مشاهده این سلف", f"i:view:{iid}")], [btn("🏠 منو", "m:home")]),
            )
            return
        if two_factor_seen(text):
            PENDING[cid] = {"iid": iid, "stage": "password"}
            await send_central(CENTRAL_SESSION, cid, "🔐 این اکانت رمز دومرحله‌ای دارد. رمز را بفرست:", CANCEL_KB)
            return
        if code_invalid_seen(text):
            PENDING[cid] = {"iid": iid, "stage": "otp"}
            await send_central(CENTRAL_SESSION, cid, "❌ کد اشتباه یا منقضی بود. کد جدید را بفرست:", CANCEL_KB)
            return
        if not process_for(iid):
            await send_central(CENTRAL_SESSION, cid, "❌ پروسه سلف بعد از ورود متوقف شد.\n\n" + (log_tail(iid) or "لاگ خالی است."), home_kb())
            PENDING.pop(cid, None)
            return
        await asyncio.sleep(1)
    await send_central(CENTRAL_SESSION, cid, "⏱️ زمان انتظار ورود تمام شد. وضعیت سلف را بررسی کن.", home_kb())
    PENDING.pop(cid, None)


async def bot_call(session, method, payload=None):
    url = f"https://api.splus.ir/bot{CENTRAL_TOKEN}/{method}"
    async with session.post(url, json=payload or {}, timeout=ClientTimeout(total=35)) as r:
        data = await r.json(content_type=None)
        if not data.get("ok"):
            raise RuntimeError(str(data))
        return data.get("result")


async def send_central(session, chat_id, text, keyboard=None):
    payload = {"chat_id": chat_id, "text": str(text)}
    if keyboard is not None:
        payload["reply_markup"] = keyboard
    return await bot_call(session, "sendMessage", payload)


async def edit_central(session, chat_id, message_id, text, keyboard=None):
    payload = {"chat_id": chat_id, "message_id": message_id, "text": str(text)}
    if keyboard is not None:
        payload["reply_markup"] = keyboard
    try:
        return await bot_call(session, "editMessageText", payload)
    except RuntimeError as e:
        if "not modified" in str(e).lower():
            return None
        try:
            return await send_central(session, chat_id, text, keyboard)
        except Exception:
            return None
    except Exception:
        try:
            return await send_central(session, chat_id, text, keyboard)
        except Exception:
            return None


async def answer_cq(session, cq_id, text=None, show_alert=False):
    """جوابِ callback رو در پس‌زمینه می‌فرسته و منتظرش نمی‌مونه - یک رفت‌وبرگشتِ
    شبکه در هر کلیک صرفه‌جویی می‌شه (نتیجه‌ش برای هیچ کدوم از هندلرها مهم نیست)."""
    payload = {"callback_query_id": cq_id}
    if text:
        payload["text"] = text[:200]
    if show_alert:
        payload["show_alert"] = True

    async def _send():
        try:
            await bot_call(session, "answerCallbackQuery", payload)
        except Exception:
            pass

    spawn(_send())


def is_owner(msg):
    return int((msg.get("from") or {}).get("id", -1)) in OWNERS


def normalize_digits(text):
    return str(text).translate(str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789"))


# ============================================================
# آپدیت کد یک/همه‌ی سلف‌ها (جایگزینی فایل main.py و مشابه)
# ============================================================

# این فایل‌ها هیچ‌وقت با آپدیتِ دستی جایگزین نمی‌شن، چون هرکدوم دلیل خاص خودش
# رو داره:
#  - config.py: هر بار start_instance (از طریق write_instance_config) کاملاً
#    از روی bot_template بازسازی می‌شه و مقادیرِ خاصِ همون سلف (توکن پنل،
#    آدرس API و ...) توش تزریق می‌شه؛ جایگزینیِ دستی‌اش بی‌فایده و خطرناکه.
#  - session.txt: سشنِ لاگین‌شده‌ی همون سلفه؛ عوض کردنش یعنی لاگ‌اوت شدن.
#  - group_bot.db / .db-shm / .db-wal: دیتابیس گروه‌های همون سلف.
UPDATE_BLOCKED_FILES = {"config.py", "session.txt", "group_bot.db", "group_bot.db-shm", "group_bot.db-wal"}


def safe_update_filename(name):
    """اسم فایلِ آپلودشده رو امن می‌کنه: فقط basename (بدون مسیر/../) و
    کاراکترهای مجاز، تا امکان نوشتن بیرون از پوشه‌ی سلف وجود نداشته باشه."""
    name = os.path.basename(str(name or "").strip())
    name = re.sub(r"[^\w.\-]+", "_", name, flags=re.UNICODE).strip("._")
    return (name[:120] or "uploaded_file.py")


async def download_bot_file(session, file_id):
    """فایلِ آپلودشده توسط اونر رو با API بات مرکزی (سازگار با الگوی Bot API)
    دانلود می‌کنه: اول getFile برای گرفتن مسیر، بعد گرفتن محتوا از سرور فایل.
    اگه در آینده مشخص شد آدرس فایل‌سرور سروش‌پلاس فرق داره، فقط همین تابع
    باید عوض بشه."""
    result = await bot_call(session, "getFile", {"file_id": file_id})
    file_path = (result or {}).get("file_path")
    if not file_path:
        raise RuntimeError("مسیر فایل از getFile برنگشت.")
    if file_path.startswith("http://") or file_path.startswith("https://"):
        url = file_path
    else:
        url = f"https://api.splus.ir/file/bot{CENTRAL_TOKEN}/{file_path}"
    async with session.get(url, timeout=ClientTimeout(total=90)) as r:
        if r.status != 200:
            raise RuntimeError(f"دانلود فایل با کد {r.status} ناموفق بود.")
        return await r.read()


async def apply_code_update(target_iids, filename, content):
    """محتوای فایل رو روی پوشه‌ی هرکدوم از سلف‌های هدف می‌نویسه (جایگزین اگه
    بود، اضافه اگه نبود) و اگه اون سلف روشن بود خودکار ری‌استارتش می‌کنه تا
    کد جدید واقعاً اجرا بشه. ری‌استارت از همون restart_instance واحد رد
    می‌شه، پس اگه پروسه‌ی قبلی واقعاً کیل نشه، عمداً پروسه‌ی دومی کنارش بالا
    نمیاد - همچین موردی به‌جای 'restarted' توی 'failed' گزارش می‌شه.
    برمی‌گردونه: (updated_existing, updated_new, restarted, failed) -
    هرکدوم لیستی از (iid, name)."""
    updated_existing, updated_new, restarted, failed = [], [], [], []
    for iid in target_iids:
        inst = STATE["instances"].get(iid)
        if not inst:
            continue
        name = inst.get("name", iid)
        try:
            d = instance_dir(iid)
            target = d / filename
            existed = target.exists()
            target.write_bytes(content)
            (updated_existing if existed else updated_new).append((iid, name))
            if is_running(iid):
                stopped_cleanly, _ = await restart_instance(iid)
                if stopped_cleanly:
                    restarted.append((iid, name))
                else:
                    failed.append((iid, name))
        except Exception:
            failed.append((iid, name))
    return updated_existing, updated_new, restarted, failed


# ============================================================
# صفحه‌های پنل (متن + کیبورد شیشه‌ای)
# ============================================================

def render_home():
    items = STATE["instances"]
    total = len(items)
    online_n = offline_n = starting_n = stale_n = 0
    total_groups = 0
    for iid, inst in items.items():
        _, label = display_status(iid, inst)
        if label == "روشن و متصل":
            online_n += 1
        elif label == "خاموش":
            offline_n += 1
        elif label == "بدون پاسخ (احتمالاً هنگ)":
            stale_n += 1
        else:
            starting_n += 1
        total_groups += group_count_of(inst)

    text = (
        "🤖 پنل مرکزی مدیریت سلف‌ها\n\n"
        f"📦 تعداد سلف‌ها: {total}\n"
        f"🟢 روشن و متصل: {online_n}   🔴 خاموش: {offline_n}\n"
        f"🟡 در حال اتصال: {starting_n}   🟠 بدون پاسخ: {stale_n}\n"
        f"👥 مجموع گروه‌های همه‌ی سلف‌ها: {total_groups}\n\n"
        "یکی از گزینه‌های زیر رو انتخاب کن:"
    )
    keyboard = kb(
        [btn("🤖 مدیریت سلف‌ها", "m:list")],
        [btn("📊 آمار کامل", "m:stats"), btn("👥 لیست گروه‌ها", "m:groups")],
        [btn("➕ افزودن سلف جدید", "m:addself")],
        [btn("🛠 بروزرسانی کد", "m:update")],
        [btn("📢 ارسال پیام همگانی", "m:bcast")],
    )
    return text, keyboard


PAGE_SIZE = 8


def render_list(page=0):
    items = list(STATE["instances"].items())
    if not items:
        return (
            "🤖 هنوز هیچ سلفی اضافه نشده.\n\nبرای شروع، یک سلف جدید اضافه کن.",
            kb([btn("➕ افزودن سلف", "m:addself")], [btn("⬅️ بازگشت به منو", "m:home")]),
        )
    pages = (len(items) - 1) // PAGE_SIZE + 1
    page = max(0, min(page, pages - 1))
    chunk = items[page * PAGE_SIZE: (page + 1) * PAGE_SIZE]

    rows = []
    for iid, inst in chunk:
        emoji, _ = display_status(iid, inst)
        gc = group_count_of(inst)
        name = inst.get("name", iid)
        rows.append([btn(f"{emoji} {name} — {gc} گروه", f"i:view:{iid}")])

    nav = []
    if page > 0:
        nav.append(btn("◀️ قبلی", f"m:list:{page - 1}"))
    nav.append(btn(f"📄 {page + 1}/{pages}", "noop"))
    if page < pages - 1:
        nav.append(btn("بعدی ▶️", f"m:list:{page + 1}"))
    rows.append(nav)
    rows.append([btn("➕ افزودن سلف", "m:addself"), btn("🔄 بروزرسانی", f"m:list:{page}")])
    rows.append([btn("⬅️ بازگشت به منو", "m:home")])

    text = f"🤖 سلف‌های ثبت‌شده ({len(items)})\n\nروی هرکدوم بزن برای مدیریت کامل (استارت/استاپ/ری‌استارت/حذف):"
    return text, kb(*rows)


def render_update_menu():
    text = (
        "🛠 بروزرسانی کد\n\n"
        "فایل کد (مثلاً main.py) رو برای یک سلفِ خاص یا همه‌ی سلف‌ها جایگزین/اضافه کن.\n"
        "اگه فایل از قبل وجود داشته باشه جایگزین می‌شه، وگرنه اضافه می‌شه؛ سلفی که "
        "روشن بوده بعد از آپدیت خودکار ری‌استارت می‌شه تا کد جدید اجرا بشه.\n\n"
        "برای کدوم سلف(ها) می‌خوای آپدیت کنی؟"
    )
    rows = [
        [btn("🎯 یک سلف خاص", "uc:target:one:0")],
        [btn("🌐 همه‌ی سلف‌ها", "uc:target:all")],
        [btn("⬅️ بازگشت به منو", "m:home")],
    ]
    return text, kb(*rows)


def render_update_pick_list(page=0):
    items = list(STATE["instances"].items())
    if not items:
        return (
            "🤖 هنوز هیچ سلفی اضافه نشده.\n\nاول یک سلف اضافه کن.",
            kb([btn("➕ افزودن سلف", "m:addself")], [btn("⬅️ بازگشت", "m:update")]),
        )
    pages = (len(items) - 1) // PAGE_SIZE + 1
    page = max(0, min(page, pages - 1))
    chunk = items[page * PAGE_SIZE: (page + 1) * PAGE_SIZE]

    rows = []
    for iid, inst in chunk:
        emoji, _ = display_status(iid, inst)
        name = inst.get("name", iid)
        rows.append([btn(f"{emoji} {name}", f"uc:pick:{iid}")])

    nav = []
    if page > 0:
        nav.append(btn("◀️ قبلی", f"uc:target:one:{page - 1}"))
    nav.append(btn(f"📄 {page + 1}/{pages}", "noop"))
    if page < pages - 1:
        nav.append(btn("بعدی ▶️", f"uc:target:one:{page + 1}"))
    rows.append(nav)
    rows.append([btn("⬅️ بازگشت", "m:update")])

    text = "🎯 آپدیت کدِ کدوم سلف؟\n\nروی سلف مورد نظر بزن، بعد فایل رو بفرست."
    return text, kb(*rows)


def render_detail(iid):
    inst = STATE["instances"].get(iid)
    if not inst:
        return "❌ این سلف دیگه وجود نداره (احتمالاً حذف شده).", kb([btn("⬅️ بازگشت به لیست", "m:list:0")])

    emoji, label = display_status(iid, inst)
    gc = group_count_of(inst)
    days = inst.get("days_active")
    exempt = bool(inst.get("join_exempt"))
    text = (
        f"{emoji} {inst.get('name', iid)}\n\n"
        f"شناسه: {iid}\n"
        f"وضعیت: {label}\n"
        f"آیدی عددی اکانت: {inst.get('account_id') or '—'}\n"
        f"تعداد گروه: {gc}\n"
        f"روزهای فعالیت: {days if days is not None else '—'}\n"
        f"لیستِ جوینِ عمومی: {'🚫 معاف' if exempt else '✅ فعال'}\n"
        f"آخرین گزارش وضعیت: {humanize_ago(inst.get('last_seen'))}"
    )

    running = is_running(iid)
    if running:
        action_row = [btn("🔄 ری‌استارت", f"i:restart:{iid}"), btn("⏹ استاپ", f"i:stop:{iid}")]
    else:
        action_row = [btn("▶️ استارت", f"i:start:{iid}")]

    join_toggle_label = "✅ افزودن به لیستِ جوینِ عمومی" if exempt else "🚫 معاف از لیستِ جوینِ عمومی"

    rows = [
        action_row,
        [btn(join_toggle_label, f"i:joinexempt:{iid}")],
        [btn("🗑 حذف این سلف", f"i:delask:{iid}")],
        [btn("🔄 بروزرسانی", f"i:view:{iid}")],
        [btn("⬅️ بازگشت به لیست", "m:list:0")],
    ]
    return text, kb(*rows)


def render_delete_confirm(iid):
    inst = STATE["instances"].get(iid)
    name = inst.get("name", iid) if inst else iid
    text = (
        f"⚠️ مطمئنی می‌خوای «{name}» رو کامل حذف کنی؟\n"
        "این کار سشن و دیتابیس این سلف رو هم پاک می‌کنه و برگشت‌پذیر نیست."
    )
    rows = [[btn("✅ بله، حذفش کن", f"i:delyes:{iid}"), btn("❌ نه", f"i:view:{iid}")]]
    return text, kb(*rows)


def render_stats():
    items = STATE["instances"]
    total = len(items)
    counts = {"روشن و متصل": 0, "خاموش": 0, "در حال اتصال...": 0, "بدون پاسخ (احتمالاً هنگ)": 0}
    total_groups = 0
    lines = []
    for iid, inst in items.items():
        emoji, label = display_status(iid, inst)
        counts[label] = counts.get(label, 0) + 1
        gc = group_count_of(inst)
        total_groups += gc
        lines.append(f"{emoji} {inst.get('name', iid)} — {gc} گروه — {humanize_ago(inst.get('last_seen'))}")

    header = (
        "📊 آمار کلی مدیریت سلف‌ها\n\n"
        f"📦 تعداد کل سلف‌ها: {total}\n"
        f"🟢 روشن و متصل: {counts.get('روشن و متصل', 0)}\n"
        f"🔴 خاموش: {counts.get('خاموش', 0)}\n"
        f"🟡 در حال اتصال: {counts.get('در حال اتصال...', 0)}\n"
        f"🟠 بدون پاسخ: {counts.get('بدون پاسخ (احتمالاً هنگ)', 0)}\n"
        f"👥 مجموع گروه‌های همه‌ی سلف‌ها: {total_groups}\n\n"
    )
    body = "\n".join(lines) if lines else "هنوز سلفی اضافه نشده."
    text = (header + body)[:4000]
    rows = [[btn("🔄 بروزرسانی", "m:stats")], [btn("⬅️ بازگشت به منو", "m:home")]]
    return text, kb(*rows)


def render_groups():
    out = []
    total_groups = 0
    for iid, inst in STATE["instances"].items():
        gs = inst.get("groups") or {}
        gc = group_count_of(inst)
        total_groups += gc
        if not gs:
            continue
        out.append(f"\n🤖 {inst.get('name', iid)} ({gc} گروه)")
        for gid, g in list(gs.items())[:80]:
            out.append(f"• {g.get('group_name') or gid} — {gid}")

    header = f"👥 لیست گروه‌ها (مجموع: {total_groups})\n"
    body = "\n".join(out) if out else "\nفعلاً گروهی گزارش نشده."
    text = (header + body)[:4000]
    rows = [[btn("🔄 بروزرسانی", "m:groups")], [btn("⬅️ بازگشت به منو", "m:home")]]
    return text, kb(*rows)


async def do_cancel(cid):
    """عملیات نیمه‌کاره (افزودن سلف/ارسال همگانی) رو لغو می‌کنه. اگه یک سلف در
    وسط ورود (قبل از لاگین موفق) رها شده باشه، برای تمیزی پنل کاملاً حذفش
    می‌کنیم تا لیست با سلف‌های ناقص/بی‌مصرف شلوغ نشه."""
    pending = PENDING.pop(cid, None)
    if pending:
        iid = pending.get("iid")
        stage = pending.get("stage")
        if iid and stage in ("name", "phone", "starting_login", "otp", "password", "login_wait"):
            inst = STATE["instances"].get(iid)
            if inst and inst.get("status") != "online":
                await delete_instance(iid)
    return render_home()


# ============================================================
# بخش عمومی: پنل کاربر معمولی (نه سازنده/OWNER)
# ============================================================
# ربات‌های رسمی (Bot API) بعد از یک تعداد گروه/عضو به محدودیتِ پلتفرم می‌خورن؛
# به همین خاطر به کاربر معمولی یکی از سلف‌ها (اکانتِ واقعی، بدونِ اون محدودیت)
# معرفی می‌شه تا خودش به گروهش اضافه کنه. این بخش کاملاً جدا از پنلِ مدیریتِ
# OWNER بالاست؛ هیچ کاربرِ معمولی به لیست/استارت/استاپ/آپدیتِ سلف‌ها یا هیچ
# دستورِ دیگه‌ی مخصوصِ OWNER دسترسی نداره.

# نگاشتِ کلیدِ داخلیِ قفل‌ها (همون کلیدهایی که توی bot_template/main.py با
# LOCK_MAP تعریف شدن) به یک برچسبِ فارسیِ خوانا برای نمایش توی پنلِ عمومی.
PUBLIC_LOCK_LABELS = {
    "link": "لینک", "photo": "عکس", "video": "ویدیو/فیلم", "voice": "صدا/ویس",
    "gif": "گیف", "file": "فایل", "id": "آیدی", "forward": "فوروارد",
    "location": "لوکیشن", "poll": "نظرسنجی", "post": "پست", "story": "استوری",
    "live": "لایو", "emoji": "اموجی", "text": "متن", "sticker": "استیکر",
    "reply": "ریپلای", "hangcode": "کد هنگی", "join_duplicate": "جوین تکراری",
    "english": "انگلیسی", "spam": "اسپم", "ad": "تبلیغ", "badword": "فحش/متن نامناسب",
    "bio_link": "بیو (لینک)", "entertainment": "سرگرمی",
}

# آیدی عددیِ کاربرهایی که گیتِ عضویتِ اجباری رو رد کردن (از STATE بارگذاری
# می‌شه تا با ری‌استارتِ manager.py دوباره از کاربرها عضویت خواسته نشه).
PUBLIC_VERIFIED = {int(x) for x in STATE.get("public_verified", [])}

# مراحلِ نیمه‌کاره‌ی بخشِ عمومی - دقیقاً مثلِ PENDING خودِ OWNER ولی جدا، تا
# هیچ‌وقت با مراحلِ افزودنِ سلف/ارسالِ همگانی قاطی نشه. فقط در حافظه (با
# ری‌استارتِ manager.py خالی می‌شه - دقیقاً همون رفتارِ PENDING اصلی).
PUBLIC_PENDING = {}


def mark_public_verified(uid):
    if uid not in PUBLIC_VERIFIED:
        PUBLIC_VERIFIED.add(uid)
        STATE["public_verified"] = sorted(PUBLIC_VERIFIED)
        save_state()


# ==================== ثبتِ «کدوم گروه مالِ کدوم کاربره» ====================
# قبلاً همچین نگاشتی وجود نداشت؛ پنل فقط با فوروارد کردنِ یک پیام از گروه کار
# می‌کرد و هیچ کنترلی رو مالکیت نداشت. الان هر گروه دقیقاً به یک uid (همون
# کسی که لینکش رو داده) وصله و هر uid هم فقط یک گروه می‌تونه داشته باشه.
# ایندکسِ uid -> chat_id: قبلاً هر بار برای پیدا کردنِ گروهِ یک کاربر کلِ
# ثبت‌ها (هزاران رکورد) پیمایش می‌شد و توی هر کلیک چندبار صدا زده می‌شد.
_REG_BY_UID = {}


def _rec_uids(rec):
    """همه‌ی uid هایی که به این گروه (به‌عنوانِ مالک/ادمین) پنل وصل کردن."""
    uids = rec.get("uids")
    if uids is None:
        uids = [rec.get("uid")]
    out = []
    for u in uids:
        try:
            out.append(int(u))
        except (TypeError, ValueError):
            continue
    return out


def _rebuild_reg_index():
    _REG_BY_UID.clear()
    for chat_id, rec in STATE.get("public_registrations", {}).items():
        for u in _rec_uids(rec):
            _REG_BY_UID.setdefault(u, chat_id)


_rebuild_reg_index()


def public_registration_for_uid(uid):
    """رکوردِ ثبتِ گروهِ این کاربر رو برمی‌گردونه: (chat_id_int, record) یا (None, None)."""
    try:
        uid = int(uid)
    except (TypeError, ValueError):
        return None, None
    chat_id = _REG_BY_UID.get(uid)
    rec = STATE.get("public_registrations", {}).get(chat_id) if chat_id is not None else None
    if rec is None or uid not in _rec_uids(rec):
        _rebuild_reg_index()
        chat_id = _REG_BY_UID.get(uid)
        rec = STATE.get("public_registrations", {}).get(chat_id) if chat_id is not None else None
        if rec is None:
            return None, None
    try:
        return int(chat_id), rec
    except (TypeError, ValueError):
        return chat_id, rec


def register_public_group(uid, iid, chat_id, group_name):
    """گروه رو به نامِ این uid ثبت می‌کنه. اگه گروه از قبل (با همون سلف) ثبت شده،
    uid به‌عنوانِ یک مالک/ادمینِ دیگه‌ی همون گروه اضافه می‌شه - ثبتِ قبلی
    بازنویسی نمی‌شه، پس ادمین‌های دیگه‌ی همون گروه هم به پنل دسترسی دارن.
    (فراخوان‌ها باید قبلاً ادمین/مالک بودنِ uid رو از خودِ سلف تایید کرده باشن.)"""
    regs = STATE.setdefault("public_registrations", {})
    key = str(chat_id)
    rec = regs.get(key)
    uid = int(uid)
    if rec and rec.get("iid") == iid:
        uids = set(_rec_uids(rec))
        uids.add(uid)
        rec["uids"] = sorted(uids)
        rec["group_name"] = group_name or rec.get("group_name")
    else:
        regs[key] = {
            "uid": uid, "uids": [uid], "iid": iid,
            "group_name": group_name, "joined_at": time.time(),
        }
    _rebuild_reg_index()
    save_state()  # ذخیره‌ی تأخیری (state_flush_loop تا ~۲ ثانیه بعد می‌نویسه؛ event loop بلاک نمی‌شه)


def unlink_public_user(uid, chat_id):
    """دسترسیِ یک کاربر به پنلِ این گروه رو می‌گیره (مثلاً چون دیگه ادمین نیست)؛
    رکوردِ گروه و بقیه‌ی ادمین‌ها دست‌نخورده می‌مونن."""
    rec = STATE.get("public_registrations", {}).get(str(chat_id))
    if not rec:
        return
    rec["uids"] = [u for u in _rec_uids(rec) if u != int(uid)]
    _rebuild_reg_index()
    save_state()  # ذخیره‌ی تأخیری (state_flush_loop تا ~۲ ثانیه بعد می‌نویسه؛ event loop بلاک نمی‌شه)


def unregister_public_group(chat_id):
    STATE.setdefault("public_registrations", {}).pop(str(chat_id), None)
    GROUP_INFO_CACHE.pop(str(chat_id), None)
    for k in [k for k in PANEL_AUTH_CACHE if k[0] == str(chat_id)]:
        PANEL_AUTH_CACHE.pop(k, None)
    _rebuild_reg_index()
    save_state()  # ذخیره‌ی تأخیری (state_flush_loop تا ~۲ ثانیه بعد می‌نویسه؛ event loop بلاک نمی‌شه)


# ---- چکِ زنده‌ی «هنوز مالک/ادمینِ این گروه هست؟» قبل از هر کلیکِ پنل ----
PANEL_AUTH_CACHE = {}    # (str(chat_id), uid) -> timestamp آخرین تاییدِ موفق
PANEL_AUTH_TTL = 45


async def ensure_panel_access(uid, chat_id, iid, fresh=False):
    """از خودِ سلف می‌پرسه که uid الان هم مالک/ادمینِ واقعیِ این گروهه یا نه.
    خروجی: ok | denied | bot_gone (ربات از گروه بن/اخراج شده) | unavailable (سلف جواب نداد؛ دسترسی داده نمی‌شه)."""
    key = (str(chat_id), int(uid))
    if not fresh and time.time() - PANEL_AUTH_CACHE.get(key, 0) < PANEL_AUTH_TTL:
        return "ok"
    result = await run_panel_group_cmd(iid, "auth", chat_id, {"uid": int(uid), "fresh": bool(fresh)})
    if not result or not result.get("ok"):
        return "unavailable"
    if result.get("bot_state") == "gone":
        return "bot_gone"
    if result.get("authorized"):
        PANEL_AUTH_CACHE[key] = time.time()
        return "ok"
    PANEL_AUTH_CACHE.pop(key, None)
    return "denied"


async def handle_bot_gone(s, cq_id, cid, mid, uid, chat_id, reg):
    """ربات از گروه بن/اخراج شده (یا خودش خارج شده): ثبتِ گروه برای همه‌ی ادمین‌ها
    خودکار پاک می‌شه و به کاربر گفته می‌شه دیگه نیازی به «حذف گروهم» نیست."""
    gname = reg.get("group_name") or chat_id
    label = self_label(reg.get("iid"))
    unregister_public_group(chat_id)
    await answer_cq(s, cq_id, f"ℹ️ ربات «{label}» دیگه توی این گروه نیست.", show_alert=True)
    text, keyboard = render_public_home(uid)
    await edit_central(
        s, cid, mid,
        f"ℹ️ ربات «{label}» دیگه عضوِ گروه «{gname}» نیست (بن/اخراج شده یا خودش خارج شده).\n\n"
        "ثبتِ گروه خودکار حذف شد؛ نیازی به زدنِ «حذف گروهم» نیست. "
        "اگه می‌خوای دوباره ربات رو اضافه کنی، از «افزودنِ گروهم» لینکِ گروه رو بفرست.\n\n" + text,
        keyboard,
    )


async def resolve_panel_group(s, cq_id, cid, mid, uid, fresh=False):
    """گروهِ ثبت‌شده‌ی این کاربر رو برمی‌گردونه *فقط اگه* هنوز مالک/ادمینِ همون
    گروه باشه. در غیر این صورت به کاربر پیام می‌ده و (None, None) برمی‌گردونه."""
    chat_id, reg = public_registration_for_uid(uid)
    if not reg:
        await answer_cq(s, cq_id, "⛔ هنوز هیچ گروهی ثبت نکردی.", show_alert=True)
        return None, None
    status = await ensure_panel_access(uid, chat_id, reg.get("iid"), fresh=fresh)
    if status == "ok":
        return chat_id, reg
    if status == "bot_gone":
        await handle_bot_gone(s, cq_id, cid, mid, uid, chat_id, reg)
        return None, None
    if status == "denied":
        unlink_public_user(uid, chat_id)
        await answer_cq(s, cq_id, "⛔ شما دیگه مالک/ادمینِ این گروه نیستی؛ دسترسی‌ت به پنل برداشته شد.", show_alert=True)
        text, keyboard = render_public_home(uid)
        await edit_central(s, cid, mid, text, keyboard)
    else:
        await answer_cq(s, cq_id, "⏳ سلفِ این گروه جواب نمی‌ده؛ برای امنیت تا تاییدِ ادمین بودنت دسترسی داده نمی‌شه. کمی بعد دوباره امتحان کن.", show_alert=True)
    return None, None


def public_join_kb():
    rows = [
        [{"text": f"📣 {ch}", "url": f"https://splus.ir/{ch.lstrip('@')}"}]
        for ch in PUBLIC_JOIN_CHANNELS
    ]
    rows.append([btn("✅ عضو شدم", "p:verify")])
    return {"inline_keyboard": rows}


def render_public_join_gate():
    return PUBLIC_JOIN_MESSAGE, public_join_kb()


def render_public_home(uid=None):
    _, reg = public_registration_for_uid(uid) if uid is not None else (None, None)
    text = (
        "🌟 به بخش کاربری خوش اومدی!\n\n"
        "از اینجا می‌تونی ربات رو به گروهت اضافه کنی و پنلِ آمارِ اختصاصیِ "
        "گروهت (رتبه + وضعیتِ قفل‌ها) رو ببینی.\n\n"
        "🔐 فقط «مالک» و «ادمین‌های» هر گروه می‌تونن گروه رو اضافه کنن یا پنلش رو مدیریت کنن.\n\n"
        "یکی از گزینه‌های زیر رو انتخاب کن:"
    )
    rows = []
    # دکمه‌ی «حذف گروهم» عمداً اینجا نیست؛ فقط داخلِ پنلِ گروه (render_group_home_kb) هست.
    if reg:
        rows.append([btn("📊 پنل گروهم", "p:mypanel")])
    else:
        rows.append([btn("➕ افزودنِ گروهم", "p:getlink")])
    rows.append([btn("💎 خرید ربات پرو", "p:buypro")])
    rows.append([btn("🆘 پشتیبانی", "p:support"), btn("📚 راهنما", "p:help")])
    return text, {"inline_keyboard": rows}


def render_public_buypro():
    rows = [[{"text": f"💬 {PRO_CONTACT}", "url": _contact_url(PRO_CONTACT)}], [btn("⬅️ بازگشت", "p:home")]]
    return PRO_MESSAGE, {"inline_keyboard": rows}


def render_public_support():
    rows = [[{"text": f"💬 {SUPPORT_CONTACT}", "url": _contact_url(SUPPORT_CONTACT)}], [btn("⬅️ بازگشت", "p:home")]]
    return "🆘 برای پشتیبانی پیام بده:", {"inline_keyboard": rows}


def render_public_help():
    text = (
        "📚 راهنما:\n\n"
        "1️⃣ لینکِ عضویتِ گروهت رو بفرست (فقط مالک/ادمینِ گروه).\n"
        "2️⃣ ربات خودکار عضو گروه می‌شه، مالک و ادمین‌ها رو شناسایی می‌کنه و خودش فعال می‌شه.\n"
        "3️⃣ ربات رو ادمین کن تا همه‌ی قفل‌ها کار کنن.\n"
        "4️⃣ پنلِ آمار و قفل‌های گروهت همینجا در دسترسه.\n\n"
        "اگه ربات از قبل توی گروهه، مالک و ادمین‌های همون گروه با فرستادنِ لینک به پنل دسترسی می‌گیرن؛ "
        "عضوِ عادی هیچ دسترسی‌ای نداره.\n"
        "هر کاربر فقط یک گروه می‌تونه ثبت کنه؛ برای گروهِ دیگه اول از «پنل گروهم» گزینه‌ی «حذف گروهم» رو بزن."
    )
    return text, {"inline_keyboard": [[btn("⬅️ بازگشت", "p:home")]]}


def self_label(iid):
    """اسمِ نمایشیِ سلف (همون اسمی که توی پنلِ OWNER هست) برای درجِ توی پیام‌ها."""
    inst = STATE.get("instances", {}).get(iid) or {}
    return inst.get("name") or str(iid or "ربات")


def render_public_delgroup_confirm(group_name, bot_label=None):
    who = f"ربات «{bot_label}»" if bot_label else "ربات"
    text = f"⚠️ مطمئنی می‌خوای گروهِ «{group_name}» رو حذف کنی؟\n\n{who} ازش خارج می‌شه و باید از اول ثبتش کنی."
    rows = [[btn("✅ بله، حذف کن", "p:delgroup_yes")], [btn("❌ نه، بازگشت", "p:home")]]
    return text, {"inline_keyboard": rows}


def eligible_join_selfs():
    """لیستِ سلف‌های واجدِ شرایط برای تستِ جوینِ یک لینک - فقط روشن‌ها و
    معاف‌نشده‌ها (دکمه‌ی «معاف از لیستِ جوین» توی صفحه‌ی شخصیِ هر سلف)، از
    سبک‌ترین (کمترین تعداد گروه) به سنگین‌ترین.
    نکته: عمداً هیچ سقفِ دستی‌ای (مثلِ MAX_GROUPS_PER_SELF قبلی) اینجا چک
    نمی‌شه - «پُر بودن» حالا فقط از رویِ نتیجه‌ی واقعیِ تلاشِ جوین (پایین‌تر،
    run_join_test) تشخیص داده می‌شه، نه یک عددِ از پیش تعیین‌شده که ممکنه با
    واقعیت جور در نیاد."""
    now = time.time()
    items = []
    for iid, inst in STATE["instances"].items():
        if not is_running(iid):
            continue
        if inst.get("join_exempt"):
            continue
        # سلفی که تازه «پُر» / flood-wait / بدون‌پاسخ بوده، برای مدتی کنار گذاشته
        # می‌شه تا کاربرِ بعدی وقتش رو پشتِ سرش تلف نکنه.
        if JOIN_COOLDOWN.get(iid, 0) > now:
            continue
        # جوین‌های هم‌زمان بینِ سلف‌ها پخش می‌شن (هر جوینِ در حالِ انجام مثلِ ۲۰
        # گروهِ اضافه حساب می‌شه) تا همه‌ی کاربرهای هم‌زمان روی یک سلف نریزن.
        load = group_count_of(inst) + 20 * JOIN_INFLIGHT.get(iid, 0)
        items.append((load, iid))
    items.sort(key=lambda t: t[0])
    return [iid for _, iid in items]


def all_running_selfs():
    """همه‌ی سلف‌های روشن، بدون فیلترِ معافیت - فقط برای چکِ «از قبل عضوه یا
    نه» (find_existing_self_for_link)، نه برای انتخاب برای یک جوینِ تازه؛ چون
    این چک هیچ جوینی انجام نمی‌ده، سلف‌های معاف‌شده از لیستِ جوینِ عمومی هم
    باید توش باشن."""
    return [iid for iid, inst in STATE["instances"].items() if is_running(iid)]


_LINK_FATAL_MARKERS = (
    "expired and is not valid", "invite hash is invalid", "invite hash is empty",
    "maximum number of users has been exceeded", "successfully requested to join",
    "لینک نامعتبر", "مالِ یک کانال", "از قبل",
)
_SELF_FULL_MARKERS = ("joined too many channels", "channels_too_much", "too many channels")
_FLOOD_RE = re.compile(r"wait of (\d+) seconds", re.IGNORECASE)


def classify_join_error(err):
    """خطای جوینِ یک سلف رو دسته‌بندی می‌کنه تا لازم نباشه یک لینکِ خراب/منقضی رو
    با همه‌ی سلف‌ها امتحان کنیم:
      ("link", 0)      → مشکل از خودِ لینک/گروهه؛ سلفِ دیگه هم همین جواب رو می‌گیره
      ("self_full", s) → این سلف پُره؛ s ثانیه کنار گذاشته می‌شه
      ("flood", s)     → flood-wait؛ s ثانیه کنار گذاشته می‌شه
      ("slow", s)      → سلف جواب نداد؛ s ثانیه کنار گذاشته می‌شه
      ("other", 0)     → نامشخص؛ سلفِ بعدی امتحان می‌شه"""
    e = str(err or "").lower()
    if any(m in e for m in _SELF_FULL_MARKERS):
        return "self_full", 1800
    m = _FLOOD_RE.search(e)
    if m:
        return "flood", min(int(m.group(1)) + 5, 3600)
    if "پاسخ نداد" in e or "timeout" in e or "بیش از حد طول کشید" in e:
        return "slow", 60
    if any(k in e for k in _LINK_FATAL_MARKERS):
        return "link", 0
    return "other", 0


PUBLIC_LINK_PATTERN = re.compile(
    r'(?:https?://)?(?:t\.me|telegram\.me|splus\.ir)/(?:joinchat/|joingroup/|\+)?([A-Za-z0-9_\-]+)',
    re.IGNORECASE,
)


def extract_public_link(text):
    """یک لینکِ گروه (splus.ir یا t.me) رو از متنِ کاربر استخراج می‌کنه؛ اگه
    پیدا نشه None برمی‌گردونه."""
    match = PUBLIC_LINK_PATTERN.search(text or "")
    if not match:
        return None
    full_link = match.group(0)
    if not full_link.lower().startswith("http"):
        full_link = "https://" + full_link
    return full_link


async def _reply_or_edit(session, cid, mid, text, keyboard=None):
    """اگه message_id داشته باشیم پیامِ قبلی رو edit می‌کنه (برای گزارشِ
    پیشرفتِ تستِ سلف‌ها)، وگرنه (مثلاً sendMessage اولیه شکست خورده بود)
    یک پیامِ تازه می‌فرسته."""
    if mid:
        return await edit_central(session, cid, mid, text, keyboard)
    return await send_central(session, cid, text, keyboard)


async def _show_join_result(session, cid, mid, iid, chat_id, head, label):
    """اول *فوراً* پیامِ موفقیت رو نشون می‌ده (بدونِ منتظرِ آمار موندن)، بعد که
    اطلاعاتِ گروه/رتبه آماده شد، همون پیام رو با پنل جایگزین می‌کنه."""
    menu_kb = {"inline_keyboard": [[btn("⬅️ منو", "p:home")]]}
    sent = await _reply_or_edit(session, cid, mid, head + "⏳ در حال آماده‌سازیِ پنل...", menu_kb)
    if isinstance(sent, dict) and sent.get("message_id"):
        mid = sent["message_id"]
    text, keyboard = head, menu_kb
    info = await fetch_group_info(chat_id, iid=iid)
    if info:
        rank, total_groups = await _compute_rank(info.get("total_messages", 0))
        text += render_group_home_text(info, rank, total_groups, label)
        keyboard = render_group_home_kb()
    else:
        text += "⏳ اطلاعاتِ گروه هنوز آماده نیست؛ چند لحظه دیگه از منو روی «پنل گروهم» بزن."
    await _reply_or_edit(session, cid, mid, text, keyboard)


async def run_remote_command(iid, command_type, message, timeout=PANEL_CMD_TIMEOUT):
    """یک دستورِ فوریِ مخصوصِ یک سلف (iid) - «تستِ جوین»، «خروج از گروه»، یا
    یکی از دستورهای پنلِ گروهی (قفل/تنظیمات/آمار) - رو ثبت می‌کنه و تا
    رسیدنِ نتیجه (یا timeout) صبر می‌کنه.

    هر دستور یک رکوردِ مستقل توی STATE["broadcasts"] می‌گیره (سلف‌های مختلف کاملاً
    موازی)؛ عمداً save_state صدا زده نمی‌شه چون این رکوردها کوتاه‌عمر و درون‌حافظه‌ان.

    بهینه‌سازی‌ها:
      • سلفِ خاموش → فوراً خطا (به‌جای انتظارِ کاملِ timeout).
      • سقفِ صف برای هر سلف → «شلوغه» به‌جای معطلیِ بی‌نتیجه.
      • سلفِ موردنظر همون لحظه از طریقِ long-poll بیدار می‌شه (نه بعد از چند ثانیه)."""
    if not is_running(iid):
        return {"ok": False, "error": "این سلف الان خاموشه."}
    if PENDING_CMDS.get(iid, 0) >= MAX_QUEUED_CMDS_PER_SELF:
        return {"ok": False, "error": "ربات الان خیلی شلوغه؛ چند ثانیه‌ی دیگه دوباره امتحان کن."}

    bid = next(_BROADCAST_ID_COUNTER)
    STATE["broadcasts"][str(bid)] = {
        "id": bid, "command_type": command_type, "message": message,
        "targets": [iid], "created_at": time.time(), "acks": [],
        # بعد از این زمان دیگه کسی منتظرِ نتیجه نیست؛ سلف نباید دستورِ منقضی رو اجرا کنه.
        "deadline": time.time() + timeout,
    }

    fut = asyncio.get_running_loop().create_future()
    JOIN_RESULT_WAITERS[bid] = fut
    PENDING_CMDS[iid] = PENDING_CMDS.get(iid, 0) + 1
    _wake_instance(iid)
    try:
        result = await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        result = {"ok": False, "error": "این سلف پاسخ نداد (شاید خاموش یا هنگ باشه)."}
    finally:
        JOIN_RESULT_WAITERS.pop(bid, None)
        STATE["broadcasts"].pop(str(bid), None)
        left = PENDING_CMDS.get(iid, 1) - 1
        if left > 0:
            PENDING_CMDS[iid] = left
        else:
            PENDING_CMDS.pop(iid, None)
    return result or {"ok": False, "error": "بدون نتیجه."}


async def run_join_test(iid, link, uid=None, timeout=JOIN_TIMEOUT):
    """uid = کسی که درخواست داده؛ سلف بعد از جوین چک می‌کنه مالک/ادمینِ همون گروه
    باشه و اگه نبود بی‌سروصدا لفت می‌ده (result[\"not_admin\"])."""
    payload = json.dumps({"link": link, "uid": int(uid)}) if uid is not None else link
    return await run_remote_command(iid, "join_test", payload, timeout)


async def run_membership_check(iid, link, uid, timeout=MEMBERSHIP_CHECK_TIMEOUT):
    return await run_remote_command(iid, "check_membership", json.dumps({"link": link, "uid": uid}), timeout)


async def find_existing_self_for_link(link, uid):
    """قبل از تستِ جوین، چک می‌کنه که آیا یکی از سلف‌های روشن - حتی اونایی که
    از لیستِ جوینِ عمومی معافن - از قبل عضوِ همین گروهه (بدون این‌که هیچ سلفی
    واقعاً جوین کنه). همه‌ی سلف‌های روشن موازی چک می‌شن.

    نکته‌ی امنیتی: صرفِ داشتنِ لینکِ عضویت (که ممکنه دستِ هرکسی افتاده باشه -
    مثلاً یکی از اعضای معمولیِ همون گروه) کافی نیست تا کسی پنلِ یک گروهِ
    از‌قبل‌ثبت‌شده رو بگیره؛ هر سلف خودش (از طریقِ check_user_permission)
    چک می‌کنه که همین uid واقعاً ادمین/سازنده/مالکِ همون گروهه یا نه.

    خروجی:
      None                        -> هیچ سلفی توی این گروه نیست؛ باید تستِ جوینِ عادی انجام بشه.
      ("denied", group_name)      -> یک سلف توی گروهه ولی uid ادمین/مالکِ اون گروه نیست.
      (iid, chat_id, group_name)  -> یک سلف توی گروهه و uid واقعاً ادمین/مالکشه."""
    candidates = all_running_selfs()
    if not candidates:
        return None
    # به‌جای asyncio.gather (که منتظرِ کندترین/مشغول‌ترین سلف می‌مونه)، نتیجه‌ها
    # به‌محضِ رسیدن بررسی می‌شن؛ اولین سلفِ «عضو + مجاز» همون لحظه برنده‌ست و
    # چک‌های باقی‌مونده کنسل می‌شن.
    tasks = {asyncio.ensure_future(run_membership_check(iid, link, uid)): iid for iid in candidates}
    pending = set(tasks)
    found_but_denied = None
    try:
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                try:
                    result = t.result()
                except Exception:
                    continue
                if not result:
                    continue
                if not (result.get("ok") and result.get("already_member") and result.get("chat_id") is not None):
                    continue
                if result.get("authorized"):
                    return tasks[t], result.get("chat_id"), result.get("group_name") or "گروه"
                found_but_denied = (result.get("group_name") or "گروه", tasks[t])
    finally:
        for t in pending:
            t.cancel()
    if found_but_denied is not None:
        return "denied", found_but_denied[0], found_but_denied[1]
    return None


async def run_leave_group(iid, chat_id, timeout=LEAVE_TIMEOUT):
    return await run_remote_command(iid, "leave_group", str(chat_id), timeout)


# ---- کشِ اطلاعاتِ گروه (قفل‌ها/تنظیمات/آمار) ----
GROUP_INFO_CACHE = {}    # str(chat_id) -> (timestamp, info)
_GET_INFLIGHT = {}       # (iid, chat_id) -> Task


def _cache_group_info(chat_id, info):
    if len(GROUP_INFO_CACHE) >= GROUP_INFO_CACHE_MAX:
        # قدیمی‌ترین ۱۰٪ رو حذف کن
        for key, _ in sorted(GROUP_INFO_CACHE.items(), key=lambda kv: kv[1][0])[: GROUP_INFO_CACHE_MAX // 10]:
            GROUP_INFO_CACHE.pop(key, None)
    GROUP_INFO_CACHE[str(chat_id)] = (time.time(), info)


async def run_panel_group_cmd(iid, action, chat_id, payload=None, timeout=PANEL_CMD_TIMEOUT):
    """یک دستورِ مدیریتی روی یک گروهِ مشخص (toggle قفل، تنظیمات، آمارِ فعالان و
    ...) رو مستقیم به همون سلفی که این گروه رو داره می‌فرسته و یک snapshot
    تازه (یا نتیجه‌ی مخصوصِ همون اکشن، مثلِ لیدربوردِ فعالان) برمی‌گردونه.
    هر snapshotِ موفق توی کش هم ذخیره می‌شه تا منوهای بعدی فوری باز بشن."""
    cmd = {"action": action, "chat_id": chat_id, "payload": payload or {}}
    result = await run_remote_command(iid, "panel_group_cmd", json.dumps(cmd), timeout)
    if action not in ("activity", "auth") and result and result.get("ok"):
        _cache_group_info(chat_id, result)
    return result


def render_public_getlink_prompt():
    text = (
        "🔗 لینکِ عضویتِ گروهت رو همینجا برام بفرست (همون لینکی که برای دعوتِ "
        "عضوِ جدید به گروه استفاده می‌کنی).\n\n"
        "خودم یکی‌یکی سلف‌های آزاد رو امتحان می‌کنم و هر کدوم موفق شد بهت "
        "می‌گم کدوم بود."
    )
    return text, {"inline_keyboard": [[btn("⬅️ بازگشت", "p:home")]]}


async def find_self_and_join(session, cid, mid, uid, link):
    """لینکِ یک گروه رو با سلف‌های واجدِ شرایط (eligible_join_selfs) یکی‌یکی تست
    می‌کنه؛ اولین سلفی که موفق بشه، گروه رو به‌نامِ همین uid ثبت می‌کنه و بلافاصله
    پنلش رو نشون می‌ده.
    اگه خطا از خودِ لینک باشه (منقضی/نامعتبر/ربات دیگه‌ای از قبل توشه) همون‌جا
    متوقف می‌شه و بقیه‌ی سلف‌ها بی‌خود امتحان نمی‌شن؛ سلفِ پُر/flood/بدون‌پاسخ هم
    برای مدتی از لیستِ کاندیداها کنار می‌ره.

    قبل از هر تلاشِ جوینی، اول چک می‌شه که آیا یکی از سلف‌ها (حتی معاف‌شده‌ها از
    لیستِ جوینِ عمومی) از قبل عضوِ همین گروهه؛ اگه بود، پنلش فقط وقتی مستقیم
    به همین uid داده می‌شه که uid واقعاً ادمین/مالکِ همون گروه باشه - نه صرفاً
    به این خاطر که لینکش رو داشته (find_existing_self_for_link)."""
    existing = await find_existing_self_for_link(link, uid)
    if existing and existing[0] == "denied":
        gname = existing[1]
        await _reply_or_edit(
            session, cid, mid,
            f"⛔ ربات «{self_label(existing[2])}» از قبل عضوِ گروه «{gname}» هست، ولی پنلش رو فقط مالک/ادمینِ همون گروه می‌تونه بگیره - "
            "نه هرکسی که لینکِ عضویت رو داشته باشه.",
            {"inline_keyboard": [[btn("⬅️ بازگشت", "p:home")]]},
        )
        return
    if existing:
        iid, chat_id, gname = existing
        register_public_group(uid, iid, chat_id, gname)
        await _show_join_result(
            session, cid, mid, iid, chat_id,
            f"✅ ربات «{self_label(iid)}» از قبل عضوِ گروه «{gname}» بود؛ پنلت باز شد!\n\n",
            self_label(iid),
        )
        return

    candidates = eligible_join_selfs()[:MAX_JOIN_TRIES]
    if not candidates:
        await _reply_or_edit(
            session, cid, mid,
            "⛔ در حال حاضر هیچ سلفِ فعالِ غیرمعافی برای عضویت وجود نداره.",
            {"inline_keyboard": [[btn("⬅️ بازگشت", "p:home")]]},
        )
        return

    errors = []
    link_error = None
    for idx, iid in enumerate(candidates, start=1):
        inst = STATE["instances"].get(iid) or {}
        label = inst.get("name", iid)
        if idx > 1:
            await _reply_or_edit(session, cid, mid, f"⏳ در حال تستِ سلفِ بعدی ({idx} از {len(candidates)})...")
        JOIN_INFLIGHT[iid] = JOIN_INFLIGHT.get(iid, 0) + 1
        try:
            result = await run_join_test(iid, link, uid)
        finally:
            left = JOIN_INFLIGHT.get(iid, 1) - 1
            if left > 0:
                JOIN_INFLIGHT[iid] = left
            else:
                JOIN_INFLIGHT.pop(iid, None)
        if result.get("ok"):
            gname = result.get("group_name") or "گروه"
            chat_id = result.get("chat_id")
            if chat_id is not None:
                register_public_group(uid, iid, chat_id, gname)
            head = (
                f"✅ ربات «{label}» با موفقیت عضو گروه «{gname}» شد و پنلت باز شد!\n\n"
                "🤖 ربات خودکار فعال می‌شه و مالک و ادمین‌های گروه رو خودش شناسایی می‌کنه؛ "
                "نیازی به نوشتنِ «فعال» نیست. فقط ربات رو ادمین کن تا همه‌ی قفل‌ها کار کنن.\n\n"
            )
            if chat_id is not None:
                await _show_join_result(session, cid, mid, iid, chat_id, head, label)
            else:
                await _reply_or_edit(session, cid, mid, head, {"inline_keyboard": [[btn("⬅️ منو", "p:home")]]})
            return

        if result.get("not_admin"):
            gname = result.get("group_name") or "گروه"
            await _reply_or_edit(
                session, cid, mid,
                f"⛔ فقط مالک یا ادمینِ گروه می‌تونه گروهش رو اضافه کنه و پنل بگیره.\n\n"
                f"شما توی گروه «{gname}» مالک/ادمین نیستی، برای همین ربات «{label}» دوباره از گروه خارج شد.\n"
                "اگه ادمین هستی، مطمئن شو که واقعاً توی همین گروه ادمین شدی و لینک رو دوباره بفرست.",
                {"inline_keyboard": [[btn("⬅️ بازگشت", "p:home")]]},
            )
            return

        err = result.get("error") or "نامشخص"
        kind, secs = classify_join_error(err)
        if kind in ("self_full", "flood", "slow") and secs:
            JOIN_COOLDOWN[iid] = time.time() + secs
        errors.append(f"«{label}»: {err}")
        if kind == "link":
            link_error = err
            break

    if link_error:
        text = (
            "⛔ این لینک قابل استفاده نیست.\n\n"
            f"علت: {link_error}\n\n"
            "لینکِ جدید/معتبرِ گروهت رو بفرست (اگه گروه «تایید ادمین» می‌خواد، اول اون رو خاموش کن)."
        )
    else:
        text = (
            "⛔ هیچ‌کدوم از سلف‌ها نتونستن عضو این گروه بشن.\n\n"
            "جزئیات:\n" + "\n".join(f"• {e}" for e in errors[:10]) +
            "\n\nممکنه لینک منقضی/نامعتبر باشه یا گروه نیاز به تاییدِ ادمین داشته باشه."
        )
    await _reply_or_edit(session, cid, mid, text, {"inline_keyboard": [[btn("⬅️ بازگشت", "p:home")]]})


def _self_db_path(iid):
    return instance_dir(iid) / "group_bot.db"


def _read_all_totals_sync(db_path):
    """فقط برای محاسبه‌ی رتبه بینِ همه‌ی گروه‌ها به کار می‌ره - یک خواندنِ محلیِ
    سبک از فایلِ sqlite هر سلف (نیازی به آنلاین بودنِ خودِ سلف نداره، برخلافِ
    fetch_group_info که از طریقِ کانالِ broadcast/ack با خودِ سلفِ زنده حرف می‌زنه)."""
    if not db_path.exists():
        return []
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5)
    try:
        conn.execute("PRAGMA busy_timeout=5000")
        c = conn.cursor()
        c.execute("SELECT total_messages FROM stats")
        return [r[0] or 0 for r in c.fetchall()]
    finally:
        conn.close()


# ---- رتبه‌ی گروه‌ها ----
# قبلاً هر بار که یک کاربر «پنل گروهم» رو باز می‌کرد، دیتابیسِ sqlite همه‌ی سلف‌ها
# (هزاران ردیف) از نو خونده و مرتب می‌شد. الان یک لیستِ مرتب‌شده‌ی کش‌شده هر
# RANK_REFRESH_SECONDS ثانیه توی پس‌زمینه ساخته می‌شه و رتبه با bisect (لگاریتمی) درمیاد.
RANK_CACHE = {"totals": [], "ts": 0.0}
_RANK_REFRESH_TASK = None


async def _refresh_rank_cache():
    totals = []
    for iid in list(STATE["instances"]):
        try:
            totals.extend(await asyncio.to_thread(_read_all_totals_sync, _self_db_path(iid)))
        except Exception:
            continue
    totals.sort()  # صعودی؛ رتبه با bisect حساب می‌شه
    RANK_CACHE["totals"] = totals
    RANK_CACHE["ts"] = time.time()


def _ensure_rank_refresh():
    global _RANK_REFRESH_TASK
    if _RANK_REFRESH_TASK is None or _RANK_REFRESH_TASK.done():
        _RANK_REFRESH_TASK = spawn(_refresh_rank_cache())
    return _RANK_REFRESH_TASK


async def _compute_rank(my_total):
    if not RANK_CACHE["ts"]:
        await _ensure_rank_refresh()          # فقط اولین بار (کش هنوز خالیه)
    elif time.time() - RANK_CACHE["ts"] > RANK_REFRESH_SECONDS * 2:
        _ensure_rank_refresh()                # کهنه شده؛ پس‌زمینه تازه کن، منتظرش نمون
    totals = RANK_CACHE["totals"]
    my_total = my_total or 0
    greater = len(totals) - bisect.bisect_right(totals, my_total)
    rank = greater + 1
    return rank, max(len(totals), rank, 1)


async def rank_refresh_loop():
    await asyncio.sleep(3)
    while True:
        try:
            await _refresh_rank_cache()
        except Exception as e:
            print("rank refresh error:", e)
        await asyncio.sleep(RANK_REFRESH_SECONDS)


def find_instance_for_chat(chat_id):
    """کدوم سلف این chat_id رو داره - اول از رجیستریِ عمومی (register_public_group،
    که همون لحظه‌ی جوینِ موفق ثبت می‌شه)، وگرنه از STATE["instances"][iid]["groups"]
    که هر ~۱۵ ثانیه از خودِ سلف به‌روز می‌شه (دیتای «لیست گروه‌ها»ی پنلِ OWNER)."""
    key = str(chat_id)
    reg = STATE.get("public_registrations", {}).get(key)
    if reg and reg.get("iid"):
        return reg["iid"]
    for iid, inst in STATE["instances"].items():
        if key in (inst.get("groups") or {}):
            return iid
    return None


async def fetch_group_info(chat_id, iid=None, max_age=0):
    """اطلاعاتِ زنده‌ی یک گروه (آمار، قفل‌ها، تنظیمات) رو از خودِ سلف (از طریقِ
    کانالِ broadcast/ack) می‌گیره. اگه سلف آفلاین/بدون‌پاسخ باشه یا اصلاً پیدا
    نشه، None برمی‌گردونه.
      • max_age > 0: اگه کشِ تازه‌تر از این‌قدر ثانیه موجود باشه همون برمی‌گرده (فوری).
      • درخواست‌های هم‌زمانِ یک گروه (مثلاً دوبار کلیک) یک دستور بیشتر به سلف نمی‌فرستن."""
    iid = iid or find_instance_for_chat(chat_id)
    if not iid:
        return None
    if max_age:
        hit = GROUP_INFO_CACHE.get(str(chat_id))
        if hit and (time.time() - hit[0]) <= max_age:
            return hit[1]
    key = (iid, str(chat_id))
    task = _GET_INFLIGHT.get(key)
    if task is None:
        task = asyncio.ensure_future(run_panel_group_cmd(iid, "get", chat_id))
        _GET_INFLIGHT[key] = task
        task.add_done_callback(lambda _t, _k=key: _GET_INFLIGHT.pop(_k, None))
    result = await asyncio.shield(task)
    if not result or not result.get("ok"):
        return None
    return result


def render_group_home_text(info, rank=None, total_groups=None, bot_label=None):
    name = info.get("group_name") or "گروه"
    locks = info.get("locks") or {}
    on_count = sum(1 for v in locks.values() if v)
    total_locks = len(locks) or 1
    lines = [
        f"📊 پنل گروه «{name}»",
        *([f"🤖 ربات: {bot_label}"] if bot_label else []), "",
        f"💬 مجموعِ پیام‌ها: {info.get('total_messages', 0):,}",
        f"🔐 قفل‌های فعال: {on_count} از {total_locks}",
    ]
    if rank is not None:
        lines.append(f"🏆 رتبه: {rank} از {total_groups} گروه (بر اساسِ فعالیت)")
    lines.append("")
    lines.append("برای جزئیاتِ بیشتر یکی از گزینه‌های زیر رو بزن:")
    return "\n".join(lines)


def render_group_home_kb():
    rows = [
        [btn("🔒 قفل‌ها", "p:lockmenu")],
        [btn("🏆 برترین‌های گروه", "p:actmenu")],
        [btn("⚙️ تنظیمات", "p:settings")],
        [btn("🔄 بروزرسانی", "p:mypanel:r"), btn("🗑 حذف گروهم", "p:delgroup")],
        [btn("⬅️ بازگشت", "p:home")],
    ]
    return {"inline_keyboard": rows}


def render_locks_kb(locks):
    rows, row = [], []
    for key in PUBLIC_LOCK_LABELS:
        label = PUBLIC_LOCK_LABELS.get(key, key)
        state = "⬢" if locks.get(key) else "⬡"
        row.append(btn(f"{state} {label}", f"p:lk:{key}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([btn("⬅️ بازگشت", "p:mypanel")])
    return {"inline_keyboard": rows}


def render_locks_text(info):
    name = info.get("group_name") or "گروه"
    return (
        f"🔒 قفل‌های گروه «{name}»\n\n"
        "روی هرکدوم بزنی وضعیتش برعکس می‌شه:\n"
        "⬢ بسته (فعال)   ⬡ باز (آزاد)"
    )


def render_settings_text(info):
    name = info.get("group_name") or "گروه"
    warn_limit = info.get("warn_limit", 3)
    action = "بن" if info.get("silent_mode") else "اخطار"
    return (
        f"⚙️ تنظیماتِ گروه «{name}»\n\n"
        f"⚠️ سقفِ اخطار: {warn_limit}\n"
        "(هر کاربر بعد از رسیدن به این تعداد اخطار بن می‌شه؛ با ➖ و ➕ عوضش کن)\n\n"
        f"🔇 اکشنِ خودکار: {action}"
    )


def render_settings_kb(info):
    warn_limit = info.get("warn_limit", 3)
    silent = info.get("silent_mode")
    rows = [
        # وسطِ ردیف فقط یک عددِ کوتاه؛ متنِ بلند باعث می‌شد دکمه بریده بشه و عدد دیده نشه.
        [btn("➖", "p:warn:-1"), btn(f"⚠️ {warn_limit}", "noop"), btn("➕", "p:warn:+1")],
        [btn(f"🔇 اکشنِ خودکار: {'بن' if silent else 'اخطار'} (تغییر)", "p:silent:toggle")],
        [btn("⬅️ بازگشت", "p:mypanel")],
    ]
    return {"inline_keyboard": rows}


ACTIVITY_PERIOD_LABELS = {"today": "امروز", "yesterday": "دیروز", "week": "این‌هفته", "month": "این‌ماه"}


def render_activity_text(leaderboard):
    period = leaderboard.get("period", "today")
    entries = leaderboard.get("entries") or []
    period_total = leaderboard.get("period_total")
    # این عددِ مجموعِ کلِ پیام‌های بازه‌ست (نه فقط جمعِ همین ۱۰ نفرِ برتر) - وقتی
    # فعالیتِ گروه بینِ کاربرهای زیادی پخش شده، جمعِ لیدربورد می‌تونه خیلی
    # کمتر از این عدد به‌نظر برسه؛ برای همین جدا نشون داده می‌شه.
    total_line = f"📨 مجموعِ پیام‌های این بازه: {period_total}\n\n" if period_total is not None else ""
    if not entries:
        body = "توی این بازه هیچ پیامی ثبت نشده."
    else:
        body = "\n".join(f"{i}. {e.get('name')} — {e.get('count')} پیام" for i, e in enumerate(entries, start=1))
    return f"🏆 برترین‌های گروه ({ACTIVITY_PERIOD_LABELS.get(period, period)})\n\n{total_line}{body}"


def render_activity_kb(period):
    def pbtn(key):
        return btn(("✅ " if key == period else "") + ACTIVITY_PERIOD_LABELS[key], f"p:act:{key}")
    rows = [
        [pbtn("today"), pbtn("yesterday")],
        [pbtn("week"), pbtn("month")],
        [btn("⬅️ بازگشت", "p:mypanel")],
    ]
    return {"inline_keyboard": rows}


async def handle_public_message(s, msg):
    frm = msg.get("from") or {}
    uid = int(frm.get("id", -1))
    cid = int((msg.get("chat") or {}).get("id", 0))
    text = (msg.get("text") or "").strip()
    if not cid or uid < 0:
        return

    if uid not in PUBLIC_VERIFIED:
        if "عضو شدم" in text:
            mark_public_verified(uid)
        else:
            join_text, join_kb = render_public_join_gate()
            await send_central(s, cid, join_text, join_kb)
            return

    if PUBLIC_PENDING.get(uid) == "awaiting_link":
        existing_chat_id, existing_reg = public_registration_for_uid(uid)
        if existing_reg:
            PUBLIC_PENDING.pop(uid, None)
            await send_central(
                s, cid,
                f"⛔ شما همین الان گروهِ «{existing_reg.get('group_name') or existing_chat_id}» رو ثبت داری.\n\n"
                "برای ثبتِ گروهِ جدید، اول همین یکی رو حذف کن.",
                {"inline_keyboard": [[btn("⬅️ بازگشت", "p:home")]]},
            )
            return
        link = extract_public_link(text)
        if not link:
            await send_central(
                s, cid,
                "❌ لینکِ معتبری توش پیدا نکردم. لینکِ عضویتِ گروهت رو بفرست "
                "(مثلاً https://splus.ir/joingroup/xxxxx) یا برای انصراف بزن بازگشت.",
                {"inline_keyboard": [[btn("⬅️ بازگشت", "p:home")]]},
            )
            return
        PUBLIC_PENDING.pop(uid, None)
        sent = await send_central(s, cid, "🔗 لینک دریافت شد؛ در حال تستِ سلف‌ها...")
        mid = (sent or {}).get("message_id")
        await find_self_and_join(s, cid, mid, uid, link)
        return

    home_text, home_keyboard = render_public_home(uid)
    await send_central(s, cid, home_text, home_keyboard)


async def handle_public_callback(s, cq):
    uid = int((cq.get("from") or {}).get("id", -1))
    cq_id = cq.get("id")
    msg = cq.get("message") or {}
    cid = int((msg.get("chat") or {}).get("id", 0))
    mid = msg.get("message_id")
    data = cq.get("data") or ""
    if not cid or not mid:
        await answer_cq(s, cq_id)
        return

    if data == "p:verify":
        mark_public_verified(uid)
        await answer_cq(s, cq_id, "✅ عضویت تایید شد.")
        text, keyboard = render_public_home(uid)
        await edit_central(s, cid, mid, text, keyboard)
        return

    if uid not in PUBLIC_VERIFIED:
        await answer_cq(s, cq_id, "⛔ اول باید عضوِ کانال‌ها بشی.", show_alert=True)
        text, keyboard = render_public_join_gate()
        await edit_central(s, cid, mid, text, keyboard)
        return

    if data == "p:home":
        PUBLIC_PENDING.pop(uid, None)
        text, keyboard = render_public_home(uid)
        await edit_central(s, cid, mid, text, keyboard)
        await answer_cq(s, cq_id)
        return

    if data == "p:getlink":
        _, existing_reg = public_registration_for_uid(uid)
        if existing_reg:
            await answer_cq(s, cq_id, "⛔ شما همین الان یک گروهِ ثبت‌شده داری. اول اونو حذف کن.", show_alert=True)
            return
        PUBLIC_PENDING[uid] = "awaiting_link"
        text, keyboard = render_public_getlink_prompt()
        await edit_central(s, cid, mid, text, keyboard)
        await answer_cq(s, cq_id)
        return

    if data == "noop":
        await answer_cq(s, cq_id)
        return

    if data in ("p:mypanel", "p:mypanel:r") or data.startswith("p:refresh:"):
        # p:refresh:<chat_id> قدیمی هم همین‌جا میاد؛ chat_id داخلِ دکمه *نادیده* گرفته
        # می‌شه و فقط گروهِ ثبت‌شده‌ی خودِ کاربر (بعد از چکِ ادمین بودن) استفاده می‌شه.
        PUBLIC_PENDING.pop(uid, None)
        is_refresh = data != "p:mypanel"
        chat_id, reg = await resolve_panel_group(s, cq_id, cid, mid, uid, fresh=is_refresh)
        if not reg:
            return
        # دکمه‌ی «🔄 بروزرسانی» همیشه از خودِ سلف می‌پرسه؛
        # بازگشت از منوهای دیگه از کشِ چند‌ثانیه‌ای فوری جواب می‌گیره.
        info = await fetch_group_info(
            chat_id, iid=reg.get("iid"),
            max_age=0 if is_refresh else GROUP_INFO_TTL,
        )
        if not info:
            await answer_cq(s, cq_id, "⏳ سلف این گروه الان جواب نمی‌ده (شاید آفلاینه)، کمی بعد دوباره امتحان کن.", show_alert=True)
            return
        rank, total_groups = await _compute_rank(info.get("total_messages", 0))
        await answer_cq(s, cq_id, "🔄 بروزرسانی شد." if data.startswith("p:refresh:") else None)
        await edit_central(s, cid, mid, render_group_home_text(info, rank, total_groups, self_label(reg.get("iid"))), render_group_home_kb())
        return

    if data == "p:lockmenu":
        chat_id, reg = await resolve_panel_group(s, cq_id, cid, mid, uid)
        if not reg:
            return
        info = await fetch_group_info(chat_id, iid=reg.get("iid"), max_age=GROUP_INFO_TTL)
        if not info:
            await answer_cq(s, cq_id, "⏳ سلف الان جواب نمی‌ده.", show_alert=True)
            return
        await answer_cq(s, cq_id)
        await edit_central(s, cid, mid, render_locks_text(info), render_locks_kb(info.get("locks") or {}))
        return

    if data.startswith("p:lk:"):
        key = data.split(":", 2)[2]
        chat_id, reg = await resolve_panel_group(s, cq_id, cid, mid, uid)
        if not reg:
            return
        result = await run_panel_group_cmd(reg.get("iid"), "toggle_lock", chat_id, {"key": key})
        if not result or not result.get("ok"):
            await answer_cq(s, cq_id, f"❌ خطا: {(result or {}).get('error') or 'نامشخص'}", show_alert=True)
            return
        newly_locked = (result.get("locks") or {}).get(key)
        await answer_cq(s, cq_id, "🔒 قفل شد." if newly_locked else "🔓 باز شد.")
        await edit_central(s, cid, mid, render_locks_text(result), render_locks_kb(result.get("locks") or {}))
        return

    if data == "p:actmenu" or data.startswith("p:act:"):
        period = data.split(":", 2)[2] if data.startswith("p:act:") else "today"
        chat_id, reg = await resolve_panel_group(s, cq_id, cid, mid, uid)
        if not reg:
            return
        leaderboard = await run_panel_group_cmd(reg.get("iid"), "activity", chat_id, {"period": period})
        if not leaderboard or not leaderboard.get("ok"):
            await answer_cq(s, cq_id, f"❌ خطا: {(leaderboard or {}).get('error') or 'نامشخص'}", show_alert=True)
            return
        await answer_cq(s, cq_id)
        await edit_central(s, cid, mid, render_activity_text(leaderboard), render_activity_kb(period))
        return

    if data == "p:settings":
        chat_id, reg = await resolve_panel_group(s, cq_id, cid, mid, uid)
        if not reg:
            return
        info = await fetch_group_info(chat_id, iid=reg.get("iid"), max_age=GROUP_INFO_TTL)
        if not info:
            await answer_cq(s, cq_id, "⏳ سلف الان جواب نمی‌ده.", show_alert=True)
            return
        await answer_cq(s, cq_id)
        await edit_central(s, cid, mid, render_settings_text(info), render_settings_kb(info))
        return

    if data.startswith("p:warn:") or data == "p:silent:toggle":
        chat_id, reg = await resolve_panel_group(s, cq_id, cid, mid, uid)
        if not reg:
            return
        if data.startswith("p:warn:"):
            try:
                action, payload = "warn_delta", {"delta": int(data.split(":", 2)[2])}
            except ValueError:
                await answer_cq(s, cq_id)
                return
        else:
            action, payload = "toggle_silent", {}
        info = await run_panel_group_cmd(reg.get("iid"), action, chat_id, payload)
        if not info or not info.get("ok"):
            await answer_cq(s, cq_id, f"❌ خطا: {(info or {}).get('error') or 'نامشخص'}", show_alert=True)
            return
        await answer_cq(s, cq_id, "✅ اعمال شد.")
        await edit_central(s, cid, mid, render_settings_text(info), render_settings_kb(info))
        return

    if data == "p:delgroup":
        # حذفِ گروه (خروجِ ربات) حساس‌ترین عمله؛ چکِ ادمین بودن همیشه تازه انجام می‌شه.
        chat_id, reg = await resolve_panel_group(s, cq_id, cid, mid, uid, fresh=True)
        if not reg:
            return
        text, keyboard = render_public_delgroup_confirm(reg.get("group_name") or "گروهم", self_label(reg.get("iid")))
        await edit_central(s, cid, mid, text, keyboard)
        await answer_cq(s, cq_id)
        return

    if data == "p:delgroup_yes":
        # صفحه‌ی تاییدِ حذف همین چند ثانیه پیش چکِ تازه‌ی ادمین‌بودن رو انجام داده؛
        # فقط اگه از اون بیش از PANEL_AUTH_TTL گذشته دوباره تازه چک می‌کنیم.
        _c0, _r0 = public_registration_for_uid(uid)
        _recent = bool(_r0) and (time.time() - PANEL_AUTH_CACHE.get((str(_c0), int(uid)), 0)) < PANEL_AUTH_TTL
        chat_id, reg = await resolve_panel_group(s, cq_id, cid, mid, uid, fresh=not _recent)
        if not reg:
            return
        await answer_cq(s, cq_id)
        label = self_label(reg.get("iid"))
        gname = reg.get("group_name") or chat_id
        await edit_central(s, cid, mid, f"🔄 در حال خارج کردنِ ربات «{label}» از گروه...")
        result = await run_leave_group(reg.get("iid"), chat_id)
        if not result.get("ok"):
            await edit_central(
                s, cid, mid,
                f"❌ خروج ربات «{label}» از گروه «{gname}» ناموفق بود: {result.get('error') or 'نامشخص'}\n\nدوباره امتحان کن یا با پشتیبانی در تماس باش.",
                {"inline_keyboard": [[btn("🔁 تلاشِ دوباره", "p:delgroup_yes")], [btn("⬅️ بازگشت", "p:home")]]},
            )
            return
        unregister_public_group(chat_id)
        text, keyboard = render_public_home(uid)
        if result.get("already_left"):
            done = (f"ℹ️ ربات «{label}» از قبل از گروه «{gname}» خارج شده بود (بن/اخراج یا لفت)؛ "
                    "ثبتِ گروه حذف شد و کاری لازم نبود.\n\n")
        else:
            done = f"✅ ربات «{label}» از گروه «{gname}» خارج شد و ثبتش حذف شد.\n\n"
        await edit_central(s, cid, mid, done + text, keyboard)
        return

    if data == "p:buypro":
        text, keyboard = render_public_buypro()
        await edit_central(s, cid, mid, text, keyboard)
        await answer_cq(s, cq_id)
        return

    if data == "p:support":
        text, keyboard = render_public_support()
        await edit_central(s, cid, mid, text, keyboard)
        await answer_cq(s, cq_id)
        return

    if data == "p:help":
        text, keyboard = render_public_help()
        await edit_central(s, cid, mid, text, keyboard)
        await answer_cq(s, cq_id)
        return

    await answer_cq(s, cq_id)


# ============================================================
# مدیریت پیام‌های متنی
# ============================================================

async def handle_message(s, msg):
    global CENTRAL_SESSION
    CENTRAL_SESSION = s
    if not is_owner(msg):
        await handle_public_message(s, msg)
        return
    cid = int((msg.get("chat") or {}).get("id", 0))
    text = (msg.get("text") or "").strip()
    if not cid:
        return

    pending = PENDING.get(cid)
    if pending:
        stage = pending.get("stage")
        iid = pending.get("iid")

        if text in ("/cancel", "لغو", "❌ لغو"):
            home_text, home_keyboard = await do_cancel(cid)
            await send_central(s, cid, "❌ عملیات لغو شد.\n\n" + home_text, home_keyboard)
            return

        if stage == "name":
            if not text:
                await send_central(s, cid, "🏷 یک نام برای سلف بفرست:", CANCEL_KB)
                return
            STATE["instances"][iid]["name"] = text[:80]
            write_instance_config(iid)
            save_state()
            PENDING[cid] = {"iid": iid, "stage": "phone"}
            await send_central(s, cid, "📱 شماره سلف را با کد کشور بفرست (مثال: +98912xxxxxxx):", CANCEL_KB)
            return

        if stage == "phone":
            phone = normalize_digits(text).replace(" ", "")
            if not re.fullmatch(r"\+?\d{8,15}", phone):
                await send_central(s, cid, "❌ شماره معتبر نیست. دوباره بفرست (مثال: +98912xxxxxxx):", CANCEL_KB)
                return
            STATE["instances"][iid]["pending_phone"] = phone
            PENDING[cid] = {"iid": iid, "stage": "starting_login"}
            save_state()
            asyncio.create_task(login_flow(iid, cid, phone))
            return

        if stage == "starting_login":
            await send_central(s, cid, "⏳ هنوز در حال آماده‌کردن ورود هستم؛ لطفاً صبر کن.", CANCEL_KB)
            return

        if stage == "otp":
            code = normalize_digits(text).replace(" ", "")
            if not re.fullmatch(r"\d{4,8}", code):
                await send_central(s, cid, "❌ فقط کد عددی ۴ تا ۸ رقمی را بفرست:", CANCEL_KB)
                return
            path = instance_dir(iid) / "otp_code.txt"
            try:
                path.write_text(code, encoding="utf-8")
            except Exception as e:
                await send_central(s, cid, f"❌ ذخیره OTP ناموفق بود: {e}", CANCEL_KB)
                return
            PENDING[cid] = {"iid": iid, "stage": "login_wait"}
            await send_central(s, cid, "⏳ کد دریافت شد؛ دارم ورود واقعی سلف را بررسی می‌کنم...")
            asyncio.create_task(wait_login_result(iid, cid))
            return

        if stage == "password":
            if not text:
                await send_central(s, cid, "🔐 رمز دومرحله‌ای را بفرست:", CANCEL_KB)
                return
            if not send_child_input(iid, text):
                await send_central(s, cid, "❌ نتوانستم رمز را به پروسه سلف بدهم.", home_kb())
                PENDING.pop(cid, None)
                return
            PENDING[cid] = {"iid": iid, "stage": "login_wait"}
            await send_central(s, cid, "⏳ رمز دریافت شد؛ در حال تکمیل ورود...")
            asyncio.create_task(wait_login_result(iid, cid))
            return

        if stage == "login_wait":
            await send_central(s, cid, "⏳ هنوز ورود در حال انجام است؛ لطفاً پیام دیگری نفرست.")
            return

        if stage == "broadcast_text":
            if not text:
                await send_central(s, cid, "📢 متن را بفرست:", CANCEL_KB)
                return
            targets = [iid2 for iid2 in STATE["instances"] if is_running(iid2)]
            total_g = sum(group_count_of(STATE["instances"][t]) for t in targets)
            PENDING[cid] = {"iid": None, "stage": "broadcast_confirm", "text": text, "targets": targets}
            preview = (
                "📢 پیش‌نمایش پیام همگانی:\n\n"
                f"{text}\n\n"
                f"🎯 مقصد: {len(targets)} سلف فعال (مجموعاً حدود {total_g} گروه)\n\n"
                "آیا همین الان ارسال بشه؟"
            )
            await send_central(s, cid, preview, kb(
                [btn("✅ تایید و ارسال", "bc:confirm")],
                [btn("❌ لغو", "m:cancel")],
            ))
            return

        if stage == "broadcast_confirm":
            await send_central(s, cid, "لطفاً از دکمه‌های بالای پیام قبلی استفاده کن (تایید/لغو).")
            return

        if stage == "update_file":
            doc = msg.get("document")
            if not doc:
                await send_central(
                    s, cid,
                    "📎 لطفاً فایل کد رو به‌صورت داکیومنت (فایل ضمیمه) بفرست، نه متن ساده.",
                    CANCEL_KB,
                )
                return
            filename = safe_update_filename(doc.get("file_name"))
            if filename in UPDATE_BLOCKED_FILES:
                await send_central(
                    s, cid,
                    f"⛔ فایل «{filename}» رو نمی‌شه از این مسیر جایگزین کرد (این فایل به‌صورت خودکار "
                    "توسط پنل مدیریت می‌شه یا شامل داده‌ی حساس سلف است).\n"
                    "یک نام دیگه (مثلاً main.py) برای فایلت انتخاب کن یا لغو کن.",
                    CANCEL_KB,
                )
                return

            targets = pending.get("targets")
            target_iids = list(STATE["instances"].keys()) if targets == "all" else [
                t for t in (targets or []) if t in STATE["instances"]
            ]
            if not target_iids:
                await send_central(s, cid, "❌ سلف مقصد دیگه وجود نداره.", home_kb())
                PENDING.pop(cid, None)
                return

            file_id = doc.get("file_id")
            await send_central(s, cid, f"⏳ در حال دانلود «{filename}»...")
            try:
                content = await download_bot_file(s, file_id)
            except Exception as e:
                await send_central(s, cid, f"❌ دانلود فایل ناموفق بود: {e}", home_kb())
                PENDING.pop(cid, None)
                return

            updated_existing, updated_new, restarted, failed = await apply_code_update(target_iids, filename, content)
            PENDING.pop(cid, None)

            lines = [f"✅ فایل «{filename}» بروزرسانی شد."]
            if updated_existing:
                lines.append(f"🔁 جایگزین شد روی: {', '.join(n for _, n in updated_existing)}")
            if updated_new:
                lines.append(f"🆕 اضافه شد به: {', '.join(n for _, n in updated_new)}")
            if restarted:
                lines.append(f"🔄 ری‌استارت خودکار شد: {', '.join(n for _, n in restarted)}")
            if failed:
                lines.append(f"⚠️ نوشتن فایل ناموفق بود روی: {', '.join(n for _, n in failed)}")
            await send_central(s, cid, "\n".join(lines), home_kb())
            return

    if text in ("/start", "/panel", "پنل", "🏠 پنل", "منو", "🏠 منو"):
        home_text, home_keyboard = render_home()
        await send_central(s, cid, home_text, home_keyboard)
        return

    m = re.match(r"^/(startbot|stopbot|restartbot)\s+([A-Za-z0-9]+)$", text)
    if m:
        action, iid = m.groups()
        if iid not in STATE["instances"]:
            await send_central(s, cid, "❌ سلف پیدا نشد.")
            return
        if action == "startbot":
            await start_instance(iid)
            note = "▶️ سلف اجرا شد."
        elif action == "stopbot":
            ok = await stop_instance(iid)
            note = "⏹ سلف متوقف شد." if ok else "⚠️ پروسه‌ی قبلی متوقف نشد. دوباره امتحان کن یا لاگ رو چک کن."
        else:
            stopped_cleanly, _ = await restart_instance(iid)
            if stopped_cleanly:
                note = "🔄 سلف ری‌استارت شد."
            else:
                note = "⚠️ پروسه‌ی قبلی کیل نشد؛ برای جلوگیری از دوبار جواب دادنِ ربات، عمداً پروسه‌ی جدیدی اجرا نکردم. دوباره امتحان کن یا لاگ رو چک کن."
        detail_text, detail_kb = render_detail(iid)
        await send_central(s, cid, note + "\n\n" + detail_text, detail_kb)
        return

    home_text, home_keyboard = render_home()
    await send_central(s, cid, home_text, home_keyboard)


# ============================================================
# مدیریت callback query ها (دکمه‌های شیشه‌ای)
# ============================================================

async def handle_callback_query(s, cq):
    global CENTRAL_SESSION
    CENTRAL_SESSION = s
    frm = cq.get("from") or {}
    uid = int(frm.get("id", -1))
    cq_id = cq.get("id")
    msg = cq.get("message") or {}
    cid = int((msg.get("chat") or {}).get("id", 0))
    mid = msg.get("message_id")
    data = cq.get("data") or ""

    if uid not in OWNERS:
        data = cq.get("data") or ""
        if data.startswith("p:"):
            await handle_public_callback(s, cq)
        else:
            await answer_cq(s, cq_id, "⛔ دسترسی نداری.", show_alert=True)
        return
    if not cid or not mid:
        await answer_cq(s, cq_id)
        return

    if data == "noop":
        await answer_cq(s, cq_id)
        return

    parts = data.split(":")
    head = parts[0] if parts else ""

    if head == "m":
        action = parts[1] if len(parts) > 1 else ""
        if action == "home":
            text, keyboard = render_home()
            await edit_central(s, cid, mid, text, keyboard)
            await answer_cq(s, cq_id)
        elif action == "list":
            page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            text, keyboard = render_list(page)
            await edit_central(s, cid, mid, text, keyboard)
            await answer_cq(s, cq_id)
        elif action == "stats":
            text, keyboard = render_stats()
            await edit_central(s, cid, mid, text, keyboard)
            await answer_cq(s, cq_id)
        elif action == "groups":
            text, keyboard = render_groups()
            await edit_central(s, cid, mid, text, keyboard)
            await answer_cq(s, cq_id)
        elif action == "addself":
            iid = create_instance(f"Self {len(STATE['instances']) + 1}")
            PENDING[cid] = {"iid": iid, "stage": "name"}
            await answer_cq(s, cq_id)
            await send_central(s, cid, "🏷 یک نام برای این سلف بفرست:", CANCEL_KB)
        elif action == "update":
            text, keyboard = render_update_menu()
            await edit_central(s, cid, mid, text, keyboard)
            await answer_cq(s, cq_id)
        elif action == "bcast":
            PENDING[cid] = {"iid": None, "stage": "broadcast_text"}
            await answer_cq(s, cq_id)
            await send_central(s, cid, "📢 متن پیامی که می‌خوای به همه‌ی سلف‌های فعال ارسال بشه رو بفرست:", CANCEL_KB)
        elif action == "cancel":
            home_text, home_keyboard = await do_cancel(cid)
            await answer_cq(s, cq_id, "لغو شد.")
            await edit_central(s, cid, mid, home_text, home_keyboard)
        else:
            await answer_cq(s, cq_id)
        return

    if head == "i":
        action = parts[1] if len(parts) > 1 else ""
        iid = parts[2] if len(parts) > 2 else None
        if not iid or iid not in STATE["instances"]:
            if action == "delyes":
                text, keyboard = render_list(0)
                await edit_central(s, cid, mid, text, keyboard)
                await answer_cq(s, cq_id)
                return
            await answer_cq(s, cq_id, "❌ این سلف دیگه وجود نداره.", show_alert=True)
            text, keyboard = render_list(0)
            await edit_central(s, cid, mid, text, keyboard)
            return

        if action == "view":
            text, keyboard = render_detail(iid)
            await edit_central(s, cid, mid, text, keyboard)
            await answer_cq(s, cq_id)
        elif action == "start":
            await start_instance(iid)
            await answer_cq(s, cq_id, "▶️ استارت شد.")
            text, keyboard = render_detail(iid)
            await edit_central(s, cid, mid, text, keyboard)
        elif action == "stop":
            ok = await stop_instance(iid)
            await answer_cq(s, cq_id, "⏹ استاپ شد." if ok else "⚠️ پروسه متوقف نشد، دوباره امتحان کن.", show_alert=not ok)
            text, keyboard = render_detail(iid)
            await edit_central(s, cid, mid, text, keyboard)
        elif action == "restart":
            stopped_cleanly, _ = await restart_instance(iid)
            if stopped_cleanly:
                await answer_cq(s, cq_id, "🔄 ری‌استارت شد.")
            else:
                await answer_cq(
                    s, cq_id,
                    "⚠️ پروسه‌ی قبلی کیل نشد؛ برای جلوگیری از دوبار جواب دادنِ ربات، پروسه‌ی جدید اجرا نشد.",
                    show_alert=True,
                )
            text, keyboard = render_detail(iid)
            await edit_central(s, cid, mid, text, keyboard)
        elif action == "delask":
            text, keyboard = render_delete_confirm(iid)
            await edit_central(s, cid, mid, text, keyboard)
            await answer_cq(s, cq_id)
        elif action == "delyes":
            name = STATE["instances"].get(iid, {}).get("name", iid)
            await delete_instance(iid)
            await answer_cq(s, cq_id, f"🗑 «{name}» حذف شد.")
            text, keyboard = render_list(0)
            await edit_central(s, cid, mid, text, keyboard)
        elif action == "joinexempt":
            inst = STATE["instances"][iid]
            inst["join_exempt"] = not inst.get("join_exempt", False)
            save_state()
            msg_txt = "🚫 این سلف از لیستِ جوینِ عمومی معاف شد." if inst["join_exempt"] else "✅ این سلف دوباره به لیستِ جوینِ عمومی اضافه شد."
            await answer_cq(s, cq_id, msg_txt)
            text, keyboard = render_detail(iid)
            await edit_central(s, cid, mid, text, keyboard)
        else:
            await answer_cq(s, cq_id)
        return

    if head == "uc":
        action = parts[1] if len(parts) > 1 else ""
        if action == "target":
            sub = parts[2] if len(parts) > 2 else ""
            if sub == "all":
                PENDING[cid] = {"iid": None, "stage": "update_file", "targets": "all"}
                await answer_cq(s, cq_id)
                await edit_central(
                    s, cid, mid,
                    "📎 فایل کد رو بفرست (مثلاً main.py).\n"
                    "این فایل روی همه‌ی سلف‌ها جایگزین/اضافه می‌شه و سلف‌های روشن خودکار ری‌استارت می‌شن.",
                    CANCEL_KB,
                )
            else:
                page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
                text, keyboard = render_update_pick_list(page)
                await edit_central(s, cid, mid, text, keyboard)
                await answer_cq(s, cq_id)
        elif action == "pick":
            iid = parts[2] if len(parts) > 2 else None
            if not iid or iid not in STATE["instances"]:
                await answer_cq(s, cq_id, "❌ این سلف دیگه وجود نداره.", show_alert=True)
                text, keyboard = render_update_pick_list(0)
                await edit_central(s, cid, mid, text, keyboard)
                return
            name = STATE["instances"][iid].get("name", iid)
            PENDING[cid] = {"iid": iid, "stage": "update_file", "targets": [iid]}
            await answer_cq(s, cq_id)
            await edit_central(
                s, cid, mid,
                f"📎 فایل کد رو برای «{name}» بفرست (مثلاً main.py).\n"
                "اگه از قبل وجود داشته باشه جایگزین می‌شه، وگرنه اضافه می‌شه؛ اگه روشن باشه خودکار ری‌استارت می‌شه.",
                CANCEL_KB,
            )
        else:
            await answer_cq(s, cq_id)
        return

    if head == "bc":
        action = parts[1] if len(parts) > 1 else ""
        if action == "confirm":
            pending = PENDING.get(cid)
            if not pending or pending.get("stage") != "broadcast_confirm":
                await answer_cq(s, cq_id, "این درخواست منقضی شده. دوباره از منو امتحان کن.", show_alert=True)
                return
            bcast_text = pending.get("text", "")
            targets = pending.get("targets", [])
            bid = next(_BROADCAST_ID_COUNTER)
            STATE["broadcasts"][str(bid)] = {
                "id": bid, "command_type": "broadcast", "message": bcast_text,
                "targets": targets, "created_at": time.time(), "acks": [],
            }
            # این یکی برخلافِ run_remote_command عمداً save_state می‌شه: یک
            # عملیاتِ نادر (دستیِ owner) و بالقوه کند (صدها سلف) که اگه
            # manager.py وسطش ری‌استارت بشه نباید کامل گم بشه.
            save_state(now=True)
            for _t in targets:
                _wake_instance(_t)
            PENDING.pop(cid, None)
            await answer_cq(s, cq_id, "📢 ثبت شد.")
            home_text, home_keyboard = render_home()
            await edit_central(
                s, cid, mid,
                f"✅ پیام همگانی برای {len(targets)} سلف فعال ثبت شد و به‌زودی ارسال می‌شه.\n\n" + home_text,
                home_keyboard,
            )
        else:
            await answer_cq(s, cq_id)
        return

    await answer_cq(s, cq_id)


_KEY_LOCKS = {}
_KEY_REFS = {}
_UPDATE_SEM = None


def _update_key(up):
    src_obj = up.get("callback_query") or up.get("message") or {}
    try:
        return int((src_obj.get("from") or {}).get("id"))
    except (TypeError, ValueError):
        return None


async def _process_update(s, up):
    """یک آپدیت رو پردازش می‌کنه. آپدیت‌های کاربرهای مختلف کاملاً موازی اجرا می‌شن
    (قبلاً حلقه‌ی اصلی منتظرِ تمام شدنِ هر هندلر می‌موند، پس یک کاربرِ کند - مثلاً
    منتظرِ جوابِ یک سلف - کلِ پنلِ همه‌ی بقیه رو معطل می‌کرد)؛ آپدیت‌های یک کاربر
    به ترتیب پشتِ هم می‌رن، و کلیکِ تکراریِ کاربرِ عادی وقتی درخواستِ قبلیش هنوز
    در حالِ انجامه، دور ریخته می‌شه (ضدِ اسپمِ دکمه)."""
    key = _update_key(up)
    cq = up.get("callback_query")
    lock = None
    if key is not None:
        lock = _KEY_LOCKS.get(key)
        if lock is None:
            lock = _KEY_LOCKS[key] = asyncio.Lock()
        if cq and key not in OWNERS and lock.locked():
            await answer_cq(s, cq.get("id"), "⏳ درخواستِ قبلیت هنوز در حالِ انجامه، یک لحظه صبر کن.")
            return
        _KEY_REFS[key] = _KEY_REFS.get(key, 0) + 1
    try:
        if lock is not None:
            await lock.acquire()
        try:
            async with _UPDATE_SEM:
                if cq:
                    await handle_callback_query(s, cq)
                elif up.get("message"):
                    await handle_message(s, up.get("message") or {})
        finally:
            if lock is not None:
                lock.release()
    except Exception as e:
        print("update error:", repr(e))
    finally:
        if key is not None:
            left = _KEY_REFS.get(key, 1) - 1
            if left <= 0:
                _KEY_REFS.pop(key, None)
                _KEY_LOCKS.pop(key, None)
            else:
                _KEY_REFS[key] = left


async def central_loop():
    global _UPDATE_SEM
    _UPDATE_SEM = asyncio.Semaphore(MAX_CONCURRENT_UPDATES)
    offset = 0
    connector = TCPConnector(limit=300, ttl_dns_cache=300, keepalive_timeout=60)
    async with ClientSession(connector=connector) as s:
        me = await bot_call(s, "getMe")
        print("Central bot:", me.get("username") or me.get("first_name"))
        while True:
            try:
                updates = await bot_call(s, "getUpdates", {
                    "offset": offset, "timeout": 25,
                    "allowed_updates": ["message", "callback_query"],
                })
                for up in updates:
                    offset = max(offset, int(up["update_id"]) + 1)
                    spawn(_process_update(s, up))
            except Exception as e:
                print("central error:", e)
                await asyncio.sleep(3)


_SECRET_INDEX = {}


def _iid_by_secret(secret):
    """secret -> iid با جستجوی O(1) (به‌جای پیمایشِ همه‌ی سلف‌ها در هر درخواست)."""
    if not secret:
        return None
    iid = _SECRET_INDEX.get(secret)
    if iid and (STATE["instances"].get(iid) or {}).get("panel_secret") == secret:
        return iid
    _SECRET_INDEX.clear()
    for k, x in STATE["instances"].items():
        sec = x.get("panel_secret")
        if sec:
            _SECRET_INDEX[sec] = k
    return _SECRET_INDEX.get(secret)


async def api_report(request):
    """گزارش دوره‌ای از خودِ سلف - فقط روی 127.0.0.1 (محلی) می‌رسه، چون
    write_instance_config برای هر سلف PANEL_API_URL رو به همین آدرس محلی ست
    می‌کنه؛ نیازی به هیچ سرور/IP عمومی بیرونی نیست. تصمیم آنلاین/آفلاین بودن
    توی UI بر مبنای پروسه‌ی محلی گرفته می‌شه (display_status)، نه صرفاً همین
    گزارش؛ این گزارش فقط اطلاعات تکمیلی (گروه‌ها/آیدی/روزهای فعالیت) و مهر
    زمانی آخرین اتصال واقعی رو می‌ده."""
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False}, status=400)
    secret = data.get("secret")
    iid = _iid_by_secret(secret)
    if not iid:
        return web.json_response({"ok": False}, status=401)
    x = STATE["instances"][iid]

    if data.get("event") == "offline":
        # گزارش خاموشیِ تمیز (atexit توی خود سلف). پروسه‌ی محلی هم به‌زودی
        # توسط reap_loop تشخیص داده می‌شه، ولی همین‌جا هم فوراً وضعیت رو
        # به‌روز می‌کنیم تا پنل بلافاصله واکنش نشون بده.
        x["status"] = "stopped"
        x["last_seen"] = time.time()
        save_state()
        return web.json_response({"ok": True})

    x["status"] = data.get("status", "online")
    x["account_id"] = data.get("account_id")
    x["panel_bot_id"] = data.get("bot_id")
    x["group_count"] = data.get("group_count", 0)
    x["groups"] = {str(g.get("group_id")): g for g in data.get("groups", [])}
    # یوزرنیمِ خودِ سلف - برای ساختنِ لینکِ splus.ir/<username> توی بخشِ عمومی.
    # اگه یک گزارش موقتاً این کلید رو نداشت (مثلاً نسخه‌ی قدیمی‌تر main.py که
    # هنوز آپدیت نشده)، مقدار قبلاً ذخیره‌شده رو خالی نمی‌کنیم.
    x["username"] = data.get("username") or x.get("username")
    x["last_seen"] = time.time()
    x["days_active"] = data.get("days_active")
    save_state()
    return web.json_response({"ok": True})


########################################################################
# رجیستری سراسری گروه‌ها (جایگزینِ داخلیِ group_registry_server.py)
#
# هر سلف (main.py) هر GROUP_REGISTRY_SYNC_INTERVAL_SECONDS یک‌بار حضورش رو
# توی هر گروهی که عضوشه اعلام می‌کنه (announce)، قبل از join هم یک‌بار چک
# می‌کنه (check)، و وقتی خودش لفت می‌ده یا بن/کیک می‌شه به رجیستری هم خبر
# می‌ده (leave). خودِ تصمیمِ «کدوم بمونه، کدوم لفت بده» توی main.py گرفته
# می‌شه (بر اساس کوچیک‌ترین account_id)؛ اینجا فقط منبع مشترک truth رو نگه
# می‌داریم.
#
# با هزاران گروه، این جدول توی یک فایل SQLite جداست (نه توی state.json)، چون
# state.json کامل هر بار روی دیسک بازنویسی می‌شه - برای هزاران ردیف که هر
# چند دقیقه به‌روز می‌شن این خیلی کند و پرمصرف می‌شد.
########################################################################

GROUP_REGISTRY_DB_PATH = DATA / "group_registry.db"
STALE_GROUP_PRESENCE_SECONDS = 72 * 3600  # هماهنگ با STALE_AFTER_HOURS قدیمی
_REGISTRY_DB_LOCK = threading.Lock()
_last_registry_prune = 0.0


def _registry_conn():
    conn = sqlite3.connect(GROUP_REGISTRY_DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def _registry_init_db():
    with _REGISTRY_DB_LOCK, contextlib.closing(_registry_conn()) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS group_presence (
                chat_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                bot_label TEXT,
                group_name TEXT,
                project TEXT,
                last_seen REAL NOT NULL,
                PRIMARY KEY (chat_id, account_id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_group_presence_chat ON group_presence(chat_id)")
        conn.commit()
        # مهاجرتِ یک‌باره از نسخه‌ی قدیمی‌ای که هنوز توی state.json بود (اگه
        # این manager.py قبلاً یک‌بار با نسخه‌ی JSON اجرا شده باشه).
        if _LEGACY_GROUP_PRESENCE:
            now = time.time()
            for chat_id, members in _LEGACY_GROUP_PRESENCE.items():
                for account_id, m in members.items():
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO group_presence
                            (chat_id, account_id, bot_label, group_name, project, last_seen)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(chat_id), str(account_id),
                            m.get("bot_label"), m.get("group_name"), m.get("project"),
                            m.get("last_seen", now),
                        ),
                    )
            conn.commit()


_registry_init_db()


def _registry_authorized(data):
    return isinstance(data, dict) and data.get("secret") == STATE["registry_secret"]


def _prune_registry_if_due(conn):
    """قبلاً (توی نسخه‌ی مستقل) این prune روی هر /check و /announce اجرا
    می‌شد؛ با هزاران گروه یعنی روی *هر* درخواستِ فقط-خواندنی هم یک DELETE
    سنگین می‌زد. الان فقط هر یک ساعت یک‌بار واقعاً prune می‌کنیم."""
    global _last_registry_prune
    now = time.time()
    if now - _last_registry_prune < 3600:
        return
    cutoff = now - STALE_GROUP_PRESENCE_SECONDS
    conn.execute("DELETE FROM group_presence WHERE last_seen < ?", (cutoff,))
    _last_registry_prune = now


def _registry_roster_sync(chat_id, prune=False):
    with _REGISTRY_DB_LOCK, contextlib.closing(_registry_conn()) as conn:
        if prune:
            _prune_registry_if_due(conn)
            conn.commit()
        rows = conn.execute(
            "SELECT account_id, bot_label, group_name, project FROM group_presence WHERE chat_id = ?",
            (str(chat_id),),
        ).fetchall()
    return [
        {
            "account_id": int(row["account_id"]) if row["account_id"].lstrip("-").isdigit() else row["account_id"],
            "bot_label": row["bot_label"],
            "group_name": row["group_name"],
            "project": row["project"],
        }
        for row in rows
    ]


def _registry_announce_sync(chat_id, account_id, bot_label, group_name, project):
    with _REGISTRY_DB_LOCK, contextlib.closing(_registry_conn()) as conn:
        _prune_registry_if_due(conn)
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
            (str(chat_id), str(account_id), bot_label, group_name, project, time.time()),
        )
        conn.commit()
        rows = conn.execute(
            "SELECT account_id, bot_label, group_name, project FROM group_presence WHERE chat_id = ?",
            (str(chat_id),),
        ).fetchall()
    return [
        {
            "account_id": int(row["account_id"]) if row["account_id"].lstrip("-").isdigit() else row["account_id"],
            "bot_label": row["bot_label"],
            "group_name": row["group_name"],
            "project": row["project"],
        }
        for row in rows
    ]


def _registry_leave_sync(chat_id, account_id):
    with _REGISTRY_DB_LOCK, contextlib.closing(_registry_conn()) as conn:
        conn.execute(
            "DELETE FROM group_presence WHERE chat_id = ? AND account_id = ?",
            (str(chat_id), str(account_id)),
        )
        conn.commit()


def _registry_mine_sync(account_id):
    """همه‌ی chat_id هایی که رجیستری فعلاً برای این account_id ثبت کرده رو
    برمی‌گردونه. برای self-reconciliation استفاده می‌شه: هر ربات، لیستِ
    دیالوگ‌های واقعیِ خودش (که get_dialogs همیشه درست می‌ده) رو با این لیست
    مقایسه می‌کنه و هر گروهی که اینجا هست ولی دیگه دیالوگِ واقعیِ اون نیست
    (چون بن/کیک شده - حتی اگه موقع بن آفلاین بوده و ایونتش رو نگرفته) رو خودش
    از رجیستری پاک می‌کنه، بدون نیازِ به get_participants (که روی این پلتفرم
    قابل‌اعتماد نیست، به‌گفته‌ی خودِ main.py)."""
    with _REGISTRY_DB_LOCK, contextlib.closing(_registry_conn()) as conn:
        rows = conn.execute(
            "SELECT chat_id FROM group_presence WHERE account_id = ?",
            (str(account_id),),
        ).fetchall()
    return [row["chat_id"] for row in rows]


async def api_registry_check(request):
    try:
        data = await request.json()
    except Exception:
        data = {}
    if not _registry_authorized(data):
        return web.json_response({"error": "unauthorized"}, status=401)
    chat_id = data.get("chat_id")
    if chat_id is None:
        return web.json_response({"error": "chat_id required"}, status=400)
    # sqlite3 sync است؛ برای اینکه با هزاران گروه event loop رو بلاک نکنه، توی
    # یک ترد جدا اجرا می‌شه.
    members = await asyncio.to_thread(_registry_roster_sync, chat_id, True)
    return web.json_response({"chat_id": chat_id, "members": members})


async def api_registry_announce(request):
    try:
        data = await request.json()
    except Exception:
        data = {}
    if not _registry_authorized(data):
        return web.json_response({"error": "unauthorized"}, status=401)
    chat_id = data.get("chat_id")
    account_id = data.get("account_id")
    if chat_id is None or account_id is None:
        return web.json_response({"error": "chat_id and account_id required"}, status=400)
    members = await asyncio.to_thread(
        _registry_announce_sync, chat_id, account_id,
        data.get("bot_label"), data.get("group_name"), data.get("project"),
    )
    return web.json_response({"chat_id": chat_id, "members": members})


async def api_registry_leave(request):
    try:
        data = await request.json()
    except Exception:
        data = {}
    if not _registry_authorized(data):
        return web.json_response({"error": "unauthorized"}, status=401)
    chat_id = data.get("chat_id")
    account_id = data.get("account_id")
    if chat_id is None or account_id is None:
        return web.json_response({"error": "chat_id and account_id required"}, status=400)
    await asyncio.to_thread(_registry_leave_sync, chat_id, account_id)
    return web.json_response({"ok": True})


async def api_registry_mine(request):
    try:
        data = await request.json()
    except Exception:
        data = {}
    if not _registry_authorized(data):
        return web.json_response({"error": "unauthorized"}, status=401)
    account_id = data.get("account_id")
    if account_id is None:
        return web.json_response({"error": "account_id required"}, status=400)
    chat_ids = await asyncio.to_thread(_registry_mine_sync, account_id)
    return web.json_response({"account_id": account_id, "chat_ids": chat_ids})


def _next_command_for(iid):
    best = None
    now = time.time()
    for b in STATE.get("broadcasts", {}).values():
        if iid not in b.get("targets", []) or iid in b.get("acks", []):
            continue
        if b.get("command_type") in CONCURRENT_CMD_TYPES:
            sent_at = (b.get("delivered") or {}).get(iid)
            if sent_at is not None and now - sent_at < REDELIVER_AFTER_SECONDS:
                continue   # قبلاً به سلف تحویل داده شده و داره اجرا می‌شه
        if best is None or b.get("created_at", 0) < best.get("created_at", 0):
            best = b
    return best


async def api_broadcast(request):
    secret = request.query.get("secret")
    bot_id = request.query.get("bot_id")
    iid = _iid_by_secret(secret)
    if not iid or (STATE["instances"].get(iid) or {}).get("panel_bot_id") != bot_id:
        return web.json_response({"ok": False}, status=401)
    # قدیمی‌ترین دستورِ منتظرِ همین سلف. اگه چیزی نبود، به‌جای جوابِ خالیِ فوری،
    # درخواست تا LONGPOLL_SECONDS نگه داشته می‌شه و همین که دستورِ تازه‌ای ثبت بشه
    # (run_remote_command / ارسالِ همگانی) همون لحظه جواب می‌ده - یعنی به‌جای
    # ۰ تا ۳ ثانیه انتظار برای هر کلیکِ کاربر، تقریباً هیچ.
    b = _next_command_for(iid)
    if b is None:
        ev = CMD_EVENTS.get(iid)
        if ev is None:
            ev = CMD_EVENTS[iid] = asyncio.Event()
        ev.clear()
        b = _next_command_for(iid)  # بعد از clear دوباره چک می‌کنیم تا سیگنالی از دست نره
        if b is None:
            try:
                await asyncio.wait_for(ev.wait(), timeout=LONGPOLL_SECONDS)
            except asyncio.TimeoutError:
                pass
            b = _next_command_for(iid)
    if b is None:
        return web.json_response({})
    if b.get("command_type") in CONCURRENT_CMD_TYPES:
        now = time.time()
        b.setdefault("delivered", {})[iid] = now
        payload = {k: v for k, v in b.items() if k not in ("delivered", "deadline")}
        if b.get("deadline"):
            payload["expires_in"] = max(0.0, b["deadline"] - now)
        return web.json_response(payload)
    return web.json_response(b)


async def api_ack(request):
    try:
        data = await request.json()
    except Exception:
        data = {}
    bid_str = str(data.get("broadcast_id", ""))
    b = STATE.get("broadcasts", {}).get(bid_str)
    if b:
        acks = set(b.get("acks", []))
        secret = data.get("secret")
        iid = _iid_by_secret(secret)
        if iid:
            acks.add(iid)
            b["acks"] = sorted(acks)
            # اگه این ack مربوط به یک دستورِ «تستِ جوینِ لینک»/«خروج از
            # گروه»/«دستورِ پنلِ گروهی» بود، نتیجه رو به فیوچرِ منتظرِ
            # run_remote_command می‌رسونیم (اگه هنوز کسی منتظرش نباشه -
            # مثلاً به‌خاطر timeout - بی‌ضرره).
            join_result = data.get("join_result")
            try:
                bid_int = int(b.get("id"))
            except (TypeError, ValueError):
                bid_int = None
            fut = JOIN_RESULT_WAITERS.get(bid_int)
            if fut and not fut.done():
                fut.set_result(join_result if join_result is not None else {"ok": False, "error": "بدون نتیجه از سلف."})
            if set(b.get("targets", [])) <= acks:
                STATE["broadcasts"].pop(bid_str, None)
                # فقط پیامِ همگانیِ owner (بالقوه چندهدفه) رو دیسک نگه
                # می‌داریم؛ دستورهای تکی/پرتکرارِ پنل (run_remote_command)
                # اصلاً save_state صدا نمی‌زنن، پس اینجا هم لازم نیست دوباره
                # برای اونا بنویسیم - فقط وقتی رکورد اصلاً از قبل پرسیست شده
                # بود (یعنی از مسیرِ broadcast/bc:confirm اومده) به‌روزش کن.
                if b.get("command_type") == "broadcast":
                    save_state()
    return web.json_response({"ok": True})


async def reap_loop():
    """هر ۳ ثانیه چک می‌کنه آیا پروسه‌ی هر سلف (چه فرزندِ مستقیم، چه اداپت‌شده)
    واقعاً مرده - این همون مکانیزمیه که تشخیصِ 'خاموش شده' رو سریع می‌کنه و
    دیگه به گزارشِ دوره‌ای/بازدیدِ دستی وابسته نیست."""
    while True:
        changed = False
        for iid, inst in STATE["instances"].items():
            mp = RUNTIME_PROCS.get(iid)
            if mp and mp.poll() is not None:
                RUNTIME_PROCS.pop(iid, None)
                if inst.get("status") != "stopped":
                    inst["pid"] = None
                    inst["pid_started_at"] = None
                    inst["status"] = "stopped"
                    changed = True
        if changed:
            save_state()
        await asyncio.sleep(3)


async def app_main():
    # باید قبل از هر چیزِ دیگه‌ای اجرا بشه: پروسه‌های زنده‌ی باقی‌مونده از
    # اجرای قبلیِ manager.py رو (اگه واقعاً زنده باشن) 'اداپت' می‌کنه، وگرنه
    # اولین استارت/ری‌استارتی که کاربر بزنه یک پروسه‌ی دومِ موازی بالا میاره.
    reconcile_on_boot()
    main_task = asyncio.current_task()
    try:
        # توقفِ تمیزِ سرویس (systemd stop / kill -TERM) هم state رو flush کنه.
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, main_task.cancel)
    except (NotImplementedError, RuntimeError, AttributeError, ValueError):
        pass

    app = web.Application()
    app.router.add_post("/api/report", api_report)
    app.router.add_get("/api/broadcast", api_broadcast)
    app.router.add_post("/api/broadcast/ack", api_ack)
    app.router.add_post("/api/registry/check", api_registry_check)
    app.router.add_post("/api/registry/announce", api_registry_announce)
    app.router.add_post("/api/registry/leave", api_registry_leave)
    app.router.add_post("/api/registry/mine", api_registry_mine)
    app.router.add_get("/health", lambda request: web.json_response({"ok": True}))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()
    print(f"Local panel API: http://{HOST}:{PORT}")
    try:
        await asyncio.gather(central_loop(), reap_loop(), state_flush_loop(), rank_refresh_loop())
    finally:
        try:
            save_state(now=True)
        except Exception as e:
            print("final state save error:", e)


if __name__ == "__main__":
    try:
        asyncio.run(app_main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
