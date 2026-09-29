"""
SPlus Management Bot — Full SplusLib rewrite.

This file intentionally preserves the complete feature set of the supplied
production bot while replacing the old standalone SplusPy dependency with the
vendored SplusLib package shipped beside this file.  The application layer is
kept in one module because many legacy features share the same runtime state;
the transport/session boundary is isolated at the import layer so the bot can
be upgraded without silently dropping handlers or group-management rules.
"""



import asyncio
import contextvars
import collections

# Bootstrap the bundled library before any vendored compatibility imports.
import spluslib  # noqa: F401
import os
import sys
import sqlite3
import re
import logging
import json
import atexit
import signal
from datetime import datetime, timedelta, timezone

# ==================== منطقه‌ی زمانی ایران ====================
# سرور (هاست اشتراکی) روی UTC یا هر تایم‌زون دیگه‌ای اجرا می‌شه، پس datetime.now()
# خام همیشه با ساعت واقعی ایران (UTC+03:30) عقب/جلوعه. همه‌جا که ساعت/تاریخ به
# کاربر نشون داده می‌شه باید از همین IRAN_TZ استفاده بشه، نه از ساعت خام سیستم.
IRAN_TZ = timezone(timedelta(hours=3, minutes=30))
from PIL import Image, ImageDraw, ImageFont, ImageFilter
try:
    import arabic_reshaper
    from bidi.algorithm import get_display as bidi_get_display
except ImportError:
    arabic_reshaper = None
    bidi_get_display = None
from functools import wraps
import time
import tempfile

from spluslib import events
from spluslib._base.client.soroushclient import SoroushClient
from spluslib._base.sessions import StringSession
from spluslib._base.tl import functions, types
from spluslib._base.utils import pack_bot_file_id, resolve_bot_file_id

# ==================== شل کردن آستانه‌ی تشخیص «شکاف» آپدیت ====================
# خودِ splusthon این آستانه رو از استاندارد پروتکل (۰.۵ ثانیه) به ۱۰۰ میلی‌ثانیه
# کاهش داده (نگاه کنید به splusthon/_updates/messagebox.py، POSSIBLE_GAP_TIMEOUT).
# یعنی اگه آپدیت هر کانال فقط ۱۰۰ میلی‌ثانیه دیر برسه یا با آپدیت دیگه‌ای قاطی
# بشه، کتابخانه فکر می‌کنه شکاف افتاده و بلافاصله GetDifference می‌زنه. با
# ۲-۳ هزار کانال فعال، این آستانه‌ی فوق‌فشرده عملاً هر لحظه یه‌جا رد می‌شه؛
# نتیجه‌ش یه سیل مداوم و بی‌وقفه‌ی «Got difference» توی لاگه (ده‌ها درخواست در
# ثانیه، بدون توقف) که هم CPU/شبکه‌ی همون event loop ای که پیام‌های واقعی رو
# پردازش می‌کنه اشغال می‌کنه (پس پیام‌ها دیر می‌رسن)، هم از نظر حجم درخواست به
# سرور سروش، دقیقاً شبیه رفتار غیرعادی/رباتیک به نظر می‌رسه (ریسک محدودیت/بن).
# این مقدار فقط همون‌جا و به‌صورت global-lookup استفاده می‌شه (نه import-by-value
# جای دیگه)، پس از بیرون و بدون دست‌کاری فایل‌های نصب‌شده‌ی کتابخانه قابل تنظیمه.
# عدد رو به ۱.۵ ثانیه بردیم (باز هم تندتر از پیش‌فرض ۰.۵ ثانیه‌ی استاندارد
# پروتکل، ولی خیلی کمتر وسواسی)؛ اگه بازم «Got difference» زیاد دیدید، این عدد
# رو می‌شه حتی بیشتر هم برد.
import spluslib._base._updates.messagebox as _spluspy_messagebox
_spluspy_messagebox.POSSIBLE_GAP_TIMEOUT = 1.5

# ==================== نمایش هشدارهای Flood Wait خود کتابخانه ====================
# خود splusthon وقتی سروش‌پلاس بهش FloodWaitError/SlowModeWaitError بده (یعنی:
# "دارید زیادی سریع درخواست می‌فرستید، N ثانیه صبر کن")، به‌صورت خودکار همون
# مدت رو با asyncio.sleep می‌خوابه و دوباره تلاش می‌کنه - این خودش هیچ‌وقت کرش
# نمی‌کنه و لاگ خطا هم نمی‌ده، فقط از طریق ماژول logging پایتون (نه print) یک
# پیام info می‌ذاره. تا الان اینجا logging اصلاً پیکربندی نشده بود، یعنی این
# خواب‌های چندثانیه‌ای/چنددقیقه‌ای (که دقیقاً با علامت «بعضی دستورات سریع، اکثراً
# با چند ثانیه تاخیر» که گزارش دادی هم‌خونی داره) کاملاً نامرئی بودن - نه توی
# کنسول نه توی هیچ لاگی. این تنظیم همون پیام‌ها رو روی stdout قابل‌مشاهده می‌کنه
# تا بشه فهمید آیا واقعاً همینه یا نه.
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(name)s] %(message)s",
    stream=sys.stdout,
)
logging.getLogger("spluspy").setLevel(logging.INFO)

import aiohttp

from config import (
    SESSION_FILE, SUPER_OWNER_IDS,
    OTP_CODE_FILE, HEARTBEAT_FILE, ACTIVATION_FILE,
    CONNECTION_WATCHDOG_INTERVAL_SECONDS, CONNECTION_WATCHDOG_TIMEOUT_SECONDS,
    RECONNECT_ATTEMPT_TIMEOUT_SECONDS, STALE_MESSAGE_SKIP_SECONDS,
    RUNTIME_CACHE_CLEANUP_INTERVAL_SECONDS, DB_MAINTENANCE_INTERVAL_SECONDS,
    PANEL_API_URL, PANEL_SECRET, PANEL_REPORT_INTERVAL_SECONDS,
    PANEL_BROADCAST_POLL_SECONDS, PANEL_BROADCAST_DELAY_SECONDS,
    PANEL_BOT_ID_FILE, PANEL_BOT_PROJECT, PANEL_LAST_COMMAND_FILE,
    GROUP_REGISTRY_API_URL, GROUP_REGISTRY_SECRET, BOT_DISPLAY_LABEL,
    GROUP_REGISTRY_SYNC_INTERVAL_SECONDS,
)

# مسیر session.txt رو مطلق می‌کنیم (کنار همین main.py) نه نسبی به پوشه‌ای که
# ازش دستور اجرا زده شده. اگه این مطلق نباشه و یه بار ربات از یک پوشه اجرا بشه
# (مثلاً دستی، توی ترمینال) و بار بعد از یه پوشه‌ی دیگه (مثلاً systemd با
# WorkingDirectory متفاوت)، کد فکر می‌کنه هنوز هیچ سشنی نداره و دوباره شماره و
# OTP می‌خواد - در حالی که سشن قبلی سرجاشه، فقط توی پوشه‌ی دیگه‌ای بوده.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SESSION_FILE = os.path.join(BASE_DIR, SESSION_FILE)

# نمایش خوانا از لیست آیدی سازنده‌ها (ممکنه یک یا چند آیدی باشه)
SUPER_OWNER_IDS_DISPLAY = "، ".join(str(x) for x in SUPER_OWNER_IDS)
from database import db

# ==================== پاکسازیِ یک‌باره‌ی کاملِ لیست «ویژه» ====================
# فقط در اولین اجرای بعد از این نسخه، همه‌ی رکوردهای جدول «ویژه» (group_admins)
# برای همه‌ی گروه‌ها پاک می‌شه و بعدش فایل نشانه‌ی .vizhe_wiped_once کنار main.py
# ساخته می‌شه. توی ری‌استارت‌های بعدی چون این فایل هست، دیگه هیچ پاکسازی‌ای انجام
# نمی‌شه. (اگه فایل نشانه رو حذف کنید، دفعه‌ی بعد دوباره یک‌بار پاک می‌کنه.)
_VIZHE_WIPE_MARKER = os.path.join(BASE_DIR, ".vizhe_wiped_once")
if not os.path.exists(_VIZHE_WIPE_MARKER):
    try:
        _wipe_conn = db._get_connection()
        _wipe_cur = _wipe_conn.cursor()
        _wipe_cur.execute("DELETE FROM group_admins")
        _wipe_count = _wipe_cur.rowcount
        _wipe_conn.commit()
        _wipe_conn.close()
        db._group_admins_cache.clear()
        with open(_VIZHE_WIPE_MARKER, "w", encoding="utf-8") as _mf:
            _mf.write(datetime.now(IRAN_TZ).isoformat())
        print(f"🧹 پاکسازیِ یک‌باره‌ی لیست ویژه انجام شد ({_wipe_count} رکورد حذف شد).")
    except Exception as _wipe_err:
        # علامت نمی‌ذاریم تا اجرای بعدی دوباره تلاش کنه.
        print(f"⚠️ پاکسازیِ یک‌باره‌ی لیست ویژه ناموفق بود: {_wipe_err}")

from utils import (
    cache_message, get_cached_sender, get_target_user, get_replied_message_info, has_permission,
    send_welcome, check_anti_spam, get_random_content, get_user_name, mention_user, mention_link, get_username,
    clear_user_cache, clear_chat_name_cache, safe_print, send_external_alert,
    get_recent_sender_message_ids, is_bare_admin_command,
    ensure_activation_recorded, get_days_active, get_activation_date,
    censor_word, trim_runtime_memory, resolve_id_or_username, extract_username, view_tag,
    truncate_display_name,
)
from utils import extract_media_info, cache_media_info, get_replied_media_info
from speaker_api import api_speaker
from fonts_api import api_fonts
from spluslib.call_audio import CallAudioSession

_SPEAKER_TASKS = set()

# ==================== Group Call runtime ====================
# یک سشن صوتی برای هر گروه؛ ایجاد/ورود با دستور «کال» و خروج با «قطع کال».
_ACTIVE_CALLS = {}
_CALL_LOCK = asyncio.Lock()

def _track_background_task(coro):
    task = asyncio.create_task(coro)
    _SPEAKER_TASKS.add(task)
    task.add_done_callback(_SPEAKER_TASKS.discard)
    return task

def _trim_hot_caches():
    limits = {"FILTER_WARN_CACHE": 20000, "WARN_MESSAGE_CACHE": 12000, "GROUP_INFO_LAST_CHECK": 2000, "AUTO_MOD_CACHE": 2000, "WELCOME_CACHE": 2000, "GOODBYE_CACHE": 2000, "PERMISSION_CACHE": 30000, "PERMISSION_CACHE_TIME": 30000, "GROUP_OWNER_CACHE": 2000, "_HASHTAG_GROUP_CACHE": 5000, "_HASHTAG_USER_CACHE": 20000}
    for name, limit in limits.items():
        obj = globals().get(name)
        if isinstance(obj, dict) and len(obj) > limit:
            for key in list(obj)[:len(obj)-limit]: obj.pop(key, None)
        elif isinstance(obj, set) and len(obj) > limit:
            for key in list(obj)[:len(obj)-limit]: obj.discard(key)

def _load_saved_session():
    """همان سشن ذخیره‌شده را از session.txt از همان لحظه ساخت Client لود می‌کند.
    اگر فایل خالی/نامعتبر باشد، یک StringSession خالی ساخته می‌شود تا فقط همان
    بار اول شماره و کد درخواست شود."""
    session_str = ""
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE, "r", encoding="utf-8") as f:
                session_str = f.read().strip()
        except Exception as e:
            safe_print(f"⚠️ خواندن session.txt ناموفق بود: {e}")

    if session_str:
        try:
            safe_print("✅ سشن قبلی لود شد؛ لاگین مجدد لازم نیست.")
            return StringSession(session_str)
        except Exception as e:
            safe_print(f"⚠️ سشن قبلی قابل استفاده نیست: {e}")

    return StringSession()


client = SoroushClient(_load_saved_session())


# ==================== لفت خودکار وقتی دسترسی ارسال پیام ربات بسته شده ====================
# اگه توی یک گروه، ادمین‌ها دسترسیِ ارسال پیامِ مخصوصِ همین اکانت (ربات) رو ببندن،
# هر تلاشی برای send_message توی اون گروه به خطای ChatWriteForbiddenError (یا
# نسخه‌ی مشابهش، UserBannedInChannelError) برمی‌خوره. قبلاً همه‌ی این خطاها همون
# لحظه‌ی رخ دادن، پراکنده توی صدها بلوکِ «except: pass» موجود توی کد silent قورت
# داده می‌شدن؛ نتیجه‌ش این بود که ربات برای همیشه توی همون گروه بی‌فایده می‌موند:
# نه خودش می‌فهمید نه کسی دیگه، فقط هیچ‌وقت هیچ پیامی نمی‌تونست بده.
#
# به‌جای دست‌کاری تک‌تک همون صدها فراخوانی، همینجا یک‌بار خودِ متد send_message
# روی نمونه‌ی client رو wrap می‌کنیم. چون event.respond/event.reply هم داخلاً
# دقیقاً از همین self._client.send_message استفاده می‌کنن (نگاه کنید به
# spluspy/tl/custom/message.py)، این یک wrap برای پوشش کامل کد کافیه.
_CHAT_WRITE_FORBIDDEN_LEAVE_IN_PROGRESS = set()



# لیست کلاس‌های خطایی که واقعاً یعنی «ارسال پیام برای همین اکانت توی این چت بسته
# شده». نسخه‌ی قبلی این فیچر فقط ChatWriteForbiddenError/UserBannedInChannelError
# رو چک می‌کرد، ولی خطای واقعی‌ای که سرور برای «ادمین دسترسی ارسال متن رو مشخصاً
# برای همین ربات بسته» برمی‌گردونه، بیشتر با ChatSendPlainForbiddenError یا
# ChatRestrictedError مطابقت داره - و هیچ‌کدوم توی لیست قبلی نبودن، برای همین
# ربات هیچ‌وقت این ارور رو تشخیص نمی‌داد و لفت نمی‌داد. اینجا لیست رو کامل‌تر
# کردیم تا این حالت‌ها هم پوشش داده بشن.
_CHAT_WRITE_FORBIDDEN_ERROR_NAMES = (
    "ChatWriteForbiddenError",
    "UserBannedInChannelError",
    "ChatSendPlainForbiddenError",
    "ChatRestrictedError",
    "RightForbiddenError",
)


def _is_chat_write_forbidden_error(exc):
    """تشخیص می‌ده که آیا خطای داده‌شده یعنی «دسترسی ارسال پیام توی این گروه،
    مخصوص همین اکانت، بسته شده» یا نه (نه هر خطای دیگه‌ای مثل فلود ویت یا خطای
    شبکه)."""
    if type(exc).__name__ in _CHAT_WRITE_FORBIDDEN_ERROR_NAMES:
        return True
    # محافظ اضافه برای حالتی که نسخه‌ی کتابخانه/سرور همون وضعیت رو با کلاس یا
    # متن متفاوتی گزارش کنه؛ فقط fallback است، نه راه اصلی تشخیص.
    msg = str(exc).lower()
    return any(kw in msg for kw in (
        "write forbidden", "can't write in this chat", "send plain",
        "chat is restricted", "chat_restricted", "right_forbidden",
    ))


async def _leave_group_write_forbidden(chat_id):
    """وقتی مشخص شد ربات دیگه توی این گروه اجازه‌ی ارسال پیام نداره، ازش خارج
    می‌شه (به همون شیوه‌ی _try_leave_chat که جای دیگه‌ی کد هم استفاده می‌شه) و
    اگه رجیستری چند-رباتی فعال باشه، خروجش رو به رجیستری هم اعلام می‌کنه تا اون
    آدرس برای ربات‌های دیگه‌ی همین مجموعه آزاد بشه."""
    if chat_id in _CHAT_WRITE_FORBIDDEN_LEAVE_IN_PROGRESS:
        return
    _CHAT_WRITE_FORBIDDEN_LEAVE_IN_PROGRESS.add(chat_id)
    try:
        try:
            entity = await client.get_entity(chat_id)
        except Exception:
            entity = chat_id
        left_ok, leave_error = await _try_leave_chat(entity, chat_id)
        if left_ok:
            safe_print(f"🚪 دسترسی ارسال پیام توی گروه {chat_id} برای ربات بسته شده بود؛ خودکار از گروه لفت داده شد.")
            if GROUP_REGISTRY_API_URL:
                try:
                    await registry_leave_group(chat_id)
                except Exception:
                    pass
        else:
            safe_print(f"⚠️ دسترسی ارسال پیام توی گروه {chat_id} بسته شده، ولی تلاش برای لفت دادن هم ناموفق بود: {leave_error}")
    finally:
        _CHAT_WRITE_FORBIDDEN_LEAVE_IN_PROGRESS.discard(chat_id)


# ==================== پایش دوره‌ای (fallback) برای همون حالت ====================
# روی همین پلتفرم (سروش)، جای دیگه‌ی همین فایل (join_group_via_link) صراحتاً ثبت
# شده که get_participants همیشه یه لیست خالی برمی‌گردونه - یعنی نمی‌شه صد در صد
# مطمئن بود هر خطای سمت سرور، دقیقاً با همون کلاس/متنی که Telegram اصلی برمی‌گردونه
# به این کتابخانه می‌رسه؛ ممکنه بعضی وقت‌ها send_message اصلاً استثنا نده (سایلنت
# fail). برای همین، علاوه بر تشخیص واکنشی بالا (موقع خودِ ارسال پیام)، یک بررسیِ
# دوره‌ی/پیش‌گیرانه هم اضافه شده: هر از گاهی مستقیماً از سرور می‌پرسیم که آیا حق
# send_messages ربات توی هر گروه بسته شده یا نه (banned_rights.send_messages)،
# مستقل از این‌که اخیراً پیامی فرستاده باشیم یا نه.
WRITE_PERMISSION_CHECK_INTERVAL_SECONDS = 1800  # هر ۳۰ دقیقه


async def _bot_write_restricted_in_chat(dialog_entity):
    """برمی‌گردونه True فقط اگه ربات هنوز عضو این گروهه ولی حق send_messages
    مشخصاً ازش گرفته شده باشه (نه این‌که کلاً بن/اخراج شده باشه - اون یعنی
    view_messages هم بسته‌ست، که یه سناریوی جدا و خارج از هدف این فیچره: اونجا
    ربات دیگه اصلاً پیامی نمی‌بینه که بخواد بفرسته)."""
    try:
        perms = await client.get_permissions(dialog_entity, 'me')
    except Exception:
        return False
    participant = getattr(perms, 'participant', None)
    banned_rights = getattr(participant, 'banned_rights', None)
    if not banned_rights:
        return False
    return bool(getattr(banned_rights, 'send_messages', False)) and not bool(getattr(banned_rights, 'view_messages', False))


async def write_permission_watch_loop():
    """هر WRITE_PERMISSION_CHECK_INTERVAL_SECONDS یک‌بار، همه‌ی گروه‌های عضو رو
    چک می‌کنه و اگه حق ارسال پیام ربات توی یکی‌شون بسته شده باشه، از همون گروه
    خارج می‌شه - این یک فالبکِ پیشگیرانه‌ست، مستقل از تشخیصِ واکنشیِ بالا که موقع
    خودِ send_message انجام می‌شه."""
    await asyncio.sleep(120)
    while True:
        try:
            try:
                dialogs = await client.get_dialogs()
            except Exception as e:
                safe_print(f"[write_permission_watch_loop] خطا در گرفتنِ دیالوگ‌ها: {e}")
                dialogs = []

            for dialog in dialogs:
                if not getattr(dialog, 'is_group', False):
                    continue
                chat_id = dialog.id
                if chat_id in _CHAT_WRITE_FORBIDDEN_LEAVE_IN_PROGRESS:
                    continue
                try:
                    restricted = await _bot_write_restricted_in_chat(dialog.entity)
                except Exception:
                    restricted = False
                if restricted:
                    await _leave_group_write_forbidden(chat_id)
                await asyncio.sleep(0.3)
        except Exception as e:
            safe_print(f"[write_permission_watch_loop] خطا: {e}")
        await asyncio.sleep(WRITE_PERMISSION_CHECK_INTERVAL_SECONDS)


_HASHTAG_CONTEXT = contextvars.ContextVar("hashtag_context", default=None)
_HASHTAG_GROUP_CACHE = {}
_HASHTAG_USER_CACHE = {}
_HASHTAG_CACHE_TTL = 300
_SPEAKER_SEMAPHORE = asyncio.Semaphore(20)

async def _expand_global_hashtags(text, target_entity=None):
    if not isinstance(text, str) or not text:
        return text
    if "#" not in text:
        return text
    ctx = _HASHTAG_CONTEXT.get() or {}
    chat_id = ctx.get("chat_id")
    user_id = ctx.get("user_id")
    if chat_id is None and isinstance(target_entity, int):
        chat_id = target_entity
    now = time.time()
    group_name = ""
    if chat_id is not None:
        cached = _HASHTAG_GROUP_CACHE.get(chat_id)
        if cached and now - cached[0] < _HASHTAG_CACHE_TTL:
            group_name = cached[1]
        else:
            try:
                ent = await client.get_entity(chat_id)
                group_name = (getattr(ent, "title", None) or getattr(ent, "name", None) or "").strip()
                _HASHTAG_GROUP_CACHE[chat_id] = (now, group_name)
            except Exception:
                group_name = cached[1] if cached else ""
    user_name = ""
    user_tag = ""
    if user_id is not None:
        cache_key = (chat_id, user_id)
        cached_user = _HASHTAG_USER_CACHE.get(cache_key)
        if cached_user and now - cached_user[0] < _HASHTAG_CACHE_TTL:
            user_name, user_tag = cached_user[1], cached_user[2]
        else:
            try:
                user_name = await get_user_name(client, user_id, chat_id)
            except Exception:
                user_name = ""
            try:
                username = await get_username(client, user_id)
            except Exception:
                username = None
            try:
                user_tag = mention_link(user_name or str(user_id), user_id, username)
            except Exception:
                user_tag = user_name or str(user_id)
            _HASHTAG_USER_CACHE[cache_key] = (now, user_name, user_tag)
    replacements = {"#اسم_گروه": group_name, "#تگ_کاربر": user_tag, "#اسم_کاربر": user_name, "#زمان": current_time_str(), "#ساعت": current_time_str(), "#تاریخ": jalali_today_str()}
    for key, value in replacements.items():
        if value:
            text = text.replace(key, str(value))
    return text

def _install_write_forbidden_auto_leave(target_client):
    original_send_message = target_client.send_message

    async def _wrapped_send_message(entity, *args, **kwargs):
        try:
            if args and isinstance(args[0], str):
                args = (await _expand_global_hashtags(args[0], entity),) + args[1:]
            elif isinstance(kwargs.get("message"), str):
                kwargs["message"] = await _expand_global_hashtags(kwargs["message"], entity)
            elif isinstance(kwargs.get("text"), str):
                kwargs["text"] = await _expand_global_hashtags(kwargs["text"], entity)
            return await original_send_message(entity, *args, **kwargs)
        except Exception as e:
            if _is_chat_write_forbidden_error(e):
                resolved_chat_id = entity if isinstance(entity, int) else getattr(entity, 'id', None)
                if resolved_chat_id is not None:
                    asyncio.create_task(_leave_group_write_forbidden(resolved_chat_id))
            raise

    target_client.send_message = _wrapped_send_message


_install_write_forbidden_auto_leave(client)


def save_session_to_file():
    """سشن فعلی رو روی دیسک ذخیره می‌کنه. قبلاً این کار *فقط* توی راه‌اندازی اولیه
    انجام می‌شد (خط اول اجرای main). اگه ربات وسط کار مجبور به لاگین دوباره می‌شد
    (توی حلقه‌ی اتصال مجدد پایین همین فایل)، سشن تازه هیچ‌وقت ذخیره نمی‌شد؛ با
    اولین ری‌استارت کامل پروسه، دوباره از سشن قدیمی و باطل استفاده می‌شد و دوباره
    OTP لازم می‌شد. این تابع باید بعد از *هر* client.start() موفق صدا زده بشه."""
    try:
        session_str = client.session.save()
        if session_str:
            with open(SESSION_FILE, "w") as f:
                f.write(session_str)
    except Exception as e:
        safe_print(f"⚠️ ذخیره‌ی سشن ناموفق بود: {e}")


def _prompt_phone():
    """شماره تلفن مالک ربات رو فقط وقتی واقعاً لازم باشه (یعنی هنوز سشن معتبری
    وجود نداره) توی ترمینال می‌پرسه - دقیقاً مثل بنر بات. اگه session.txt معتبر
    باشه، اصلاً صدا زده نمی‌شه و شماره هیچ‌جا (نه توی کد، نه توی کانفیگ) ذخیره
    نمی‌شه."""
    while True:
        phone = input("📱 شماره تلفن اکانت ربات را با کد کشور وارد کن (مثلاً +98912xxxxxxx): ").strip()
        if phone:
            return phone


async def _wait_for_otp_code(timeout_seconds=600):
    """کد تایید (OTP) رو یا از ترمینال (اگه تعاملیه) یا از فایل OTP_CODE_FILE
    می‌گیره - هرکدوم زودتر آماده بشه. برخلاف input() ساده‌ی پیش‌فرض کتابخانه، این
    تابع باقی ربات (بقیه‌ی گروه‌ها، هندلرها، لوپ‌های پس‌زمینه) رو در حین انتظار
    فریز نمی‌کنه، چون input() رو توی یک ترد جدا اجرا می‌کنه (asyncio.to_thread)."""
    async def from_stdin():
        try:
            return await asyncio.to_thread(input, "🔑 کد تایید (OTP) رو وارد کن: ")
        except Exception:
            # ورودی تعاملی در دسترس نیست (مثلاً اجرا زیر systemd/nohup بدون ترمینال)
            await asyncio.sleep(10 ** 9)

    async def from_file():
        while True:
            try:
                if os.path.exists(OTP_CODE_FILE):
                    with open(OTP_CODE_FILE, "r", encoding="utf-8") as f:
                        code = f.read().strip()
                    if code:
                        try:
                            os.remove(OTP_CODE_FILE)
                        except Exception:
                            pass
                        return code
            except Exception:
                pass
            await asyncio.sleep(2)

    task_stdin = asyncio.create_task(from_stdin())
    task_file = asyncio.create_task(from_file())
    done, pending = await asyncio.wait(
        [task_stdin, task_file],
        timeout=timeout_seconds,
        return_when=asyncio.FIRST_COMPLETED
    )
    for t in pending:
        t.cancel()
    if done:
        return list(done)[0].result()
    return ""


async def _otp_code_callback():
    """جایگزین امن‌تر برای ورودی پیش‌فرض input() کتابخانه، وقتی سشن باطل شده و
    نیاز به OTP جدید داریم. اول یه هشدار فوری می‌فرسته (هم توی لاگ، هم از کانال
    خارجی مثل ntfy اگه تنظیم شده باشه - چون خود ربات فعلاً نمی‌تونه پیام بده)،
    بعد بدون فریز کردن بقیه‌ی ربات منتظر کد می‌مونه."""
    await send_external_alert(
        "⚠️ سشن ربات باطل شده و نیاز به کد تایید (OTP) جدید داره!\n"
        f"کد رو یا مستقیم توی ترمینال ربات وارد کن، یا داخل فایل «{OTP_CODE_FILE}» "
        "(کنار فایل‌های ربات، مثلاً از طریق فایل‌منیجر هاست) بنویس و ذخیره کن.\n"
        "تا این کد وارد نشه، ربات نمی‌تونه پیام بفرسته یا بگیره."
    )
    return await _wait_for_otp_code()


async def connection_watchdog_loop():
    """هر چند دقیقه یک درخواست سبک (GetState) با تایم‌اوت می‌فرسته تا مطمئن بشه
    اتصال واقعاً پاسخگوئه - نه فقط ظاهراً وصل. مشکل رایج توی کلاینت‌های مبتنی بر
    وب‌سوکت اینه که client.is_connected() می‌تونه True برگردونه حتی وقتی اتصال
    عملاً "زامبی" شده (نه پیام می‌فرسته نه می‌گیره)، و run_until_disconnected()
    هم تا وقتی سوکت رسماً بسته نشه برنمی‌گرده. این واچ‌داگ دقیقاً همین حالت رو
    شناسایی و با یک قطع اجباری، رفع می‌کنه (حلقه‌ی اصلی توی main() خودش دوباره
    وصل می‌کنه)."""
    consecutive_failures = 0

    # عمداً از یک API سطح‌بالای شناخته‌شده (`get_me`) برای health-check استفاده
    # می‌کنیم. نسخه‌ی قبلی با GetStateRequest خصوصیِ لایه TL کار می‌کرد؛ در بعضی
    # نسخه‌های SplusPy این درخواست یا پشتیبانی نمی‌شود یا رفتار متفاوتی دارد و
    # نتیجه‌اش این بود که حتی اتصال سالم هم دو بار «خراب» تشخیص داده می‌شد و
    # خود ربات اتصال سالمش را قطع می‌کرد. health-check باید همان مسیری را تست کند
    # که خود کلاینت واقعاً استفاده می‌کند، نه یک درخواست TL حساس به نسخه.
    while True:
        await asyncio.sleep(CONNECTION_WATCHDOG_INTERVAL_SECONDS)

        if not client.is_connected():
            consecutive_failures = 0
            continue  # حلقه‌ی اصلی main() مسئول اتصال مجدد است.

        try:
            await asyncio.wait_for(
                client.get_me(),
                timeout=CONNECTION_WATCHDOG_TIMEOUT_SECONDS
            )
            consecutive_failures = 0
            try:
                with open(HEARTBEAT_FILE, "w") as f:
                    f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            except Exception:
                pass
        except asyncio.TimeoutError:
            consecutive_failures += 1
            safe_print(f"⏱️ واچ‌داگ اتصال: پاسخی طی {CONNECTION_WATCHDOG_TIMEOUT_SECONDS} ثانیه نیومد (تلاش {consecutive_failures}).")
        except Exception as e:
            consecutive_failures += 1
            safe_print(f"⏱️ واچ‌داگ اتصال: خطا در بررسی سلامت اتصال (تلاش {consecutive_failures}): {e}")

        if consecutive_failures >= 2:
            safe_print("🔌 واچ‌داگ: اتصال احتمالاً «زامبی» شده (وصل ولی بی‌پاسخ)؛ در حال قطع اجباری برای اتصال مجدد...")
            await send_external_alert("ربات: اتصال هنگ کرده بود؛ در حال قطع و وصل اجباری.")
            try:
                await client.disconnect()
            except Exception as e:
                safe_print(f"⚠️ خطا هنگام قطع اجباری: {e}")
            consecutive_failures = 0


def safe_handler(func):
    """جلوگیری از قطع/هنگ شدن کل ربات به‌خاطر یک خطای پیش‌بینی‌نشده در یک پیام.
    wraps علاوه بر حفظ رفتار قبلی، تابع اصلی را برای اجرای داخلی Timer در دسترس
    نگه می‌دارد؛ اجرای Timer هم همچنان از همان message_handler اصلی عبور می‌کند
    و Command Handler دومی ساخته نمی‌شود.

    علاوه بر این، زمانِ اجرای واقعیِ هر هندلر رو اندازه می‌گیره: اگه یک پیام
    (پی‌وی یا گروه) بیشتر از SLOW_HANDLER_WARN_SECONDS طول بکشه، توی لاگ ثبت
    می‌شه که دقیقاً کدوم هندلر و کدوم چت/کاربر کند بوده. این تنها راهیه که
    می‌شه فهمید «پی‌وی دیر جواب می‌ده» واقعاً از کجا میاد - از خودِ ارسالِ
    پاسخ (پیام‌ها یا مشکل شبکه/فلادوِیت)، یا از اینکه یک پیامِ گروهیِ سنگین
    قبلش صف رو نگه داشته؛ بدونِ این لاگ فقط می‌شه حدس زد."""
    @wraps(func)
    async def wrapper(*args):
        # SplusLib dispatches callbacks as callback(event).  Some legacy
        # internal callers used callback(client, event), so support both
        # forms without changing the application handlers.
        if len(args) == 1:
            client_arg = globals().get("client")
            event = args[0]
        elif len(args) == 2:
            client_arg, event = args
        else:
            raise TypeError(f"{func.__name__} expected event or (client, event), got {len(args)} arguments")
        started = time.monotonic()
        try:
            await func(client_arg, event)
        except Exception as e:
            import traceback
            safe_print(f"❌ خطای مدیریت‌نشده در {func.__name__}: {e}")
            traceback.print_exc()
        finally:
            elapsed = time.monotonic() - started
            if elapsed > SLOW_HANDLER_WARN_SECONDS:
                safe_print(
                    f"🐢 کندی: {func.__name__} برای chat={getattr(event, 'chat_id', None)} "
                    f"sender={getattr(event, 'sender_id', None)} حدود {elapsed:.1f} ثانیه طول "
                    f"کشید (بیشتر از حد عادی). اگه این مکرر توی لاگ دیده بشه، دقیقاً همینجا "
                    f"نقطه‌ی کند شدنه، نه جای دیگه."
                )
    return wrapper


# اگه پردازشِ یک پیام (پی‌وی یا گروه) بیشتر از این طول بکشه، هشدارِ کندی توی
# لاگ ثبت می‌شه (نگاه کنید به safe_handler بالا).
SLOW_HANDLER_WARN_SECONDS = 3


async def pv_reply(event, text):
    """پاسخ در پی‌وی - مقاوم در برابر خالی‌بودن کش entity بعد از هر ری‌استارت.

    ریشه‌ی باگ «بعد از ری‌استارت پیوی جواب نمی‌ده»:
    event.reply() آدرس مقصد رو *فقط* از کش داخلی خود کتابخونه‌ی spluspy
    (client._mb_entity_cache) می‌خونه (نگاه کنید به ChatGetter.input_chat در
    spluspy/tl/custom/chatgetter.py) و اگه اونجا نبود، فقط ۱۰۰ تا از
    دیالوگ‌های اخیر رو می‌گرده - نه بیشتر. چون این پروژه از StringSession
    استفاده می‌کنه (نه SQLiteSession)، این کش هیچ‌وقت روی دیسک ذخیره نمی‌شه؛
    با *هر* ری‌استارت (چه با پنل مدیریت، چه ری‌استارت خودکار داخلی) کاملاً
    خالی شروع می‌شه و فقط از همون لحظه به بعد، پیام به پیام، دوباره پر می‌شه.
    با چند هزار گروه/کانال فعال، دیالوگ‌های پی‌وی معمولاً حتی جزو همون ۱۰۰ تای
    اول هم نیستن (گروه‌ها به‌خاطر ترافیک بالا همیشه بالای لیست دیالوگ‌ها هستن).
    نتیجه: اولین پیام هر کاربر در پی‌وی بعد از هر ری‌استارت (که چون
    PV_JOIN_VERIFIED هم هربار خالی می‌شه، عملاً یعنی *همه‌ی* پیام‌ها) با یک
    ValueError/TypeError داخلی توی event.reply() شکست می‌خورد - و چون همه‌جا
    با except/except: pass قورت داده می‌شد، نه پاسخی به کاربر می‌رسید نه اثری
    توی لاگ می‌موند. گروه‌ها این مشکل رو ندارن چون message_handler خودش با یک
    get_entity(chat_id) دوره‌ای، entity هر گروه رو با اولین پیامش تازه می‌کنه؛
    برای پی‌وی معادلی وجود نداشت.

    این تابع اول همون event.reply() عادی رو امتحان می‌کنه (سریع‌ترین حالت، وقتی
    کاربر قبلاً کش شده). اگه شکست خورد، به‌جاش از client.send_message() با آیدی
    عددی فرستنده استفاده می‌کنه: این متد entity رو کاملاً مستقل و از نو resolve
    می‌کنه (نه فقط از کش، بلکه با یک تلاش شبکه‌ای واقعی هم - GetUsersRequest)، پس
    حتی وقتی reply شکست خورده معمولاً جواب می‌ده. اگه بازم شکست خورد (مثلاً خود
    اتصال قطعه)، برخلاف قبل خطای کامل رو توی لاگ ثبت می‌کنه تا اگه دوباره تکرار
    شد، دیگه حدس‌زدن لازم نباشه و دلیل دقیقش از لاگ (دکمه‌ی «لاگ» توی پنل) معلوم
    باشه. همیشه فقط این تابع رو صدا بزنید، نه event.reply مستقیم، برای هر پاسخی
    که توی پی‌وی فرستاده می‌شه."""
    started = time.monotonic()
    try:
        result = await event.reply(text)
        elapsed = time.monotonic() - started
        if elapsed > SLOW_HANDLER_WARN_SECONDS:
            safe_print(f"🐢 [pv_reply] event.reply به {event.sender_id} حدود {elapsed:.1f} ثانیه طول کشید.")
        return result
    except Exception as e1:
        try:
            result = await client.send_message(event.sender_id, text)
            elapsed = time.monotonic() - started
            if elapsed > SLOW_HANDLER_WARN_SECONDS:
                safe_print(
                    f"🐢 [pv_reply] event.reply شکست خورد و fallback به send_message برای "
                    f"{event.sender_id} در مجموع حدود {elapsed:.1f} ثانیه طول کشید "
                    f"(احتمالاً resolve شدنِ entity یا FloodWait روی خودِ ارسال - نه صف پیام‌های دیگه)."
                )
            return result
        except Exception as e2:
            import traceback
            safe_print(f"❌ ارسال پاسخ پی‌وی به {event.sender_id} با هر دو روش (reply و send_message) شکست خورد.")
            safe_print(f"   خطای reply: {e1!r}")
            safe_print(f"   خطای send_message: {e2!r}")
            traceback.print_exc()
            return None


# ==================== Timer / Scheduler ====================

TIMER_CHECK_INTERVAL_SECONDS = 5
TIMER_MIN_SECONDS = 5
TIMER_RUNNING = set()
TIMER_RUNNING_LOCK = asyncio.Lock()

_TIMER_DIGIT_TRANSLATION = str.maketrans(
    "۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩",
    "01234567890123456789"
)

_TIMER_UNITS = {
    "ثانیه": 1,
    "ثانیه‌ای": 1,
    "ثانیه": 1,
    "دقیقه": 60,
    "دقیقه‌ای": 60,
    "ساعت": 60 * 60,
    "ساعته": 60 * 60,
    "روز": 24 * 60 * 60,
    "روزه": 24 * 60 * 60,
    "هفته": 7 * 24 * 60 * 60,
    "هفته‌ای": 7 * 24 * 60 * 60,
    "ماه": 30 * 24 * 60 * 60,
    "ماهه": 30 * 24 * 60 * 60,
    "سال": 365 * 24 * 60 * 60,
    "ساله": 365 * 24 * 60 * 60,
}

_TIMER_UNIT_PATTERN = "|".join(
    sorted((re.escape(x) for x in _TIMER_UNITS), key=len, reverse=True)
)


def _timer_normalize_digits(value):
    return str(value).translate(_TIMER_DIGIT_TRANSLATION)


def _timer_format_seconds(seconds):
    seconds = max(0, int(seconds))
    parts = []
    units = (
        ("سال", 365 * 24 * 60 * 60),
        ("ماه", 30 * 24 * 60 * 60),
        ("روز", 24 * 60 * 60),
        ("ساعت", 60 * 60),
        ("دقیقه", 60),
        ("ثانیه", 1),
    )
    for name, size in units:
        if seconds >= size:
            count, seconds = divmod(seconds, size)
            parts.append(f"{count} {name}")
    return " و ".join(parts) if parts else "0 ثانیه"


def _timer_parse_repeat(schedule_text):
    """پسوند -N را از زمان جدا می‌کند. نبود آن یعنی تکرار نامحدود."""
    normalized = _timer_normalize_digits(schedule_text).strip()
    match = re.search(r"\s*-\s*(\d+)\s*$", normalized)
    if not match:
        return normalized, True, 0
    count = int(match.group(1))
    if count < 1:
        raise ValueError("تعداد تکرار باید حداقل 1 باشد.")
    return normalized[:match.start()].strip(), False, count


def _timer_parse_schedule(first_line):
    """
    خروجی:
      (type, interval_seconds, hour, minute, is_loop, loop_count)

    نمونه‌های معتبر:
      10 دقیقه
      1 ساعت و 30 دقیقه
      2 روز و 5 ساعت -5
      12:30
      12:30 -5
    """
    body = first_line.strip()
    if body.startswith("تایمر"):
        body = body[len("تایمر"):].strip()
    if not body:
        raise ValueError(
            "زمان وارد نشده است.\n"
            "مثال: `تایمر 10 دقیقه\\nپیام تست`"
        )

    body, is_loop, loop_count = _timer_parse_repeat(body)

    # ساعت مشخص روزانه
    clock_match = re.fullmatch(r"(\d{1,2})\s*:\s*(\d{1,2})", body)
    if clock_match:
        hour = int(clock_match.group(1))
        minute = int(clock_match.group(2))
        if not (0 <= hour <= 23):
            raise ValueError("ساعت باید بین 0 تا 23 باشد.")
        if not (0 <= minute <= 59):
            raise ValueError("دقیقه باید بین 0 تا 59 باشد.")
        return "daily", None, hour, minute, is_loop, loop_count

    # مدت‌زمان ترکیبی
    total = 0
    pos = 0
    found = False
    pattern = re.compile(rf"(\d+)\s*({_TIMER_UNIT_PATTERN})")
    for match in pattern.finditer(body):
        between = body[pos:match.start()].strip()
        if between and between not in ("و",):
            raise ValueError(f"بخش نامعتبر در زمان: `{between}`")
        amount = int(match.group(1))
        unit = match.group(2)
        total += amount * _TIMER_UNITS[unit]
        found = True
        pos = match.end()

    tail = body[pos:].strip()
    if tail:
        raise ValueError(f"واحد زمان نامعتبر است: `{tail}`")
    if not found:
        raise ValueError(
            "زمان نامعتبر است. واحدهای مجاز: ثانیه، دقیقه، ساعت، روز، هفته، ماه، سال"
        )

    # طبق قرارداد کاربر، کمتر از 5 ثانیه به 5 ثانیه تبدیل می‌شود.
    total = max(TIMER_MIN_SECONDS, int(total))
    return "interval", total, None, None, is_loop, loop_count


def _timer_next_daily_timestamp(hour, minute, from_ts=None):
    now = datetime.fromtimestamp(from_ts if from_ts is not None else time.time())
    candidate = now.replace(hour=int(hour), minute=int(minute), second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate.timestamp()


def _timer_next_run_after(timer, now_ts):
    if timer["type"] == "daily":
        return _timer_next_daily_timestamp(timer["run_hour"], timer["run_minute"], now_ts)
    return float(now_ts) + max(TIMER_MIN_SECONDS, int(timer["interval_seconds"] or TIMER_MIN_SECONDS))


def _timer_schedule_label(timer):
    if timer["type"] == "daily":
        return f"{int(timer['run_hour']):02d}:{int(timer['run_minute']):02d} روزانه"
    return f"هر {_timer_format_seconds(timer['interval_seconds'])}"


def _timer_status_label(timer):
    status = timer.get("status", "active")
    if status == "error":
        return "❌ خطادار"
    if status == "active":
        return "✅ فعال"
    return status


class _TimerEvent:
    """Event حداقلی برای عبور دادن دستور زمان‌بندی‌شده از همان message_handler اصلی."""

    def __init__(self, chat_id, sender_id, text, timer_id):
        self.chat_id = int(chat_id)
        self.sender_id = int(sender_id)
        self.text = str(text)
        self.id = -int(time.time_ns() // 1000)
        self.is_group = True
        self.is_channel = False
        self.out = False
        self.date = datetime.now().astimezone()
        self.timer_id = timer_id
        self.reply_to_msg_id = None

    async def reply(self, message, *args, **kwargs):
        return await client.send_message(self.chat_id, message)

    async def respond(self, message, *args, **kwargs):
        return await client.send_message(self.chat_id, message)

    async def delete(self, *args, **kwargs):
        # این event روی سرور وجود خارجی ندارد، بنابراین چیزی برای حذف نیست.
        return None

    async def get_chat(self):
        return await client.get_entity(self.chat_id)


async def _timer_permission_allowed(chat_id, user_id):
    """همان منطق Permission فعلی پروژه را برای اجرای خودکار دوباره بررسی می‌کند."""
    try:
        is_super_owner = user_id in SUPER_OWNER_IDS
        is_group_owner = is_group_owner_check(chat_id, user_id)
        if not is_group_owner and db.is_group_owner(chat_id, user_id):
            is_group_owner = True

        user_permission = await check_user_permission(chat_id, user_id)
        is_creator = user_permission == "creator"
        is_group_admin = user_permission in ["admin", "creator"]
        is_group_special_admin = db.is_group_special_admin(chat_id, user_id)
        is_admin = (
            is_group_owner or is_creator or is_group_admin or is_super_owner
            or db.is_admin(user_id)
            or db.is_group_admin(chat_id, user_id)
            or is_group_special_admin
        )
        return bool(is_super_owner or is_group_owner or is_admin)
    except Exception as e:
        safe_print(f"[Timer] خطا در بررسی Permission تایمر: {e}")
        return False


async def _run_timer(timer):
    timer_id = timer["id"]
    try:
        # رکورد را دوباره می‌خوانیم تا حذف/تغییر هم‌زمان توسط کاربر روی نسخه قدیمی
        # باعث اجرای ناخواسته نشود.
        current = db.get_timer(timer_id)
        if not current or current["status"] != "active":
            return
        if current["next_run"] > time.time():
            return

        now_ts = time.time()

        # برای تایمر ساعت مشخص، حتی اگر next_run به هر دلیل عقب افتاده باشد،
        # یک روز تقویمی فقط یک اجرای موفق/ثبت‌شده دارد.
        if current["type"] == "daily":
            today = datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d")
            if current.get("last_daily_date") == today:
                db.update_timer_error(
                    timer_id,
                    _timer_next_daily_timestamp(current["run_hour"], current["run_minute"], now_ts),
                    "این تایمر امروز قبلاً اجرا شده است."
                )
                return

        # ربات خاموش یا سازنده فاقد Permission: هیچ Command Handler دیگری دور زده نمی‌شود.
        if not db.is_bot_enabled():
            next_run = _timer_next_run_after(current, now_ts)
            db.update_timer_error(timer_id, next_run, "ربات در این گروه غیرفعال است.")
            return

        if not await _timer_permission_allowed(current["chat_id"], current["creator_id"]):
            next_run = _timer_next_run_after(current, now_ts)
            db.update_timer_error(
                timer_id, next_run,
                "سازنده تایمر دیگر Permission لازم را ندارد؛ اجرای بعدی دوباره بررسی می‌شود."
            )
            return

        commands = current.get("commands") or []
        if not commands:
            db.update_timer_error(
                timer_id,
                _timer_next_run_after(current, now_ts),
                "لیست دستورات تایمر خالی یا خراب است."
            )
            return

        errors = []
        # همان Command Handler اصلی؛ فقط Event واقعی با یک adapter داخلی شبیه‌سازی می‌شود.
        dispatcher = getattr(message_handler, "__wrapped__", message_handler)
        for command in commands:
            command = str(command).strip()
            if not command:
                errors.append("یک دستور خالی در تایمر وجود داشت.")
                continue
            try:
                event = _TimerEvent(
                    current["chat_id"], current["creator_id"], command, timer_id
                )
                await dispatcher(client, event)
            except Exception as e:
                errors.append(f"`{command[:120]}` → {e}")
                # خطای یک دستور نباید اجرای دستور بعدی یا Scheduler را متوقف کند.
                continue

        run_ts = time.time()
        next_run = _timer_next_run_after(current, run_ts)
        new_use = current["loop_count_use"] + 1
        last_daily_date = (
            datetime.fromtimestamp(run_ts).strftime("%Y-%m-%d")
            if current["type"] == "daily"
            else current.get("last_daily_date")
        )
        error_text = " | ".join(errors) if errors else None

        changed, deleted = db.update_timer_after_run(
            timer_id=timer_id,
            last_run=run_ts,
            next_run=next_run,
            loop_count_use=new_use,
            last_daily_date=last_daily_date,
            last_error=error_text,
        )
        if not changed:
            safe_print(f"[Timer] تایمر #{timer_id} هنگام ذخیره نتیجه دیگر وجود نداشت.")
        elif deleted:
            safe_print(f"[Timer] تایمر #{timer_id} پس از تکمیل تعداد اجرا حذف شد.")
        elif errors:
            safe_print(f"[Timer] خطا در اجرای تایمر #{timer_id}: {error_text}")

    except Exception as e:
        # خطای کل یک تایمر فقط همان تایمر را تحت تأثیر قرار می‌دهد.
        safe_print(f"❌ [Timer] خطای Scheduler برای تایمر #{timer_id}: {e}")
        try:
            current = db.get_timer(timer_id)
            if current:
                db.update_timer_error(
                    timer_id,
                    _timer_next_run_after(current, time.time()),
                    f"خطای داخلی Scheduler: {e}",
                )
        except Exception as persist_error:
            safe_print(f"❌ [Timer] خطا هنگام ذخیره خطای تایمر #{timer_id}: {persist_error}")
    finally:
        async with TIMER_RUNNING_LOCK:
            TIMER_RUNNING.discard(timer_id)


async def timer_scheduler_loop():
    """Scheduler دائمی؛ هر 5 ثانیه فقط یک SELECT سبک روی جدول AUTOS انجام می‌دهد."""
    safe_print(f"⏱️ Scheduler تایمر فعال شد (بررسی هر {TIMER_CHECK_INTERVAL_SECONDS} ثانیه).")
    while True:
        try:
            due_timers = db.get_due_timers(time.time())
            for timer in due_timers:
                timer_id = timer["id"]
                async with TIMER_RUNNING_LOCK:
                    if timer_id in TIMER_RUNNING:
                        continue
                    TIMER_RUNNING.add(timer_id)
                asyncio.create_task(_run_timer(timer))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            safe_print(f"❌ [Timer] خطا در حلقه Scheduler: {e}")
        await asyncio.sleep(TIMER_CHECK_INTERVAL_SECONDS)


async def _timer_create_from_event(event, chat_id, sender_id, text):
    """پارسر/ثبت دستور تایمر؛ فقط از Permission فعلی هندلر استفاده می‌کند."""
    raw_lines = str(text).splitlines()
    if not raw_lines:
        await event.reply(
            "📌 **نحوه استفاده:**\n`تایمر 10 دقیقه\\nپیام تست`\n\n"
            "برای تکرار محدود: `تایمر 10 دقیقه -5`"
        )
        return

    first_line = raw_lines[0].strip()
    if first_line == "تایمر" or first_line.lower() == "timer":
        await event.reply(
            "📌 **نحوه استفاده از تایمر:**\n\n"
            "• `تایمر 10 دقیقه\\nپیام تست`\n"
            "• `تایمر 1 ساعت و 30 دقیقه\\nدستور اول\\nدستور دوم`\n"
            "• `تایمر 10 دقیقه -5\\nپیام تست` → فقط 5 بار\n"
            "• `تایمر 12:30\\nپیام تست` → هر روز ساعت 12:30\n"
            "• `تایمر 12:30 -5\\nپیام تست` → روزانه، فقط 5 بار\n\n"
            "حداقل زمان: 5 ثانیه."
        )
        return

    commands = [line.strip() for line in raw_lines[1:] if line.strip()]
    if not commands:
        await event.reply(
            "❌ هیچ دستوری برای اجرای خودکار ثبت نشده است.\n"
            "بعد از خط زمان، حداقل یک دستور در خط بعدی بنویس."
        )
        return

    try:
        timer_type, interval_seconds, hour, minute, is_loop, loop_count = _timer_parse_schedule(first_line)
    except ValueError as e:
        await event.reply(f"❌ {e}")
        return

    now_ts = time.time()
    if timer_type == "daily":
        next_run = _timer_next_daily_timestamp(hour, minute, now_ts)
    else:
        next_run = now_ts + interval_seconds

    try:
        timer_id = db.add_timer(
            chat_id=chat_id,
            timer_type=timer_type,
            commands=commands,
            creator_id=sender_id,
            next_run=next_run,
            interval_seconds=interval_seconds,
            run_hour=hour,
            run_minute=minute,
            is_loop=is_loop,
            loop_count=loop_count,
            created_at=now_ts,
        )
    except Exception as e:
        safe_print(f"[Timer] خطا در ذخیره تایمر: {e}")
        await event.reply(f"❌ ذخیره تایمر انجام نشد: `{e}`")
        return

    repeat_text = "نامحدود" if is_loop else f"{loop_count} بار"
    schedule_text = (
        f"هر {_timer_format_seconds(interval_seconds)}"
        if timer_type == "interval"
        else f"هر روز ساعت {hour:02d}:{minute:02d}"
    )
    await event.reply(
        f"✅ **تایمر #{timer_id} ثبت شد.**\n\n"
        f"⏱ **زمان‌بندی:** {schedule_text}\n"
        f"🔁 **تکرار:** {repeat_text}\n"
        f"📝 **تعداد دستور:** {len(commands)}\n"
        f"▶️ **اولین اجرا:** {datetime.fromtimestamp(next_run).strftime('%Y-%m-%d %H:%M:%S')}"
    )


async def _timer_list_command(event, chat_id):
    try:
        timers = db.get_timers(chat_id)
    except Exception as e:
        await event.reply(f"❌ خواندن لیست تایمرها ناموفق بود: `{e}`")
        return

    if not timers:
        await event.reply("📋 برای این گروه هیچ تایمری ثبت نشده است.")
        return

    chunks = ["⏱ **لیست تایمرهای این گروه**\n"]
    for display_index, timer in enumerate(timers, 1):
        repeat_text = (
            f"{timer['loop_count_use']}/{timer['loop_count']}"
            if not timer["is_loop"]
            else f"{timer['loop_count_use']}/∞"
        )
        remaining = (
            "∞"
            if timer["is_loop"]
            else str(max(0, timer["loop_count"] - timer["loop_count_use"]))
        )
        next_text = datetime.fromtimestamp(timer["next_run"]).strftime("%Y-%m-%d %H:%M:%S")
        commands_text = "\n".join(
            f"   {idx}. `{cmd[:250]}`"
            for idx, cmd in enumerate(timer["commands"], 1)
        ) or "   —"
        error_text = f"\n⚠️ آخرین خطا: `{timer['last_error'][:300]}`" if timer.get("last_error") else ""

        block = (
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔢 **شماره:** `{display_index}` (شناسه داخلی: `{timer['id']}`)\n"
            f"📌 **نوع:** {'مدت‌زمانی' if timer['type'] == 'interval' else 'ساعت مشخص روزانه'}\n"
            f"⏱ **زمان:** {_timer_schedule_label(timer)}\n"
            f"🗓 **اجرای بعدی:** `{next_text}`\n"
            f"🔁 **اجرا:** `{repeat_text}`\n"
            f"📊 **باقی‌مانده:** `{remaining}`\n"
            f"👤 **سازنده:** `{timer['creator_id']}`\n"
            f"📍 **وضعیت:** {_timer_status_label(timer)}\n"
            f"📝 **دستورات:**\n{commands_text}"
            f"{error_text}"
        )
        chunks.append(block)

    # برای جلوگیری از پیام بیش‌ازحد بزرگ، چند بخش ارسال می‌شود.
    output = "\n".join(chunks)
    max_len = 3800
    for start in range(0, len(output), max_len):
        await event.reply(output[start:start + max_len])


async def _timer_delete_command(event, chat_id, argument):
    normalized = _timer_normalize_digits(argument).strip()
    if not normalized.isdigit():
        await event.reply("❌ شماره تایمر نامعتبر است. مثال: `حذف تایمر 2`")
        return
    display_index = int(normalized)
    timers = db.get_timers(chat_id)
    if display_index < 1 or display_index > len(timers):
        await event.reply(f"❌ تایمر شماره `{display_index}` پیدا نشد.")
        return
    timer = timers[display_index - 1]
    if db.delete_timer(chat_id, timer["id"]):
        await event.reply(f"✅ تایمر شماره `{display_index}` حذف شد.")
    else:
        await event.reply("❌ تایمر پیدا نشد یا قبلاً حذف شده است.")


async def _timer_clear_command(event, chat_id):
    try:
        count = db.clear_timers(chat_id)
        await event.reply(f"🗑️ **{count} تایمر** از لیست این گروه حذف شد.")
    except Exception as e:
        await event.reply(f"❌ پاکسازی تایمرها ناموفق بود: `{e}`")


async def handle_timer_commands(event, chat_id, sender_id, text, is_super_owner, is_group_owner, is_admin, is_only_special_tier=False):
    """مسیر واحد مدیریت تایمرها؛ Command Handler عادی را برای خود دستورات تکرار نمی‌کند."""
    lower = str(text).strip().lower()
    if lower == "تایمر" or lower.startswith("تایمر "):
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            await event.reply("⛔ فقط مالک یا ادمین می‌تواند تایمر ثبت کند.")
            return True
        await _timer_create_from_event(event, chat_id, sender_id, text)
        return True

    if lower.startswith("حذف تایمر"):
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return True
        argument = str(text)[len("حذف تایمر"):].strip()
        if not argument:
            await event.reply("📌 مثال: `حذف تایمر 2`")
            return True
        await _timer_delete_command(event, chat_id, argument)
        return True

    if lower in ("لیست تایمر", "لیست تایمرها"):
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return True
        await _timer_list_command(event, chat_id)
        return True

    if lower in ("پاکسازی لیست تایمر", "پاکسازی تایمر", "پاکسازی تایمرها"):
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return True
        await _timer_clear_command(event, chat_id)
        return True

    return False


async def handle_random_name_commands(event, chat_id, is_super_owner, is_group_owner, text):
    """مدیریت تنظیم «انتخاب اسم تصادفی»؛ فقط مالک گروه/سازنده مجاز به تغییر است."""
    lower = str(text).strip().lower()
    enable_cmds = {"انتخاب اسم تصادفی فعال", "اسم تصادفی فعال"}
    disable_cmds = {"انتخاب اسم تصادفی غیرفعال", "اسم تصادفی غیرفعال"}
    status_cmds = {"انتخاب اسم تصادفی", "وضعیت انتخاب اسم تصادفی", "وضعیت اسم تصادفی"}

    if lower in enable_cmds:
        if not (is_super_owner or is_group_owner):
            await event.reply("⛔ فقط مالک گروه می‌تواند این تنظیم را تغییر دهد.")
            return True
        db.set_random_name_enabled(chat_id, True)
        clear_chat_name_cache(chat_id)
        await event.reply(
            "✅ **انتخاب اسم تصادفی فعال شد.**\n"
            "کاربرانی که اسم نمایشی و لقب ندارند، با یک اسم فانتزی تصادفی نمایش داده می‌شوند."
        )
        return True

    if lower in disable_cmds:
        if not (is_super_owner or is_group_owner):
            await event.reply("⛔ فقط مالک گروه می‌تواند این تنظیم را تغییر دهد.")
            return True
        db.set_random_name_enabled(chat_id, False)
        clear_chat_name_cache(chat_id)
        await event.reply(
            "🔕 **انتخاب اسم تصادفی غیرفعال شد.**\n"
            "کاربرانی که اسم و لقب ندارند، بدون اسم فانتزی نمایش داده می‌شوند."
        )
        return True

    if lower in status_cmds:
        enabled = db.get_random_name_enabled(chat_id)
        status = "فعال ✅" if enabled else "غیرفعال 🔕"
        await event.reply(f"🎲 **انتخاب اسم تصادفی:** {status}")
        return True

    return False


SPEAKER_DATA = {}

# چالش‌ها مستقیم همینجا (بدون فایل جدا) نگه داشته می‌شن؛ چون یه دیکشنری معمولی
# پایتونه که همون اول اجرا با بقیه‌ی main.py لود می‌شه، هیچ هزینه‌ی اضافه‌ای
# (نه فایل بیشتر، نه خوندن دیسک موقع اجرا) نداره.


# فهرست قفل‌ها: علاوه بر قفل‌های قدیمی، قفل‌های جدید مطابق لیست مرجع کاربر اضافه شده‌اند.
# کلیدهای داخلی را پایدار نگه می‌داریم تا تنظیمات قبلی دیتابیس از بین نروند.
LOCK_MAP = {
    "متن": "text",
    "گیف": "gif",
    "ویس": "voice",
    "صدا": "voice",
    "اموجی": "emoji",
    "عکس": "photo",
    "فیلم": "video",
    "ویدیو": "video",
    "فایل": "file",
    "ایدی": "id",
    "آیدی": "id",
    "لینک": "link",
    "هایپرلینک": "link",
    "فوروارد": "forward",
    "لوکیشن": "location",
    "نظرسنجی": "poll",
    "پست": "post",
    "استوری": "story",
    "لایو": "live",
    "متن نامناسب": "badword",
    "فحش": "badword",
    "اسپم": "spam",
    "جوین تکراری": "join_duplicate",
    "انگلیسی": "english",
    # قفل‌های قدیمی/تکمیلی برای سازگاری
    "تبلیغ": "ad",
    "استیکر": "sticker",
    "ریپلای": "reply",
    "کد هنگی": "hangcode",
    "هنگی": "hangcode",
    # قفل سرگرمی/بازی (مثل بازی دوز) - وقتی روشنه، کاربرهای عادی نمی‌تونن
    # دستورهای بازی رو اجرا کنن؛ ادمین/مالک/معاف/ویژه همیشه آزادن.
    "سرگرمی": "entertainment",
    # قفل بیو: کاربرِ عضو عادی/معاف/ویژه که توی بیوش لینک داره اخطار می‌گیره؛ بعد از
    # سقف اخطار بن می‌شه. (ادمین و بالاتر مستثنی‌ان)
    "بیو": "bio_link",
    "بیو لینک": "bio_link",
}

# قفل‌های پیش‌فرض وقتی ربات وارد گروه می‌شود / فعال می‌شود
# action: warn | mute | ban
DEFAULT_GROUP_LOCKS = {
    "link": "warn",       # لینک
    "spam": "warn",       # اسپم
    "ad": "warn",         # تبلیغ
    "hangcode": "warn",   # کد هنگی
}

# اسمِ نمایشیِ هر کلیدِ قفل - برای دکمه‌های شیشه‌ایِ پنلِ مرکزی (باید دقیقاً با
# PUBLIC_LOCK_LABELS توی manager.py هم‌راستا بمونه).
LOCK_DISPLAY_NAMES = {
    "link": "لینک", "photo": "عکس", "video": "ویدیو/فیلم", "voice": "صدا/ویس",
    "gif": "گیف", "file": "فایل", "id": "آیدی", "forward": "فوروارد",
    "location": "لوکیشن", "poll": "نظرسنجی", "post": "پست", "story": "استوری",
    "live": "لایو", "emoji": "اموجی", "text": "متن", "sticker": "استیکر",
    "reply": "ریپلای", "hangcode": "کد هنگی", "join_duplicate": "جوین تکراری",
    "english": "انگلیسی", "spam": "اسپم", "ad": "تبلیغ", "badword": "فحش/متن نامناسب",
    "bio_link": "بیو (لینک)", "entertainment": "سرگرمی",
}
# کلیدهایی که قفل‌بودنشون از جدولِ locks نمیاد، بلکه از auto_mod_settings میاد.
LOCK_AUTOMOD_FIELDS = {"spam": "spam_enabled", "ad": "ad_enabled", "badword": "badword_enabled"}



def _normalize_fa_name(s):
    """یکسان‌سازی حروف فارسی/عربی برای مقایسهٔ اسم ربات (آ/ا، ی/ي، ک/ك، نیم‌فاصله)."""
    if not s:
        return ""
    s = str(s).strip().casefold()
    trans = str.maketrans({
        "آ": "ا", "أ": "ا", "إ": "ا", "ٱ": "ا", "ء": "",
        "ة": "ه", "ۀ": "ه",
        "ي": "ی", "ى": "ی",
        "ك": "ک",
        "‌": "",  # ZWNJ
        "‍": "",
        "‌": "",
    })
    s = s.translate(trans)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _message_addresses_bot(text, bot_name):
    """اگر پیام اسم ربات را صدا بزند (تنها / اول جمله / اول + فاصله)."""
    if not text or not bot_name:
        return False
    t = _normalize_fa_name(text)
    n = _normalize_fa_name(bot_name)
    if not n or len(n) < 1:
        return False
    if t == n:
        return True
    # اول جمله: «آنیتا هوی» / «آنیتا، سلام»
    if t.startswith(n + " ") or t.startswith(n + "،") or t.startswith(n + ","):
        return True
    # کلمهٔ اول بعد از جداکننده
    first = re.split(r"[\s،,!.؟?]+", t, maxsplit=1)[0]
    if first == n:
        return True
    return False


def _strip_bot_name_prefix(text, bot_name):
    """اگر پیام با اسم ربات شروع شود، پیشوند اسم را برای اجرای دستور جدا کن."""
    if not text or not bot_name:
        return text, False
    t_norm = _normalize_fa_name(text)
    n = _normalize_fa_name(bot_name)
    if not n or not t_norm.startswith(n):
        return text, False
    # طول پیشوند در متن اصلی را تقریبی با split پیدا کن
    raw = text.strip()
    # حذف پیشوند با regex انعطاف‌پذیر روی متن خام
    parts = re.split(r"[\s،,:]+", raw, maxsplit=1)
    if parts and _normalize_fa_name(parts[0]) == n:
        rest = parts[1].strip() if len(parts) > 1 else ""
        return rest, True
    return text, False

def apply_default_group_locks(chat_id):
    """اعمال قفل‌های پیش‌فرض روی گروه تازه. ادمین بعداً با «قفل X» / «بازکردن X» عوض می‌کند."""
    try:
        db.set_auto_mod_settings(
            chat_id,
            spam_enabled=True,
            ad_enabled=True,
            # فحش به‌صورت پیش‌فرض بازه؛ فقط وقتی مالک/ادمین قفلش کنه روشن می‌شه.
            badword_enabled=False,
            silent_mode=False,
        )
    except Exception as e:
        safe_print(f"[default_locks] auto_mod: {e}")
    for key, action in DEFAULT_GROUP_LOCKS.items():
        try:
            db.set_lock(chat_id, key, True, action=action)
        except Exception as e:
            safe_print(f"[default_locks] set_lock {key}: {e}")

FILTER_WARN_CACHE = {}
WARN_MESSAGE_CACHE = {}
FIRST_MESSAGE_SENT = {}
GROUP_OWNER_CACHE = {}

# ==================== عضویت اجباری در کانال‌ها برای استفاده از ربات در پی‌وی ====================
PV_JOIN_VERIFIED = set()  # user_id هایی که «عضو شدم» رو فرستادن

# ==================== ضد اسپم پی‌وی (نادیده گرفتن + بلاک خودکار) ====================
# اگه کاربری توی یک بازه‌ی کوتاه بیش از حد مجاز پیام توی پی‌وی بفرسته (اسپم)، به‌جای
# پردازش/پاسخ به تک‌تکشون، از همون لحظه کاملاً نادیده گرفته می‌شه (دقیقاً همون
# مکانیزم «نادید گرفتن» دستیِ سازنده - db.add_ignored_user) و در صورت امکان هم
# بلاک می‌شه. سازنده‌ها هیچ‌وقت مشمول این چک نمی‌شن.
PV_SPAM_WINDOW_SECONDS = 8       # بازه‌ی زمانی‌ای که پیام‌ها توش شمرده می‌شن
PV_SPAM_MAX_MESSAGES = 6         # بیش از این تعداد پیام توی همون بازه = اسپم
PV_SPAM_MESSAGE_TIMES = collections.defaultdict(collections.deque)  # user_id -> deque[timestamp]

# متن پیش‌فرض (اگه هیچ‌وقت سازنده‌ای دستی تغییرش نده از همین استفاده می‌شه)
DEFAULT_JOIN_REQUIRED_MSG = """@Soft_Studio
@soft_Helper

🔒 برای استفاده از ربات، اول باید توی این دو تا عضو بشی!

✅ بعد از عضویت بنویس: عضو شدم"""

JOIN_REQUIRED_MSG_SETTING_KEY = "join_required_msg"


def get_join_required_msg():
    """متن فعلی پیام «عضویت اجباری» رو برمی‌گردونه؛ اول از دیتابیس (اگه سازنده‌ای
    دستی تغییرش داده) وگرنه متن پیش‌فرض بالا. این‌طوری سازنده‌ها می‌تونن هر وقت
    خواستن این متن رو با یک دستور توی پی‌وی عوض کنن، بدون این‌که نیاز به تغییر
    کد و ری‌استارت ربات باشه."""
    try:
        return db.get_setting(JOIN_REQUIRED_MSG_SETTING_KEY, DEFAULT_JOIN_REQUIRED_MSG) or DEFAULT_JOIN_REQUIRED_MSG
    except Exception:
        return DEFAULT_JOIN_REQUIRED_MSG


def set_join_required_msg(new_text):
    db.set_setting(JOIN_REQUIRED_MSG_SETTING_KEY, new_text)


def reset_join_required_msg():
    db.set_setting(JOIN_REQUIRED_MSG_SETTING_KEY, DEFAULT_JOIN_REQUIRED_MSG)

GROUP_ADMINS_CACHE = {}
GROUP_ADMINS_CACHE_TIME = {}

LINK_COUNT_CACHE = {}
LINK_WARN_CACHE = {}

# ردیابی بن‌های پی‌درپی هر ادمین در هر گروه، برای اطلاع‌رسانی به مالک ربات

BOT_ID = None
# توجه: حالت سخنگو دیگه اینجا global نگه‌داری نمی‌شه (قبلاً یک متغیر مشترک بین همه‌ی
# گروه‌ها بود که باعث می‌شد تغییر حالت توسط ادمین یک گروه، روی همه‌ی گروه‌های دیگه هم
# اثر بذاره). الان با db.get_speaker_mode(chat_id) / db.set_speaker_mode(chat_id, mode)
# مخصوص هر گروه جداگانه ذخیره و خونده می‌شه (با کش داخلی، پس کند هم نیست).

# ==================== بهینه‌سازی سرعت برای تعداد زیاد گروه (~۱۰۰۰ گروه) ====================
# ۱) قبلاً روی *هر* پیام یک client.get_entity(chat_id) (یعنی یک درخواست شبکه‌ای به
#    سروش/تلگرام) و یک db.add_group زده می‌شد، فقط برای این‌که اسم گروه ثبت/به‌روز
#    بشه. زیر بار هزار گروه همین یک خط باعث کندی/تأخیر محسوس پاسخ‌گویی ربات می‌شد.
#    حالا این کار فقط یک‌بار برای هر گروه (و بعد از اون هر چند ساعت یک‌بار برای
#    به‌روز نگه داشتن اسم گروه) انجام می‌شه، نه روی تک‌تک پیام‌ها.
GROUP_INFO_REFRESH_SECONDS = 6 * 3600  # هر ۶ ساعت یک‌بار اسم گروه رو تازه کن
GROUP_INFO_LAST_CHECK = {}  # chat_id -> timestamp آخرین باری که get_entity زده شد

# همین منطق برای تعدادِ اعضا (پنلِ مرکزی) و اسمِ نمایشیِ کاربرها (لیدربوردِ
# فعالان) هم تکرار شده - هیچ‌کدوم مستقیم و زیرِ فشارِ کلیکِ کاربر روی پنل
# صدا زده نمی‌شن، فقط از همینجا و کاملاً throttled/پخش‌شده در طولِ زمان.
USER_INFO_REFRESH_SECONDS = 24 * 3600  # هر ۲۴ ساعت یک‌بار اسمِ هر کاربر رو تازه کن
USER_INFO_LAST_CHECK = {}  # user_id -> timestamp آخرین باری که get_entity زده شد

# ۲) قبلاً db.update_stats و db.update_user_stats (هرکدوم یک نوشتن روی دیتابیس)
#    روی *هر* پیام اجرا می‌شدن. حالا این‌ها فقط در حافظه جمع می‌شن و یک تسک پس‌زمینه
#    (flush_stats_loop) هر چند ثانیه یک‌بار یکجا روی دیتابیس می‌نویسدشون؛ آمار گم
#    نمی‌شه، فقط نوشتن روی دیسک دسته‌جمعی و کم‌تعداد می‌شه.
STATS_FLUSH_SECONDS = 20
_pending_chat_stats = {}          # chat_id -> تعداد پیام‌های جمع‌نشده
_pending_user_stats = {}          # (user_id, chat_id) -> تعداد پیام‌های جمع‌نشده
_pending_daily_activity = {}      # (user_id, chat_id, "YYYY-MM-DD" به‌وقتِ ایران) -> تعداد
_pending_hourly_activity = {}     # (chat_id, hour_ts) -> تعداد پیام (برای چکِ لغزنده‌ی ۲۴ ساعته)
_last_daily_prune_at = 0.0        # آخرین باری که ردیف‌های خیلی قدیمیِ فعالیتِ روزانه پاک شدن

# ۳) گزارش‌های قدیمی (تعداد پیام حذف‌شده، چه کسی حذف کرده، چند نفر بن/کیک شدن و ...)
#    به‌طور کامل حذف شدند؛ نیازی نیست کسی این جزئیات رو بداند. تنها چیزی که باقی
#    مانده یک پیام «آنلاینم» ساده هر نیم ساعت یک‌بار به پی‌وی سازنده(های) ربات است،
#    فقط برای اطمینان از اینکه ربات خاموش نشده.
ONLINE_HEARTBEAT_INTERVAL_SECONDS = 30 * 60  # هر نیم ساعت

# ==================== متغیرهای جدید Auto Mod ====================
AUTO_MOD_CACHE = {}
WELCOME_CACHE = {}
GOODBYE_CACHE = {}
MESSAGE_HISTORY = {}
MESSAGE_TEXT_HISTORY = {}
REPEATED_SPAM_WINDOW_SECONDS = 10
REPEATED_SPAM_COUNT = 4

# ==================== لینک‌ها ====================
CHANNEL_LINK = "@Soft_Studio"
HELPER_LINK = "https://splus.ir/joingroup/AVLC37-ZZctLQrFuB8No9g"

HELP_MAIN = f"""✦ ───── SOFT ────── ✦

𝑮𝒓𝒐𝒖𝒑 𝑴𝒂𝒏𝒂𝒈𝒆𝒓

بخش موردنظرت رو انتخاب کن.

⤣  `راهنما مالک`
⤣  `راهنما ادمین`
⤣  `راهنما سخنگو`
⤣  `راهنما فان`
⤣  `راهنما تایمر`

✦ ─── {CHANNEL_LINK} ─── ✦"""

# ==================== راهنمای مالک/سازنده (دسته‌بندی‌شده) ====================
HELP_OWNER = """✦ ───── SOFT ────── ✦

 فعال‌سازی

✦ `فعال`
فعال‌سازی ربات
فقط اولین نفر مالک می‌شود

✦ `غیرفعال`
غیرفعال‌سازی ربات

 ویژه

✦ `تنظیم ویژه`
تعیین ویژه برای این گروه

✦ `حذف ویژه`
حذف ویژه از این گروه

✦ `لیست ویژه`
مشاهده ویژه‌های این گروه

 مالکیت

✦ `انتقال مالکیت` | `واگذاری مالکیت`
ریپلای یا آیدی عددی
فقط توسط مالک فعلی گروه

✦ `لیست مالک`
مشاهده مالک فعلی گروه

 انتخاب اسم تصادفی

✦ `انتخاب اسم تصادفی فعال`
فعال‌سازی انتخاب اسم فانتزی
پیش‌فرض: فعال

✦ `انتخاب اسم تصادفی غیرفعال`
غیرفعال‌سازی انتخاب اسم فانتزی

✦ `انتخاب اسم تصادفی`
مشاهده وضعیت فعلی

 اسم اختصاصی ربات

✦ `اسم ربات`
نمایش اسم فعلی

✦ `حذف اسم ربات`
حذف اسم اختصاصی

✦ `تنظیم اسم ربات [اسم]`
تعیین اسم اختصاصی ربات

✦ ─── @Soft_Studio ─── ✦"""

HELP_OWNER_ACTIVATION = """👑 **راهنمای مالک - فعال‌سازی و مالکیت گروه**

**فعال‌سازی:**
• `فعال` - فعال‌سازی ربات (فقط اولین نفر مالک میشه)
• `غیرفعال` - غیرفعال‌سازی ربات

**ویژه (مخصوص این گروه):**
• `تنظیم ویژه` (ریپلای) - تعیین ویژه برای این گروه
• `حذف ویژه` (ریپلای) - حذف ویژه از این گروه
• `لیست ویژه` - مشاهده ویژه‌های این گروه

**ادمین (مخصوص این گروه):**
• `تنظیم ادمین` (ریپلای) - تعیین ادمین برای این گروه؛ مقامش از ویژه بالاتره
• `حذف ادمین` (ریپلای) - حذف ادمین از این گروه
• `لیست ادمین` - مشاهده ادمین‌های این گروه

**مالکیت:**
• `انتقال مالکیت` یا `واگذاری مالکیت` (ریپلای یا آیدی عددی، فقط مالک فعلی گروه) - واگذاری مالکیت این گروه به عضو دیگه
• `تنظیم مالک اصلی` یا `بازیابی مالکیت` (ریپلای یا آیدی عددی، فقط سازنده) - تنظیم فرد موردنظر به‌عنوان مالک گروه
• `تغییر مالک [آیدی عددی]` (فقط سازنده) - مشابه بالا، با نوشتن مستقیم آیدی
• `لیست مالک` - مشاهده مالک فعلی گروه

**انتخاب اسم تصادفی:**
• `انتخاب اسم تصادفی فعال` - برای اعضایی که اسم و لقب ندارند، اسم فانتزی تصادفی انتخاب می‌کند (پیش‌فرض فعال)
• `انتخاب اسم تصادفی غیرفعال` - انتخاب اسم فانتزی را خاموش می‌کند
• `انتخاب اسم تصادفی` - نمایش وضعیت فعلی
⚠️ تغییر این تنظیم فقط برای مالک گروه و سازنده مجاز است.

**اسم اختصاصی ربات (مخصوص این گروه):**
• `اسم ربات` - نمایش اسم فعلی
• `حذف اسم ربات` - برگشت به حالت بدون اسم
• `تنظیم اسم ربات [اسم]` - انتخاب اسم؛ صدا زدن اسم → جانم؛ دستور با اسم هم کار می‌کند

🔙 `راهنما مالک` برای بازگشت به فهرست مالک | `راهنما` برای منوی اصلی"""

HELP_OWNER_SUPER = f"""👑 **راهنمای مالک - دستورات ویژه‌ی سازنده اصلی**
(فقط آیدی {SUPER_OWNER_IDS_DISPLAY})

**پنل سازنده (پی‌وی ربات):**
• `پنل سازنده` - نمایش پنل کامل کنترل ربات
• `پیام عضویت` / `تنظیم پیام عضویت [متن]` / `ریست پیام عضویت` - مدیریت متن قفل عضویت اجباری پی‌وی

(دستورهای «بگو» و «یاد بده» به‌طور کامل غیرفعال شدند؛ هیچ کاربری - حتی سازنده - نمی‌تواند ربات را وادار به تکرار یا گفتن متن دلخواه کند)

**پیام‌رسانی و آمار (پی‌وی ربات):**
• `ارسال به همه [متن]` - ارسال پیام به همه گروه‌ها
• `لفت همه` / `تایید لفت همه` - خروج از همه گروه‌ها
• `آمار کلی` - آمار کلی همه‌ی گروه‌ها: تعداد گروه‌ها، مجموع پیام‌ها و پرپیام‌ترین گروه‌ها
• `لیست روزشمار` - روزشمار فعال‌سازی تک‌تک گروه‌ها؛ هر گروه با «فعال» به ۳۰ روز که برسه، خودکار به پیوی سازنده‌ها خبر داده می‌شه

**مدیریت سراسری کاربرها:**
• `محروم کامل [آیدی عددی] [دلیل اختیاری]` - سکوت سراسری کاربر در همه‌ی گروه‌های ربات (صرف‌نظر از ادمین/مالک بودنش)
• `رفع محروم کامل [آیدی عددی]` (یا `حذف محروم کامل`) - لغو سکوت سراسری
• `لیست محروم کامل` - مشاهده‌ی لیست محروم‌های کامل
• `نادید گرفتن [آیدی عددی]` (پی‌وی ربات) - از این به بعد ربات به هیچ پیامی از این کاربر (نه پی‌وی، نه هیچ گروهی) جواب نمی‌ده؛ بدون بن یا سکوت شدنش
• `رفع نادید گرفتن [آیدی عددی]` (یا `حذف نادید گرفتن`، پی‌وی ربات) - لغو نادید گرفتن
• `لیست نادید گرفته ها` (پی‌وی ربات) - مشاهده‌ی لیست کاربران نادیده‌گرفته‌شده

🔙 `راهنما مالک` برای بازگشت به فهرست مالک | `راهنما` برای منوی اصلی"""

HELP_TIMER = """⏱️ **راهنمای تایمر / زمان‌بندی**

با تایمر می‌تونی یک یا چند دستور رو طوری زمان‌بندی کنی که ربات در زمان مشخص، همون دستورها رو به‌صورت خودکار اجرا کنه.

**1️⃣ تایمر مدت‌زمانی**
• `تایمر 10 دقیقه`
  دستور در اولین زمان اجرای تعیین‌شده اجرا می‌شود.
• دستور یا دستورها را در خط‌های بعدی بنویس.
• چند دستور هم می‌توانی پشت سر هم ثبت کنی.

**واحدهای زمانی:**
• `ثانیه`، `دقیقه`، `ساعت`، `روز`، `هفته`، `ماه`، `سال`
• زمان‌های ترکیبی هم پشتیبانی می‌شوند؛ مثل:
  `تایمر 1 ساعت و 30 دقیقه`
  `تایمر 2 روز و 5 ساعت`
• اگر زمان کمتر از 5 ثانیه باشد، حداقل زمان 5 ثانیه در نظر گرفته می‌شود.

**2️⃣ تایمر تکرارشونده**
• `تایمر 10 دقیقه` → نامحدود؛ بعد از هر اجرا برای اجرای بعدی آماده می‌شود.
• `تایمر 10 دقیقه -5` → محدود؛ فقط 5 بار اجرا می‌شود و بعد خودکار حذف می‌شود.

**3️⃣ تایمر ساعت مشخص (روزانه)**
• `تایمر 12:30` → هر روز ساعت 12:30 اجرا می‌شود.
• `تایمر 08:00 -7` → هر روز ساعت 08:00، فقط 7 بار اجرا می‌شود.
• ساعت باید بین `00:00` تا `23:59` باشد.
• هر تایمر ساعت مشخص در یک روز بیشتر از یک بار اجرا نمی‌شود.

**4️⃣ چند دستور در یک تایمر**
مثال:
`تایمر 1 ساعت و 30 دقیقه`
`دستور اول`
`دستور دوم`
`دستور سوم`

دستورهای خطوط بعدی هنگام رسیدن زمان تایمر، از مسیر اصلی اجرای دستورات ربات پردازش می‌شوند.

**5️⃣ مدیریت تایمرها**
• `لیست تایمر` → نمایش تایمرهای همین گروه، زمان اجرا، تعداد اجرا و وضعیت
• `حذف تایمر 2` → حذف تایمر شماره 2
• `پاکسازی لیست تایمر` → حذف تمام تایمرهای همین گروه

🔐 اجرای تایمر از سیستم پردازش اصلی ربات انجام می‌شود و Permission فعلی ربات دور زده نمی‌شود.
💾 تایمرها ذخیره می‌شوند و بعد از Restart ربات از بین نمی‌روند.

🔙 `راهنما` برای بازگشت به منوی اصلی"""

HELP_ADMIN = """✦ ───── SOFT ────── ✦

راهنمای ادمین

دستور بخش موردنظر رو ارسال کن:

① `راهنما اعضا`
بن، سکوت، اخطار، سنجاق، لقب و پاک‌سازی

② `راهنما قفل`
قفل عکس، ویدیو، لینک، فحش و سایر قفل‌ها

③ `راهنما فیلتر`
مدیریت و تنظیم کلمات فیلتر

④ `راهنما خوشامد`
تنظیم پیام خوشامد و خداحافظی

⑤ `راهنما مقام`
مقام‌بندی و مدیریت لیست‌های گروه

✦ ─── @Soft_Studio ─── ✦"""

_HELP_ADMIN_BACK = "🔙 `راهنما ادمین` برای بازگشت به فهرست ادمین | `راهنما` برای منوی اصلی"

HELP_ADMIN_MEMBERS = """✦ ───── SOFT ────── ✦

✦ راهنمای اعضا

 بن / سکوت / اخطار / سنجاق
✦ `بن` ✦ `رفع بن` / `حذف بن` ✦ `سکوت` ✦ `رفع سکوت` / `حذف سکوت`
✦ `ban` — بن واقعی: کاربر از گروه بیرون انداخته می‌شود (با `رفع بن` / `حذف بن` آزاد می‌شود)
✦ `اخطار` ✦ `حذف اخطار` / `رفع اخطار` ✦ `سنجاق` ✦ `حذف سنجاق`
↳ سکوت زمان‌دار: با ریپلای روی پیام فرد بنویس `سکوت 30` تا ۳۰ دقیقه سکوت بشه و بعدش خودکار رفع بشه

 لقب کاربر
✦ `لقب` — نمایش لقب
✦ `حذف لقب` — حذف لقب

 پاک‌سازی
✦ `پاک [تعداد]` — حداکثر ۱۰۰ پیام

✦ ─── @Soft_Studio ─── ✦"""

HELP_ADMIN_LOCKS = """✦ ───── SOFT ────── ✦

✦ راهنمای قفل‌ها

قفل محتوا
`لیست قفل`

اکشن‌های قفل

✦ با نوشتن عمل مثل بن یا سکوت کنار قفل
می‌تونید عمل ربات رو تغییر بدید:
✦ `قفل لینک بن`  ✦ `قفل لینک سکوت`
✦ `قفل اسپم بن`  ✦ `قفل اسپم سکوت`

✦ `قفل بیو` — تا وقتی بیوی عضو عادی/معاف/ویژه لینک داره، هر پیامش پاک می‌شه و اخطار می‌گیره؛ با رسیدن به سقف اخطار بن می‌شه (`بازکردن بیو` برای خاموش‌کردن؛ ادمین‌ها مستثنی‌ان)

✦ تنظیمات و وضعیت

• `لیست قفل` — نمایش وضعیت قفل‌ها و عمل آن‌ها
• `محدودیت لینک [عدد]` — تعیین سقف لینک در حالت اخطار
• `تنظیم اخطار [عدد]` — تعیین سقف اخطار پیش از بن
• `محدودیت` — نمایش قفل‌های فعال و اخطارهای فعلی

✦ ─── @Soft_Studio ─── ✦"""

HELP_ADMIN_FILTERS = """✦ ───── SOFT ────── ✦

✦ راهنمای فیلتر کلمات و جملات

✦ `فیلتر` — ابتدا کلمه یا جمله موردنظر را ارسال کنید؛
سپس با ریپلای، دستور مربوطه را وارد کنید.

✦ `فیلتر بن` — حذف پیام + بن کاربر
✦ `فیلتر سکوت` — حذف پیام + سکوت کاربر
✦ `حذف فیلتر` — حذف فیلتر ثبت‌شده
✦ `لیست فیلتر` — نمایش فهرست فیلترها
✦ `پاکسازی فیلتر` — حذف تمامی فیلترها

✦ فیلتر رسانه (گیف، ویدیو، آهنگ، استیکر، عکس، فایل)
روی همان رسانه ریپلای کنید و بنویسید:
✦ `فیلتر رسانه` — حذف پیام + اخطار به فرستنده
✦ `فیلتر رسانه بن` — حذف پیام + بن
✦ `فیلتر رسانه سکوت` — حذف پیام + سکوت
✦ `حذف فیلتر رسانه` — حذف فیلتر (ریپلای یا با شماره لیست)
✦ `لیست فیلتر رسانه` — نمایش فهرست
✦ `پاکسازی فیلتر رسانه` — حذف همه
↳ روی عضو عادی، معاف و ویژه اعمال می‌شود؛ ادمین و بالاتر مستثنی‌اند.

✦ ─── @Soft_Studio ─── ✦"""

HELP_ADMIN_AUTOMOD = """✦ ───── SOFT ────── ✦

✦ مدیریت اسپم / فحش / تبلیغ

از `لیست قفل` و دستورهای `قفل اسپم` / `قفل فحش` / `قفل تبلیغ` استفاده کن.
تنظیم جداگانه‌ی «خودکار» لازم نیست.

✦ ─── @Soft_Studio ─── ✦"""

HELP_ADMIN_WELCOME = f"""🛡️ **راهنمای ادمین - خوشامد و خداحافظی**

• خوشامدگویی و خداحافظی ثابت هستند و قابل تغییر یا خاموش‌کردن نیستند.
{_HELP_ADMIN_BACK}"""

HELP_ADMIN_RANKS = """✦ ───── SOFT ────── ✦

✦ راهنمای مقام‌بندی و لیست‌ها

✦ لیست‌های گروه
✦ `لیست مدیر`  ✦ `لیست بن`  ✦ `لیست سکوت`
✦ `پاکسازی لیست بن` — حذف کامل لیست بن گروه
✦ `پاکسازی لیست سکوت` — حذف کامل لیست سکوت گروه

✦ `تنظیم معاف`  ✦ `حذف معاف`  ✦ `لیست معاف`
↳ معاف از قفل‌ها و فیلترها، بدون دسترسی مدیریتی

✦ `تنظیم ویژه` / `حذف ویژه` — ریپلای، فقط مالک/سازنده  ✦ `لیست ویژه`  ✦ `ویژه پاک` (فقط سازنده؛ پاک‌کردن همه‌ی ویژه‌های گروه)
↳ ادمین رباتِ این گروه؛ فقط می‌تونه بن/سکوت/اخطار بده، رفع‌شون کنه و سنجاق کنه؛ بقیه‌ی دستورهای مدیریتی (پاکسازی، معاف‌کردن، قفل‌ها، فیلترها، تایمر، کال، تنظیم لقب و...) رو نداره

✦ `تنظیم ادمین` / `حذف ادمین` — ریپلای، فقط مالک/سازنده  ✦ `لیست ادمین`
↳ مقامش از ویژه بالاتره؛ ویژه‌ها نمی‌تونن روش بن/سکوت/اخطار بزنن

• `مقام` — ریپلای یا بدون ریپلای برای نمایش مقام خودتان

✦ ─── @Soft_Studio ─── ✦"""

# توجه امنیتی: امکان «یاد بده / حذف از سخنگو / لیست سخنگو / پاکسازی سخنگو»
# (یعنی وادار کردن ربات به تکرار/گفتنِ متن دلخواهِ کاربر) به‌طور کامل غیرفعال شده.
# ربات فقط از API سخنگو پاسخ می‌گیرد و پاسخ ثابت محلی ندارد.
HELP_SPEAKER = """✦ ───── SOFT ────── ✦

✦ راهنمای سخنگو | تنظیم لحن

✦ حالت‌های سخنگو
✦ `حالت مؤدب`
✦ `حالت بی ادب`
✦ `حالت کم حرف`
✦ `حالت پر حرف`
✦ `حالت خاموش`
✦ `حالت کنونی`

✦ ─── @Soft_Studio ─── ✦"""

HELP_FUN = """🎉 **راهنمای سرگرمی**

دستورهای سرگرمی:
• `جوک` • `حدیث` • `فال` • `فکت` • `دعا`
• `دانستنی` • `دقت` • `داستان` • `چیستان` • `بیو`
• `انگیزشی` • `خاطره` • `خطبه` • `پ ن پ` • `تکست` • `چالش`

🔙 `راهنما` برای بازگشت به منوی اصلی"""

# ==================== دیکشنری ترجمه ====================
PERSIAN_TO_ENGLISH = {
    'سلام': 'Hello', 'خوبی': 'How are you', 'چطوری': 'How are you',
    'خوبم': "I'm fine", 'بد نیستم': 'Not bad', 'خوبی؟': 'How are you?',
    'سلامتی': 'Health', 'خوش اومدی': 'Welcome', 'خوش اومدید': 'Welcome',
    'مرسی': 'Thanks', 'ممنون': 'Thank you', 'ممنونم': 'Thank you',
    'تشکر': 'Thanks', 'متشکرم': 'Thank you', 'خواهش': 'Please',
    'خواهش میکنم': "You're welcome", 'بفرما': 'Here you go',
    'بیا': 'Come', 'برو': 'Go', 'باشه': 'Okay', 'اوکی': 'OK',
    'خداحافظ': 'Goodbye', 'خدانگهدار': 'Goodbye', 'بدرود': 'Farewell',
    'شب بخیر': 'Good night', 'صبح بخیر': 'Good morning',
    'ظهر بخیر': 'Good noon', 'عصر بخیر': 'Good evening',
    'روز بخیر': 'Good day',
    'من': 'I', 'تو': 'You', 'او': 'He/She', 'ما': 'We', 'شما': 'You',
    'آنها': 'They', 'این': 'This', 'آن': 'That', 'اینها': 'These',
    'چه': 'What', 'کی': 'Who', 'کجا': 'Where', 'چرا': 'Why',
    'چگونه': 'How', 'چقدر': 'How much', 'کدام': 'Which',
    'رفتن': 'Go', 'آمدن': 'Come', 'داشتن': 'Have', 'بودن': 'Be',
    'شدن': 'Become', 'کردن': 'Do', 'گفتن': 'Say', 'دیدن': 'See',
    'شنیدن': 'Hear', 'خوردن': 'Eat', 'نوشیدن': 'Drink', 'خوابیدن': 'Sleep',
    'بیدار شدن': 'Wake up', 'نشستن': 'Sit', 'ایستادن': 'Stand',
    'راه رفتن': 'Walk', 'دویدن': 'Run', 'پریدن': 'Jump',
    'شنا کردن': 'Swim', 'خواندن': 'Read', 'نوشتن': 'Write',
    'گرفتن': 'Take', 'دادن': 'Give', 'آوردن': 'Bring',
    'بستن': 'Close', 'باز کردن': 'Open',
    'خوب': 'Good', 'بد': 'Bad', 'زیبا': 'Beautiful', 'زشت': 'Ugly',
    'بزرگ': 'Big', 'کوچک': 'Small', 'بلند': 'Tall', 'کوتاه': 'Short',
    'سریع': 'Fast', 'آهسته': 'Slow', 'گرم': 'Hot', 'سرد': 'Cold',
    'گران': 'Expensive', 'ارزان': 'Cheap', 'سخت': 'Hard',
    'آسان': 'Easy', 'شاد': 'Happy', 'غمگین': 'Sad', 'خسته': 'Tired',
    'عصبانی': 'Angry', 'آرام': 'Calm', 'نگران': 'Worried',
    'خوشحال': 'Happy', 'متاسف': 'Sorry', 'خوشگل': 'Beautiful',
    'قشنگ': 'Nice', 'ناز': 'Cute', 'مهربان': 'Kind',
    'انسان': 'Human', 'مرد': 'Man', 'زن': 'Woman', 'کودک': 'Child',
    'پدر': 'Father', 'مادر': 'Mother', 'برادر': 'Brother',
    'خواهر': 'Sister', 'پسر': 'Son', 'دختر': 'Daughter',
    'دوست': 'Friend', 'همسایه': 'Neighbor', 'همکار': 'Colleague',
    'معلم': 'Teacher', 'دانش آموز': 'Student', 'پزشک': 'Doctor',
    'مهندس': 'Engineer', 'وکیل': 'Lawyer', 'پلیس': 'Police',
    'خانه': 'House', 'مدرسه': 'School', 'دانشگاه': 'University',
    'بیمارستان': 'Hospital', 'مسجد': 'Mosque', 'فروشگاه': 'Store',
    'بازار': 'Market', 'رستوران': 'Restaurant', 'هتل': 'Hotel',
    'فرودگاه': 'Airport', 'ایستگاه': 'Station', 'پارک': 'Park',
    'باغ': 'Garden', 'اتاق': 'Room', 'آشپزخانه': 'Kitchen',
    'حمام': 'Bathroom', 'توالت': 'Toilet', 'زمین': 'Earth',
    'آسمان': 'Sky', 'دریا': 'Sea', 'رودخانه': 'River',
    'دریاچه': 'Lake', 'کوه': 'Mountain', 'جنگل': 'Forest',
    'بیابان': 'Desert', 'جزیره': 'Island', 'ساحل': 'Beach',
    'خورشید': 'Sun', 'ماه': 'Moon', 'ستاره': 'Star', 'ابر': 'Cloud',
    'باران': 'Rain', 'برف': 'Snow', 'باد': 'Wind', 'طوفان': 'Storm',
    'گل': 'Flower', 'درخت': 'Tree', 'گیاه': 'Plant', 'میوه': 'Fruit',
    'سبزی': 'Vegetable', 'نان': 'Bread', 'برنج': 'Rice',
    'گوشت': 'Meat', 'مرغ': 'Chicken', 'ماهی': 'Fish',
    'تخم مرغ': 'Egg', 'شیر': 'Milk', 'پنیر': 'Cheese',
    'چای': 'Tea', 'قهوه': 'Coffee', 'آب': 'Water',
    'کتاب': 'Book', 'قلم': 'Pen', 'مداد': 'Pencil', 'کیف': 'Bag',
    'ساعت': 'Clock', 'در': 'Door', 'پنجره': 'Window',
    'صندلی': 'Chair', 'میز': 'Table', 'تخت': 'Bed',
    'یخچال': 'Refrigerator', 'تلویزیون': 'TV', 'موبایل': 'Phone',
    'کامپیوتر': 'Computer', 'اینترنت': 'Internet', 'چراغ': 'Lamp',
    'گربه': 'Cat', 'سگ': 'Dog', 'اسب': 'Horse', 'گاو': 'Cow',
    'گوسفند': 'Sheep', 'بز': 'Goat', 'خروس': 'Rooster',
    'اردک': 'Duck', 'کبوتر': 'Pigeon', 'عقاب': 'Eagle',
    'شیر': 'Lion', 'ببر': 'Tiger', 'خرس': 'Bear', 'روباه': 'Fox',
    'گرگ': 'Wolf', 'خرگوش': 'Rabbit', 'موش': 'Mouse',
    'فیل': 'Elephant', 'زرافه': 'Giraffe', 'میمون': 'Monkey',
    'امروز': 'Today', 'دیروز': 'Yesterday', 'فردا': 'Tomorrow',
    'هفته': 'Week', 'ماه': 'Month', 'سال': 'Year',
    'بهار': 'Spring', 'تابستان': 'Summer', 'پاییز': 'Autumn',
    'زمستان': 'Winter', 'صبح': 'Morning', 'ظهر': 'Noon',
    'عصر': 'Evening', 'شب': 'Night', 'دقیقه': 'Minute',
    'ثانیه': 'Second', 'ساعت': 'Hour',
    'اینجا': 'Here', 'آنجا': 'There', 'بالا': 'Up', 'پایین': 'Down',
    'راست': 'Right', 'چپ': 'Left', 'جلو': 'Front', 'عقب': 'Back',
    'داخل': 'Inside', 'خارج': 'Outside', 'نزدیک': 'Near', 'دور': 'Far',
    'عشق': 'Love', 'نفرت': 'Hate', 'شادی': 'Happiness', 'غم': 'Sadness',
    'ترس': 'Fear', 'شجاعت': 'Courage', 'امید': 'Hope', 'صبر': 'Patience',
    'قرمز': 'Red', 'آبی': 'Blue', 'سبز': 'Green', 'زرد': 'Yellow',
    'سفید': 'White', 'سیاه': 'Black', 'بنفش': 'Purple',
    'نارنجی': 'Orange', 'صورتی': 'Pink', 'قهوه‌ای': 'Brown',
    'خاکستری': 'Gray', 'طلایی': 'Gold', 'نقره‌ای': 'Silver',
    'یک': 'One', 'دو': 'Two', 'سه': 'Three', 'چهار': 'Four',
    'پنج': 'Five', 'شش': 'Six', 'هفت': 'Seven', 'هشت': 'Eight',
    'نه': 'Nine', 'ده': 'Ten', 'بیست': 'Twenty', 'سی': 'Thirty',
    'چهل': 'Forty', 'پنجاه': 'Fifty', 'صد': 'Hundred',
    'هزار': 'Thousand', 'میلیون': 'Million',
    'آره': 'Yes', 'نه': 'No', 'بله': 'Yes', 'خیر': 'No',
    'اگر': 'If', 'اما': 'But', 'و': 'And', 'یا': 'Or',
    'پس': 'Then', 'زیرا': 'Because', 'چون': 'Because',
    'که': 'That', 'برای': 'For', 'به': 'To', 'از': 'From',
    'با': 'With', 'بدون': 'Without', 'درباره': 'About',
    'مثل': 'Like', 'مانند': 'Like',
    'سر': 'Head', 'مو': 'Hair', 'چشم': 'Eye', 'گوش': 'Ear',
    'بینی': 'Nose', 'دهان': 'Mouth', 'دندان': 'Tooth',
    'زبان': 'Tongue', 'گردن': 'Neck', 'شانه': 'Shoulder',
    'دست': 'Hand', 'انگشت': 'Finger', 'پا': 'Foot',
    'زانو': 'Knee', 'قلب': 'Heart', 'مغز': 'Brain',
    'کباب': 'Kebab', 'خورشت': 'Stew', 'آش': 'Soup',
    'سالاد': 'Salad', 'دسر': 'Dessert', 'غذا': 'Food',
    'صبحانه': 'Breakfast', 'نهار': 'Lunch', 'شام': 'Dinner',
    'ماشین': 'Car', 'اتوبوس': 'Bus', 'تاکسی': 'Taxi',
    'قطار': 'Train', 'هواپیما': 'Airplane', 'کشتی': 'Ship',
    'قایق': 'Boat', 'دوچرخه': 'Bicycle',
    'فوتبال': 'Football', 'والیبال': 'Volleyball',
    'بسکتبال': 'Basketball', 'تنیس': 'Tennis', 'شنا': 'Swimming',
    'فیزیک': 'Physics', 'شیمی': 'Chemistry',
    'زیست شناسی': 'Biology', 'ریاضی': 'Mathematics',
    'خدا': 'God', 'پیامبر': 'Prophet', 'قرآن': 'Quran',
    'نماز': 'Prayer', 'روزه': 'Fasting', 'ایمان': 'Faith',
    'کار': 'Work', 'درس': 'Lesson', 'امتحان': 'Exam',
    'تعطیلات': 'Holiday', 'مسافرت': 'Travel', 'خرید': 'Shopping',
    'پول': 'Money', 'قیمت': 'Price', 'تخفیف': 'Discount',
    'هدیه': 'Gift', 'جشن': 'Celebration', 'عروسی': 'Wedding',
    'تولد': 'Birthday', 'زندگی': 'Life', 'مرگ': 'Death',
    'تشنه': 'Thirsty', 'گرسنه': 'Hungry', 'خوشمزه': 'Delicious',
    'نرم': 'Soft', 'سفت': 'Hard', 'مرطوب': 'Wet', 'خشک': 'Dry',
    'تمیز': 'Clean', 'کثیف': 'Dirty', 'ساکت': 'Quiet',
    'روشن': 'Bright', 'تاریک': 'Dark', 'سنگین': 'Heavy',
    'سبک': 'Light', 'عمیق': 'Deep', 'وسیع': 'Wide', 'تنگ': 'Narrow',
    'طولانی': 'Long', 'کوتاه': 'Short', 'ضخیم': 'Thick',
    'نازک': 'Thin'
}

ENGLISH_TO_PERSIAN = {}
for persian, english in PERSIAN_TO_ENGLISH.items():
    english_lower = english.lower()
    if english_lower not in ENGLISH_TO_PERSIAN:
        ENGLISH_TO_PERSIAN[english_lower] = persian

# ==================== سخنگو فقط با API ====================
# پاسخ‌های ثابت قبلی حذف شده‌اند؛ تمام پاسخ‌های سخنگو از API دریافت می‌شوند.

# ==================== توابع ====================
def translate_text(text):
    text_lower = text.lower().strip()
    if text_lower in ENGLISH_TO_PERSIAN:
        return ENGLISH_TO_PERSIAN[text_lower]
    if text in PERSIAN_TO_ENGLISH:
        return PERSIAN_TO_ENGLISH[text]
    words = text.split()
    translated = []
    is_persian = any('\u0600' <= c <= '\u06FF' for c in text)
    for word in words:
        word_clean = word.strip('.,!?;:')
        word_lower = word_clean.lower()
        if is_persian:
            if word_clean in PERSIAN_TO_ENGLISH:
                translated.append(PERSIAN_TO_ENGLISH[word_clean])
            else:
                translated.append(word_clean)
        else:
            if word_lower in ENGLISH_TO_PERSIAN:
                translated.append(ENGLISH_TO_PERSIAN[word_lower])
            else:
                translated.append(word_clean)
    return ' '.join(translated)

def extract_user_id(text):
    if not text:
        return None
    match = re.search(r'(\d{5,})', text)
    return int(match.group(1)) if match else None

def contains_emoji(text):
    """تشخیص سبک‌وزن اموجی برای قفل اموجی؛ بدون وابستگی خارجی."""
    if not text:
        return False
    for ch in text:
        cp = ord(ch)
        if (0x1F000 <= cp <= 0x1FAFF or
            0x2600 <= cp <= 0x27BF or
            0x2300 <= cp <= 0x23FF or
            0x2B00 <= cp <= 0x2BFF):
            return True
    return False


def is_hang_code(text):
    if not text:
        return False

    # کاراکترهای کنترلی جهت متن (بمب RTL/LTR) - حتی یکی هم مشکوکه
    bidi_override_chars = ['\u202a', '\u202b', '\u202c', '\u202d', '\u202e',
                            '\u2066', '\u2067', '\u2068', '\u2069']
    if any(ch in text for ch in bidi_override_chars):
        return True

    # کاراکترهای با عرض صفر (zero-width) - سیل این کاراکترها معمولاً برای هنگ کردنه
    zero_width_count = sum(text.count(ch) for ch in ['\u200b', '\u200c', '\u200d', '\ufeff'])
    if zero_width_count > 30:
        return True

    # علامت‌های ترکیبی روی حروف (زالگو/zalgo) - انباشت زیاد باعث هنگ رندر میشه
    combining_count = sum(1 for ch in text if 0x0300 <= ord(ch) <= 0x036F)
    if combining_count > 20:
        return True

    return False

def is_advertisement(text):
    if not text or len(text) < 3:
        return False
    text_lower = text.lower()

    # سیگنال‌های قوی: به‌تنهایی کافی هستند چون تقریباً همیشه یعنی تبلیغ واقعی
    strong_patterns = [
        r't\.me\/\w+',
        r'splus\.ir\/\w+',
        r'https?://[^\s]+\.(com|org|net|ir|club|shop|store)',
        r'(\+98|0)?9\d{9}\b',                       # شماره موبایل ایران
        r'(کانال|چنل)\s*(ما|رو|را)?\s*(جوین|عضو)',   # دعوت به عضویت در کانال
        r'(سفارش|تماس)\s*(بدید|بدهید|بگیرید|بگیر)',  # فراخوان مستقیم برای تماس/سفارش
    ]
    for pattern in strong_patterns:
        if re.search(pattern, text_lower):
            return True

    # سیگنال‌های ضعیف: کلمات عادی روزمره‌اند، فقط وقتی حداقل ۲ تا با هم بیان
    # (مثلاً هم "فروش" هم "قیمت" تو یک پیام) به عنوان تبلیغ در نظر گرفته می‌شوند
    weak_words = ["خرید", "فروش", "تخفیف", "قیمت", "فروشنده", "فروشگاه", "محصول",
                  "خدمات", "تبلیغ", "آگهی", "سفارش", "خریدار", "فروخت", "فروخته", "فروشیم"]
    hits = sum(1 for word in weak_words if word in text_lower)
    return hits >= 2

# مدت زمانی (به ثانیه) که پیام‌های اخطار/هشدار (اسپم، فیلتر کلمه، تبلیغ، قفل‌ها و...)
# بعد از ارسال، خودکار پاک می‌شن تا گپ شلوغ نشه. قبلاً بعضی از این پیام‌ها اصلاً پاک
# نمی‌شدن و بعضی‌هاشون ۶۰۰ ثانیه (۱۰ دقیقه) طول می‌کشید که خیلی زیاده.
WARNING_AUTO_DELETE_SECONDS = 90  # ۱٫۵ دقیقه

# وقتی یک کاربر رگباری (پشت سر هم، با فاصله‌ی کم) چند تخلف مختلف انجام می‌ده
# (مثلاً پشت سر هم لینک/عکس/فحش می‌فرسته)، هر کدوم پیام اخطار جدا می‌گیره و چون
# همه‌شون هم‌زمان روی صفحه می‌مونن (هر کدوم تا ۱۵ ثانیه)، گپ به‌شدت شلوغ می‌شه.
# برای همین فقط تو همین حالت رگباری، پیام‌های اخطار خیلی سریع‌تر (۳ ثانیه) پاک
# می‌شن؛ یک تخلف تکی و جدا (نه پشت سر هم) هنوز همون ۱۵ ثانیه‌ی عادی رو داره.
RAPID_VIOLATION_WINDOW_SECONDS = 4
RAPID_WARNING_AUTO_DELETE_SECONDS = 25  # در حالت رگباری کمی سریع‌تر، ولی نه خیلی کوتاه
_LAST_VIOLATION_TIME = {}

# سقف پیام‌های اخطار/بن در حالت رگباری: بیش از این تعداد در یک بازه،
# پیام جدید ارسال نمی‌شود و پیام‌های قبلی اخطار پاک می‌شوند.
MAX_RAPID_MOD_MESSAGES = 3
RAPID_MOD_MSG_WINDOW_SECONDS = 45
_MOD_MSG_TRACKER = {}  # (chat_id, sender_id) -> [(msg_id, timestamp), ...]


def get_warning_delete_delay(chat_id, sender_id):
    """اگه آخرین تخلفِ همین کاربر (از هر نوعی که باشه: قفل، اسپم، فحش، تبلیغ،
    لینک، فیلتر کلمه) ظرف چند ثانیه‌ی اخیر بوده باشه، یعنی داره رگباری می‌ره؛
    در این حالت یک تایمِ پاک‌شدنِ سریع‌تر برمی‌گردونه. برای یک تخلف تکی/جدا،
    تایم عادی (WARNING_AUTO_DELETE_SECONDS) برمی‌گرده."""
    key = (chat_id, sender_id)
    now = time.time()
    last = _LAST_VIOLATION_TIME.get(key)
    _LAST_VIOLATION_TIME[key] = now
    if last is not None and (now - last) <= RAPID_VIOLATION_WINDOW_SECONDS:
        return RAPID_WARNING_AUTO_DELETE_SECONDS
    return WARNING_AUTO_DELETE_SECONDS


# ==================== قالب یکسان برای پیام‌های اخطار/بن/سکوت ====================
# همه‌ی پیام‌های مدیریتی کاربر، کوتاه و یکدست نمایش داده می‌شوند.
def _clean_reason(reason_text):
    return str(reason_text or "تخلف از قوانین").strip().rstrip(".!۰۱۲۳۴۵۶۷۸۹")


async def build_view_tag(user_id, chat_id=None):
    """تگِ «چه کسی» برای همه‌ی اخطارها/بن‌های خودکار (قفل‌ها، اسپم، فحش،
    تبلیغ، کد هنگی و ...) - اسمِ واقعیِ کاربر (همراهِ لقبش توی همین گروه، اگه
    داشته باشه)، به‌صورتِ لینکِ قابل‌کلیک به پروفایلش (اگه یوزرنیم داشته باشه)،
    وگرنه فقط اسمش پررنگ. قبلاً این تابع فقط یک لینکِ گنگِ «مشاهده» می‌ساخت که
    هیچ‌جا نمی‌گفت واقعاً کی اخطار گرفته - دقیقاً همون مشکل."""
    try:
        name = await get_user_name(client, user_id, chat_id)
        uname = await get_username(client, user_id)
        if uname and not str(uname).isdigit():
            return mention_link(name, user_id, uname)
        return mention_link(name, user_id, None)
    except Exception:
        pass
    return view_tag(user_id, None)


def _user_action_message(action, reason_text, tag=None, count=None, warn_limit=None):
    # برای اخطارهای خودکار، شناسه‌ی کاربر از context پیام جاری گرفته می‌شود تا
    # «مشاهده» واقعاً قابل کلیک باشد و کاربر را به PV همان شخص ببرد.
    # متن پیام بولد است؛ فقط خودِ لینک «مشاهده» (اگر لینک باشه) بیرون از ** می‌مونه
    # تا هم بولد بماند و هم لینک خراب نشود.
    if not tag:
        try:
            ctx = _HASHTAG_CONTEXT.get() or {}
            uid = ctx.get("user_id")
            if uid:
                tag = view_tag(uid)
        except Exception:
            pass
    reason = _clean_reason(reason_text)
    tag_text = tag or "مشاهده"
    # اگر تگ لینک مارک‌داون است، آن را از بولد جدا می‌کنیم تا لینک خراب نشود
    is_md_link = tag_text.startswith("[") and "](" in tag_text
    if action == "warn":
        # شماره‌ی اخطار: «اخطار چندم از سقف» تا کاربر بدونه چندمین اخطارشه.
        if count and warn_limit:
            body = f"به دلیل {reason} اخطار گرفت ({count} از {warn_limit}). ↺"
        elif count:
            body = f"به دلیل {reason} اخطار گرفت (اخطار {count}). ↺"
        else:
            body = f"به دلیل {reason} اخطار گرفت. ↺"
    elif action == "ban":
        body = f"به دلیل {reason} بن شد. ↺"
    elif action == "mute":
        body = f"به دلیل {reason} سکوت شد. ↺"
    else:
        body = f"{reason} ↺"
    if is_md_link:
        return f"**» کاربر »** {tag_text} **« {body}**"
    return f"**» کاربر » {tag_text} « {body}**"


def format_warn_message(reason_text, count=None, warn_limit=None, tag=None):
    return _user_action_message("warn", reason_text, tag=tag, count=count, warn_limit=warn_limit)


def format_ban_message(reason_text, tag=None):
    return _user_action_message("ban", reason_text, tag=tag)


def format_mute_message(reason_text, tag=None):
    return _user_action_message("mute", reason_text, tag=tag)


def format_lock_message(name, opened, action_label=None):
    """همون قالب کوتاه اخطارها (» ... « ... » ... ↺) برای پیام‌های باز/بسته‌شدن قفل."""
    if opened:
        return f"» قفل « {name} » باز شد. ↺"
    if action_label:
        return f"» قفل « {name} » بسته شد. عمل: {action_label} ↺"
    return f"» قفل « {name} » بسته شد. ↺"



async def auto_delete_warning(bot, chat_id, message_id, delay=WARNING_AUTO_DELETE_SECONDS):
    if message_id is None:
        return
    await asyncio.sleep(delay)
    try:
        # نکته‌ی مهم: کلاینت spluspy متد delete_message (مفرد) نداره - قبلاً همینجا
        # صدا زده می‌شد و چون متد وجود نداشت با خطا مواجه می‌شد که با except خالی
        # قورت داده می‌شد؛ در نتیجه پیام اخطار هیچ‌وقت واقعاً پاک نمی‌شد. متد درست
        # delete_messages (جمع) هست که یک لیست از آیدی‌ها می‌گیره - دقیقاً مثل جاهای
        # دیگه‌ی کد (clear_user_messages و ...).
        await bot.delete_messages(chat_id, [message_id])
    except Exception as e:
        safe_print(f"❌ خطا در پاک کردن خودکار پیام اخطار: {e}")

async def clear_user_messages(bot, chat_id, user_id, limit=100):
    try:
        messages = await bot.get_messages(chat_id, limit=limit)
        msg_ids = []
        for msg in messages:
            if msg.sender_id == user_id:
                msg_ids.append(msg.id)
        
        if msg_ids:
            for i in range(0, len(msg_ids), 100):
                batch = msg_ids[i:i+100]
                await bot.delete_messages(chat_id, batch)
            safe_print(f"🧹 {len(msg_ids)} پیام از کاربر {user_id} پاک شد")
            return len(msg_ids)
    except Exception as e:
        safe_print(f"❌ خطا در پاک کردن پیام‌های کاربر: {e}")
        return 0
    return 0


async def send_moderation_message(event, chat_id, text, sender_id=None):
    """ارسال مطمئن پیام اخطار/بن/سکوت.

    - لینک‌های tg:// حذف می‌شوند؛ لینک‌های https://splus.ir و **بولد** با parse_mode=md می‌مانند.
    - در حالت رگباری حداکثر MAX_RAPID_MOD_MESSAGES پیام اخطار برای هر کاربر ارسال می‌شود؛
      بعد از آن پیام‌های اخطار قبلی پاک می‌شوند و پیام جدید فرستاده نمی‌شود
      (تمرکز روی پاک‌کردن پیام تخلف، نه اسپم اخطار).
    """
    import re as _re
    if sender_id is None:
        try:
            sender_id = getattr(event, "sender_id", None)
        except Exception:
            sender_id = None

    # کنترل رگباری: بیش از ۳ پیام اخطار در بازه → پاک کن و ارسال نکن
    if sender_id is not None:
        key = (chat_id, sender_id)
        now = time.time()
        entries = [
            (mid, t) for mid, t in _MOD_MSG_TRACKER.get(key, [])
            if now - t < RAPID_MOD_MSG_WINDOW_SECONDS
        ]
        if len(entries) >= MAX_RAPID_MOD_MESSAGES:
            try:
                ids = [mid for mid, _ in entries if mid]
                if ids:
                    await client.delete_messages(chat_id, ids)
            except Exception as e:
                safe_print(f"⚠️ پاک‌سازی پیام‌های اخطار رگباری: {e}")
            _MOD_MSG_TRACKER[key] = []
            return None

    raw = str(text or "").strip() or "اخطار"
    cleaned = _re.sub(r"\[([^\]]+)\]\(tg://[^)]+\)", r"\1", raw)
    has_splus = bool(_re.search(r"\]\(https://splus\.ir/", cleaned))
    has_bold = "**" in cleaned
    cleaned = _re.sub(
        r"\[([^\]]+)\]\((?!https://splus\.ir/)[^)]+\)",
        r"\1",
        cleaned,
    )
    parse = "md" if (has_splus or has_bold) else None
    send_text = cleaned.strip() or "اخطار"
    last_err = None
    msg = None
    try:
        msg = await client.send_message(chat_id, send_text, parse_mode=parse)
    except Exception as e:
        last_err = e
        safe_print(f"⚠️ send_message ناموفق برای اخطار: {e}")
    if msg is None:
        try:
            msg = await event.respond(send_text, parse_mode=parse)
        except Exception as e:
            last_err = e
            safe_print(f"⚠️ event.respond ناموفق برای اخطار: {e}")
    if msg is None:
        try:
            msg = await event.reply(send_text, parse_mode=parse)
        except Exception as e:
            last_err = e
            safe_print(f"❌ ارسال پیام اخطار کامل ناموفق: {e}")
    if msg is None:
        raise last_err if last_err else RuntimeError("send_moderation_message failed")

    # ثبت برای سقف رگباری
    if sender_id is not None and getattr(msg, "id", None) is not None:
        key = (chat_id, sender_id)
        now = time.time()
        entries = [
            (mid, t) for mid, t in _MOD_MSG_TRACKER.get(key, [])
            if now - t < RAPID_MOD_MSG_WINDOW_SECONDS
        ]
        entries.append((msg.id, now))
        _MOD_MSG_TRACKER[key] = entries

    return msg



async def handle_lock_violation(event, chat_id, sender_id, label, lock_type=None, cache_ttl=4, cleanup_prev_count=0):
    """برای قفل‌های عمومی (عکس/ویدیو/صدا/فایل/گیف/استیکر/ایدی/فحش/فوروارد/ریپلای/کد هنگی/اسپم):
    پیام رو پاک می‌کنه. طبق «عمل» تنظیم‌شده‌ی همین قفل (با دستور «قفل X» یا «قفل X بن»
    یا «قفل X سکوت») سه رفتار ممکنه:
    - warn (پیش‌فرض): یک اخطار ثبت می‌کنه و طبق سقف اخطار گروه (پیش‌فرض ۳، با
      «تنظیم اخطار [عدد]» قابل تغییره) بعد از رسیدن به سقف بن می‌کنه؛ توی حالت
      سایلنت (Auto Mod) همون بار اول هم بدون اخطار مستقیم بن می‌کنه.
    - ban: بدون هیچ شمارشی، همون بار اول تخلف مستقیم بن می‌کنه.
    - mute: بدون هیچ شمارشی، همون بار اول تخلف مستقیم و دائم سکوت می‌کنه.
    cleanup_prev_count: برای قفل‌هایی مثل اسپم که پیام‌های قبلیِ همون کاربر هم مشکوک‌ان
    (متن/عکس/گیف فرقی نداره)، N تا پیام قبلی کاربر رو هم از کش پیدا و پاک می‌کنه."""
    # تگ «مشاهده» با یوزرنیم (لینک splus.ir) تا قابل کلیک باشه
    _tag = await build_view_tag(sender_id, chat_id)
    try:
        await event.delete()
    except Exception:
        pass
    if cleanup_prev_count:
        try:
            prev_ids = get_recent_sender_message_ids(chat_id, sender_id, exclude_msg_id=event.id, limit=cleanup_prev_count)
            if prev_ids:
                await client.delete_messages(chat_id, prev_ids)
        except Exception as e:
            safe_print(f"❌ خطا در پاکسازی پیام‌های قبلی (قفل {label}): {e}")

    lock_action = db.get_lock_action(chat_id, lock_type) if lock_type else "warn"

    # عمل «بن»: بدون هیچ اخطار/شمارشی، همون بار اول مستقیم بن.
    if lock_action == "ban":
        try:
            db.ban_user(chat_id, sender_id, f"قفل {label} (عمل: بن)")
            db.reset_user_warnings(chat_id, sender_id)
            try:
                await client.edit_permissions(chat_id, sender_id, send_messages=False)
            except Exception:
                pass
            msg = await send_moderation_message(event, chat_id, format_ban_message(label, tag=_tag), sender_id=sender_id)
            if msg:
                asyncio.create_task(auto_delete_warning(client, chat_id, msg.id))
        except Exception as e:
            safe_print(f"❌ خطا در بن قفل {label}: {e}")
        return

    # عمل «سکوت»: بدون هیچ اخطار/شمارشی، همون بار اول مستقیم و دائم سکوت.
    if lock_action == "mute":
        try:
            db.mute_user(chat_id, sender_id, "permanent", f"قفل {label} (عمل: سکوت)")
            try:
                await client.edit_permissions(chat_id, sender_id, send_messages=False)
            except Exception:
                pass
            msg = await send_moderation_message(event, chat_id, format_mute_message(label, tag=_tag), sender_id=sender_id)
            if msg:
                asyncio.create_task(auto_delete_warning(client, chat_id, msg.id))
        except Exception as e:
            safe_print(f"❌ خطا در سکوت قفل {label}: {e}")
        return

    settings = db.get_auto_mod_settings(chat_id)
    warn_limit = settings.get("warn_limit", 3)
    silent_mode = settings.get("silent_mode", False)

    # حالت سایلنت: هیچ پیام اخطاری داده نمی‌شه و اصلاً شمارش اخطار هم معنی نداره -
    # همون بار اول تخلف، مستقیم و سریع بن می‌شه.
    if silent_mode:
        try:
            db.ban_user(chat_id, sender_id, f"سایلنت: قفل {label}")
            db.reset_user_warnings(chat_id, sender_id)
            try:
                await client.edit_permissions(chat_id, sender_id, send_messages=False)
            except Exception:
                pass
            msg = await send_moderation_message(event, chat_id, format_ban_message(label, tag=_tag), sender_id=sender_id)
            if msg:
                asyncio.create_task(auto_delete_warning(client, chat_id, msg.id))
        except Exception as e:
            safe_print(f"❌ خطا در بن سایلنت قفل {label}: {e}")
        return

    try:
        count = db.add_user_warning(chat_id, sender_id, f"قفل {label}")
    except Exception:
        count = 1
    try:
        if count >= warn_limit:
            db.ban_user(chat_id, sender_id, f"رسیدن به {warn_limit} اخطار (قفل {label})")
            db.reset_user_warnings(chat_id, sender_id)
            try:
                await client.edit_permissions(chat_id, sender_id, send_messages=False)
            except Exception:
                pass
            msg = await send_moderation_message(event, chat_id, format_ban_message(f"رسیدن به {warn_limit} اخطار (قفل {label})", tag=_tag), sender_id=sender_id)
            if msg:
                asyncio.create_task(auto_delete_warning(client, chat_id, msg.id))
        else:
            # اول ارسال پیام اخطار، بعد پر کردن کش تا اگر ارسال خطا بدهد دوباره تلاش شود.
            cache_key = f"{chat_id}_{sender_id}_{label}_lock"
            if cache_key not in FILTER_WARN_CACHE:
                try:
                    warn_text = format_warn_message(f"ارسال {label} در این گروه قفل است", count, warn_limit, tag=_tag)
                    msg = await send_moderation_message(event, chat_id, warn_text, sender_id=sender_id)
                    if msg:
                        asyncio.create_task(auto_delete_warning(client, chat_id, msg.id, delay=get_warning_delete_delay(chat_id, sender_id)))
                    FILTER_WARN_CACHE[cache_key] = True
                    async def clear_cache(ck=cache_key, ttl=cache_ttl):
                        await asyncio.sleep(ttl)
                        FILTER_WARN_CACHE.pop(ck, None)
                    asyncio.create_task(clear_cache())
                    safe_print(f"✅ اخطار قفل {label} ارسال شد (count={count}/{warn_limit}) chat={chat_id} user={sender_id}")
                except Exception as e:
                    FILTER_WARN_CACHE.pop(cache_key, None)
                    safe_print(f"❌ خطا در ارسال پیام اخطار قفل {label}: {e}")
    except Exception as e:
        safe_print(f"❌ خطا در handle_lock_violation (قفل {label}): {e}")


MEDIA_FILTER_ACTION_LABELS = {"warn": "اخطار", "ban": "بن", "mute": "سکوت"}


async def handle_media_filter_violation(event, chat_id, sender_id, action, label):
    """ارسال یک رسانه‌ی «فیلترشده» (گیف/ویدیو/آهنگ/استیکر/عکس/فایل مشخص).

    پیام همیشه پاک می‌شه. بعدش طبق «اکشن» ثبت‌شده‌ی همون فیلتر:
    - warn (پیش‌فرض): یک اخطار ثبت و پیام اخطار فرستاده می‌شه؛ با رسیدن به سقف اخطار گروه، بن.
    - ban / mute: همون بار اول مستقیم بن / سکوت دائم.
    """
    _tag = await build_view_tag(sender_id, chat_id)
    try:
        await event.delete()
    except Exception:
        pass

    reason = f"ارسال {label} ممنوعه"
    settings = db.get_auto_mod_settings(chat_id)
    warn_limit = settings.get("warn_limit", 3)
    silent_mode = settings.get("silent_mode", False)

    async def _notify(text, delay=None):
        try:
            msg = await send_moderation_message(event, chat_id, text, sender_id=sender_id)
            if msg:
                if delay is None:
                    asyncio.create_task(auto_delete_warning(client, chat_id, msg.id))
                else:
                    asyncio.create_task(auto_delete_warning(client, chat_id, msg.id, delay=delay))
        except Exception as e:
            safe_print(f"❌ خطا در ارسال پیام فیلتر رسانه: {e}")

    async def _ban(why):
        try:
            db.ban_user(chat_id, sender_id, why)
            db.reset_user_warnings(chat_id, sender_id)
            try:
                await client.edit_permissions(chat_id, sender_id, send_messages=False)
            except Exception:
                pass
            await _notify(format_ban_message(reason, tag=_tag))
        except Exception as e:
            safe_print(f"❌ خطا در بن فیلتر رسانه: {e}")

    if action == "ban":
        await _ban(f"فیلتر رسانه ({label})")
        return

    if action == "mute":
        try:
            db.mute_user(chat_id, sender_id, "permanent", f"فیلتر رسانه ({label})")
            try:
                await client.edit_permissions(chat_id, sender_id, send_messages=False)
            except Exception:
                pass
            await _notify(format_mute_message(reason, tag=_tag))
        except Exception as e:
            safe_print(f"❌ خطا در سکوت فیلتر رسانه: {e}")
        return

    # حالت سایلنت: مثل بقیه‌ی قفل‌ها بدون پیام اخطار؛ اعضا همون بار اول بن می‌شن.
    if silent_mode:
        await _ban(f"سایلنت: فیلتر رسانه ({label})")
        return

    try:
        count = db.add_user_warning(chat_id, sender_id, f"فیلتر رسانه ({label})")
    except Exception:
        count = 1

    if count >= warn_limit:
        await _ban(f"رسیدن به {warn_limit} اخطار (فیلتر رسانه)")
        return

    # اگه کاربر پشت‌سرهم همون رسانه رو بفرسته، فقط یک پیام اخطار (نه یکی برای هر پیام).
    cache_key = f"{chat_id}_{sender_id}_media_filter"
    if cache_key not in FILTER_WARN_CACHE:
        FILTER_WARN_CACHE[cache_key] = True

        async def _clear_cache(ck=cache_key):
            await asyncio.sleep(4)
            FILTER_WARN_CACHE.pop(ck, None)
        asyncio.create_task(_clear_cache())
        await _notify(
            format_warn_message(reason, count, warn_limit, tag=_tag),
            delay=get_warning_delete_delay(chat_id, sender_id),
        )


# مدت اعتبار «کامل و مطمئن» کش ادمین‌های گروه. تا این مدت، اصلاً اسکن جدید
# (حتی در پس‌زمینه) لازم نیست.
GROUP_ADMINS_CACHE_FRESH_SECONDS = 1800   # ۳۰ دقیقه
GROUP_ADMINS_REFRESHING = set()

async def _scan_group_admins(chat_id):
    """اسکن مقاوم ادمین‌های واقعی گروه با SplusLib.

    اول فیلتر ادمین‌ها را امتحان می‌کنیم؛ اگر backend/نوع گروه فیلتر را
    پشتیبانی نکرد، بدون فیلتر اعضا را می‌گیریم و نقش participant را خودمان
    تشخیص می‌دهیم. به همین دلیل فعال‌سازی به پیام همان ادمین وابسته نیست.
    """
    from spluslib._base.tl.types import (
        ChannelParticipantsAdmins,
        ChannelParticipantAdmin, ChannelParticipantCreator,
        ChatParticipantAdmin, ChatParticipantCreator,
    )

    def is_admin_role(role):
        if role is None:
            return False, False
        creator = isinstance(role, (ChannelParticipantCreator, ChatParticipantCreator))
        admin = isinstance(role, (ChannelParticipantAdmin, ChatParticipantAdmin))
        # fallback برای نسخه‌هایی که کلاس participant از namespace دیگری ساخته شده.
        role_name = type(role).__name__.lower()
        if 'creator' in role_name or role_name.endswith('owner'):
            creator = True
            admin = True
        elif 'admin' in role_name:
            admin = True
        return admin, creator

    admins = {}
    creator_id = None

    async def consume(participants):
        nonlocal creator_id
        count = 0
        for user in participants or []:
            uid = getattr(user, 'id', None)
            if not uid:
                continue
            role = getattr(user, 'participant', None)
            is_admin, is_creator = is_admin_role(role)
            if is_admin:
                admins[int(uid)] = user
                count += 1
                if is_creator:
                    creator_id = int(uid)
        return count

    try:
        # مسیر سریع: خود سرور فقط ادمین‌ها را برگرداند.
        participants = await client.get_participants(
            chat_id, filter=ChannelParticipantsAdmins
        )
        await consume(participants)
    except Exception as e:
        print(f"[admin_scan] فیلتر ادمین SplusLib شکست خورد، fallback اجرا می‌شود: {e}")

    # fallback قطعی‌تر برای گروه‌هایی که فیلتر Admin را درست برنمی‌گردانند.
    # حتی اگر مسیر اول لیست خالی بدهد، این مسیر حتماً امتحان می‌شود.
    if not admins:
        try:
            participants = await client.get_participants(chat_id)
            await consume(participants)
        except Exception as e:
            print(f"[admin_scan] اسکن بدون فیلتر هم شکست خورد: {e}")

    # creator را اگر participant نقش واضح نداشت، از مالک ذخیره‌شده هم بازیابی کن.
    if creator_id is None:
        try:
            saved_owner = db.get_group_owner(chat_id)
            if saved_owner:
                creator_id = int(saved_owner)
                admins.setdefault(creator_id, None)
        except Exception:
            pass

    admin_ids = list(admins.keys())
    result = {"admins": admin_ids, "creator": creator_id}
    GROUP_ADMINS_CACHE[chat_id] = result
    GROUP_ADMINS_CACHE_TIME[chat_id] = time.time()
    print(f"[admin_scan] گروه {chat_id}: {len(admin_ids)} ادمین، creator={creator_id}")
    GROUP_ADMINS_REFRESHING.discard(chat_id)
    return result


async def _get_real_group_creator(chat_id):
    """آیدیِ سازنده‌ی واقعیِ گروه (طبق سروش) رو برمی‌گردونه؛ اگه تشخیص ندادیم
    None برمی‌گردونه. موقعِ «فعال» استفاده می‌شه: مالکِ ربات باید سازنده‌ی
    واقعیِ گروه باشه، نه لزوماً همون ادمینی که دستورِ «فعال» رو زده."""
    try:
        admins_data = await _scan_group_admins(chat_id)
        creator_id = admins_data.get("creator")
        return int(creator_id) if creator_id else None
    except Exception:
        return None


async def get_group_admins(chat_id):
    """قبلاً هر ۵ دقیقه یک‌بار (سر اولین دستوری که بهش نیاز داشت، مثلاً «آمار
    گروه» یا فیلترها) کل اسکن سنگین بالا به‌صورت سنکرون اجرا می‌شد و کاربر
    باید منتظر صدها درخواست پشت‌سرهم به سروش می‌موند. حالا: اگه کش هنوز
    کاملاً تازه‌ست (کمتر از ۳۰ دقیقه) همون لحظه برگردونده می‌شه. اگه کش
    قدیمی‌تر ولی موجوده، همون کش قدیمی فوری برگردونده می‌شه (تا کاربر معطل
    نشه) و بروزرسانی‌ش در پس‌زمینه (بدون مسدود کردن پاسخ) شروع می‌شه. فقط
    وقتی هیچ کشی از قبل وجود نداره (اولین بار)، مجبوریم منتظر اسکن بمونیم."""
    now = time.time()
    cache_age = now - GROUP_ADMINS_CACHE_TIME.get(chat_id, 0)

    if chat_id in GROUP_ADMINS_CACHE:
        if cache_age < GROUP_ADMINS_CACHE_FRESH_SECONDS:
            return GROUP_ADMINS_CACHE[chat_id]
        # کش قدیمیه ولی وجود داره: همینو برگردون و بروزرسانی رو پس‌زمینه بذار.
        if chat_id not in GROUP_ADMINS_REFRESHING:
            GROUP_ADMINS_REFRESHING.add(chat_id)
            asyncio.create_task(_scan_group_admins(chat_id))
        return GROUP_ADMINS_CACHE[chat_id]

    # هیچ کشی وجود نداره (اولین بار) - چاره‌ای جز منتظرموندن نیست.
    if chat_id not in GROUP_ADMINS_REFRESHING:
        GROUP_ADMINS_REFRESHING.add(chat_id)
        return await _scan_group_admins(chat_id)
    # یه اسکن دیگه همین الان در حال اجراست؛ چند لحظه صبر کن و کش رو چک کن.
    for _ in range(50):
        await asyncio.sleep(0.1)
        if chat_id in GROUP_ADMINS_CACHE:
            return GROUP_ADMINS_CACHE[chat_id]
    return {"admins": [], "creator": None}

async def _record_group_activation_safe(chat_id):
    """رد‌کردن این گروه به‌عنوان «فعال‌شده» توی جدول روزشمار (group_activation).
    قبلاً این کار فقط وقتی انجام می‌شد که یکی دستی توی گروه بنویسه «فعال»؛ ولی
    مالکیت گروه از چند مسیر دیگه هم به‌صورت خودکار تشخیص داده و ثبت می‌شه (وقتی
    ربات به گروه اضافه می‌شه و سازنده‌ی گروه شناخته می‌شه، یا وقتی سازنده‌ی گروه
    برای اولین بار پیام می‌ده) - و توی اون مسیرها روزشمار اصلاً ثبت نمی‌شد، برای
    همین «لیست روزشمار» گروه‌هایی که این‌طوری فعال شده بودن رو نشون نمی‌داد، در
    حالی که ربات توشون کاملاً فعال و کاربردی بود. حالا هر مسیری که مالک گروه رو
    ثبت می‌کنه، همینجا هم روزشمار رو ثبت می‌کنه (idempotent - اگه قبلاً ثبت شده
    باشه فقط اسم گروه آپدیت می‌شه، تاریخ فعال‌سازی دست‌نخورده می‌مونه)."""
    try:
        try:
            group_title = (await client.get_entity(chat_id)).title
        except Exception:
            group_title = await resolve_group_name(chat_id)
        db.record_group_activation(chat_id, group_title)
    except Exception:
        pass


async def sync_group_admins(chat_id):
    """ادمین‌ها و سازنده‌ی واقعی گروه (سروش/تلگرام) رو یکجا می‌گیره و بدون نیاز به
    دستور دستی «تنظیم ادمین»، همه‌شون رو به‌عنوان ادمین ربات در همین گروه ثبت می‌کنه.
    این تابع موقع اضافه‌شدن ربات به گروه صدا زده می‌شه تا ادمین‌ها همون لحظه شناخته بشن،
    بدون این‌که لازم باشه هر ادمین اول یه پیام بفرسته.
    دستور «تنظیم ادمین» برای شرایط خاص (مثلاً تعیین یه نفر به‌عنوان ادمین ربات بدون
    این‌که ادمین واقعی گروه باشه) همچنان در دسترسه و این همگام‌سازی جایگزینش نمی‌شه."""
    try:
        admins_data = await get_group_admins(chat_id)
    except Exception:
        return
    admin_ids = admins_data.get("admins", []) or []
    creator_id = admins_data.get("creator")
    for admin_id in admin_ids:
        try:
            if not db.is_group_admin(chat_id, admin_id):
                db.add_group_admin(chat_id, admin_id)
        except Exception:
            pass
    if creator_id and not db.get_group_owner(chat_id):
        try:
            db.set_group_owner(chat_id, creator_id)
            db.add_group_admin(chat_id, creator_id)
        except Exception:
            pass
        else:
            await _record_group_activation_safe(chat_id)


async def announce_and_register_group_admins(chat_id, send_message=True):
    """در فعال‌سازی: ادمین‌های واقعی گروه را پیدا و تگ می‌کند (بدون ثبتِ خودکار
    در جدول‌های «ویژه»/«ادمین» - آن دو فقط با دستورهای دستی پر می‌شوند)."""
    print(f"[admin_announce] شروع برای گروه {chat_id}")
    global BOT_ID
    try:
        if BOT_ID is None:
            BOT_ID = (await client.get_me()).id
    except Exception as e:
        print(f"[admin_announce] دریافت BOT_ID ناموفق: {e}")

    # چند تلاش کوتاه چون بلافاصله بعد از فعال‌سازی ممکن است participant cache
    # هنوز از سمت سرور کامل نشده باشد.
    admins_data = {"admins": [], "creator": None}
    for attempt in range(1, 4):
        try:
            # برای فعال‌سازی همیشه اسکن تازه انجام شود، نه کش قدیمی.
            GROUP_ADMINS_CACHE.pop(chat_id, None)
            GROUP_ADMINS_CACHE_TIME.pop(chat_id, None)
            GROUP_ADMINS_REFRESHING.discard(chat_id)
            admins_data = await _scan_group_admins(chat_id)
            if admins_data.get("admins"):
                break
        except Exception as e:
            print(f"[admin_announce] تلاش {attempt} برای اسکن شکست خورد: {e}")
        if attempt < 3:
            await asyncio.sleep(1.2)

    scanned_ids = list(admins_data.get("admins") or [])
    creator_id = admins_data.get("creator")
    if creator_id and creator_id not in scanned_ids:
        scanned_ids.append(creator_id)

    try:
        known_ids = list(db.get_group_admins(chat_id) or [])
    except Exception:
        known_ids = []
    try:
        owner_id = db.get_group_owner(chat_id)
    except Exception:
        owner_id = None

    admin_ids = []
    for aid in scanned_ids + known_ids + ([owner_id] if owner_id else []):
        if aid and aid != BOT_ID and aid not in admin_ids:
            admin_ids.append(int(aid))

    print(f"[admin_announce] گروه {chat_id}: live={scanned_ids}, known={known_ids}, owner={owner_id}, final={admin_ids}")
    if not admin_ids:
        print(f"[admin_announce] هیچ ادمینی پیدا نشد؛ پیام تگ ارسال نمی‌شود.")
        return False

    # نکته: قبلاً اینجا هر ادمینِ واقعیِ اسکن‌شده هم‌زمان توی هر دو جدولِ «ویژه»
    # (group_admins) و «ادمین» (group_special_admins) ثبت می‌شد؛ چون هر دو از یک
    # حلقه‌ی db.add_group_admin(...) + db.add_group_special_admin(...) پر می‌شدن،
    # «لیست ویژه» و «لیست ادمین» همیشه دقیقاً همون آدم‌ها رو نشون می‌دادن.
    # طبق مقام‌بندیِ فعلی، ادمین‌های واقعیِ گروه موقعِ «فعال»‌سازی باید فقط توی
    # تیرِ «ادمین» (همون چیزی که قبلاً «ادمین‌ویژه» بود، جدول group_special_admins)
    # ثبت بشن؛ تیرِ «ویژه» (group_admins) جداست و فقط با دستورِ دستیِ «تنظیم ویژه»
    # پر می‌شه.
    tag_lines = []
    registered = 0
    for admin_id in admin_ids:
        try:
            if not db.is_group_special_admin(chat_id, admin_id):
                db.add_group_special_admin(chat_id, admin_id)
            registered += 1
        except Exception as e:
            print(f"[admin_announce] ثبت {admin_id} شکست خورد: {e}")
        try:
            name = await get_user_name(client, admin_id, chat_id)
            username = await get_username(client, admin_id)
            if username == str(admin_id):
                username = None
            # نام ادمین‌ها عمداً نمایش داده نمی‌شود تا راست‌چین/چپ‌چین شدن
            # نام فارسی و انگلیسی چیدمان پیام را به‌هم نزند؛ فقط «مشاهده» نشان داده می‌شود.
            tag_lines.append(f"│  ✦ {view_tag(admin_id, username)}")
            registered += 1
        except Exception as e:
            print(f"[admin_announce] ساخت تگ {admin_id} شکست خورد: {e}")

    if creator_id and creator_id != BOT_ID:
        try:
            db.set_group_owner(chat_id, int(creator_id))
            db.add_group_admin(chat_id, int(creator_id))
        except Exception as e:
            print(f"[admin_announce] ثبت مالک {creator_id} شکست خورد: {e}")

    if not tag_lines:
        return registered > 0

    msg = (
        "╭─〔 🛡️ **مدیران گروه** 〕\n"
        "│\n"
        "│  ادمین‌های گروه شناسایی و ثبت شدند.\n"
        "│  ─────────────────────────\n"
        + "\n".join(tag_lines)
        + "\n│\n"
        "╰─〔 🔗 برای ورود به پی‌وی هرکدوم، روی تگش بزن 〕"
    )
    if not send_message:
        return msg
    try:
        await client.send_message(chat_id, msg)
        print(f"[admin_announce] پیام تگ {len(tag_lines)} ادمین ارسال شد؛ {registered} ثبت شد.")
        return True
    except Exception as e:
        print(f"[admin_announce] ثبت انجام شد ولی ارسال پیام تگ شکست خورد: {e}")
        return False


# ==================== دسترسیِ پنلِ مرکزی + فعال‌سازیِ خودکار ====================
# فقط این سطح‌ها اجازه‌ی افزودن/مدیریتِ گروه از پنلِ مرکزی (manager.py) دارن.
# عمداً «db_admin» (ادمینِ عمومیِ همین سلف) و «member» توش نیست.
PANEL_ALLOWED_LEVELS = ("super_owner", "creator", "admin", "group_owner")
_PANEL_ACCESS_CACHE = {}     # (chat_id, user_id) -> (timestamp, level)
_PANEL_ACCESS_TTL = 20
_PANEL_JOINS_IN_FLIGHT = 0   # تعداد جوین‌های پنل که هنوز چکِ ادمین‌شون تموم نشده
AUTO_ACTIVATED_AT = {}       # chat_id -> timestamp آخرین فعال‌سازیِ خودکار
_AUTO_ACTIVATE_INFLIGHT = set()


async def get_panel_access_level(chat_id, user_id, fresh=False):
    """سطحِ واقعیِ کاربر در همین گروه رو (زنده از سروش) برمی‌گردونه:
    super_owner / creator / admin / group_owner / member / unknown.
    نتیجه‌ی زنده‌ی «member» بر ثبتِ قدیمیِ دیتابیس غلبه می‌کنه (ادمینِ برکنارشده
    دیگه دسترسی نداره). فقط وقتی سروش جواب نده (unknown) به مالکِ ثبت‌شده‌ی
    دیتابیس برمی‌گردیم. هر خطایی = عدمِ دسترسی (fail-closed)."""
    try:
        chat_id = int(chat_id)
        user_id = int(user_id)
    except (TypeError, ValueError):
        return "unknown"
    if user_id in SUPER_OWNER_IDS:
        return "super_owner"

    key = (chat_id, user_id)
    now = time.time()
    if not fresh:
        hit = _PANEL_ACCESS_CACHE.get(key)
        if hit and now - hit[0] < _PANEL_ACCESS_TTL:
            return hit[1]

    live = None
    try:
        perms = await client.get_permissions(chat_id, user_id)
        if getattr(perms, "is_creator", False):
            live = "creator"
        elif getattr(perms, "is_admin", False):
            live = "admin"
        else:
            live = "member"
    except Exception:
        live = None

    level = live
    if live not in ("creator", "admin"):
        # بعضی بک‌اندها نقش رو از get_permissions نمی‌دن؛ لیستِ ادمین‌ها رو هم چک کن.
        try:
            data = await (_scan_group_admins(chat_id) if fresh else get_group_admins(chat_id))
            if data.get("creator") is not None and int(data["creator"]) == user_id:
                level = "creator"
            elif user_id in [int(a) for a in (data.get("admins") or [])]:
                level = "admin"
        except Exception:
            pass
    if level not in ("creator", "admin"):
        if live is None:
            try:
                level = "group_owner" if db.is_group_owner(chat_id, user_id) else "unknown"
            except Exception:
                level = "unknown"
        else:
            level = "member"

    if level == "creator":
        try:
            if not db.get_group_owner(chat_id):
                db.set_group_owner(chat_id, user_id)
                db.add_group_admin(chat_id, user_id)
        except Exception:
            pass
    _PANEL_ACCESS_CACHE[key] = (now, level)
    if len(_PANEL_ACCESS_CACHE) > 5000:
        for k in sorted(_PANEL_ACCESS_CACHE, key=lambda k: _PANEL_ACCESS_CACHE[k][0])[:1000]:
            _PANEL_ACCESS_CACHE.pop(k, None)
    return level


async def auto_activate_group(chat_id, group_name=None, force=False):
    """معادلِ «فعال» ولی بدونِ اینکه کسی توی گروه چیزی بنویسه: مالکِ واقعی و
    ادمین‌ها رو از خودِ سروش می‌گیره، مالکِ گروه رو ثبت می‌کنه، روزشمار و
    تنظیماتِ کارخانه رو اعمال می‌کنه و یک پیامِ کوتاه با لیستِ مدیران می‌فرسته.
    idempotent: برای یک گروه در ۱۰ دقیقه بیشتر از یک‌بار اجرا نمی‌شه."""
    global BOT_ID
    try:
        chat_id = int(chat_id)
    except (TypeError, ValueError):
        return None
    now = time.time()
    if not force and now - AUTO_ACTIVATED_AT.get(chat_id, 0) < 600:
        return None
    if chat_id in _AUTO_ACTIVATE_INFLIGHT:
        return None
    _AUTO_ACTIVATE_INFLIGHT.add(chat_id)
    try:
        AUTO_ACTIVATED_AT[chat_id] = now
        if BOT_ID is None:
            try:
                BOT_ID = (await client.get_me()).id
            except Exception:
                pass

        # اسکنِ تازه (چند تلاش، چون بلافاصله بعد از ورود ممکنه لیست کامل نباشه)
        admins_data = {"admins": [], "creator": None}
        for attempt in range(3):
            try:
                GROUP_ADMINS_CACHE.pop(chat_id, None)
                GROUP_ADMINS_CACHE_TIME.pop(chat_id, None)
                GROUP_ADMINS_REFRESHING.discard(chat_id)
                admins_data = await _scan_group_admins(chat_id)
                if admins_data.get("creator") or admins_data.get("admins"):
                    break
            except Exception as e:
                safe_print(f"[auto_activate] اسکن {chat_id} تلاش {attempt + 1}: {e}")
            await asyncio.sleep(1.2)

        creator_id = admins_data.get("creator")
        if creator_id and BOT_ID is not None and int(creator_id) == int(BOT_ID):
            creator_id = None

        newly_owned = False
        if creator_id:
            try:
                if db.set_group_owner(chat_id, int(creator_id)):
                    newly_owned = True
                db.add_group_admin(chat_id, int(creator_id))
                GROUP_OWNER_CACHE[chat_id] = db.get_group_owner(chat_id) or int(creator_id)
            except Exception as e:
                safe_print(f"[auto_activate] ثبت مالک: {e}")
        await _record_group_activation_safe(chat_id)

        if newly_owned:
            # همون «تنظیمات کارخانه‌ی اولین فعال»
            try:
                db.set_lock(chat_id, "link", True, action="warn")
                db.set_lock(chat_id, "spam", True, action="warn")
                db.set_lock(chat_id, "hangcode", True, action="ban")
                db.set_auto_mod_settings(chat_id, spam_enabled=True, silent_mode=False)
            except Exception as e:
                safe_print(f"[auto_activate] تنظیمات کارخانه: {e}")

        # ثبتِ ادمین‌ها در تیرِ «ادمین» (بدونِ ارسالِ پیام؛ متن رو خودمون می‌سازیم)
        admin_msg = None
        try:
            admin_msg = await announce_and_register_group_admins(chat_id, send_message=False)
        except Exception as e:
            safe_print(f"[auto_activate] ثبت ادمین‌ها: {e}")

        try:
            owner_id = db.get_group_owner(chat_id)
        except Exception:
            owner_id = None
        lines = [f"✅ **ربات «{BOT_DISPLAY_LABEL}» به‌صورت خودکار فعال شد**", ""]
        if owner_id:
            try:
                owner_view = view_tag(owner_id, await get_username(client, owner_id))
                lines.append(f"👑 **مالک گروه:** {owner_view}")
            except Exception:
                lines.append("👑 مالک گروه شناسایی و ثبت شد.")
        else:
            lines.append("⚠️ مالکِ گروه از سروش مشخص نشد؛ یکی از ادمین‌ها بنویسه `فعال`.")
        if isinstance(admin_msg, str) and admin_msg:
            lines += ["", admin_msg]
        lines += ["", "🔹 برای مشاهده‌ی دستورها بنویسید `راهنما`", "🔹 برای کارکردِ کاملِ قفل‌ها، ربات رو ادمین کنید."]
        try:
            await client.send_message(chat_id, "\n".join(lines))
        except Exception as e:
            safe_print(f"[auto_activate] ارسالِ پیامِ فعال‌سازی ناموفق: {e}")

        return {"creator": creator_id, "admins": list(admins_data.get("admins") or [])}
    finally:
        _AUTO_ACTIVATE_INFLIGHT.discard(chat_id)


async def _on_bot_added_to_group(chat_id):
    """ربات به گروه اضافه شد (دستی یا با هر روشی): فعال‌سازیِ خودکار + اعلام به
    رجیستری. اگه همون لحظه یک جوینِ پنلِ مرکزی در حالِ چکِ ادمینه، صبر می‌کنیم
    تا اگه ربات به‌خاطرِ «ادمین نبودنِ درخواست‌دهنده» از گروه خارج شد، هیچ
    پیام/ثبتی توی اون گروه انجام نشه."""
    waited = 0.0
    while _PANEL_JOINS_IN_FLIGHT > 0 and waited < 60:
        await asyncio.sleep(1.5)
        waited += 1.5
    if waited:
        try:
            await client.get_permissions(chat_id, 'me')
        except Exception:
            return  # دیگه عضو نیستیم
    await auto_activate_group(chat_id)
    if GROUP_REGISTRY_API_URL:
        try:
            group_title = getattr(await client.get_entity(chat_id), 'title', None)
        except Exception:
            group_title = None
        asyncio.create_task(registry_announce_group(chat_id, group_title))

PERMISSION_CACHE = {}
PERMISSION_CACHE_TIME = {}
AUTO_REGISTERED_ADMINS = set()

async def check_user_permission(chat_id, user_id):
    if user_id in SUPER_OWNER_IDS:
        return "super_owner"

    cache_key = (chat_id, user_id)
    now = time.time()
    if cache_key in PERMISSION_CACHE and (now - PERMISSION_CACHE_TIME.get(cache_key, 0)) < 120:
        cached = PERMISSION_CACHE[cache_key]
    else:
        cached = None
        try:
            perms = await client.get_permissions(chat_id, user_id)
            if getattr(perms, 'is_creator', False):
                cached = "creator"
            elif getattr(perms, 'is_admin', False):
                cached = "admin"
            else:
                cached = "member"
        except Exception:
            cached = None
        PERMISSION_CACHE[cache_key] = cached
        PERMISSION_CACHE_TIME[cache_key] = now

    if cached in ("creator", "admin"):
        # نکته: قبلاً اینجا هر ادمینِ واقعیِ گروه (نه فقط سازنده) با
        # db.add_group_admin(...) به‌صورت خودکار توی جدولِ «ویژه» ثبت می‌شد - همین
        # باعث می‌شد «لیست ویژه» با ادمین‌های واقعیِ گروه قاطی بشه. حالا فقط
        # تشخیصِ خودکارِ «مالک گروه» (سازنده) حفظ شده؛ عضویت در تیرِ «ویژه» فقط با
        # دستورِ دستیِ «تنظیم ویژه» ثبت می‌شه.
        if cache_key not in AUTO_REGISTERED_ADMINS:
            newly_became_owner = False
            try:
                if cached == "creator" and not db.is_group_owner(chat_id, user_id):
                    db.set_group_owner(chat_id, user_id)
                    if not db.is_group_admin(chat_id, user_id):
                        db.add_group_admin(chat_id, user_id)
                    newly_became_owner = True
            except Exception:
                pass
            AUTO_REGISTERED_ADMINS.add(cache_key)
            if newly_became_owner:
                await _record_group_activation_safe(chat_id)
        return cached
    # اگه تشخیص خودکار جواب نداد (مثلاً خطای شبکه)، به ثبت دستی/قبلی داخل ربات برمی‌گردیم
    if db.is_group_owner(chat_id, user_id):
        return "group_owner"
    if db.is_admin(user_id):
        return "db_admin"
    return "member"

def is_group_owner_check(chat_id, user_id):
    if db.is_group_owner(chat_id, user_id):
        return True
    if chat_id in GROUP_OWNER_CACHE and GROUP_OWNER_CACHE[chat_id] == user_id:
        return True
    return False

# ==================== تشخیصِ دلیلِ شکستِ بن/رفع‌بن (edit_permissions) ====================
# خطای «Chat admin privileges are required...» که موقع بن/رفع‌بن دیده می‌شه، همیشه
# یعنی خودِ سروش (نه ربات) داره درخواستِ EditBannedRequest رو رد می‌کنه؛ سه دلیلِ
# شناخته‌شده‌ی رایج داره:
#   ۱) خودِ ربات توی اون گروه اصلاً ادمین نیست.
#   ۲) ربات ادمینه ولی دسترسیِ مشخصِ «محدودکردن اعضا» (ban users) رو نداره.
#   ۳) کاربرِ هدف خودش ادمین یا سازنده‌ی همون گروهه - سروش هیچ‌وقت اجازه نمی‌ده
#      سطحِ دسترسیِ پیام‌دهیِ یک ادمین/سازنده مستقیم با edit_permissions تغییر کنه.
# قبلاً این خطا فقط عیناً (متنِ خامِ اکسپشن) به ادمین گروه نشون داده می‌شد و
# رکوردِ دیتابیس (بن/رفع‌بن نرم‌افزاری - همونی که پیام‌های کاربر رو حذف می‌کنه)
# بدونِ هیچ توضیحی جداگانه ثبت/حذف می‌شد؛ الان قبل از تلاش، این چک‌های ارزون‌قیمت
# انجام می‌شن تا به‌جای خطای گنگ، دلیلِ دقیق و قابل‌اقدام نشون داده بشه.
async def _get_own_restrict_permission_issue(chat_id):
    """اگه خودِ ربات امکانِ محدودکردن/رفع‌محدودیتِ کاربر توی این گروه رو نداشته
    باشه، یه توضیحِ فارسیِ قابل‌فهم برمی‌گردونه؛ وگرنه None."""
    try:
        me = await client.get_me()
        bot_perms = await client.get_permissions(chat_id, me.id)
    except Exception:
        return None  # نتونستیم بررسی کنیم؛ بذار خودِ درخواستِ اصلی امتحانش کنه

    if not (getattr(bot_perms, 'is_admin', False) or getattr(bot_perms, 'is_creator', False)):
        return "ربات در این گروه **ادمین نیست**؛ برای بن/رفع‌بنِ واقعی (نه فقط دیتابیسی) باید ربات رو ادمین کنید."

    if not getattr(bot_perms, 'is_creator', False) and getattr(bot_perms, 'ban_users', None) is False:
        return "ربات توی این گروه ادمینه ولی دسترسیِ «محدودکردن اعضا» (Ban Users) رو نداره؛ این دسترسی رو براش فعال کنید."

    return None

async def _get_target_restrict_issue(chat_id, target_id):
    """اگه کاربرِ هدف خودش سازنده/ادمینِ همین گروه باشه (که با edit_permissions
    قابل‌محدودکردن نیست)، توضیح برمی‌گردونه؛ وگرنه None."""
    try:
        target_perms = await client.get_permissions(chat_id, target_id)
    except Exception:
        return None

    if getattr(target_perms, 'is_creator', False):
        return "این کاربر **سازنده‌ی همین گروه** است؛ هیچ رباتی نمی‌تونه سطحِ دسترسیِ سازنده رو تغییر بده."
    if getattr(target_perms, 'is_admin', False):
        return ("این کاربر **ادمینِ خودِ گروه** است؛ سروش تغییرِ مستقیمِ سطحِ پیام‌دهیِ یک ادمین رو نمی‌پذیره "
                "(دقیقاً همون خطای «Chat admin privileges required»). اگه واقعاً لازمه، اول با «حذف ادمین» ادمینیتش رو بردارید.")
    return None

async def _diagnose_restrict_issue(chat_id, target_id):
    """ترکیبِ دو چکِ بالا: اول خودِ ربات، بعد کاربرِ هدف. اگه هیچ‌کدوم مشکلی
    نشون ندن، None برمی‌گرده (یعنی می‌شه edit_permissions رو امتحان کرد)."""
    issue = await _get_own_restrict_permission_issue(chat_id)
    if issue:
        return issue
    return await _get_target_restrict_issue(chat_id, target_id)

async def _diagnose_restrict_issue_kind(chat_id, target_id):
    """مثل _diagnose_restrict_issue، ولی به‌جای متنِ توضیحیِ بلند، فقط نوعِ مشکل رو
    برمی‌گردونه تا در پیام‌های کوتاهِ یکدست («» ... ↺») استفاده بشه:
    'own'    -> خودِ ربات ادمین نیست یا دسترسیِ محدودکردن نداره
    'target' -> کاربرِ هدف خودش ادمین/سازنده‌ی همین گروهه
    None     -> مشکلی شناسایی نشد (می‌شه edit_permissions رو امتحان کرد)."""
    if await _get_own_restrict_permission_issue(chat_id):
        return "own"
    if await _get_target_restrict_issue(chat_id, target_id):
        return "target"
    return None

# ==================== قالب یکسان «» ... ↺» برای پیام‌های بن/سکوت ====================
# دقیقاً همون قالبِ کوتاهی که برای قفل‌ها (قفل لینک، قفل گروه و ...) استفاده
# می‌شه؛ به‌جای پیام‌های چندخطی و نمایشِ خامِ خطا، همه‌ی حالت‌های بن/رفع‌بن/سکوت/
# رفع‌سکوت از همین یک تابع استفاده می‌کنن تا لحن و ظاهرشون یکدست باشه.
def format_restrict_notice(kind, tag, action):
    """action یکی از 'ban', 'unban', 'mute', 'unmute' هست."""
    if kind == "target":
        return "**» به دلیل ادمین بودن شخص پیام‌هاش حذف می‌شه ↺**" if action in ("ban", "mute") \
            else "**» چون این شخص ادمینه، این محدودیت روی سروش اعمال نشده بود؛ فقط از دیتابیس ربات پاک شد ↺**"
    if kind == "own":
        if action in ("ban", "mute"):
            verb = "بن واقعی" if action == "ban" else "سکوت واقعی"
            return f"**» ربات توی این گروه دسترسی لازم برای {verb} رو نداره؛ فقط پیام‌هاش حذف می‌شه — {tag} ↺**"
        return "**» ربات دسترسی لازم رو نداره؛ فقط از دیتابیس ربات پاک شد ↺**"
    return None


AD_TEXT = f"""
🤖 **ربات مدیریت گروه «بات سافت»**

✨ امنیت کامل، حذف اسپم و مدیریت هوشمند گروه شما با بالاترین سرعت! 🚀

👇 همین حالا ربات خودت رو تحویل بگیر:
📣 {CHANNEL_LINK}
"""

MEMBER_SEEN_CACHE = {}

def _get_seen_cache(chat_id):
    if chat_id not in MEMBER_SEEN_CACHE:
        try:
            MEMBER_SEEN_CACHE[chat_id] = db.get_seen_members(chat_id)
        except:
            MEMBER_SEEN_CACHE[chat_id] = set()
    return MEMBER_SEEN_CACHE[chat_id]

def mark_member_seen(chat_id, user_id):
    _get_seen_cache(chat_id).add(user_id)
    try:
        db.add_seen_member(chat_id, user_id)
    except:
        pass

def unmark_member_seen(chat_id, user_id):
    _get_seen_cache(chat_id).discard(user_id)
    try:
        db.remove_seen_member(chat_id, user_id)
    except:
        pass

def gregorian_to_jalali(gy, gm, gd):
    g_days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    j_days_in_month = [31, 31, 31, 31, 31, 31, 30, 30, 30, 30, 30, 29]
    gy2 = gy - 1600
    gm2 = gm - 1
    gd2 = gd - 1
    g_day_no = 365 * gy2 + (gy2 + 3) // 4 - (gy2 + 99) // 100 + (gy2 + 399) // 400
    for i in range(gm2):
        g_day_no += g_days_in_month[i]
    if gm2 > 1 and ((gy % 4 == 0 and gy % 100 != 0) or (gy % 400 == 0)):
        g_day_no += 1
    g_day_no += gd2
    j_day_no = g_day_no - 79
    j_np = j_day_no // 12053
    j_day_no %= 12053
    jy = 979 + 33 * j_np + 4 * (j_day_no // 1461)
    j_day_no %= 1461
    if j_day_no >= 366:
        jy += (j_day_no - 1) // 365
        j_day_no = (j_day_no - 1) % 365
    jm, jd = 12, j_day_no + 1
    for i in range(11):
        if j_day_no < j_days_in_month[i]:
            jm = i + 1
            jd = j_day_no + 1
            break
        j_day_no -= j_days_in_month[i]
    return jy, jm, jd

def jalali_today_str():
    now = datetime.now(IRAN_TZ)
    jy, jm, jd = gregorian_to_jalali(now.year, now.month, now.day)
    return f"{jy}/{jm:02d}/{jd:02d}"

def current_time_str():
    return datetime.now(IRAN_TZ).strftime("%H:%M")

def format_welcome_template(template, name, group_name):
    result = (template
            .replace("#اسم", name)
            .replace("#گپ", group_name)
            .replace("#تاریخ", jalali_today_str())
            .replace("#ساعت", current_time_str())
            .replace("{name}", name))
    # هر چقدرم مالک متن رو تغییر بده، تگ/تاریخ/ساعت طرف همیشه باید باشه؛
    # اگه متن دستی این‌ها رو نداشت، خودمون اضافه‌ش می‌کنیم.
    missing = []
    if "#اسم" not in template and "{name}" not in template:
        missing.append(name)
    if "#تاریخ" not in template:
        missing.append(f"📅 {jalali_today_str()}")
    if "#ساعت" not in template:
        missing.append(f"🕒 {current_time_str()}")
    if missing:
        result += "\n\n" + " | ".join(missing)
    return result

DEFAULT_WELCOME_TEMPLATE = """∞ سلام « #اسم » به گروه « #گپ » خوش اومدی !

◂ تاریخ » #تاریخ
◂ زمان » #ساعت"""

async def greet_new_member(chat_id, user_id):
    if user_id == BOT_ID:
        return
    if user_id in _get_seen_cache(chat_id):
        return  # قبلاً خوشامد گفته شده - جلوگیری از خوشامدگویی تکراری
    try:
        # خوشامدگویی ثابت است: متن پیش‌فرض و روشن‌بودن آن قابل تغییر نیست.
        name = await get_user_name(client, user_id, chat_id)
        username = None
        try:
            entity = await client.get_entity(user_id)
            username = getattr(entity, 'username', None)
        except:
            pass
        tag = mention_user(name, user_id, username)

        group_name = await resolve_group_name(chat_id)

        template = DEFAULT_WELCOME_TEMPLATE
        msg = format_welcome_template(template, tag, group_name)
        await client.send_message(chat_id, msg)
    except Exception as e:
        safe_print(f"[greet_new_member] خطا: {e}")

async def farewell_member(chat_id, user_id):
    try:
        name = await get_user_name(client, user_id, chat_id)
        group_name = await resolve_group_name(chat_id)
        template = "👋 #اسم از گروه « #گپ » خارج شد.\n◂ تاریخ » #تاریخ\n◂ زمان » #ساعت"
        msg = format_welcome_template(template, name, group_name)
        await client.send_message(chat_id, msg)
    except Exception as e:
        safe_print(f"[farewell_member] خطا: {e}")

# ==================== بررسی بیوی عضو تازه‌وارد (اخطار بیو / بیو خاموش / بیو قفل) ====================
# سه حالت، مخصوص هر گروه (در db.bio_mode_settings ذخیره می‌شه):
#   off  -> پیش‌فرض؛ کاری با بیوی عضو تازه‌وارد نداره
#   warn -> اگه بیوی عضو تازه‌وارد لینک داشته باشه، فقط اخطار می‌ده
#   lock -> اگه بیوی عضو تازه‌وارد لینک داشته باشه، بند (بن) می‌شه

async def get_user_bio(user_id):
    """متن بیو/درباره‌ی کاربر رو می‌گیره. splusthon یک فورک از Telethon هست (همون
    اسکیمای TL رو داره)، پس دقیقاً مثل Telethon باید از GetFullUserRequest استفاده
    کرد؛ خودِ entity ساده (get_entity) فیلد بیو رو نداره - فقط توی UserFull
    (خروجی GetFullUserRequest) موجوده. برای اطمینان از سازگاری، چند مسیر ایمپورت
    و چند اسم فیلد احتمالی رو هم به‌عنوان پشتیبان امتحان می‌کنیم.
    """
    try:
        entity = await client.get_entity(user_id)
    except Exception as e:
        safe_print(f"[get_user_bio] خطا در get_entity: {e}")
        entity = user_id

    full = None
    GetFullUserRequest = None
    # اول ماژولِ اصلیِ خودِ پروژه (spluslib، همون که بقیه‌ی RPCها ازش استفاده می‌کنن)؛ قبلاً فقط
    # spluspy/telethon امتحان می‌شد و اگه نصب نبودن، بیو هیچ‌وقت خونده نمی‌شد (و «قفل بیو» و
    # «بیو قفل» عملاً هیچ‌وقت لینکی پیدا نمی‌کردن).
    try:
        GetFullUserRequest = functions.users.GetFullUserRequest
    except Exception:
        GetFullUserRequest = None
    for module_path in (() if GetFullUserRequest else ("spluspy.tl.functions.users", "telethon.tl.functions.users")):
        try:
            mod = __import__(module_path, fromlist=["GetFullUserRequest"])
            GetFullUserRequest = getattr(mod, "GetFullUserRequest", None)
            if GetFullUserRequest:
                break
        except Exception:
            continue

    if GetFullUserRequest is not None:
        try:
            full = await client(GetFullUserRequest(entity))
        except Exception as e:
            safe_print(f"[get_user_bio] خطا در GetFullUserRequest: {e}")
            full = None

    if full is not None:
        candidates = [full]
        for attr in ("full_user", "user_full", "user"):
            inner = getattr(full, attr, None)
            if inner:
                candidates.append(inner)
        for cand in candidates:
            for attr in ("about", "bio", "description"):
                val = getattr(cand, attr, None)
                if val:
                    return val

    # مسیرهای پشتیبان، برای مواقعی که GetFullUserRequest در دسترس نبود
    if entity is not None and not isinstance(entity, int):
        for attr in ("bio", "about", "description", "user_about"):
            val = getattr(entity, attr, None)
            if val:
                return val

    for method_name in ("get_full_user", "get_entity_full", "get_user_full"):
        method = getattr(client, method_name, None)
        if not method:
            continue
        try:
            result = await method(user_id)
        except Exception:
            continue
        if not result:
            continue
        candidates = [result]
        inner = getattr(result, "user", None) or getattr(result, "full_user", None)
        if inner:
            candidates.append(inner)
        for cand in candidates:
            for attr in ("bio", "about", "description"):
                val = getattr(cand, attr, None)
                if val:
                    return val

    return None


def _text_has_link(text):
    """تشخیص وجود لینک در یک متن (چه بیو، چه هرجای دیگه) - دقیقاً همون منطقی که
    برای قفل لینک توی پیام‌های گروه استفاده می‌شه."""
    if not text:
        return False
    lower = text.lower()
    if "http" in lower or "t.me/" in lower or "telegram.me/" in lower or "splus.ir/" in lower or "://" in lower or "www." in lower:
        return True
    if LINK_PATTERN.search(text):
        return True
    return False


# ==================== قفل بیو (بررسی بیوی کاربرانِ فعال توی گروه) ====================
# «قفل بیو» روشن باشه، هر کاربرِ عضو عادی/معاف/ویژه که بیوش لینک داره:
#   • هر پیامی که می‌فرسته «همون لحظه پاک» می‌شه، تا وقتی که لینک رو از بیوش برنداشته
#     (هیچ مهلتی داده نمی‌شه؛ چون با بیوِ لینک‌دار فقط ممبر می‌دزده)،
#   • برای هر پیام یک اخطار ثبت می‌شه («اخطار X از Y») و با رسیدن به سقف اخطارِ گروه بن می‌شه؛
#     پیام اخطار پشت‌سرهم اسپم نمی‌شه (فقط یکی در چند ثانیه).
#   «قفل بیو بن» / «قفل بیو سکوت» هم مثل بقیه‌ی قفل‌ها اکشن مستقیم می‌دن.
# سرعت: بیو با GetFullUserRequest خونده می‌شه که RPC سنگینیه، پس:
#   • کاربرِ «بدون لینک» فقط هر BIO_LOCK_RECHECK_CLEAN_SECONDS دوباره چک می‌شه،
#   • کاربرِ «لینک‌دار» بین دو چکِ RPC حداکثر BIO_LOCK_RECHECK_LINKED_SECONDS فاصله داره؛
#     توی این فاصله پیام‌هاش بدون RPC و فوری پاک می‌شن، و به‌محض اینکه لینک رو برداره، اولین
#     چکِ بعدی آزادش می‌کنه.
BIO_LOCK_RECHECK_CLEAN_SECONDS = 1800
BIO_LOCK_RECHECK_LINKED_SECONDS = 15
_BIO_LOCK_STATE = {}       # (chat_id, user_id) -> {"checked": ts, "has_link": bool}
_BIO_LOCK_INFLIGHT = set()
_BIO_LOCK_NOTICE_CACHE = {}


async def _bio_lock_apply(event, chat_id, user_id, action):
    """اعمال نتیجه‌ی قفل بیو روی کاربری که بیوش لینک داره (پیامش پاک نمی‌شه)."""
    _tag = await build_view_tag(user_id, chat_id)
    reason = "داشتن لینک در بیو"
    settings = db.get_auto_mod_settings(chat_id)
    warn_limit = settings.get("warn_limit", 3)
    silent_mode = settings.get("silent_mode", False)

    async def _notify(text, delay=None):
        try:
            msg = await send_moderation_message(event, chat_id, text, sender_id=user_id)
            if msg:
                if delay is None:
                    asyncio.create_task(auto_delete_warning(client, chat_id, msg.id))
                else:
                    asyncio.create_task(auto_delete_warning(client, chat_id, msg.id, delay=delay))
        except Exception as e:
            safe_print(f"❌ خطا در ارسال پیام قفل بیو: {e}")

    async def _ban(why):
        try:
            db.ban_user(chat_id, user_id, why)
            db.reset_user_warnings(chat_id, user_id)
            try:
                await client.edit_permissions(chat_id, user_id, send_messages=False)
            except Exception:
                pass
            await _notify(format_ban_message(reason, tag=_tag))
        except Exception as e:
            safe_print(f"❌ خطا در بن قفل بیو: {e}")

    if action == "ban":
        await _ban("قفل بیو (لینک در بیو)")
        return
    if action == "mute":
        try:
            db.mute_user(chat_id, user_id, "permanent", "قفل بیو (لینک در بیو)")
            try:
                await client.edit_permissions(chat_id, user_id, send_messages=False)
            except Exception:
                pass
            await _notify(format_mute_message(reason, tag=_tag))
        except Exception as e:
            safe_print(f"❌ خطا در سکوت قفل بیو: {e}")
        return
    if silent_mode:
        await _ban("سایلنت: قفل بیو (لینک در بیو)")
        return

    try:
        count = db.add_user_warning(chat_id, user_id, "قفل بیو (لینک در بیو)")
    except Exception:
        count = 1
    if count >= warn_limit:
        await _ban(f"رسیدن به {warn_limit} اخطار (لینک در بیو)")
        return
    # اگه پشت‌سرهم پیام بفرسته، فقط یک پیام اخطار (نه یکی برای هر پیام)
    _nk = (chat_id, user_id)
    if time.time() - _BIO_LOCK_NOTICE_CACHE.get(_nk, 0) < 4:
        return
    if len(_BIO_LOCK_NOTICE_CACHE) > 5000:
        _BIO_LOCK_NOTICE_CACHE.clear()
    _BIO_LOCK_NOTICE_CACHE[_nk] = time.time()
    await _notify(
        format_warn_message(reason, count, warn_limit, tag=_tag)
        + "\n**» تا وقتی لینک بیوتون حذف نشه پیام‌هاتون پاک می‌شه ↺**",
        delay=get_warning_delete_delay(chat_id, user_id),
    )


async def _bio_lock_handle(event, chat_id, user_id):
    """True یعنی بیوِ این کاربر لینک داره و پیامش پاک و اخطار/بن اعمال شد (هندلر باید return کنه)."""
    key = (chat_id, user_id)
    now = time.time()
    st = _BIO_LOCK_STATE.get(key) or {}
    has_link = None

    if st.get("has_link"):
        if now - st.get("checked", 0) < BIO_LOCK_RECHECK_LINKED_SECONDS or key in _BIO_LOCK_INFLIGHT:
            has_link = True          # همین الان چک شده؛ بدون RPC، فوری پاک می‌کنیم
    else:
        if now - st.get("checked", 0) < BIO_LOCK_RECHECK_CLEAN_SECONDS:
            return False
        if key in _BIO_LOCK_INFLIGHT:
            return False

    if has_link is None:
        _BIO_LOCK_INFLIGHT.add(key)
        try:
            bio_text = await get_user_bio(user_id)
            has_link = _text_has_link(bio_text)
        except Exception as e:
            safe_print(f"[bio_lock] خطا: {e}")
            return False
        finally:
            _BIO_LOCK_INFLIGHT.discard(key)
        if len(_BIO_LOCK_STATE) > 20000:
            _BIO_LOCK_STATE.clear()
        _BIO_LOCK_STATE[key] = {"checked": time.time(), "has_link": bool(has_link)}

    if not has_link:
        return False

    try:
        await event.delete()
    except Exception:
        pass
    try:
        await _bio_lock_apply(event, chat_id, user_id, db.get_lock_action(chat_id, "bio_link"))
    except Exception as e:
        safe_print(f"[bio_lock] خطا در اعمال: {e}")
    return True


async def check_new_member_bio(chat_id, user_id):
    """بر اساس حالت «بیو» تنظیم‌شده برای گروه، بیوی عضو تازه‌وارد رو چک می‌کنه."""
    try:
        mode = db.get_bio_mode(chat_id)
        if mode == "off":
            return

        bio_text = await get_user_bio(user_id)
        if not _text_has_link(bio_text):
            return

        name = await get_user_name(client, user_id, chat_id)
        username = None
        try:
            entity = await client.get_entity(user_id)
            username = getattr(entity, 'username', None)
        except Exception:
            pass
        tag = mention_user(name, user_id, username)

        if mode == "warn":
            try:
                warn_msg = await client.send_message(
                    chat_id,
                    f"⚠️ {tag} توجه: بیوی شما لینک داره؛ لطفاً لینک رو از بیوتون حذف کنید."
                )
                asyncio.create_task(auto_delete_warning(client, chat_id, warn_msg.id))
            except Exception as e:
                safe_print(f"[check_new_member_bio] خطا در ارسال اخطار: {e}")

        elif mode == "lock":
            try:
                db.ban_user(chat_id, user_id, "بیوی لینک‌دار هنگام ورود")
                await client.edit_permissions(chat_id, user_id, send_messages=False)
            except Exception as e:
                safe_print(f"[check_new_member_bio] خطا در بند کردن: {e}")
            try:
                await client.send_message(chat_id, f"🔒 {tag} به‌دلیل داشتن لینک در بیو، بند شد!")
            except Exception as e:
                safe_print(f"[check_new_member_bio] خطا در ارسال پیام بند: {e}")
    except Exception as e:
        safe_print(f"[check_new_member_bio] خطا: {e}")


LOW_ACTIVITY_CHECK_HOURS = 24      # بازه‌ی «لغزنده»: همیشه ۲۴ ساعتِ اخیر چک می‌شه (نه فقط ۲۴ ساعتِ اول)
LOW_ACTIVITY_MIN_MESSAGES = 100    # کمتر از این تعداد پیام توی ۲۴ ساعتِ اخیر -> خروج خودکار
LOW_ACTIVITY_CHECK_INTERVAL_SECONDS = 600   # هر چند ثانیه یک‌بار چک بشه
MIN_GROUP_MEMBERS = 20             # ربات عضو گروه‌های کمتر از این تعداد عضو نمی‌شه

LINK_PATTERN = re.compile(
    r'(?:https?://)?(?:t\.me|telegram\.me|splus\.ir)/(?:joinchat/|joingroup/|\+)?([A-Za-z0-9_\-]+)',
    re.IGNORECASE
)


def _extract_group_link(text):
    """یک لینک گروه (splus.ir یا t.me، عمومی یا خصوصی) رو از متن پیام استخراج می‌کنه.
    نکته‌ی مهم: سروش برای لینک‌های خصوصی از پیشوند «joingroup/» استفاده می‌کنه (نه
    joinchat/ که مخصوص تلگرامه)؛ قبلاً چون این پیشوند رو نمی‌شناختیم، توی لینک
    splus.ir/joingroup/HASH، خودِ کلمه‌ی «joingroup» به‌عنوان identifier استخراج
    می‌شد (نه HASH واقعی) و لینک به‌اشتباه «عمومی» تشخیص داده می‌شد، در نتیجه ربات
    اصلاً نمی‌تونست باهاش عضو گروه بشه."""
    match = LINK_PATTERN.search(text)
    if not match:
        return None, None
    full_link = match.group(0)
    if not full_link.lower().startswith("http"):
        full_link = "https://" + full_link
    identifier = match.group(1)
    is_private_hash = "joinchat" in full_link.lower() or "joingroup" in full_link.lower() or "/+" in full_link
    return full_link, (identifier if is_private_hash else identifier)


async def _try_join_chat(link, identifier):
    """عضویت در گروه از روی لینک، بدون نیاز به تغییر خود SplusLib.

    لینک خصوصی سروش‌پلاس (joingroup/joinchat/+) با
    messages.ImportChatInviteRequest و لینک عمومی با
    channels.JoinChannelRequest پردازش می‌شود.
    """
    last_error = None
    is_private = any(
        marker in link.lower()
        for marker in ("joinchat", "joingroup", "/+")
    )

    if is_private:
        try:
            # identifier قبلاً از _extract_group_link استخراج شده و فقط hash است.
            result = await client(
                functions.messages.ImportChatInviteRequest(hash=identifier)
            )
            chats = getattr(result, "chats", None) or []
            if chats:
                return chats[0], None

            # بعضی پاسخ‌ها ممکن است entity را مستقیم برگردانند.
            if isinstance(result, (types.Chat, types.Channel)):
                return result, None

            last_error = RuntimeError(
                "ImportChatInviteRequest returned no chat"
            )
        except Exception as e:
            last_error = e
    else:
        try:
            target_entity = await client.get_entity(identifier)
            # خود TLRequest در زمان invoke، entity را به InputChannel تبدیل می‌کند.
            await client(functions.channels.JoinChannelRequest(target_entity))
            return target_entity, None
        except Exception as e:
            last_error = e

    return None, last_error


async def _confirm_left(chat_id):
    """بعد از این‌که یکی از متدهای لفت دادن بدون خطا برگشت، دوباره چک می‌کنیم که
    ربات واقعاً دیگه توی این گروه نیست - چون توی این پلتفرم (سروش) بعضی از این
    متدها (مخصوصاً leave_chat/leave_channel) می‌تونن بدون این‌که استثنایی بدن،
    عملاً هیچ کاری نکنن (سایلنت fail). قبلاً همین «بدون خطا برگشتن» مساوی «واقعاً
    لفت دادن» گرفته می‌شد؛ برای همین بود که پیام «خودکار از گروه لفت داده شد»
    چاپ می‌شد ولی ربات همچنان توی گروه می‌موند.
    اگه نتونیم مطمئن بشیم (خودِ چک‌کردن هم خطا داد)، None برمی‌گردونیم تا صدازننده
    نتیجه‌ی اولیه رو قبول کنه - نمی‌خوایم یک خطای موقتِ شبکه باعث بشه یک لفتِ
    واقعاً موفق، ناموفق گزارش بشه."""
    # مسیر سریع: یک RPC سبک به‌جای کشیدنِ کلِ لیستِ دیالوگ‌ها (که با زیاد شدنِ
    # گروه‌ها ثانیه‌ها طول می‌کشه). فقط «قطعاً خارج شده» رو باور می‌کنیم؛ هر حالتِ
    # دیگه (هنوز عضو/نامشخص) مثلِ قبل با get_dialogs (منبعِ قابل‌اعتماد) تصمیم می‌گیره.
    if await _fast_left_probe(chat_id) is True:
        return True
    try:
        dialogs = await client.get_dialogs()
    except Exception:
        return None
    still_there = any(getattr(d, "id", None) == chat_id for d in dialogs)
    return not still_there


async def _fast_left_probe(chat_id):
    """True فقط وقتی مطمئنیم ربات دیگه عضوِ این گروه نیست (خطای صریحِ «عضو نیستی»
    یا participantِ Left/Banned)؛ در هر حالتِ دیگه None."""
    try:
        perms = await asyncio.wait_for(client.get_permissions(chat_id, 'me'), timeout=6)
    except asyncio.TimeoutError:
        return None
    except Exception as e:
        msg = str(e).lower()
        return True if any(k in msg for k in _GONE_ERR_MARKERS) else None
    participant = getattr(perms, 'participant', None)
    pname = type(participant).__name__.lower() if participant is not None else ""
    if "banned" in pname or pname.endswith("left"):
        return True
    rights = getattr(participant, 'banned_rights', None)
    if rights and bool(getattr(rights, 'view_messages', False)):
        return True
    return None


async def _try_leave_chat(entity, chat_id):
    """درست به همون دلیلی که برای عضو شدن (_try_join_chat بالا) چند روش امتحان می‌کنیم،
    برای خروج هم همین‌طوره: leave_chat که جای دیگه‌ی کد فقط روی گروه‌های معمولی تست
    شده، روی کانال‌ها (broadcast) قابل‌اعتماد نیست و گاهی سایلنت fail می‌کنه. قبلاً
    خطاش فقط لاگ می‌شد و به کاربر همیشه گفته می‌شد «عضو کانال نمی‌شم» - در حالی که
    ربات واقعاً همچنان عضو کانال باقی می‌موند. الان همه‌ی روش‌های ممکن رو امتحان
    می‌کنیم و نتیجه‌ی واقعی رو برمی‌گردونیم تا پیام به کاربر درست باشه.

    نکته‌ی تازه: چون «بدون خطا برگشتن» به‌تنهایی یعنی «واقعاً لفت داده شد» نیست
    (نگاه کن به _confirm_left)، بعد از هر تلاشِ بدون-خطا، یک‌بار واقعاً چک می‌کنیم
    که ربات دیگه توی دیالوگ‌ها نیست؛ اگه هنوز هست، به‌جای قبول کردن موفقیت، می‌ریم
    سراغ روش بعدی."""
    last_error = None

    async def _attempt(method, arg):
        nonlocal last_error
        try:
            await method(arg)
        except Exception as e:
            last_error = e
            return False
        confirmed = await _confirm_left(chat_id)
        if confirmed is False:
            last_error = RuntimeError(
                f"{getattr(method, '__name__', method)} بدون خطا برگشت ولی ربات هنوز عضو گروهه (سایلنت fail)"
            )
            return False
        return True

    for method_name in ("leave_chat", "leave_channel", "delete_dialog"):
        method = getattr(client, method_name, None)
        if not method:
            continue
        if await _attempt(method, entity):
            return True, None
        if await _attempt(method, chat_id):
            return True, None

    # قبلاً فقط ماژول‌های spluspy امتحان می‌شد که روی این پروژه (spluslib) وجود ندارن؛ نتیجه:
    # روش‌های خامِ LeaveChannelRequest/DeleteChatUserRequest هیچ‌وقت اجرا نمی‌شد و اگه
    # leave_chat سایلنت fail می‌کرد، ربات عملاً لفت نمی‌داد. الان اول ماژولِ اصلیِ
    # پروژه (spluslib) و بعد spluspy امتحان می‌شه.
    _fn = globals().get("functions")
    if _fn is None:
        for import_path in ("spluspy.tl.functions", "spluspy.functions"):
            try:
                _fn = __import__(import_path, fromlist=["messages", "channels"])
                break
            except ImportError:
                continue

    if _fn:
        try:
            await client(_fn.channels.LeaveChannelRequest(entity))
        except Exception as e:
            last_error = e
        else:
            if (await _confirm_left(chat_id)) is not False:
                return True, None
            last_error = RuntimeError("LeaveChannelRequest بدون خطا برگشت ولی ربات هنوز عضو گروهه")
        try:
            me = await client.get_me()
            await client(_fn.messages.DeleteChatUserRequest(chat_id, me.id))
        except Exception as e:
            last_error = e
        else:
            if (await _confirm_left(chat_id)) is not False:
                return True, None
            last_error = RuntimeError("DeleteChatUserRequest بدون خطا برگشت ولی ربات هنوز عضو گروهه")

    return False, last_error


def _positive_int(value):
    try:
        value = int(value)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


async def _probe_member_count(entity, chat_id=None):
    """تعداد اعضای گروه با «چند روش هم‌زمان»؛ بزرگ‌ترین عدد مثبتِ به‌دست‌اومده برگردونده می‌شه.
    قبلاً فقط participants_count خودِ entity (که روی سروش اغلب خالیه) و یک
    get_participants(limit=0) امتحان می‌شد و چون هیچ‌کدوم عدد نمی‌دادن، چک ۲۰ نفره
    عملاً هیچ‌وقت کار نمی‌کرد. الان: entity تازه، GetFullChannel، GetFullChat، لیست
    اعضا با limit مشخص (همون روشی که جاهای دیگه‌ی همین کد روی سروش جواب می‌ده) و در
    آخر لیست دیالوگ‌ها. نتیجه‌ی هر روش لاگ می‌شه تا معلوم باشه کدوم روی سروش جواب می‌ده.
    None یعنی «نمی‌دونیم» (نه «۰ نفر»)."""
    found = {}

    def _add(name, value):
        v = _positive_int(value)
        if v:
            found[name] = v

    target_id = chat_id
    if target_id is None and entity is not None and not isinstance(entity, int):
        try:
            target_id = await client.get_peer_id(entity)
        except Exception:
            target_id = None
    if entity is None:
        entity = target_id

    _add("entity", getattr(entity, "participants_count", None))

    if target_id is not None:
        try:
            fresh = await client.get_entity(target_id)
            _add("fresh_entity", getattr(fresh, "participants_count", None))
            if isinstance(entity, int):
                entity = fresh
        except Exception:
            pass

    try:
        full = await client(functions.channels.GetFullChannelRequest(entity))
        _add("full_channel", getattr(getattr(full, "full_chat", None), "participants_count", None))
    except Exception:
        pass
    try:
        raw_id = getattr(entity, "id", None)
        if raw_id and not isinstance(entity, int):
            full = await client(functions.messages.GetFullChatRequest(chat_id=raw_id))
            participants = getattr(getattr(full, "full_chat", None), "participants", None)
            _add("full_chat", len(getattr(participants, "participants", None) or []))
    except Exception:
        pass

    for name, target in (("participants_id", target_id), ("participants_entity", entity)):
        if target is None:
            continue
        try:
            parts = await client.get_participants(target, limit=MIN_GROUP_MEMBERS + 1)
            _add(name + "_total", getattr(parts, "total", None))
            _add(name + "_len", len(parts))
        except Exception:
            pass

    if not found and target_id is not None:
        try:
            for d in await client.get_dialogs():
                if getattr(d, "id", None) == target_id:
                    _add("dialog", getattr(getattr(d, "entity", None), "participants_count", None))
                    break
        except Exception:
            pass

    safe_print(f"[member_count] گروه {target_id}: {found if found else 'نامعلوم'}")
    return max(found.values()) if found else None


async def _precheck_link_member_count(link, identifier):
    """قبل از هر عضویتی، تعداد اعضای گروهِ پشتِ لینک رو (اگه سرور بگه) برمی‌گردونه؛ وگرنه None.
    لینک خصوصی: CheckChatInviteRequest (بدون جوین)؛ لینک عمومی: get_entity."""
    is_private = any(m in link.lower() for m in ("joinchat", "joingroup", "/+"))
    try:
        if is_private:
            result = await client(functions.messages.CheckChatInviteRequest(hash=identifier))
            count = _positive_int(getattr(result, "participants_count", None))
            if not count:
                count = _positive_int(getattr(getattr(result, "chat", None), "participants_count", None))
            return count
        ent = await client.get_entity(identifier)
        return _positive_int(getattr(ent, "participants_count", None))
    except Exception:
        return None


def _too_small_message(count):
    return (f"این گروه فقط {count} عضو داره؛ ربات فقط توی گروه‌هایی که حداقل "
            f"{MIN_GROUP_MEMBERS} عضو دارن عضو می‌شه.")


# سقف تعداد لینک‌هایی که هر کاربر توی پیوی می‌تونه بفرسته تا ربات باهاش عضو گروه بشه.
# نفر سوم به بعد رد می‌شه (ربات عضو نمی‌شه)، تا از سوءاستفاده/عضو شدن بی‌رویه توی
# گروه‌های نامربوط جلوگیری بشه.
MAX_JOIN_LINKS_PER_USER = 2

# چند ورود هم‌زمان، برای اینکه نصب تعداد زیادی گروه پشت سر هم کند نشود.
# عدد 5 عمداً محافظه‌کارانه است تا فشار و Flood/Rate Limit غیرضروری ایجاد نشود.
JOIN_CONCURRENCY = 5
_JOIN_SEMAPHORE = asyncio.Semaphore(JOIN_CONCURRENCY)
_ACTIVE_JOIN_KEYS = set()


async def _join_group_via_link_worker(event, text, join_key=None):
    async with _JOIN_SEMAPHORE:
        try:
            await join_group_via_link(event, text)
        except Exception as e:
            safe_print(f"[join_group_worker] خطا: {e}")
        finally:
            if join_key:
                _ACTIVE_JOIN_KEYS.discard(join_key)


def schedule_group_join(event, text):
    """ورود را در worker اجرا می‌کند تا handler اصلی منتظر RPC ورود نماند."""
    link, identifier = _extract_group_link(text)
    if not identifier:
        return False
    key = identifier.lower()
    if key in _ACTIVE_JOIN_KEYS:
        return True
    _ACTIVE_JOIN_KEYS.add(key)
    asyncio.create_task(_join_group_via_link_worker(event, text, key))
    return True


async def join_group_via_link(event, text):
    """کاربر یک لینک گروه فرستاده: ربات سعی می‌کنه واردش بشه و برای چک فعالیت بعد از
    ۲۴ ساعت ثبتش می‌کنه.
    نکته‌ی مهم: قبلاً اینجا یه چک فوری روی تعداد اعضا (client.get_participants) هم
    بود که اگه عضو کمتر از ۵۰ نفر بود، ربات بلافاصله خارج می‌شد. این چک حذف شد چون
    get_participants روی این پلتفرم (سروش) قابل‌اعتماد نیست و همیشه یه لیست خالی
    (۰ عضو) برمی‌گردوند - یعنی فرقی نمی‌کرد گروه واقعاً ۲۰۰ نفر باشه یا ۱ نفر، ربات
    فکر می‌کرد ۰ عضو داره و بلافاصله از همه‌ی گروه‌ها خارج می‌شد! پس دیگه اصلاً تعداد
    عضو رو نمی‌گیریم و هیچ شرطی روش نمی‌ذاریم؛ تنها معیار موندن یا نموندن ربات، همون
    چک فعالیت بعد از ۲۴ ساعت (تعداد پیام رد و بدل شده) در check_low_activity_groups
    هست که چون از دیتابیس داخلی خودمون (نه یه RPC مشکوک) میاد، قابل‌اعتماده.
    (به‌روزرسانی: حالا حداقل MIN_GROUP_MEMBERS عضو هم چک می‌شه، ولی فقط وقتی عدد «قطعی و
    مثبت» از سرور بیاد؛ عدد نامعلوم/صفر یعنی «نمی‌دونیم» و مانع عضویت نمی‌شه، پس همون
    باگ قدیمی تکرار نمی‌شه. fail-open عمداً انتخاب شده.)

    محدودیت‌های جدید:
    • هر کاربر حداکثر MAX_JOIN_LINKS_PER_USER لینک می‌تونه بفرسته (نفر سوم رد می‌شه).
    • لینک حتماً باید مال یک «گروه» باشه؛ اگه لینک مال یک «کانال» (broadcast) باشه،
      ربات فوراً از همون کانال خارج می‌شه و شمارشگر کاربر هم افزایش پیدا نمی‌کنه."""
    sender_id = getattr(event, 'sender_id', None)

    # سازنده(های) اصلی ربات از سقف تعداد لینک آزادن؛ هر چقدر لازم باشه می‌تونن
    # لینک گروه بفرستن بدون اینکه ربات بهشون گیر بده یا بعد از دو لینک ردشون کنه.
    if sender_id is not None and sender_id not in SUPER_OWNER_IDS:
        try:
            used = db.get_user_join_link_count(sender_id)
        except Exception:
            used = 0
        if used >= MAX_JOIN_LINKS_PER_USER:
            try:
                await pv_reply(event, 
                    f"⛔ شما قبلاً از سقف مجاز ({MAX_JOIN_LINKS_PER_USER} لینک) برای عضو کردن ربات در گروه استفاده کردید.\n"
                    f"امکان ارسال لینک بیشتر برای عضویت ربات وجود نداره."
                )
            except Exception:
                pass
            return

    link, identifier = _extract_group_link(text)
    if not link or not identifier:
        return

    is_private_hash = "joinchat" in link.lower() or "joingroup" in link.lower() or "/+" in link

    # پیش‌چک رجیستری سراسری برای لینک‌های عمومی: چون get_entity برای یک
    # یوزرنیمِ عمومی بدون نیاز به عضویت جواب می‌ده، می‌تونیم *قبل* از هرگونه
    # تلاش برای عضو شدن بفهمیم ربات دیگه‌ای از این مجموعه از قبل اونجاست یا نه
    # (رد واقعی، نه join-و-بعد-leave). برای لینک‌های خصوصی (joinchat/joingroup/+)
    # این پیش‌چک ممکن نیست (resolve بدون عضویت روی این پلتفرم تست‌نشده‌ست)، پس
    # برای اونا فقط چکِ بعد از عضویتِ واقعی (پایین‌تر) اجرا می‌شه.
    if not is_private_hash and GROUP_REGISTRY_API_URL:
        try:
            preview_entity = await client.get_entity(identifier)
            # نکته‌ی مهم: entity.id آیدیِ خامِ بدون‌مارکه (همیشه مثبت)، ولی همه‌جای
            # دیگه‌ی این فایل (پیام‌های واقعی، قفل‌ها، تنظیمات) با آیدیِ مارک‌دار
            # (event.chat_id، همیشه منفی) کار می‌کنن. اگه اینجا از آیدیِ خام
            # استفاده می‌شد، گروه با یک کلیدِ اشتباه ثبت می‌شد و قفل‌ها/تنظیماتش
            # هیچ‌وقت موقعِ پیامِ واقعی پیدا نمی‌شدن (ثبت می‌شه ولی اعمال نمی‌شه).
            preview_chat_id = await client.get_peer_id(preview_entity)
        except Exception:
            preview_chat_id = None
        if preview_chat_id:
            occupied, owner_label, _ = await registry_check_group(preview_chat_id)
            if occupied:
                try:
                    await pv_reply(event,
                        f"❌ نمی‌تونم عضو این گروه بشم؛ ربات «{owner_label}» از قبل توی این گروه هست "
                        f"و طبق تنظیمات، هر گروه فقط یک ربات از این مجموعه می‌تونه داشته باشه."
                    )
                except Exception:
                    pass
                return

    # حداقل تعداد عضو: اگه سرور تعداد رو بده و کمتر از حد باشه، اصلاً عضو نمی‌شیم و
    # از سقف لینک‌های کاربر هم چیزی کم نمی‌شه. اگه تعداد معلوم نبود، بعد از عضویت دوباره چک می‌شه.
    _pre_count = await _precheck_link_member_count(link, identifier)
    if _pre_count is not None and _pre_count < MIN_GROUP_MEMBERS:
        try:
            await pv_reply(event, f"❌ {_too_small_message(_pre_count)}")
        except Exception:
            pass
        return

    status_msg = await pv_reply(event, "🔗 لینک گروه دریافت شد!\n⏳ در حال ورود به گروه...")

    entity, join_error = await _try_join_chat(link, identifier)
    if not entity:
        try:
            await pv_reply(event, 
                f"❌ نتونستم وارد گروه بشم.\n"
                f"ممکنه لینک نامعتبر یا منقضی‌شده باشه، یا نیاز به تایید ادمین گروه داشته باشه.\n"
                f"جزئیات فنی: {join_error}"
            )
        except Exception:
            pass
        return

    # همون توضیحِ بالا: آیدیِ مارک‌دار می‌خوایم (نه entity.id خام) تا با
    # event.chat_id واقعی که موقعِ پیام‌های این گروه استفاده می‌شه یکی باشه.
    try:
        join_chat_id = await client.get_peer_id(entity)
    except Exception:
        join_chat_id = None
    if not join_chat_id:
        try:
            await pv_reply(event, "❌ عضو گروه شدم ولی نتونستم آیدی گروه رو تشخیص بدم.")
        except Exception:
            pass
        return

    # لینک باید مال یک گروه باشه نه کانال. تشخیص: entity هایی که broadcast=True و
    # megagroup نیستن، کانال محسوب می‌شن (سوپرگروه‌ها megagroup=True دارن، گروه‌های
    # معمولی/قدیمی اصلاً attribute broadcast ندارن -> پیش‌فرض False یعنی گروه).
    is_channel = bool(getattr(entity, 'broadcast', False)) and not bool(getattr(entity, 'megagroup', False))
    if is_channel:
        left_ok, leave_error = await _try_leave_chat(entity, join_chat_id)
        if not left_ok:
            safe_print(f"[join_group_via_link] خطا در خروج از کانال نامعتبر: {leave_error}")
        try:
            if left_ok:
                await pv_reply(event, 
                    "❌ این لینک مال یک **کانال** بود، نه گروه. از کانال خارج شدم؛ ربات فقط توی "
                    "گروه‌ها عضو می‌شه، لطفاً لینک یک گروه رو بفرستید."
                )
            else:
                # صادقانه به کاربر بگیم که با اینکه نباید عضو کانال بمونه، خروج خودکار
                # موفق نشده - به‌جای اینکه دروغ بگیم «عضو نمی‌شم» در حالی که عضو مونده.
                await pv_reply(event, 
                    "⚠️ این لینک مال یک **کانال** بود، نه گروه. ربات نباید عضو کانال بمونه، ولی "
                    "نتونستم به‌صورت خودکار از این کانال خارج بشم (خطای فنی). لطفاً به سازنده اطلاع "
                    "بدید یا خودتون ربات رو از اعضای کانال حذف کنید. لینک یک گروه رو هم می‌تونید بفرستید."
                )
        except Exception:
            pass
        return

    raw_name = getattr(entity, 'title', None) or getattr(entity, 'name', None)
    group_name = raw_name or "گروه"

    # چکِ حداقل عضو بعد از عضویت (برای وقتی که پیش‌چکِ قبل از جوین نتونسته بود تعداد رو بگیره).
    # اگه تعداد هنوز هم نامعلوم بود، نگه می‌داریم (قبلاً همین‌جا هر گروهی «۰ نفر» دیده می‌شد).
    _member_count = await _probe_member_count(entity, join_chat_id)
    if _member_count is not None and _member_count < MIN_GROUP_MEMBERS:
        left_ok, leave_error = await _try_leave_chat(entity, join_chat_id)
        try:
            if left_ok:
                await pv_reply(event, f"❌ {_too_small_message(_member_count)} از گروه خارج شدم.")
            else:
                await pv_reply(event,
                    f"⚠️ {_too_small_message(_member_count)} ولی نتونستم خودکار خارج بشم (خطای فنی: {leave_error}). "
                    f"لطفاً به سازنده اطلاع بدید.")
        except Exception:
            pass
        return
    _count_unknown_line = ""
    if _member_count is None:
        # سروش تعداد رو نداد: چند دقیقه‌ی بعد (و دوباره بعدتر) دوباره چک می‌شه.
        asyncio.create_task(_small_group_guard(join_chat_id, raw_name or "گروه", sender_id))
        _count_unknown_line = (f"\n⚠️ تعداد اعضای گروه از سروش مشخص نشد؛ چند دقیقه‌ی دیگه دوباره چک می‌کنم و "
                               f"اگه کمتر از {MIN_GROUP_MEMBERS} نفر باشه، خارج می‌شم.\n")

    # چکِ نهایی *بعد از* عضویتِ واقعی - پوششِ لینک‌های خصوصی (که پیش‌چک بالا
    # روشون اجرا نشد) و پوششِ ریسِ احتمالی بین پیش‌چک و لحظه‌ی واقعیِ عضویت
    # (مثلاً دو نمونه هم‌زمان روی یک لینک). اگه معلوم شد ربات دیگه‌ای از این
    # مجموعه از قبل اینجاست، صادقانه به کاربر می‌گیم که عضو شدیم ولی بلافاصله
    # به‌خاطر این قانون خارج شدیم (join-then-leave)، نه اینکه دروغ بگیم اصلاً
    # عضو نشدیم.
    if GROUP_REGISTRY_API_URL:
        occupied, owner_label, _ = await registry_check_group(join_chat_id)
        if occupied:
            left_ok, leave_error = await _try_leave_chat(entity, join_chat_id)
            try:
                if left_ok:
                    await pv_reply(event,
                        f"❌ عضو گروه «{group_name}» شدم ولی چون ربات «{owner_label}» از قبل اونجا بود، "
                        f"طبق تنظیمات (هر گروه فقط یک ربات از این مجموعه) بلافاصله ازش خارج شدم."
                    )
                else:
                    await pv_reply(event,
                        f"⚠️ ربات «{owner_label}» از قبل توی گروه «{group_name}» هست، ولی نتونستم به‌صورت "
                        f"خودکار از این گروه خارج بشم (خطای فنی: {leave_error}). لطفاً دستی بررسی کنید."
                    )
            except Exception:
                pass
            return

    try:
        baseline = db.get_stats(join_chat_id).get("total_messages", 0) or 0
    except Exception:
        baseline = 0
    try:
        db.add_group(join_chat_id, group_name)
        # همون دلیلِ perform_admin_join_test: مطمئن می‌شیم اسمِ واقعی (وقتی
        # داریمش) همیشه ذخیره بشه، حتی اگه ردیفِ گروه از قبل بدونِ اسم بوده.
        if raw_name:
            db.set_group_name(join_chat_id, raw_name)
        db.register_auto_joined_group(join_chat_id, group_name, baseline)
        # قفل‌های پیش‌فرض (لینک/اسپم/کد هنگی/تبلیغ) همون لحظه‌ی ورود هم فعال بشن، نه فقط
        # موقع «فعال» شدن ربات توسط مالک.
        apply_default_group_locks(join_chat_id)
    except Exception as e:
        safe_print(f"[join_group_via_link] خطا در ثبت گروه «{group_name}» در دیتابیس: {e}")

    # ثبتِ حضورِ خودمون توی رجیستری سراسری - تا هیچ نمونه‌ی دیگه‌ای بعد از این
    # لحظه، همین گروه رو «آزاد» نبینه.
    asyncio.create_task(registry_announce_group(join_chat_id, group_name))
    asyncio.create_task(auto_activate_group(join_chat_id, group_name))

    is_exempt_sender = sender_id is not None and sender_id in SUPER_OWNER_IDS
    remaining = MAX_JOIN_LINKS_PER_USER
    if sender_id is not None and not is_exempt_sender:
        try:
            used_now = db.increment_user_join_link_count(sender_id)
            remaining = max(0, MAX_JOIN_LINKS_PER_USER - used_now)
        except Exception:
            pass

    remaining_line = "🔢 شما سازنده‌اید، محدودیتی روی تعداد لینک ندارید." if is_exempt_sender \
        else f"🔢 لینک‌های باقی‌مانده‌ی شما: {remaining} از {MAX_JOIN_LINKS_PER_USER}"

    try:
        await pv_reply(event, f"""✅ با موفقیت وارد گروه «{group_name}» شدم!

⏳ فعالیت گروه همیشه بررسی می‌شه؛ اگه در هر بازه‌ی {LOW_ACTIVITY_CHECK_HOURS} ساعته کمتر از
{LOW_ACTIVITY_MIN_MESSAGES} پیام رد و بدل بشه، ربات به‌صورت خودکار از گروه خارج می‌شه.

📋 مراحل بعد:
1️⃣ ربات رو ادمین کن
2️⃣ فعال‌سازی خودکار انجام شد (مالک و ادمین‌های گروه شناسایی می‌شن)

{remaining_line}
{_count_unknown_line}
📌 **کانال اصلی:** {CHANNEL_LINK}
🆘 **پشتیبانی:** {HELPER_LINK}
""")
    except Exception:
        pass


async def perform_membership_check(link_text, requester_uid=None):
    """چکِ سبک و بدونِ‌جوین: آیا خودِ همین سلف از قبل عضوِ گروهِ این لینکه یا نه -
    هیچ جوینِ واقعی‌ای انجام نمی‌ده (برخلافِ perform_admin_join_test).
    برای لینکِ خصوصی (joinchat/joingroup/+) از CheckChatInviteRequest استفاده
    می‌شه: اگه از قبل عضو باشیم، سرور بدونِ جوینِ تازه یک ChatInviteAlready با
    خودِ چت برمی‌گردونه. برای لینکِ عمومی، اول entity رو resolve می‌کنیم (که
    خودش نیازی به عضویت نداره) و بعد با client.get_permissions(entity, 'me') -
    همون روشی که _bot_write_restricted_in_chat هم برای تشخیصِ عضویت استفاده
    می‌کنه - چک می‌کنیم که واقعاً عضوشیم یا نه.

    نکته‌ی امنیتی: صرفِ داشتنِ لینکِ عضویت (که ممکنه دستِ هرکسی افتاده باشه)
    دلیل نمی‌شه اون آدم بتونه پنلِ یک گروهِ از‌قبل‌ثبت‌شده رو بگیره. اگه
    requester_uid داده بشه، با check_user_permission همون منطقِ داخلِ خودِ
    گروه (سازنده/ادمینِ واقعیِ سروش، یا مالکِ ثبت‌شده‌ی داخلِ دیتابیس) چک
    می‌شه و نتیجه توی کلیدِ "authorized" برمی‌گرده - تصمیمِ نهایی که پنل به
    کی داده بشه با خودِ manager.py (بر اساسِ همین پرچم) گرفته می‌شه."""
    link, identifier = _extract_group_link(link_text)
    if not link or not identifier:
        return {"ok": False, "error": "لینک نامعتبر است."}

    is_private_hash = "joinchat" in link.lower() or "joingroup" in link.lower() or "/+" in link

    if is_private_hash:
        try:
            result = await client(functions.messages.CheckChatInviteRequest(hash=identifier))
        except Exception as e:
            return {"ok": False, "error": f"چکِ لینک ناموفق: {e}"}
        if not isinstance(result, types.ChatInviteAlready):
            return {"ok": True, "already_member": False}
        chat = result.chat
    else:
        try:
            chat = await client.get_entity(identifier)
        except Exception as e:
            return {"ok": False, "error": f"چکِ لینک ناموفق: {e}"}
        try:
            await client.get_permissions(chat, 'me')
        except Exception:
            return {"ok": True, "already_member": False}

    try:
        chat_id = await client.get_peer_id(chat)
    except Exception:
        return {"ok": False, "error": "آیدیِ گروه مشخص نشد."}

    is_channel = bool(getattr(chat, 'broadcast', False)) and not bool(getattr(chat, 'megagroup', False))
    if is_channel:
        return {"ok": True, "already_member": False}

    # اسمِ واقعیِ گروه رو هرجا الان resolve کردیم توی دیتابیس هم به‌روز می‌کنیم
    # (نه فقط برمی‌گردونیم) تا اگه ردیفِ این گروه قبلاً بدونِ اسم ساخته شده
    # بود (مثلاً از مسیرِ ثبتِ ادمین)، پنل دیگه هیچ‌وقت گیرِ «گروه «گروه»»
    # نمونه؛ فقط اگه واقعاً چیزی resolve نشد، از آخرین مقدارِ ذخیره‌شده و در
    # آخرین حالت از یک برچسبِ عمومی (که هیچ‌وقت ذخیره نمی‌شه) استفاده می‌کنیم.
    raw_name = getattr(chat, 'title', None) or getattr(chat, 'name', None)
    if raw_name:
        try:
            db.set_group_name(chat_id, raw_name)
        except Exception:
            pass
    group_name = raw_name or await resolve_group_name(chat_id)

    # فقط مالکِ واقعیِ گروه (سازنده/ادمینِ سروش، یا مالکِ ثبت‌شده‌ی داخلِ
    # دیتابیس) حق داره پنلِ یک گروهِ از‌قبل‌عضوشده رو بگیره - نه هرکسی که
    # لینکِ عضویت (که ممکنه توی خودِ گروه هم پخش شده باشه) رو داشته باشه.
    authorized = requester_uid is None
    if requester_uid is not None:
        try:
            req_uid_int = int(requester_uid)
        except (TypeError, ValueError):
            req_uid_int = None
        if req_uid_int is not None:
            try:
                level = await get_panel_access_level(chat_id, req_uid_int, fresh=True)
                authorized = level in PANEL_ALLOWED_LEVELS
            except Exception:
                authorized = False
            if authorized:
                # گروه از قبل بوده ولی شاید هنوز فعال/ثبت‌مالک نشده؛ idempotent
                asyncio.create_task(auto_activate_group(chat_id))

    return {
        "ok": True, "already_member": True, "chat_id": chat_id,
        "group_name": group_name, "authorized": authorized, "bot": BOT_DISPLAY_LABEL,
    }


async def perform_admin_join_test(link_text, requester_uid=None):
    """نسخه‌ی سبک‌ترِ join_group_via_link که مستقیماً از پنلِ مرکزی
    (manager.py، از طریقِ همون کانالِ broadcast/ack) دستور داده می‌شه - نه
    از یک پیامِ PM. برخلافِ join_group_via_link:
    • event/pv_reply لازم نداره (نتیجه به‌صورتِ dict برگردونده می‌شه تا
      panel_broadcast_loop اون رو ack کنه).
    • هیچ محدودیتِ MAX_JOIN_LINKS_PER_USER ای روش اعمال نمی‌شه، چون این
      دستور از طرفِ خودِ ادمین/پنله، نه یک کاربرِ عادی.
    منطقِ اصلیِ join (تشخیصِ خصوصی/عمومی، پیش‌چک و بعدچکِ رجیستریِ سراسری،
    ردِ کانال‌ها، ثبتِ گروه توی دیتابیس) عمداً همینجا هم تکرار شده - نه
    از join_group_via_link صدا زده شده - تا این مسیرِ تازه از منطقِ
    حساس و جاافتاده‌ی PM (که خودش کلی باگِ لبه‌ای توش فیکس شده) کاملاً جدا
    بمونه و ریسکِ شکستنِ اون رو نداشته باشه."""
    link, identifier = _extract_group_link(link_text)
    if not link or not identifier:
        return {"ok": False, "error": "لینک نامعتبر است."}

    is_private_hash = "joinchat" in link.lower() or "joingroup" in link.lower() or "/+" in link

    async def _public_registry_preview():
        if is_private_hash or not GROUP_REGISTRY_API_URL:
            return None
        try:
            preview_entity = await client.get_entity(identifier)
            # همون دلیلِ join_group_via_link: آیدیِ مارک‌دار می‌خوایم، نه خامِ entity.id.
            preview_chat_id = await client.get_peer_id(preview_entity)
        except Exception:
            preview_chat_id = None
        if preview_chat_id:
            occupied, owner_label, _ = await registry_check_group(preview_chat_id)
            if occupied:
                return {"ok": False, "error": f"از قبل ربات «{owner_label}» توی این گروهه."}
        return None

    # این دو چک مستقل‌اند؛ پشت‌سرهم نیستن، هم‌زمان اجرا می‌شن.
    _early_err, _pre_count = await asyncio.gather(
        _public_registry_preview(), _precheck_link_member_count(link, identifier)
    )
    if _early_err:
        return _early_err
    if _pre_count is not None and _pre_count < MIN_GROUP_MEMBERS:
        return {"ok": False, "error": _too_small_message(_pre_count)}

    entity, join_error = await _try_join_chat(link, identifier)
    if not entity:
        return {"ok": False, "error": f"عضو نشد: {join_error}"}

    # آیدیِ مارک‌دار (منفی) - همونی که event.chat_id واقعی هم برمی‌گردونه؛
    # وگرنه قفل/تنظیماتِ این گروه با کلیدِ اشتباه ثبت می‌شه و هیچ‌وقت موقعِ
    # پیامِ واقعیِ گروه اعمال نمی‌شه.
    try:
        join_chat_id = await client.get_peer_id(entity)
    except Exception:
        join_chat_id = None
    if not join_chat_id:
        return {"ok": False, "error": "عضو شد ولی آیدیِ گروه مشخص نشد."}

    is_channel = bool(getattr(entity, 'broadcast', False)) and not bool(getattr(entity, 'megagroup', False))
    if is_channel:
        left_ok, leave_error = await _try_leave_chat(entity, join_chat_id)
        detail = " (لفت داده شد)" if left_ok else f" (لفت هم ناموفق: {leave_error})"
        return {"ok": False, "error": f"این لینک مالِ یک کانال بود نه گروه{detail}."}

    raw_name = getattr(entity, 'title', None) or getattr(entity, 'name', None)
    group_name = raw_name or "گروه"

    # ---- سه چکِ «فقط‌خوندنی» که به‌هم وابسته نیستن، هم‌زمان اجرا می‌شن ----
    # (قبلاً پشت‌سرهم بودن: چکِ ادمین ← شمارشِ اعضا (چندین RPC) ← رجیستری.)
    # تصمیم‌گیری پایین دقیقاً با همون اولویتِ قبلی انجام می‌شه: اول ادمین، بعد
    # تعدادِ اعضا، بعد رجیستری.
    async def _requester_level():
        try:
            req_uid_int = int(requester_uid)
        except (TypeError, ValueError):
            return "unknown"
        level = "unknown"
        for attempt in range(3):
            level = await get_panel_access_level(join_chat_id, req_uid_int, fresh=True)
            if level in PANEL_ALLOWED_LEVELS:
                break
            if attempt < 2:
                await asyncio.sleep(0.8)   # لیستِ اعضا بعد از ورود ممکنه کمی دیر آماده بشه
        return level

    async def _members_count():
        # اگه قبل از جوین از خودِ لینک شمرده بودیم، دوباره پروب نمی‌کنیم.
        if _pre_count is not None:
            return _pre_count
        return await _probe_member_count(entity, join_chat_id)

    async def _registry_state():
        if not GROUP_REGISTRY_API_URL:
            return (None, None, None)
        return await registry_check_group(join_chat_id)

    async def _no_check():
        return None

    level, _member_count, _reg = await asyncio.gather(
        _requester_level() if requester_uid is not None else _no_check(),
        _members_count(),
        _registry_state(),
        return_exceptions=True,
    )
    if isinstance(level, Exception):
        level = "unknown"          # fail-closed: چک نشد = دسترسی نیست
    if isinstance(_member_count, Exception):
        _member_count = None
    if isinstance(_reg, Exception):
        _reg = (None, None, None)

    # ---- فقط مالک/ادمینِ واقعیِ گروه حق داره گروه رو از پنل اضافه کنه ----
    # هنوز هیچ چیزی (پیام، دیتابیس، رجیستری) ثبت نشده؛ اگه درخواست‌دهنده ادمین
    # نبود، بی‌سروصدا لفت می‌دیم.
    if requester_uid is not None and level not in PANEL_ALLOWED_LEVELS:
        left_ok, leave_error = await _try_leave_chat(entity, join_chat_id)
        try:
            db.remove_auto_joined_group(join_chat_id)
        except Exception:
            pass
        detail = "" if left_ok else f" (لفت ناموفق: {leave_error})"
        return {
            "ok": False, "not_admin": True, "group_name": group_name, "left": bool(left_ok),
            "bot": BOT_DISPLAY_LABEL,
            "error": f"درخواست‌دهنده مالک/ادمینِ گروه «{group_name}» نیست؛ ربات «{BOT_DISPLAY_LABEL}» خارج شد{detail}.",
        }

    if _member_count is not None and _member_count < MIN_GROUP_MEMBERS:
        left_ok, leave_error = await _try_leave_chat(entity, join_chat_id)
        detail = " (لفت داده شد)" if left_ok else f" (لفت هم ناموفق: {leave_error})"
        return {"ok": False, "error": f"{_too_small_message(_member_count)}{detail}"}
    if _member_count is None:
        asyncio.create_task(_small_group_guard(join_chat_id, raw_name or "گروه", None))

    occupied, owner_label, _ = _reg
    if occupied:
        left_ok, leave_error = await _try_leave_chat(entity, join_chat_id)
        detail = " (لفت داده شد)" if left_ok else f" (لفت هم ناموفق: {leave_error})"
        return {"ok": False, "error": f"ربات «{owner_label}» از قبل توی «{group_name}» بود{detail}."}

    try:
        baseline = db.get_stats(join_chat_id).get("total_messages", 0) or 0
    except Exception:
        baseline = 0
    try:
        db.add_group(join_chat_id, group_name)
        # اگه ردیفِ این گروه قبلاً (از یک مسیرِ دیگه، مثلاً ثبتِ ادمین) بدونِ
        # اسم ساخته شده باشه، add_group به‌خاطرِ INSERT OR IGNORE اسمِ تازه رو
        # نادیده می‌گیره؛ برای همین با set_group_name هم مطمئن می‌شیم اسمِ
        # واقعی (وقتی داریمش) همیشه ذخیره بشه.
        if raw_name:
            db.set_group_name(join_chat_id, raw_name)
        db.register_auto_joined_group(join_chat_id, group_name, baseline)
        apply_default_group_locks(join_chat_id)
        member_count = getattr(entity, "participants_count", None)
        if member_count:
            db.set_group_member_count(join_chat_id, member_count)
            GROUP_INFO_LAST_CHECK[join_chat_id] = time.time()
    except Exception as e:
        safe_print(f"[perform_admin_join_test] خطا در ثبت گروه «{group_name}» در دیتابیس: {e}")

    asyncio.create_task(registry_announce_group(join_chat_id, group_name))
    # فعال‌سازیِ خودکار: مالک و ادمین‌ها رو از سروش می‌گیره، بدونِ نوشتنِ «فعال»
    asyncio.create_task(auto_activate_group(join_chat_id, group_name))

    return {"ok": True, "group_name": group_name, "chat_id": join_chat_id, "bot": BOT_DISPLAY_LABEL, "error": None}



_GONE_ERR_MARKERS = (
    "channel_private", "channelprivate", "kicked", "banned", "not a member", "not participant",
    "user_not_participant", "chat_forbidden", "chat forbidden", "you can't access", "cannot access",
    "no user has", "chat not found", "peer_id_invalid",
)


async def bot_membership_state(chat_id):
    """وضعیتِ واقعیِ خودِ این ربات توی گروه: "member" | "gone" | "unknown".
    gone = بن/اخراج شده یا خودش لفت داده (دیگه عضو نیست). unknown = نتونستیم مطمئن بشیم
    (مثلاً خطای شبکه) - در این حالت هیچ‌چیزی حذف نمی‌شه."""
    try:
        chat_id = int(chat_id)
    except (TypeError, ValueError):
        return "unknown"
    try:
        perms = await client.get_permissions(chat_id, 'me')
    except Exception as e:
        msg = str(e).lower()
        if any(k in msg for k in _GONE_ERR_MARKERS):
            return "gone"
        confirmed = await _confirm_left(chat_id)   # True = توی دیالوگ‌ها نیست
        return "gone" if confirmed is True else "unknown"
    participant = getattr(perms, 'participant', None)
    pname = type(participant).__name__.lower() if participant is not None else ""
    if "banned" in pname or pname.endswith("left"):
        return "gone"
    rights = getattr(participant, 'banned_rights', None)
    if rights and bool(getattr(rights, 'view_messages', False)):
        return "gone"
    return "member"


async def _cleanup_after_bot_gone(chat_id):
    """ربات دیگه توی این گروه نیست؛ ردپاش از لیستِ چکِ فعالیت و رجیستری پاک بشه."""
    try:
        db.remove_auto_joined_group(chat_id)
    except Exception:
        pass
    if GROUP_REGISTRY_API_URL:
        try:
            await registry_leave_group(chat_id)
        except Exception:
            pass


async def perform_admin_leave_group(chat_id):
    """درخواستِ خروجِ ربات از یک گروهِ مشخص (chat_id) که از پنلِ مرکزی می‌رسه -
    برای وقتی که یک کاربرِ بخشِ عمومی ثبتِ گروهش رو حذف می‌کنه."""
    try:
        chat_id = int(chat_id)
    except (TypeError, ValueError):
        return {"ok": False, "error": "chat_id نامعتبر."}
    # اگه ربات از قبل از این گروه بن/اخراج شده (یا لفت داده)، نیازی به لفت نیست.
    if await bot_membership_state(chat_id) == "gone":
        await _cleanup_after_bot_gone(chat_id)
        return {"ok": True, "chat_id": chat_id, "already_left": True, "bot": BOT_DISPLAY_LABEL, "error": None}
    try:
        entity = await client.get_entity(chat_id)
    except Exception:
        entity = chat_id
    left_ok, leave_error = await _try_leave_chat(entity, chat_id)
    if not left_ok:
        # شاید همین الان بن/اخراج شده باشه؛ یک بار دیگه وضعیت رو بسنج.
        if await bot_membership_state(chat_id) == "gone":
            await _cleanup_after_bot_gone(chat_id)
            return {"ok": True, "chat_id": chat_id, "already_left": True, "bot": BOT_DISPLAY_LABEL, "error": None}
        return {"ok": False, "error": str(leave_error) or "نامشخص", "bot": BOT_DISPLAY_LABEL}
    try:
        asyncio.create_task(registry_leave_group(chat_id))
    except Exception:
        pass
    return {"ok": True, "chat_id": chat_id, "bot": BOT_DISPLAY_LABEL, "error": None}


async def migrate_legacy_unmarked_group_ids():
    """یک باگِ قدیمی توی مسیرِ «جوین با لینک» (چه از پی‌وی خودِ سلف، چه از
    دستورِ پنلِ مرکزی) باعث می‌شد گروه با entity.id خام (همیشه مثبت) به‌جای
    آیدیِ مارک‌دارِ درست (همیشه منفی - همونی که event.chat_id واقعی موقعِ هر
    پیام برمی‌گردونه) توی دیتابیس ثبت بشه. نتیجه: قفل‌ها/تنظیماتی که مالکِ
    گروه از پنل تنظیم می‌کرد واقعاً INSERT/UPDATE می‌شد («ثبت می‌شد») ولی روی
    آیدیِ اشتباهی می‌نشست که هیچ پیامِ واقعیِ گروهی باهاش match نمی‌شد («اعمال
    نمی‌شد»). این تابع یک‌بار در هر استارت چک می‌کنه چنین گروه‌هایی هست یا نه
    و اگه بود (و هنوز عضوشونیم)، خودکار داده‌ها رو به آیدیِ درست منتقل می‌کنه -
    مالکِ گروه لازم نیست کاری بکنه یا ربات رو دوباره اضافه کنه."""
    try:
        stale_ids = await asyncio.to_thread(db.get_unmarked_group_ids)
    except Exception as e:
        safe_print(f"[migrate_legacy_unmarked_group_ids] خطا در خوندنِ لیستِ آیدی‌های خراب: {e}")
        return
    if not stale_ids:
        return
    safe_print(f"[migrate_legacy_unmarked_group_ids] {len(stale_ids)} گروه با آیدیِ قدیمی/خرابِ باگِ جوین-با-لینک پیدا شد؛ در حال اصلاحِ خودکار...")
    fixed, failed = 0, 0
    for old_id in stale_ids:
        try:
            entity = await client.get_entity(old_id)
            new_id = await client.get_peer_id(entity)
        except Exception as e:
            failed += 1
            safe_print(f"[migrate_legacy_unmarked_group_ids] نتونستم گروهِ {old_id} رو resolve کنم (شاید دیگه توش نیستیم - دفعه‌ی بعد دوباره امتحان می‌شه): {e}")
            continue
        try:
            await asyncio.to_thread(db.remap_group_id, old_id, new_id)
            fixed += 1
            safe_print(f"[migrate_legacy_unmarked_group_ids] گروهِ {old_id} -> {new_id} اصلاح شد؛ قفل‌ها/تنظیماتش از این به بعد درست اعمال می‌شن.")
        except Exception as e:
            failed += 1
            safe_print(f"[migrate_legacy_unmarked_group_ids] خطا در انتقالِ دیتای گروهِ {old_id}: {e}")
    safe_print(f"[migrate_legacy_unmarked_group_ids] تمام: {fixed} گروه اصلاح شد، {failed} مورد ناموفق.")


def _iran_date_str(offset_days=0):
    return (datetime.now(IRAN_TZ) - timedelta(days=offset_days)).strftime("%Y-%m-%d")


async def _get_member_count(chat_id):
    """تعدادِ اعضای گروه - عمداً اینجا هیچ client.get_entity زنده‌ای صدا زده
    نمی‌شه؛ فقط از کشِ groups.member_count (که خودِ ربات هر ۶ ساعت یک‌بار،
    throttled و پخش‌شده در طولِ زمان، تازه نگه می‌داره - نگاه کنید به بخشِ
    «بهینه‌سازیِ سرعت» بالای فایل) می‌خونیم. علتش این بود که یک get_entity
    مستقیم و بی‌مهابا دقیقاً لحظه‌ای که کاربر روی «پنل» می‌زد، زیرِ فشارِ
    چندین کلیکِ هم‌زمان باعثِ هنگ‌کردنِ کاملِ ربات می‌شد. اگه هنوز عددی کش
    نشده باشه (گروهِ خیلی تازه)، به‌جاش از تعدادِ اعضای «دیده‌شده» تخمین
    می‌زنیم؛ هر دو حالت با approx مشخص می‌شن."""
    count, approx, _ = db.get_group_member_count(chat_id)
    if count:
        return int(count), approx
    try:
        seen = db.get_seen_members(chat_id)
        if seen:
            return len(seen), True
    except Exception:
        pass
    return None, False


async def build_admin_group_info(chat_id):
    """همه‌ی اطلاعاتی که پنلِ مرکزی برای نمایشِ آمار/قفل‌ها/تنظیماتِ یک گروه
    لازم داره، یکجا. بعدِ هر تغییر (toggle قفل، تنظیمات و...) هم دوباره صدا
    زده می‌شه تا پنل همیشه یک snapshot تازه نشون بده."""
    member_count, member_approx = await _get_member_count(chat_id)
    total_messages = (db.get_stats(chat_id) or {}).get("total_messages", 0)
    auto_mod = db.get_auto_mod_settings(chat_id) or {}
    welcome = db.get_welcome_settings(chat_id) or {}
    try:
        group_name = await resolve_group_name(chat_id)
    except Exception:
        group_name = db.get_group_name(chat_id) or "گروه"

    # نکته: عمداً از db.is_locked (کش‌شده، بلافاصله بعدِ set_lock هم‌زمان
    # به‌روزه) استفاده می‌کنیم، نه db.get_active_locks (که مستقیم SQLite رو
    # می‌خونه) - چون set_lock نوشتنش صف‌بندی‌شده/پس‌زمینه‌ست و یک خواندنِ
    # مستقیم درست بعدِ toggle ممکنه هنوز مقدارِ قدیمی رو ببینه.
    locks_state = {}
    for key in LOCK_DISPLAY_NAMES:
        if key in LOCK_AUTOMOD_FIELDS:
            locks_state[key] = bool(auto_mod.get(LOCK_AUTOMOD_FIELDS[key]))
        else:
            locks_state[key] = db.is_locked(chat_id, key)

    # نکته: فعالان/رتبه‌بندی (user_activity_daily) عمداً اینجا نیست - چون
    # رزالوکردنِ اسمِ هرکاربر یک client.get_entity جداست و این تابع باید
    # سریع بمونه (بعدِ هر toggle یک‌بار صدا زده می‌شه). برای اون از
    # build_activity_leaderboard (فقط وقتی ادمین واقعاً می‌خواد ببینتش) استفاده کن.

    return {
        "ok": True,
        "chat_id": chat_id,
        "group_name": group_name,
        "member_count": member_count,
        "member_count_approx": member_approx,
        "total_messages": total_messages,
        "locks": locks_state,
        "warn_limit": auto_mod.get("warn_limit", 3),
        "silent_mode": bool(auto_mod.get("silent_mode")),
        "welcome_enabled": bool(welcome.get("enabled", True)),
        "welcome_message": welcome.get("message"),
    }


def _is_placeholder_group_name(name):
    """نام‌های الکی/جایگزین (خالی یا دقیقاً همون کلمه‌ی خامِ «گروه» که قبلاً
    به‌اشتباه توی دیتابیس ذخیره می‌شد) رو تشخیص می‌ده - تا این مقدارهای
    قدیمی/خراب مثلِ یک اسمِ واقعی رفتار نشن و دوباره تلاش برای resolve
    کردنِ اسمِ واقعی انجام بشه."""
    return not name or str(name).strip() == "گروه"


async def resolve_group_name(chat_id):
    """اسمِ واقعیِ یک گروه رو برمی‌گردونه - نه فقط کلمه‌ی خامِ «گروه» که قبلاً
    جای خالیِ اسم رو پر می‌کرد. اول از دیتابیس (سریع، بدونِ شبکه) می‌خونه؛
    اگه خالی بود، یا قبلاً خودش با همون مقدارِ خرابِ «گروه» ذخیره شده بود
    (باگِ قدیمی)، مستقیم از خودِ سروش resolve می‌کنه و بلافاصله ذخیره‌ش
    می‌کنه (با set_group_name) تا دفعه‌ی بعد دیگه نیازی به شبکه نباشه. فقط
    اگه هیچ‌کدوم جواب نداد (مثلاً ربات دیگه توی گروه نیست)، به‌عنوانِ
    آخرین راهِ‌حل «گروه» برمی‌گردونه."""
    name = db.get_group_name(chat_id)
    if not _is_placeholder_group_name(name):
        return name
    try:
        entity = await client.get_entity(chat_id)
        live_name = getattr(entity, 'title', None) or getattr(entity, 'name', None)
    except Exception:
        live_name = None
    if live_name:
        try:
            db.set_group_name(chat_id, live_name)
        except Exception:
            pass
        return live_name
    return name or "گروه"


_ACTIVITY_PERIOD_RANGES = {
    "today": lambda: (_iran_date_str(0), _iran_date_str(0)),
    "yesterday": lambda: (_iran_date_str(1), _iran_date_str(1)),
    "week": lambda: (_iran_date_str(6), _iran_date_str(0)),
    "month": lambda: (_iran_date_str(29), _iran_date_str(0)),
}


async def build_activity_leaderboard(chat_id, period):
    """برترین‌های یک گروه توی یک بازه‌ی زمانی. اسمِ نمایشی از کشِ محلیِ
    user_profiles خونده می‌شه (که با نرخِ کم و throttled توسطِ خودِ حلقه‌ی
    پیام‌ها پر می‌شه - نگاه کنید به USER_INFO_LAST_CHECK)، نه یک
    client.get_entity زنده - وگرنه با تا ۱۰ درخواستِ شبکه‌ای پشتِ‌سرِهم
    دقیقاً همون هنگِ کاملِ ربات پیش میومد که برای _get_member_count رخ می‌داد."""
    date_from, date_to = _ACTIVITY_PERIOD_RANGES.get(period, _ACTIVITY_PERIOD_RANGES["today"])()
    top = db.get_top_active(chat_id, date_from, date_to, 10)
    entries = []
    for user_id, count in top:
        try:
            profile = db.get_user_profile(user_id) or {}
        except Exception:
            profile = {}
        name = profile.get("display_name") or profile.get("custom_name")
        entries.append({"user_id": user_id, "count": count, "name": truncate_display_name(name or str(user_id))})
    # مجموعِ کلِ پیام‌های این بازه (نه فقط ۱۰ نفرِ برتر) - از همون جدولِ
    # user_activity_daily که خودِ ربات با هر پیام پر می‌کنه؛ بدونِ این، وقتی
    # فعالیتِ گروه بینِ کاربرهای زیادی پخش می‌شه، عددهای لیدربورد خیلی کمتر از
    # فعالیتِ واقعیِ گروه به‌نظر می‌رسیدن.
    try:
        period_total = db.get_period_total_messages(chat_id, date_from, date_to)
    except Exception:
        period_total = None
    return {"ok": True, "chat_id": chat_id, "period": period, "entries": entries, "period_total": period_total}


# ==================== گزارشِ شبانه‌ی فعالیت (برترین‌های هر گروه) ====================
NIGHTLY_REPORT_HOUR = 23   # ساعت (به‌وقتِ ایران) که گزارشِ شبانه برای هر گروه فرستاده می‌شه
NIGHTLY_REPORT_TOP_N = 10  # چند نفرِ برتر از هر بازه توی گزارش بیاد
_NIGHTLY_REPORT_RANK_MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}
_NIGHTLY_REPORT_PERIODS = [
    ("today", "امروز", "📅"),
    ("yesterday", "دیروز", "🌙"),
    ("week", "این‌هفته", "📈"),
    ("month", "این‌ماه", "🏆"),
]


def _format_leaderboard_block(title, emoji, entries):
    """یک بخشِ تمیز و مرتب برای یک بازه (امروز/دیروز/هفته/ماه): سه نفرِ اول با
    مدال، بقیه با شماره. اگه هیچ فعالیتی نبوده، به‌جای لیستِ خالی صادقانه
    همینو می‌گه."""
    if not entries:
        return f"{emoji} **{title}**\n   – بدون فعالیت"
    lines = [f"{emoji} **{title}**"]
    for i, e in enumerate(entries, start=1):
        rank = _NIGHTLY_REPORT_RANK_MEDALS.get(i, f"{i}.")
        lines.append(f"   {rank} {e['name']} — {e['count']:,}")
    return "\n".join(lines)


_NIGHTLY_REPORT_PERIOD_MIN_DAYS = {
    "today": 0,
    "yesterday": 1,
    "week": 7,
    "month": 30,
}


def _group_days_since_joined(chat_id):
    """چند روز از اولین باری که این گروه ثبت شده گذشته. اگه معلوم نبود
    None برمی‌گردونه (یعنی احتیاط کن، فرض نکن گروه قدیمیه یا تازه‌ست)."""
    try:
        joined_at = db.get_group_joined_at(chat_id)
        if not joined_at:
            return None
        joined_date = datetime.fromisoformat(joined_at).date()
        return (datetime.now().date() - joined_date).days
    except Exception:
        return None


async def build_nightly_group_report(chat_id):
    """گزارشِ شبانه‌ی یک گروه: امروز بالا (با ۱۰ نفرِ برتر)، بعدش دیروز/
    این‌هفته/این‌ماه زیرش - همه توی یک پیامِ تمیزِ واحد. هر بازه فقط وقتی
    نشون داده می‌شه که واقعاً از عمرِ گروه گذشته باشه: دیروز فقط اگه گروه
    حداقل ۱ روزه، هفته فقط اگه حداقل ۷ روزه، ماه فقط اگه حداقل ۳۰ روزه -
    وگرنه نشون‌دادنِ یک «هفته‌ی» بی‌معنی برای گروهی که دیروز اضافه شده
    گمراه‌کننده‌ست."""
    days_since_joined = _group_days_since_joined(chat_id)
    if days_since_joined is None:
        periods = _NIGHTLY_REPORT_PERIODS
    else:
        periods = [
            (key, label, emoji) for key, label, emoji in _NIGHTLY_REPORT_PERIODS
            if days_since_joined >= _NIGHTLY_REPORT_PERIOD_MIN_DAYS[key]
        ]
    blocks = []
    for period_key, label, emoji in periods:
        info = await build_activity_leaderboard(chat_id, period_key)
        entries = (info.get("entries") or [])[:NIGHTLY_REPORT_TOP_N]
        blocks.append(_format_leaderboard_block(label, emoji, entries))
    group_name = await resolve_group_name(chat_id)
    header = f"📊 **گزارشِ شبانه‌ی «{group_name}»**\n━━━━━━━━━━━━━━━━━━"
    if days_since_joined is not None and days_since_joined < 1:
        header += "\n_(ربات امروز به این گروه اضافه شده؛ فقط آمارِ امروز موجوده)_"
    return header + "\n\n" + "\n\n".join(blocks)


async def nightly_activity_report_loop():
    """هر شب یک‌بار (ساعتِ NIGHTLY_REPORT_HOUR به‌وقتِ ایران)، برای همه‌ی
    گروه‌های شناخته‌شده، گزارشِ برترین‌های امروز + دیروز/هفته/ماه رو می‌فرسته.
    یک فلگِ روی دیسک (last_nightly_report_date، از طریقِ settings) نگه‌داشته
    می‌شه تا اگه ربات دقیقاً همون ساعت ری‌استارت شد، گزارشِ همون شب دوباره
    فرستاده نشه."""
    await asyncio.sleep(30)
    while True:
        try:
            now = datetime.now(IRAN_TZ)
            today_str = now.strftime("%Y-%m-%d")
            if now.hour == NIGHTLY_REPORT_HOUR and db.get_setting("last_nightly_report_date") != today_str:
                group_ids = db.get_known_groups()
                safe_print(f"[nightly_activity_report_loop] شروعِ ارسالِ گزارشِ شبانه برای {len(group_ids)} گروه...")
                sent, failed = 0, 0
                for gid in group_ids:
                    try:
                        report_text = await build_nightly_group_report(gid)
                        await client.send_message(gid, report_text)
                        sent += 1
                    except Exception as e:
                        failed += 1
                        safe_print(f"[nightly_activity_report_loop] خطا در ارسالِ گزارش به گروهِ {gid}: {e}")
                    await asyncio.sleep(1.5)
                db.set_setting("last_nightly_report_date", today_str)
                safe_print(f"[nightly_activity_report_loop] تمام: {sent} گروه موفق، {failed} ناموفق.")
        except Exception as e:
            safe_print(f"[nightly_activity_report_loop] خطا: {e}")
        await asyncio.sleep(300)


async def perform_admin_panel_command(action, chat_id, payload=None):
    """یک عملِ مدیریتی روی یک گروهِ مشخص (از طرفِ پنلِ مرکزی/بخشِ عمومی) اجرا
    می‌کنه و همیشه یک snapshot تازه از build_admin_group_info برمی‌گردونه -
    چه برای «فقط گرفتنِ اطلاعات» (action == "get")، چه بعدِ یک تغییر - تا پنل
    مجبور نباشه برای هر تغییر دو بار درخواست بزنه."""
    payload = payload or {}
    try:
        chat_id = int(chat_id)
    except (TypeError, ValueError):
        return {"ok": False, "error": "chat_id نامعتبر."}

    if action == "auth":
        bot_state = await bot_membership_state(chat_id)
        if bot_state == "gone":
            asyncio.create_task(_cleanup_after_bot_gone(chat_id))
            return {"ok": True, "authorized": False, "level": "unknown", "bot_state": "gone", "bot": BOT_DISPLAY_LABEL}
        level = await get_panel_access_level(chat_id, payload.get("uid"), fresh=bool(payload.get("fresh")))
        return {"ok": True, "authorized": level in PANEL_ALLOWED_LEVELS, "level": level,
                "bot_state": bot_state, "bot": BOT_DISPLAY_LABEL}

    if action == "activity":
        try:
            return await build_activity_leaderboard(chat_id, payload.get("period", "today"))
        except Exception as e:
            return {"ok": False, "error": str(e)}

    try:
        if action == "toggle_lock":
            key = payload.get("key")
            if key not in LOCK_DISPLAY_NAMES:
                return {"ok": False, "error": "نوع قفل نامعتبر."}
            if key in LOCK_AUTOMOD_FIELDS:
                field = LOCK_AUTOMOD_FIELDS[key]
                current = bool((db.get_auto_mod_settings(chat_id) or {}).get(field))
                db.set_auto_mod_settings(chat_id, **{field: not current})
            else:
                current = db.is_locked(chat_id, key)
                db.set_lock(chat_id, key, not current)

        elif action == "warn_delta":
            delta = int(payload.get("delta", 0))
            current = (db.get_auto_mod_settings(chat_id) or {}).get("warn_limit", 3)
            db.set_auto_mod_settings(chat_id, warn_limit=max(1, min(20, int(current) + delta)))

        elif action == "toggle_silent":
            current = bool((db.get_auto_mod_settings(chat_id) or {}).get("silent_mode"))
            db.set_auto_mod_settings(chat_id, silent_mode=not current)

        elif action == "toggle_welcome":
            current = bool((db.get_welcome_settings(chat_id) or {}).get("enabled", True))
            db.set_welcome_settings(chat_id, enabled=not current)

        elif action == "set_welcome_message":
            message = (payload.get("message") or "").strip()
            if not message:
                return {"ok": False, "error": "متن نمی‌تونه خالی باشه."}
            db.set_welcome_settings(chat_id, message=message)

        elif action != "get":
            return {"ok": False, "error": "عملِ نامعتبر."}
    except Exception as e:
        return {"ok": False, "error": str(e)}

    return await build_admin_group_info(chat_id)


async def runtime_maintenance_loop():
    """هر چند ساعت کش‌های موقت RAM را سبک می‌کند و SQLite/WAL را مرتب می‌کند.
    هیچ داده‌ی تنظیماتی یا رکورد دائمی حذف نمی‌شود."""
    # کمی بعد از startup اجرا شود تا شروع ربات تحت تأثیر نگهداری قرار نگیرد.
    await asyncio.sleep(RUNTIME_CACHE_CLEANUP_INTERVAL_SECONDS)
    while True:
        try:
            # کش‌های کوتاه‌عمر این فایل
            trim_runtime_memory()
            _trim_hot_caches()
            for cache_name in ("FILTER_WARN_CACHE", "WARN_MESSAGE_CACHE", "GROUP_INFO_LAST_CHECK", "AUTO_MOD_CACHE", "WELCOME_CACHE", "GOODBYE_CACHE"):
                cache = globals().get(cache_name)
                if hasattr(cache, "clear"): cache.clear()

            # صف writer و فایل SQLite/WAL هم دوره‌ای مرتب می‌شوند.
            await asyncio.to_thread(db.clear_runtime_caches)
            await asyncio.to_thread(db.perform_maintenance, True)

            safe_print("🧹 نگهداری خودکار انجام شد: کش‌های موقت سبک و دیتابیس مرتب شد؛ اطلاعات دائمی حذف نشد.")
        except Exception as e:
            safe_print(f"[runtime_maintenance_loop] خطا: {e}")

        await asyncio.sleep(DB_MAINTENANCE_INTERVAL_SECONDS)

async def flush_stats_loop():
    """آمار پیام‌ها (کل گروه + هر کاربر) که در message_handler فقط در حافظه جمع
    می‌شن رو هر چند ثانیه یک‌بار به‌صورت دسته‌جمعی روی دیتابیس می‌نویسه. این کار
    باعث می‌شه به‌جای دو تا نوشتن روی دیتابیس به ازای *هر* پیام (که زیر بار هزار
    گروه خیلی سنگین بود)، فقط هر ۲۰ ثانیه یک‌بار و برای هرکدوم فقط یک کوئری زده بشه."""
    global _pending_chat_stats, _pending_user_stats, _pending_daily_activity, _pending_hourly_activity, _last_daily_prune_at
    while True:
        await asyncio.sleep(STATS_FLUSH_SECONDS)
        try:
            chat_batch = _pending_chat_stats
            user_batch = _pending_user_stats
            daily_batch = _pending_daily_activity
            hourly_batch = _pending_hourly_activity
            _pending_chat_stats = {}
            _pending_user_stats = {}
            _pending_daily_activity = {}
            _pending_hourly_activity = {}

            # قبلاً اینجا به ازای هر گروه/کاربر یک INSERT+commit جداگانه زده می‌شد
            # (ببینید بحث در database.py -> bulk_update_stats)؛ زیر بار ۲-۳ هزار
            # گروه فعال همین حلقه‌ی به‌ظاهر ساده، هر ۲۰ ثانیه چند ثانیه (گاهی بیشتر)
            # کل event loop رو فریز می‌کرد و باعث می‌شد همه‌ی دستورات توی همه‌ی
            # گروه‌ها دیر جواب بدن. الان همه با هم و توی یک تراکنش نوشته می‌شن.
            try:
                await asyncio.to_thread(db.bulk_update_stats, chat_batch, user_batch)
            except Exception as e:
                safe_print(f"[flush_stats_loop] خطا در ذخیره‌ی دسته‌جمعی آمار: {e}")
            try:
                await asyncio.to_thread(db.bulk_update_daily_activity, daily_batch)
            except Exception as e:
                safe_print(f"[flush_stats_loop] خطا در ذخیره‌ی فعالیتِ روزانه: {e}")
            try:
                await asyncio.to_thread(db.bulk_update_hourly_activity, hourly_batch)
            except Exception as e:
                safe_print(f"[flush_stats_loop] خطا در ذخیره‌ی فعالیتِ ساعتی: {e}")

            # هر ~۶ ساعت یک‌بار ردیف‌های خیلی قدیمیِ فعالیتِ روزانه رو پاک کن
            # (رتبه‌بندی بیشتر از یک ماه عقب نیازی نداره).
            now_ts = time.time()
            if now_ts - _last_daily_prune_at > 6 * 3600:
                _last_daily_prune_at = now_ts
                try:
                    await asyncio.to_thread(db.prune_daily_activity, 35)
                except Exception as e:
                    safe_print(f"[flush_stats_loop] خطا در پاک‌سازیِ فعالیتِ روزانه‌ی قدیمی: {e}")
                try:
                    # آمار ساعتی فقط تا کمی بیشتر از بازه‌ی چک (۲۴ ساعت) لازمه.
                    await asyncio.to_thread(db.prune_hourly_activity, now_ts - (LOW_ACTIVITY_CHECK_HOURS + 24) * 3600)
                except Exception as e:
                    safe_print(f"[flush_stats_loop] خطا در پاک‌سازیِ فعالیتِ ساعتیِ قدیمی: {e}")
        except Exception as e:
            safe_print(f"[flush_stats_loop] خطا: {e}")


async def send_online_heartbeat():
    """فقط یک پیام کوتاه «آنلاینم» به پی‌وی سازنده(های) ربات می‌فرسته؛ هیچ جزئیاتی
    درباره‌ی پیام حذف‌شده/کاربر حذف‌شده/تعداد ممبر پاک‌شده و... گزارش نمی‌شه."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"🟢 من آنلاینم.\n🕒 {now_str}"
    for owner_id in SUPER_OWNER_IDS:
        try:
            await client.send_message(owner_id, msg)
        except Exception as e:
            safe_print(f"[send_online_heartbeat] خطا در ارسال به {owner_id}: {e}")


async def online_heartbeat_loop():
    """هر نیم ساعت یک‌بار فقط یک پیام «آنلاینم» به مالک(های) ربات می‌فرسته."""
    await asyncio.sleep(ONLINE_HEARTBEAT_INTERVAL_SECONDS)
    while True:
        try:
            await send_online_heartbeat()
        except Exception as e:
            safe_print(f"[online_heartbeat_loop] خطا: {e}")
        await asyncio.sleep(ONLINE_HEARTBEAT_INTERVAL_SECONDS)


# ==================== اتصال به پنل مرکزی سازنده ====================
def _panel_file_path(filename):
    return os.path.join(BASE_DIR, filename)


def get_or_create_panel_bot_id():
    """یه شناسه‌ی یکتا برای این نمونه از ربات می‌سازه (یک‌بار) و توی یک فایل کنار
    main.py نگه می‌داره تا با هر ری‌استارت همون شناسه‌ی قبلی استفاده بشه. این یعنی
    نیازی نیست دستی برای هر گوشی/نصب یه آیدی جدا تنظیم کنی."""
    path = _panel_file_path(PANEL_BOT_ID_FILE)
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                existing = f.read().strip()
                if existing:
                    return existing
    except Exception:
        pass
    import uuid
    new_id = uuid.uuid4().hex[:12]
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(new_id)
    except Exception as e:
        safe_print(f"[panel] هشدار: نتونستم شناسه‌ی پنل رو ذخیره کنم: {e}")
    return new_id


def _load_last_applied_command_id():
    """آخرین دستوری که این نمونه واقعاً اجرا کرده رو از دیسک می‌خونه (نه فقط از
    حافظه) - چون دستور «ری‌استارت» با os.execv کل پروسه رو عوض می‌کنه و هر
    متغیر پایتونی از بین می‌ره. بدون این فایل، بات بعد از هر ری‌استارت دوباره
    فکر می‌کرد این دستور رو ندیده و بی‌نهایت ری‌استارت می‌کرد."""
    path = _panel_file_path(PANEL_LAST_COMMAND_FILE)
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                val = f.read().strip()
                if val:
                    return int(val)
    except Exception:
        pass
    return None


def _save_last_applied_command_id(command_id):
    path = _panel_file_path(PANEL_LAST_COMMAND_FILE)
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(str(command_id))
    except Exception as e:
        safe_print(f"[panel] هشدار: نتونستم آخرین دستور اجراشده رو ذخیره کنم: {e}")


PANEL_BOT_ID = get_or_create_panel_bot_id() if PANEL_API_URL else None
_panel_last_applied_broadcast_id = _load_last_applied_command_id() if PANEL_API_URL else None


def _report_offline_to_panel():
    """گزارش «دارم می‌رم آفلاین» به پنل مرکزی، به‌محض یک خروج تمیز (Ctrl+C، سیگنال
    SIGTERM، یا هر بازگشت عادی از main()). بدون این تابع، وقتی بات رو تمیز خاموش
    می‌کنی، پنل تا مدتی (OFFLINE_AFTER_SECONDS توی panel_server.py) هنوز
    «آنلاین» نشونش می‌ده. این تابع sync و بدون asyncio نوشته شده چون ممکنه event
    loop در حال بسته‌شدن باشه و دیگه قابل‌اعتماد نباشه.

    توجه: این فقط خاموشی‌های *تمیز* رو پوشش می‌ده. کشته‌شدن ناگهانی پروسه توسط
    سیستم‌عامل (مثلاً OOM killer اندروید) قابل‌گرفتن نیست - در اون حالت پنل
    همچنان باید تا timeout معمولی صبر کنه."""
    if not PANEL_API_URL or not PANEL_BOT_ID:
        return
    try:
        import urllib.request
        payload = json.dumps({
            "secret": PANEL_SECRET,
            "bot_id": PANEL_BOT_ID,
            "event": "offline",
        }).encode("utf-8")
        req = urllib.request.Request(
            f"{PANEL_API_URL}/report",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        # چند تلاش کوتاه (نه فقط یک‌بار) - چون سرور پنل ممکنه لحظه‌ای زیر بار
        # چند بات دیگه که هم‌زمان دارن گزارش می‌دن باشه؛ یک تلاش تنها ممکنه
        # درست همون لحظه با یک خطای موقت (نه واقعی) رد بشه.
        last_err = None
        for attempt in range(3):
            try:
                urllib.request.urlopen(req, timeout=5)
                safe_print("📴 گزارش خاموشی تمیز به پنل مرکزی فرستاده شد.")
                return
            except Exception as e:
                last_err = e
                time.sleep(1)
        safe_print(f"[panel] هشدار: نتونستم گزارش خاموشی رو به پنل بفرستم: {last_err}")
    except Exception as e:
        safe_print(f"[panel] هشدار: نتونستم گزارش خاموشی رو به پنل بفرستم: {e}")


if PANEL_API_URL:
    atexit.register(_report_offline_to_panel)

    def _handle_term_signal(signum, frame):
        safe_print(f"🛑 سیگنال خروج ({signum}) دریافت شد؛ در حال خروج تمیز (و اطلاع به پنل)...")
        raise SystemExit(0)

    try:
        signal.signal(signal.SIGTERM, _handle_term_signal)
    except Exception:
        pass  # روی بعضی محیط‌ها (مثلاً بعضی نسخه‌های ویندوز) ممکنه SIGTERM در دسترس نباشه


async def _panel_send_report(first_attempt=False):
    """یک گزارش رو می‌سازه و می‌فرسته؛ در صورت خطا، دلیل دقیق (نه فقط پیام کلی
    aiohttp) رو چاپ می‌کنه تا مشخص بشه مشکل از آدرس/DNS/فایروال/پورت بسته‌ست یا
    از secret اشتباه یا هرچیز دیگه. خروجی: True اگه سرور با 200 جواب داد."""
    groups = db.get_known_groups_with_names()
    payload = {
        "bot_id": PANEL_BOT_ID,
        "secret": PANEL_SECRET,
        "status": "online",
        "group_count": len(groups),
        "groups": groups,
        "days_active": get_days_active(),
        "account_id": BOT_ID,
        "project": PANEL_BOT_PROJECT,
    }
    url = f"{PANEL_API_URL}/report"
    tag = "[panel] اولین تلاش برای اتصال" if first_attempt else "[panel_report_loop]"
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    if first_attempt:
                        safe_print(f"{tag} ✅ وصل شد و به پنل ({url}) اضافه شد. شناسه: {PANEL_BOT_ID}")
                    return True
                body = await resp.text()
                safe_print(f"{tag} ⛔ سرور جواب {resp.status} داد (نه 200) - آدرس={url} | پاسخ={body[:300]}")
                if resp.status == 401:
                    safe_print(f"{tag} ⛔ علت ۹۹٪ ارورهای 401: PANEL_SECRET توی این config.py با PANEL_SECRET "
                                f"توی panel_server.py که واقعاً روی سرور در حال اجراست یکی نیست. دقیقاً همون "
                                f"رشته رو از سرور کپی کن توی config.py (یا برعکس).")
                return False
    except aiohttp.ClientConnectorError as e:
        safe_print(f"{tag} ⛔ اصلاً نتونستم به سرور وصل بشم (Connection refused / بسته بودن پورت / آدرس اشتباه). "
                    f"آدرس={url} | جزئیات={e}\n"
                    f"    -> مطمئن شو panel_server.py واقعاً روی همون سرور و پورت 8787 در حال اجراست "
                    f"(نه فقط قبلاً اجرا شده و الان خاموشه)، و فایروال/هاست پورت 8787 رو برای اتصال بیرونی باز گذاشته.")
        return False
    except asyncio.TimeoutError:
        safe_print(f"{tag} ⛔ سرور توی ۱۵ ثانیه جواب نداد (Timeout). آدرس={url} - "
                    f"یا سرور خیلی کنده/گیر کرده، یا یک فایروال بین راه بی‌سروصدا پکت‌ها رو دراپ می‌کنه.")
        return False
    except Exception as e:
        safe_print(f"{tag} ⛔ خطای غیرمنتظره در گزارش به پنل: {type(e).__name__}: {e}")
        return False


async def panel_report_loop():
    """اگه پنل مرکزی تنظیم شده باشه (PANEL_API_URL توی config.py)، هر چند دقیقه
    یک‌بار یک گزارش کوتاه (آنلاین بودن + تعداد گروه‌های شناخته‌شده + اسم همون
    گروه‌ها + نام حساب) به سرور پنل می‌فرسته. هیچ محتوای پیام یا اطلاعات
    اعضای گروه فرستاده نمی‌شه - فقط شناسه و اسم خود گروه، دقیقاً همونی که توی
    گزینه‌ی «لیست گروه‌ها»/«لفت همه» خود ربات هم قابل مشاهده‌ست."""
    if not PANEL_API_URL:
        return
    # اولین گزارش عمداً تقریباً فوریه (نه بعد از ۱۰-۲۵ ثانیه) و نتیجه‌ش با
    # جزئیات کامل چاپ می‌شه - همونی که بشه فهمید «وصل شد» یا «چرا وصل نشد».
    await asyncio.sleep(3)
    await _panel_send_report(first_attempt=True)
    while True:
        await asyncio.sleep(PANEL_REPORT_INTERVAL_SECONDS)
        await _panel_send_report(first_attempt=False)


# ===================== اجرای هم‌زمانِ دستورهای پنل =====================
# قبلاً panel_broadcast_loop هر دستور (جوین/چکِ عضویت/خروج/دستورِ پنلِ گروهی) رو
# با await داخلِ خودِ حلقه اجرا می‌کرد؛ یعنی یک جوینِ ~۴۰ ثانیه‌ای همه‌ی کلیک‌ها و
# لفت‌های بعدیِ همین سلف رو معطل می‌کرد. الان هر دستور توی task جداگانه اجرا می‌شه
# و حلقه بلافاصله سراغِ poll بعدی می‌ره. محدودیت‌ها:
#   • جوین: حداکثر PANEL_JOIN_CONCURRENCY هم‌زمان (احتیاط برای flood-wait)
#   • بقیه: حداکثر PANEL_CMD_CONCURRENCY هم‌زمان
#   • دو دستور روی «یک لینک» یا «یک گروه» همچنان پشت‌سرهم اجرا می‌شن (قفلِ کلیددار)
#   • دستوری که manager دیگه منتظرش نیست (expires_in تموم شده) اجرا نمی‌شه؛ وگرنه
#     ممکن بود ربات عضوِ گروهی بشه که هیچ‌وقت ثبت نمی‌شه.
# restart و broadcast (پیام همگانی) عمداً مثلِ قبل داخلِ حلقه اجرا می‌شن.
import contextlib

_PANEL_CONCURRENT_TYPES = ("join_test", "check_membership", "leave_group", "panel_group_cmd")
PANEL_JOIN_CONCURRENCY = 3
PANEL_CMD_CONCURRENCY = 16
_PANEL_JOIN_SEM = asyncio.Semaphore(PANEL_JOIN_CONCURRENCY)
_PANEL_CMD_SEM = asyncio.Semaphore(PANEL_CMD_CONCURRENCY)
_PANEL_TASKS_INFLIGHT = set()   # bcast_id هایی که همین الان در حالِ اجران
_PANEL_DONE_RESULTS = {}        # bcast_id -> نتیجه (برای ack مجدد اگه ack قبلی گم شد)
_PANEL_DONE_MAX = 300
_PANEL_BG_TASKS = set()         # نگه‌داشتنِ ارجاع به taskها تا garbage-collect نشن
_PANEL_KEY_LOCKS = {}           # key -> [asyncio.Lock, تعدادِ منتظرها/دارنده‌ها]


@contextlib.asynccontextmanager
async def _panel_key_lock(key):
    """قفلِ مخصوصِ یک کلید (مثلاً یک لینک یا یک chat_id)؛ کلیدِ None = بدونِ قفل."""
    if key is None:
        yield
        return
    entry = _PANEL_KEY_LOCKS.get(key)
    if entry is None:
        entry = _PANEL_KEY_LOCKS[key] = [asyncio.Lock(), 0]
    entry[1] += 1
    try:
        async with entry[0]:
            yield
    finally:
        entry[1] -= 1
        if entry[1] <= 0:
            _PANEL_KEY_LOCKS.pop(key, None)


def _panel_time_left(expire_at, cap):
    """چقدر وقت مونده؛ اگه manager مهلتی نفرستاده باشه همون سقفِ قبلی (cap)."""
    if expire_at is None:
        return cap
    return min(cap, expire_at - time.monotonic())


async def _execute_panel_command(command_type, text, expire_at):
    """منطقِ دقیقاً همون منطقِ قبلیِ هر دستوره (فقط از حلقه بیرون اومده)؛ نتیجه‌ی
    dict رو برمی‌گردونه. تایم‌اوت‌های داخلی هم به مهلتِ باقی‌مونده محدود شدن."""
    global _PANEL_JOINS_IN_FLIGHT
    expired = {"ok": False, "error": "درخواست منقضی شد؛ صفِ این سلف شلوغ بود."}
    slow = {"ok": False, "error": "عملیات بیش از حد طول کشید (timeout داخلی)."}
    try:
        if command_type == "join_test":
            join_link, join_req_uid = text, None
            if text.lstrip().startswith("{"):
                try:
                    _jp = json.loads(text)
                    join_link, join_req_uid = _jp.get("link"), _jp.get("uid")
                except Exception:
                    join_link, join_req_uid = text, None
            key = None
            try:
                _l, _ident = _extract_group_link(join_link)
                key = ("link", str(_ident).lower()) if _ident else None
            except Exception:
                key = None
            async with _panel_key_lock(key):
                async with _PANEL_JOIN_SEM:
                    left = _panel_time_left(expire_at, 40)
                    if left < 2:
                        return expired
                    _PANEL_JOINS_IN_FLIGHT += 1
                    try:
                        return await asyncio.wait_for(
                            perform_admin_join_test(join_link, join_req_uid), timeout=left
                        )
                    except asyncio.TimeoutError:
                        return slow
                    finally:
                        _PANEL_JOINS_IN_FLIGHT = max(0, _PANEL_JOINS_IN_FLIGHT - 1)

        if command_type == "check_membership":
            try:
                check_payload = json.loads(text)
                check_link = check_payload.get("link")
                check_uid = check_payload.get("uid")
            except Exception:
                # سازگاری با فرمتِ قدیمی‌تر (فقط خودِ لینک، بدون uid)
                check_link, check_uid = text, None
            async with _PANEL_CMD_SEM:
                left = _panel_time_left(expire_at, 15)
                if left < 1:
                    return expired
                try:
                    return await asyncio.wait_for(perform_membership_check(check_link, check_uid), timeout=left)
                except asyncio.TimeoutError:
                    return slow

        if command_type == "leave_group":
            async with _panel_key_lock(("chat", str(text).strip())):
                async with _PANEL_CMD_SEM:
                    left = _panel_time_left(expire_at, 20)
                    if left < 1:
                        return expired
                    try:
                        return await asyncio.wait_for(perform_admin_leave_group(text), timeout=left)
                    except asyncio.TimeoutError:
                        return slow

        if command_type == "panel_group_cmd":
            try:
                cmd = json.loads(text)
            except Exception:
                cmd = {}
            key = ("chat", str(cmd.get("chat_id")).strip()) if cmd.get("chat_id") is not None else None
            async with _panel_key_lock(key):
                async with _PANEL_CMD_SEM:
                    left = _panel_time_left(expire_at, 20)
                    if left < 1:
                        return expired
                    try:
                        return await asyncio.wait_for(
                            perform_admin_panel_command(cmd.get("action", "get"), cmd.get("chat_id"), cmd.get("payload") or {}),
                            timeout=left,
                        )
                    except asyncio.TimeoutError:
                        return slow

        return {"ok": False, "error": "نوع دستور ناشناخته."}
    except Exception as e:
        return {"ok": False, "error": f"خطای داخلی سلف: {e}"}


async def _post_panel_ack(bcast_id, result):
    result = result or {"ok": False, "error": "بدون نتیجه."}
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            ack_payload = {
                "bot_id": PANEL_BOT_ID, "secret": PANEL_SECRET,
                "broadcast_id": bcast_id,
                "sent_count": 1 if result.get("ok") else 0,
                "failed_count": 0 if result.get("ok") else 1,
                "join_result": result,
            }
            await session.post(f"{PANEL_API_URL}/broadcast/ack", json=ack_payload)
        return True
    except Exception as e:
        safe_print(f"[panel_command] خطا در تایید نتیجه (id={bcast_id}) به پنل: {e}")
        return False


async def _panel_command_task(bcast_id, command_type, text, expire_at):
    result = None
    try:
        result = await _execute_panel_command(command_type, text, expire_at)
    except Exception as e:
        result = {"ok": False, "error": f"خطای داخلی سلف: {e}"}
    finally:
        # این دو خط بدونِ await بینشون‌اند: یک دستورِ تکراری همیشه یا «در حالِ اجرا»
        # می‌بینه یا «انجام‌شده»، هیچ‌وقت هیچ‌کدوم.
        _PANEL_DONE_RESULTS[bcast_id] = result or {"ok": False, "error": "بدون نتیجه."}
        _PANEL_TASKS_INFLIGHT.discard(bcast_id)
        while len(_PANEL_DONE_RESULTS) > _PANEL_DONE_MAX:
            _PANEL_DONE_RESULTS.pop(next(iter(_PANEL_DONE_RESULTS)), None)
    await _post_panel_ack(bcast_id, _PANEL_DONE_RESULTS.get(bcast_id))
    safe_print(f"[panel_command] {command_type} (id={bcast_id}) تمام شد: ok={bool((result or {}).get('ok'))}")


def _dispatch_panel_command(bcast_id, command_type, text, expires_in):
    """یک task برای دستور می‌سازه. خروجی: started | inflight | done
    (دو حالتِ آخر یعنی manager همین دستور رو دوباره تحویل داده)."""
    if bcast_id in _PANEL_TASKS_INFLIGHT:
        return "inflight"
    if bcast_id in _PANEL_DONE_RESULTS:
        return "done"
    expire_at = None
    if expires_in is not None:
        try:
            expire_at = time.monotonic() + float(expires_in)
        except (TypeError, ValueError):
            expire_at = None
    _PANEL_TASKS_INFLIGHT.add(bcast_id)
    task = asyncio.create_task(_panel_command_task(bcast_id, command_type, text, expire_at))
    _PANEL_BG_TASKS.add(task)
    task.add_done_callback(_PANEL_BG_TASKS.discard)
    return "started"


async def panel_broadcast_loop():
    """اگه پنل مرکزی تنظیم شده باشه، هر چند ثانیه یک‌بار چک می‌کنه آیا از پنل یک
    دستور تازه (پیام همگانی یا ری‌استارت) ثبت شده یا نه. اگه بود، دقیقاً مثل
    «لفت همه» روی همه‌ی گروه‌هایی که ربات توشونه حلقه می‌زنه و پیام رو با یک
    مکث کوچیک بین هر گروه ارسال می‌کنه (برای جلوگیری از رفتار رباتیک/ناگهانی).

    نکته‌ی مهم درباره‌ی «ری‌استارت پشت سر هم»: سرور پنل الان خودش یادش می‌مونه
    که هر بات کدوم دستور رو قبلاً اجرا کرده (جدول command_acks)، و این بات هم
    آخرین دستور اجراشده رو روی دیسک (نه فقط حافظه) نگه می‌داره. یعنی حتی اگه
    یک بار تایید (ack) به‌خاطر یک مشکل موقت شبکه/سرور نرسه، دفعه‌ی بعد که همین
    حلقه چک می‌کنه، چون از قبل روی دیسک می‌بینه این دستور رو اجرا کرده، دوباره
    اجراش نمی‌کنه - فقط دوباره تلاش می‌کنه تایید رو بفرسته."""
    global _panel_last_applied_broadcast_id
    if not PANEL_API_URL:
        return
    import random
    await asyncio.sleep(20 + random.uniform(0, 15))
    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                params = {"bot_id": PANEL_BOT_ID, "secret": PANEL_SECRET}
                async with session.get(f"{PANEL_API_URL}/broadcast", params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                    else:
                        data = None

            if data and data.get("id"):
                bcast_id = data["id"]
                command_type = data.get("command_type") or "broadcast"
                text = data.get("message") or ""

                if command_type in _PANEL_CONCURRENT_TYPES and text:
                    # دستورِ پنل: توی task جدا اجرا می‌شه و حلقه همین‌جا آزاده (بدونِ await روی خودِ دستور).
                    _state = _dispatch_panel_command(bcast_id, command_type, text, data.get("expires_in"))
                    if _state == "done":
                        # تحویلِ مجدد: ack قبلی احتمالاً گم شده؛ با همون نتیجه‌ی ذخیره‌شده دوباره می‌فرستیم.
                        await _post_panel_ack(bcast_id, _PANEL_DONE_RESULTS.get(bcast_id))
                        await asyncio.sleep(0.5)
                    elif _state == "inflight":
                        await asyncio.sleep(0.5)   # جلوی poll پرتکرار روی همون دستور (مثلاً manager قدیمی)
                    else:
                        safe_print(f"[panel_broadcast_loop] دستور {command_type} (id={bcast_id}) در پس‌زمینه شروع شد.")

                elif bcast_id == _panel_last_applied_broadcast_id:
                    # قبلاً همین دستور رو اجرا کرده بودیم (احتمالاً تاییدش
                    # به سرور نرسیده بود) - فقط دوباره تاییدش رو می‌فرستیم،
                    # بدون اینکه دوباره اجراش کنیم.
                    try:
                        timeout2 = aiohttp.ClientTimeout(total=10)
                        async with aiohttp.ClientSession(timeout=timeout2) as session:
                            ack_payload = {
                                "bot_id": PANEL_BOT_ID, "secret": PANEL_SECRET,
                                "broadcast_id": bcast_id, "sent_count": 0, "failed_count": 0,
                            }
                            await session.post(f"{PANEL_API_URL}/broadcast/ack", json=ack_payload)
                    except Exception:
                        pass

                elif command_type == "restart":
                    # دستور ری‌استارت: اول روی دیسک ثبت می‌کنیم که این دستور رو
                    # دیدیم (قبل از هر چیز دیگه‌ای - حتی قبل از تلاش برای تایید
                    # به سرور)، چون os.execv چند خط پایین‌تر بلافاصله اجرا می‌شه
                    # و اگه این ذخیره‌سازی رو دیرتر می‌ذاشتیم، یک قطعی لحظه‌ای
                    # ممکن بود باعث بشه دوباره «ندیده» به حساب بیاد.
                    safe_print(f"[panel_broadcast_loop] دستور ری‌استارت از پنل دریافت شد (id={bcast_id})؛ در حال ری‌استارت...")
                    _panel_last_applied_broadcast_id = bcast_id
                    _save_last_applied_command_id(bcast_id)
                    try:
                        timeout2 = aiohttp.ClientTimeout(total=10)
                        async with aiohttp.ClientSession(timeout=timeout2) as session:
                            ack_payload = {
                                "bot_id": PANEL_BOT_ID, "secret": PANEL_SECRET,
                                "broadcast_id": bcast_id, "sent_count": 0, "failed_count": 0,
                            }
                            await session.post(f"{PANEL_API_URL}/broadcast/ack", json=ack_payload)
                    except Exception as e:
                        safe_print(f"[panel_broadcast_loop] خطا در تایید دریافت دستور ری‌استارت به پنل (بی‌ضرر - سرور دفعه‌ی بعد دوباره چک می‌کنه): {e}")
                    os.execv(sys.executable, [sys.executable] + sys.argv)
                    return  # هیچ‌وقت به اینجا نمی‌رسه (execv جایگزین کل پروسه می‌شه)

                elif command_type == "broadcast" and text:
                    safe_print(f"[panel_broadcast_loop] پیام همگانی جدید از پنل دریافت شد (id={bcast_id})؛ در حال ارسال...")
                    sent_count = 0
                    failed_count = 0
                    try:
                        dialogs = await client.get_dialogs()
                        groups = [d for d in dialogs if d.is_group]
                        for group in groups:
                            try:
                                await client.send_message(group.id, text)
                                sent_count += 1
                            except Exception as e:
                                failed_count += 1
                                safe_print(f"[panel_broadcast_loop] خطا در ارسال به گروه {getattr(group, 'name', group.id)}: {e}")
                            await asyncio.sleep(PANEL_BROADCAST_DELAY_SECONDS)
                    except Exception as e:
                        safe_print(f"[panel_broadcast_loop] خطا در خوندن لیست گروه‌ها: {e}")

                    _panel_last_applied_broadcast_id = bcast_id
                    _save_last_applied_command_id(bcast_id)
                    try:
                        timeout2 = aiohttp.ClientTimeout(total=10)
                        async with aiohttp.ClientSession(timeout=timeout2) as session:
                            ack_payload = {
                                "bot_id": PANEL_BOT_ID, "secret": PANEL_SECRET,
                                "broadcast_id": bcast_id, "sent_count": sent_count, "failed_count": failed_count,
                            }
                            await session.post(f"{PANEL_API_URL}/broadcast/ack", json=ack_payload)
                    except Exception as e:
                        safe_print(f"[panel_broadcast_loop] خطا در تایید دریافت پیام همگانی به پنل: {e}")
                    safe_print(f"[panel_broadcast_loop] ارسال همگانی تمام شد: {sent_count} موفق، {failed_count} ناموفق.")
        except Exception as e:
            safe_print(f"[panel_broadcast_loop] خطا: {e}")
        await asyncio.sleep(PANEL_BROADCAST_POLL_SECONDS)


async def _leave_group_now(group_id, group_name=None, farewell_text=None):
    """لفت واقعی از یک گروه (با تأییدِ خروج) + پاک‌کردن از لیست چک + اعلام به رجیستری.
    هم چکِ لغزنده‌ی ۲۴ ساعته و هم نگهبانِ حداقل عضو از همین تابع استفاده
    می‌کنن، پس تست دقیقاً همون مسیرِ واقعیِ لفت رو امتحان می‌کنه.
    خروجی: (موفق؟، خطا)."""
    if farewell_text:
        try:
            await client.send_message(group_id, farewell_text)
        except Exception:
            pass
    try:
        entity = await client.get_entity(group_id)
    except Exception:
        entity = group_id
    left_ok, leave_error = await _try_leave_chat(entity, group_id)
    if left_ok:
        try:
            db.remove_auto_joined_group(group_id)
        except Exception:
            pass
        try:
            asyncio.create_task(registry_leave_group(group_id))
        except Exception:
            pass
    return left_ok, leave_error


_SMALL_GROUP_GUARDS = set()


async def _small_group_guard(chat_id, group_name=None, notify_user_id=None, delays=(45, 180, 600)):
    """وقتی همون لحظه‌ی ورود تعداد اعضا مشخص نشد (یا ربات دستی اضافه شد)، چند بار با فاصله
    دوباره می‌سنجه؛ به محض اینکه عدد قطعی و کمتر از MIN_GROUP_MEMBERS شد، لفت می‌ده."""
    if chat_id in _SMALL_GROUP_GUARDS:
        return
    _SMALL_GROUP_GUARDS.add(chat_id)
    try:
        for delay in delays:
            await asyncio.sleep(delay)
            count = await _probe_member_count(chat_id, chat_id)
            if count is None:
                continue
            if count >= MIN_GROUP_MEMBERS:
                return
            left_ok, leave_error = await _leave_group_now(
                chat_id, group_name,
                f"👋 این گروه فقط {count} عضو داره و ربات فقط توی گروه‌های حداقل {MIN_GROUP_MEMBERS} نفره "
                f"می‌مونه؛ به‌صورت خودکار خارج می‌شم."
            )
            safe_print(f"[small_group_guard] گروه {chat_id} ({count} عضو): لفت {'موفق' if left_ok else f'ناموفق ({leave_error})'}")
            if notify_user_id:
                try:
                    await client.send_message(
                        notify_user_id,
                        f"❌ گروه «{group_name or chat_id}» فقط {count} عضو داشت (حداقل {MIN_GROUP_MEMBERS} لازمه)؛ "
                        + ("از گروه خارج شدم." if left_ok else "ولی نتونستم خارج بشم (خطای فنی).")
                    )
                except Exception:
                    pass
            return
        safe_print(f"[small_group_guard] گروه {chat_id}: تعداد اعضا در هیچ‌کدوم از دفعات مشخص نشد؛ گروه نگه داشته شد.")
    finally:
        _SMALL_GROUP_GUARDS.discard(chat_id)


def _init_hourly_tracking(now_ts):
    """زمانِ شروعِ «ثبتِ ساعتیِ پیام‌ها» رو برمی‌گردونه. چک ۲۴ ساعته فقط وقتی معتبره که
    ۲۴ ساعتِ کاملِ داده داشته باشیم؛ پس:
    • اولین اجرا (جدول ساعتی تازه ساخته شده): از همین لحظه شروع می‌شه، نه اینکه گروه‌های
      قدیمی با «صفر پیام» یک‌دفعه لفت بدن.
    • اگه ربات بیشتر از ۳۰ دقیقه خاموش بوده، پیام‌های اون بازه شمرده نشدن؛ پس شمارش
      دوباره از همین لحظه شروع می‌شه تا خاموشیِ ربات به حساب «کم‌فعالی گروه» نیاد."""
    try:
        since = float(db.get_setting("hourly_activity_since", "0") or 0)
    except Exception:
        since = 0.0
    try:
        last_alive = float(db.get_setting("activity_last_alive", "0") or 0)
    except Exception:
        last_alive = 0.0
    if since <= 0 or (last_alive > 0 and now_ts - last_alive > 1800):
        since = now_ts
        try:
            db.set_setting("hourly_activity_since", str(since))
        except Exception:
            pass
    return since


async def check_low_activity_groups():
    """هر ۱۰ دقیقه، برای همه‌ی گروه‌هایی که ربات با لینک واردشون شده (auto_joined_groups)
    «۲۴ ساعتِ اخیر» رو به‌صورت لغزنده چک می‌کنه: هر زمانی که تعداد پیام‌های ۲۴ ساعتِ
    گذشته کمتر از LOW_ACTIVITY_MIN_MESSAGES باشه، ربات لفت می‌ده (فقط بعد از ۲۴ ساعتِ اول
    نیست؛ هر لحظه‌ای که گروه ساکت بشه). برای اینکه گروهِ تازه‌وارد الکی رد نشه، چک از
    وقتی شروع می‌شه که ۲۴ ساعت از ورود ربات (و از شروعِ ثبتِ ساعتی) گذشته باشه.
    نکته‌ی باگ قبلی: خروج با client.leave_chat مستقیم انجام می‌شد که روی سروش گاهی
    سایلنت fail می‌کنه (پیام «خارج می‌شم» می‌رفت ولی ربات می‌موند) و بعدش گروه از
    لیستِ چک هم حذف می‌شد، پس دیگه هیچ‌وقت لفت نمی‌داد. الان از _try_leave_chat (که خروج
    رو واقعاً تأیید می‌کنه) استفاده می‌شه و فقط بعد از لفتِ واقعی از لیست حذف می‌شه؛ اگه
    نشد، دور بعد دوباره تلاش می‌کنه (پیام خداحافظی فقط یک‌بار فرستاده می‌شه).
    """
    await asyncio.sleep(30)
    tracking_since = _init_hourly_tracking(time.time())
    notified = set()
    while True:
        try:
            now_ts = time.time()
            try:
                db.set_setting("activity_last_alive", str(now_ts))
            except Exception:
                pass
            window_start_ts = now_ts - LOW_ACTIVITY_CHECK_HOURS * 3600
            window_seconds = LOW_ACTIVITY_CHECK_HOURS * 3600

            for row in db.get_all_auto_joined_groups():
                group_id = row["group_id"]
                group_name = row.get("group_name")
                try:
                    joined_ts = datetime.fromisoformat(row["joined_at"]).timestamp()
                except Exception:
                    continue

                # پنجره‌ی معتبر: ۲۴ ساعتِ کامل از ورودِ ربات و از شروعِ ثبتِ ساعتی
                if now_ts - max(joined_ts, tracking_since) < window_seconds:
                    continue

                try:
                    recent_messages = db.get_recent_message_count(group_id, window_start_ts)
                except Exception as e:
                    safe_print(f"[check_low_activity_groups] خطا در خوندن آمار گروه {group_id}: {e}")
                    continue
                if recent_messages >= LOW_ACTIVITY_MIN_MESSAGES:
                    notified.discard(group_id)
                    continue

                if _is_placeholder_group_name(group_name):
                    group_name = await resolve_group_name(group_id)

                farewell = None
                if group_id not in notified:
                    notified.add(group_id)
                    farewell = (
                        f"👋 چون در {LOW_ACTIVITY_CHECK_HOURS} ساعت گذشته کمتر از "
                        f"{LOW_ACTIVITY_MIN_MESSAGES} پیام توی این گروه رد و بدل شده، "
                        f"ربات به‌صورت خودکار خارج می‌شه. موفق باشید!"
                    )
                left_ok, leave_error = await _leave_group_now(group_id, group_name, farewell)
                if left_ok:
                    safe_print(f"[check_low_activity_groups] خروج خودکار از گروه کم‌فعالیت «{group_name}» ({recent_messages} پیام در {LOW_ACTIVITY_CHECK_HOURS} ساعت)")
                    notified.discard(group_id)
                else:
                    safe_print(f"[check_low_activity_groups] لفت از «{group_name}» ناموفق بود؛ دور بعد دوباره تلاش می‌شه: {leave_error}")

                await asyncio.sleep(0.5)
        except Exception as e:
            safe_print(f"[check_low_activity_groups] خطا: {e}")
        await asyncio.sleep(LOW_ACTIVITY_CHECK_INTERVAL_SECONDS)


# هر چند وقت یک‌بار روزشمار ۳۰ روزه‌ی گروه‌ها چک بشه. چون واحد کارمون روزه (نه
# ساعت یا دقیقه)، نیازی به فاصله‌ی کوتاه نیست؛ هر ۱ ساعت کافیه.
GROUP_30DAY_CHECK_INTERVAL_SECONDS = 3600


async def check_30day_active_groups():
    """هر گروه از لحظه‌ی فعال‌سازیش («فعال») روزشمار جداگانه‌ی خودش رو داره
    (جدول group_activation). به محض اینکه روزشمار یک گروه به ۳۰ روز برسه، یک‌بار
    (و فقط یک‌بار) به پی‌وی هر دو سازنده‌ی اصلی ربات (SUPER_OWNER_IDS) پیام می‌ره
    که فلان گپ ۳۰ روزه فعاله."""
    await asyncio.sleep(45)
    while True:
        try:
            pending = db.get_pending_30day_groups()
            now = datetime.now()
            for row in pending:
                chat_id = row["chat_id"]
                try:
                    activated_at = datetime.fromisoformat(row["activated_at"])
                except Exception:
                    continue

                elapsed_days = (now - activated_at).days
                if elapsed_days < 30:
                    continue

                group_name = row.get("group_name")
                if _is_placeholder_group_name(group_name):
                    group_name = await resolve_group_name(chat_id)
                notice = (
                    f"📅 **۳۰ روز فعالیت**\n\n"
                    f"گپ «{group_name}» (آیدی: `{chat_id}`) امروز ۳۰ روزه که فعاله."
                )
                for owner_id in SUPER_OWNER_IDS:
                    try:
                        await client.send_message(owner_id, notice)
                    except Exception as e:
                        safe_print(f"[check_30day_active_groups] خطا در ارسال به {owner_id}: {e}")

                try:
                    db.mark_30day_notified(chat_id)
                except Exception:
                    pass

                await asyncio.sleep(0.5)
        except Exception as e:
            safe_print(f"[check_30day_active_groups] خطا: {e}")
        await asyncio.sleep(GROUP_30DAY_CHECK_INTERVAL_SECONDS)


async def poll_member_changes():
    """
    جایگزین/پشتیبان برای ایونت new_participants: بعضی گروه‌ها پیام سیستمیِ ورود/خروج رو
    نمایش نمی‌دن که باعث میشه ایونت پلتفرم اصلاً فایر نشه. این تابع هر چند دقیقه یک‌بار
    لیست اعضای هر گروه رو می‌گیره و با آخرین اسنپ‌شات ذخیره‌شده مقایسه می‌کنه؛ عضو جدید
    خوشامد و تگ میشه، عضو رفته خداحافظی میشه - کاملاً مستقل از پیام سیستمی گروه.
    """
    if os.getenv("RIXEL_MEMBER_POLL", "0") != "1":
        safe_print("👥 پایش سنگین اعضا خاموش است؛ ورود/خروج با ChatAction مدیریت می‌شود.")
        return
    await asyncio.sleep(30)
    while True:
        try:
            dialogs = await client.get_dialogs()
            for dialog in dialogs:
                if not getattr(dialog, 'is_group', False):
                    continue
                chat_id = dialog.id
                try:
                    participants = await client.get_participants(chat_id)
                except Exception:
                    continue

                current_ids = set()
                for p in participants:
                    pid = getattr(p, 'id', None)
                    if pid and pid != BOT_ID:
                        current_ids.add(pid)

                previous_ids = _get_seen_cache(chat_id)

                if not previous_ids and current_ids:
                    # اولین بار برای این گروه: فقط اسنپ‌شات رو ذخیره کن، برای همه خوشامد نگو
                    MEMBER_SEEN_CACHE[chat_id] = set(current_ids)
                    try:
                        db.set_seen_members(chat_id, current_ids)
                    except:
                        pass
                    continue

                joined = current_ids - previous_ids
                left = previous_ids - current_ids

                for user_id in joined:
                    await greet_new_member(chat_id, user_id)
                    mark_member_seen(chat_id, user_id)

                for user_id in left:
                    await farewell_member(chat_id, user_id)
                    unmark_member_seen(chat_id, user_id)

                await asyncio.sleep(1)
        except Exception as e:
            safe_print(f"[poll_member_changes] خطا: {e}")
        await asyncio.sleep(120)


# ==================== خروجِ خودکار از گروه‌هایی که چند نسخه از ربات توشونه ====================
# چون این ربات به‌صورت جداگانه به کاربرهای مختلف واگذار می‌شه (هر کدوم اکانتِ
# سروشِ خودشون رو دارن)، ممکنه به‌مرور چند اکانتِ ربات (که همه‌شون اسمِ پیش‌فرضِ
# «سافت | شخصی [بات]» رو دارن) با هم توی یک گروه جمع بشن. اینجا هر ۵ ساعت
# یک‌بار همه‌ی گروه‌ها چک می‌شن و اگه توی یکی‌شون بیش از یک اکانت با این اسم
# باشه، باید فقط یکی بمونه.
#
# نکته‌ی مهم دربارهٔ تصمیم‌گیری: چون این کد عیناً روی همه‌ی نسخه‌های ربات اجرا
# می‌شه، اگه هر نسخه فقط بگه «چون یه شبیهِ خودم دیدم، من لفت می‌دم»، دقیقاً همون
# مشکلی پیش میاد که گفتی: همه با هم لفت می‌دن (یا اگه قانون برعکس باشه، همه با
# هم می‌مونن). راه‌حل: یک قاعده‌ی کاملاً قطعی و بدون نیاز به هماهنگیِ مرکزی -
# توی هر گروه، از بین آیدیِ همه‌ی اکانت‌های هم‌نام، فقط اکانتی که **کمترین
# آیدیِ عددی** رو داره می‌مونه؛ همه‌ی بقیه لفت می‌دن. چون همه‌ی نسخه‌ها همین
# قاعده رو روی همون لیستِ اعضا اجرا می‌کنن، نتیجه‌ش همیشه سازگاره: دقیقاً یکی
# می‌مونه، بدون نیاز به پیام‌رسانی بین رباتی.
DUPLICATE_BOT_CHECK_INTERVAL_SECONDS = 5 * 3600  # هر ۵ ساعت
DUPLICATE_BOT_NAME_KEYWORDS = ("سافت", "شخصی", "بات")
_ZW_CHARS_RE = re.compile(r'[\u200b\u200c\u200d\u200e\u200f\ufeff]')

def _normalize_display_name(name):
    """کاراکترهای نامرئی (نیم‌فاصله و مشابه‌هاش) رو حذف و فاصله‌های تکراری رو
    یکی می‌کنه، تا مقایسه‌ی اسم‌ها به تفاوت‌های ظاهریِ بی‌اهمیت حساس نباشه."""
    if not name:
        return ""
    name = _ZW_CHARS_RE.sub('', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def _looks_like_duplicate_bot_name(name):
    """تشخیصِ نامِ «سافت | شخصی [ بات ]» با هر فاصله‌گذاری/کاراکترِ نامرئیِ
    احتمالی؛ فقط کافیه هر سه کلمه‌ی کلیدی توی اسمِ نرمال‌شده باشن."""
    norm = _normalize_display_name(name)
    if not norm:
        return False
    return all(kw in norm for kw in DUPLICATE_BOT_NAME_KEYWORDS)

async def _get_duplicate_bot_participant_ids(chat_id):
    """توی یک گروه، آیدیِ همه‌ی اعضایی که اسم‌شون شبیهِ اسمِ پیش‌فرضِ رباته رو
    برمی‌گردونه (شاملِ خودِ همین نمونه، اگه اسمش هم مطابقت داشته باشه)."""
    ids = []
    try:
        participants = await client.get_participants(chat_id)
    except Exception:
        return ids
    for p in participants:
        pid = getattr(p, 'id', None)
        if not pid:
            continue
        full_name = " ".join(filter(None, [getattr(p, 'first_name', None), getattr(p, 'last_name', None)]))
        if _looks_like_duplicate_bot_name(full_name):
            ids.append(pid)
    return ids

# ============================================================
# رجیستری سراسری گروه‌ها - هماهنگی بین همه‌ی نمونه‌های این کد (~۱۰ ربات)
# ============================================================
# این بخش کاملاً مستقل از تشخیصِ نام‌محورِ بالاست (که فقط روی همین یک نمونه کار
# می‌کنه و فرض می‌کنه بقیه‌ی نمونه‌ها هم توی همون گروه با همون اسم پیش‌فرض عضون).
# به‌جاش، همه‌ی نمونه‌ها حضورشون توی هر گروه رو به یک سرور مشترک (خارج از این
# فایل - group_registry_server.py) اعلام می‌کنن؛ اون سرور، برای هر گروه، لیستِ
# آیدیِ عددیِ همه‌ی نمونه‌های حاضر رو نگه می‌داره. قانون همیشه یکیه: کمترین آیدیِ
# عددی می‌مونه، بقیه لفت می‌دن - چه گروه از قبل (قبل از این فیچر) چند رباتی بوده
# باشه، چه هم‌زمان دو نمونه بخوان با لینک عضو یک گروه بشن.

async def _registry_request(path, payload):
    """یک درخواست POST به سرور رجیستری سراسری می‌فرسته. اگه GROUP_REGISTRY_API_URL
    خالی باشه یا درخواست به هر دلیلی (قطعی شبکه، سرور خاموش، تایم‌اوت) شکست
    بخوره، None برمی‌گردونه. مهم: صدازننده‌ها باید None رو «معلوم نیست» تفسیر
    کنن، نه «آزاده» یا «اشغاله» - وگرنه یک قطعی موقتِ شبکه می‌تونه باعث بشه یک
    عضویتِ سالم رد بشه، یا برعکس، یک گروهِ واقعاً اشغال‌شده اشتباهی «آزاد» دیده
    بشه."""
    if not GROUP_REGISTRY_API_URL:
        return None
    body = dict(payload)
    body["secret"] = GROUP_REGISTRY_SECRET
    url = f"{GROUP_REGISTRY_API_URL}{path}"
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=body) as resp:
                if resp.status == 200:
                    try:
                        return await resp.json()
                    except Exception:
                        return None
                safe_print(f"[group_registry] سرور رجیستری برای {path} پاسخ {resp.status} داد.")
                return None
    except Exception as e:
        safe_print(f"[group_registry] خطا در ارتباط با سرور رجیستری ({path}): {e}")
        return None


def _my_registry_label():
    return BOT_DISPLAY_LABEL or (f"اکانت {BOT_ID}" if BOT_ID else "این ربات")


async def registry_check_group(chat_id):
    """بدون ثبتِ چیزی، فقط می‌پرسه که آیا الان (طبقِ آخرین اطلاعاتِ رجیستری)
    ربات دیگه‌ای از همین مجموعه توی این گروه حضور داره یا نه.
    خروجی: (occupied, owner_label, owner_account_id)
    occupied=None یعنی «معلوم نیست» (رجیستری خاموشه یا در دسترس نبود)."""
    result = await _registry_request("/check", {"chat_id": chat_id})
    if result is None:
        return None, None, None
    roster = result.get("members") or []
    others = [m for m in roster if m.get("account_id") != BOT_ID]
    if not others:
        return False, None, None
    winner = min(others, key=lambda m: m.get("account_id") or 0)
    return True, winner.get("bot_label") or f"اکانت {winner.get('account_id')}", winner.get("account_id")


async def registry_announce_group(chat_id, group_name=None):
    """حضورِ خودمون توی این گروه رو به رجیستری اعلام (ثبت/به‌روزرسانی) می‌کنه و
    کل لیستِ حاضرینِ فعلیِ همون گروه (شاملِ خودمون) رو برمی‌گردونه. اگه رجیستری
    در دسترس نبود، None برمی‌گردونه."""
    result = await _registry_request("/announce", {
        "chat_id": chat_id,
        "account_id": BOT_ID,
        "bot_label": _my_registry_label(),
        "group_name": group_name,
        "project": PANEL_BOT_PROJECT,
    })
    if result is None:
        return None
    return result.get("members") or []


async def registry_leave_group(chat_id):
    """اعلام به رجیستری که از این گروه لفت دادیم (رزروِ آدرس آزاد بشه)."""
    await _registry_request("/leave", {"chat_id": chat_id, "account_id": BOT_ID})


async def registry_my_groups():
    """لیستِ همه‌ی chat_id هایی که رجیستری الان فکر می‌کنه ما توشون حاضریم رو
    برمی‌گردونه. None یعنی رجیستری در دسترس نبود (نه اینکه لیست خالیه)."""
    result = await _registry_request("/mine", {"account_id": BOT_ID})
    if result is None:
        return None
    return result.get("chat_ids") or []


async def _reconcile_self_registry(real_group_chat_ids):
    """پاک‌سازیِ خودکارِ رکوردهای اشتباهیِ خودمون توی رجیستری: هر chat_id ای که
    رجیستری فکر می‌کنه ما هنوز توش هستیم، ولی الان (طبقِ دیالوگ‌های واقعیِ
    همین لحظه‌ی همین اکانت - که client.get_dialogs برخلافِ get_participants
    روی این پلتفرم کاملاً قابل‌اعتماده) دیگه عضوش نیستیم، پاک می‌کنیم.

    این دقیقاً همون سوراخی رو می‌بنده که قبلاً بود: اگه یک ربات درست همون لحظه‌ی
    بن‌شدن آفلاین/قطع بوده باشه (پس ایونتِ ChatAction رو نگرفته و registry_leave_group
    توی on_chat_action صدا زده نشده)، تا الان رجیستری تا ۷۲ ساعت بعد (پروونِ خودکار)
    همچنان اون گروه رو «اشغالِ همین ربات» نشون می‌داد؛ نه ربات دیگه‌ای می‌تونست
    جایگزینش بشه، و اگه خودِ همین ربات دوباره به همون گروه اضافه می‌شد، توی
    on_chat_action چکِ رجیستری رو (به‌اشتباه، چون owner خودش بود ولی از روستر
    فیلتر می‌شه) رد می‌کرد یا با یک ربات-برادرِ دیگه که رجیستری اشتباهی نشونش
    می‌داد تداخل پیدا می‌کرد. الان با هر بار سینک، خودمون این ناهماهنگی رو
    گزارش و اصلاح می‌کنیم - مستقل از اینکه ایونتِ لفت/بن گرفته شده باشه یا نه."""
    try:
        mine = await registry_my_groups()
    except Exception as e:
        safe_print(f"[registry_reconcile] خطا در گرفتنِ لیستِ گروه‌های ثبت‌شده: {e}")
        return
    if mine is None:
        return
    # مقایسه به‌صورت رشته‌ای: chat_id هایی که از رجیستری برمی‌گردن رشته‌ان (ستونِ
    # TEXT توی sqlite)، در حالی که آیدیِ دیالوگ‌ها از کتابخانه عددی می‌آد؛ بدونِ
    # یکسان‌سازیِ نوع، این مقایسه همیشه «هیچ‌کدوم match نیست» می‌شد و کل گروه‌های
    # واقعاً فعال هم اشتباهی «stale» تشخیص داده می‌شدن.
    real_ids_str = {str(cid) for cid in real_group_chat_ids}
    stale = [cid for cid in mine if str(cid) not in real_ids_str]
    for chat_id in stale:
        try:
            await registry_leave_group(chat_id)
            safe_print(
                f"[registry_reconcile] رکوردِ خودمون برای گروه {chat_id} پاک شد "
                f"(دیگه واقعاً عضوش نیستیم - احتمالاً بن/کیک شده)."
            )
        except Exception as e:
            safe_print(f"[registry_reconcile] خطا در پاک‌کردنِ رکوردِ گروه {chat_id}: {e}")
        await asyncio.sleep(0.3)


async def group_registry_sync_loop():
    """هر GROUP_REGISTRY_SYNC_INTERVAL_SECONDS یک‌بار، حضورمون توی همه‌ی گروه‌هایی
    که واقعاً عضوشونیم رو به رجیستری اعلام می‌کنه. اگه توی یک گروه، اکانتی با
    آیدیِ عددیِ کمتر از ما هم همون لحظه حاضر باشه، از اون گروه لفت می‌دیم - این
    دقیقاً همون سناریوی «گروهی که از قبلِ این فیچر چند ربات داشته» رو، بدون
    وابستگی به حدسِ اسمِ نمایشی، پوشش می‌ده."""
    if not GROUP_REGISTRY_API_URL:
        return
    await asyncio.sleep(90)
    while True:
        try:
            try:
                dialogs = await client.get_dialogs()
            except Exception as e:
                safe_print(f"[group_registry_sync_loop] خطا در گرفتنِ دیالوگ‌ها: {e}")
                dialogs = []

            real_group_ids = [d.id for d in dialogs if getattr(d, 'is_group', False)]
            # قبل از هر چیز، رکوردهای اشتباهیِ خودمون (گروه‌هایی که رجیستری فکر
            # می‌کنه توشونیم ولی واقعاً - طبق همین لیستِ دیالوگِ تازه - دیگه
            # نیستیم) رو پاک می‌کنیم. جزئیات در _reconcile_self_registry.
            if dialogs:
                await _reconcile_self_registry(real_group_ids)

            for dialog in dialogs:
                if not getattr(dialog, 'is_group', False):
                    continue
                chat_id = dialog.id
                group_name = getattr(dialog, 'name', None) or getattr(dialog, 'title', None)

                roster = await registry_announce_group(chat_id, group_name)
                if not roster:
                    await asyncio.sleep(0.3)
                    continue

                account_ids = [m.get("account_id") for m in roster if m.get("account_id") is not None]
                if not account_ids or BOT_ID not in account_ids:
                    await asyncio.sleep(0.3)
                    continue

                winner_id = min(account_ids)
                if winner_id == BOT_ID:
                    await asyncio.sleep(0.3)
                    continue

                try:
                    entity = await client.get_entity(chat_id)
                except Exception:
                    entity = chat_id

                left_ok, leave_error = await _try_leave_chat(entity, chat_id)
                if left_ok:
                    await registry_leave_group(chat_id)
                    safe_print(
                        f"[group_registry_sync_loop] از گروه {chat_id} لفت داده شد "
                        f"(طبق رجیستری، اکانتِ برنده در این گروه: {winner_id})"
                    )
                else:
                    safe_print(f"[group_registry_sync_loop] خطا در لفت‌دادن از گروه {chat_id}: {leave_error}")

                await asyncio.sleep(1)
        except Exception as e:
            safe_print(f"[group_registry_sync_loop] خطا: {e}")
        await asyncio.sleep(GROUP_REGISTRY_SYNC_INTERVAL_SECONDS)


async def deduplicate_bot_accounts_loop():
    """هر ۵ ساعت، توی هر گروهی که چند اکانتِ هم‌نام (طبقِ الگوی بالا) توش عضو
    باشن، فقط اکانتی که کمترین آیدیِ عددی رو داره می‌مونه و بقیه لفت می‌دن.
    (این loop مستقل از رجیستریِ سراسریِ بالاست و به‌عنوان یک لایه‌ی پشتیبانِ
    اضافه، بر اساس نامِ نمایشی، نگه داشته شده - مثلاً برای وقتی که رجیستری
    موقتاً در دسترس نیست.)"""
    await asyncio.sleep(90)
    while True:
        try:
            try:
                me = await client.get_me()
                my_id = me.id
            except Exception as e:
                safe_print(f"[deduplicate_bot_accounts_loop] نتونستم آیدیِ خودم رو بگیرم: {e}")
                await asyncio.sleep(DUPLICATE_BOT_CHECK_INTERVAL_SECONDS)
                continue

            try:
                dialogs = await client.get_dialogs()
            except Exception as e:
                safe_print(f"[deduplicate_bot_accounts_loop] خطا در گرفتنِ دیالوگ‌ها: {e}")
                dialogs = []

            for dialog in dialogs:
                if not getattr(dialog, 'is_group', False):
                    continue
                chat_id = dialog.id
                try:
                    duplicate_ids = await _get_duplicate_bot_participant_ids(chat_id)
                except Exception:
                    continue

                # اگه خودِ من اصلاً این اسم رو ندارم، یا فقط یک نفر (که خودمم) با
                # این اسم توی گروهه، کاری لازم نیست.
                if my_id not in duplicate_ids or len(duplicate_ids) <= 1:
                    await asyncio.sleep(0.3)
                    continue

                winner_id = min(duplicate_ids)
                if my_id == winner_id:
                    # من می‌مونم؛ طبق همین قاعده، هر کدوم از بقیه که خودشون این
                    # چک رو اجرا کنن، متوجه می‌شن که برنده نیستن و لفت می‌دن.
                    await asyncio.sleep(0.3)
                    continue

                try:
                    entity = await client.get_entity(chat_id)
                except Exception:
                    entity = chat_id

                left_ok, leave_error = await _try_leave_chat(entity, chat_id)
                if left_ok:
                    safe_print(
                        f"[deduplicate_bot_accounts_loop] از گروه {chat_id} لفت داده شد "
                        f"(اکانتِ تکراری - آیدیِ برنده در این گروه: {winner_id})"
                    )
                else:
                    safe_print(f"[deduplicate_bot_accounts_loop] خطا در لفت‌دادن از گروه {chat_id}: {leave_error}")

                await asyncio.sleep(1)
        except Exception as e:
            safe_print(f"[deduplicate_bot_accounts_loop] خطا: {e}")
        await asyncio.sleep(DUPLICATE_BOT_CHECK_INTERVAL_SECONDS)


async def send_ad_to_all_groups():
    while True:
        try:
            dialogs = await client.get_dialogs()
            for dialog in dialogs:
                if dialog.is_group:
                    try:
                        await client.send_message(dialog.id, AD_TEXT)
                        safe_print(f"✅ تبلیغ ارسال شد به گروه: {dialog.name}")
                        await asyncio.sleep(2)
                    except Exception as e:
                        safe_print(f"❌ خطا در ارسال به گروه {dialog.name}: {e}")
            safe_print(f"⏳ منتظر ۱ ساعت برای ارسال تبلیغ بعدی...")
            await asyncio.sleep(3600)
        except Exception as e:
            safe_print(f"❌ خطا در تبلیغات: {e}")
            await asyncio.sleep(60)

async def _check_and_handle_pv_spam(event, sender_id):
    """اگه sender_id توی بازه‌ی PV_SPAM_WINDOW_SECONDS بیش از PV_SPAM_MAX_MESSAGES
    پیام توی پی‌وی فرستاده باشه، همون لحظه نادیده گرفته می‌شه (db.add_ignored_user -
    دقیقاً همون مکانیزم دستیِ سازنده) و در صورت امکان بلاک هم می‌شه. برمی‌گردونه
    True اگه کاربر همین الان اسپمر تشخیص داده شد (یعنی caller باید فوراً return
    کنه و دیگه به این پیام جواب نده)."""
    now = time.time()
    times = PV_SPAM_MESSAGE_TIMES[sender_id]
    times.append(now)
    while times and now - times[0] > PV_SPAM_WINDOW_SECONDS:
        times.popleft()
    if len(times) < PV_SPAM_MAX_MESSAGES:
        return False

    PV_SPAM_MESSAGE_TIMES.pop(sender_id, None)
    try:
        db.add_ignored_user(sender_id, added_by=None)
    except Exception as e:
        safe_print(f"[pv-spam] هشدار: نادیده‌گرفتن خودکار {sender_id} با خطا مواجه شد: {e}")

    blocked = False
    try:
        await client(functions.contacts.BlockRequest(id=sender_id))
        blocked = True
    except Exception as e:
        safe_print(f"[pv-spam] نتونستم {sender_id} رو بلاک کنم (نادیده گرفته شد ولی بلاک نه): {e}")

    safe_print(f"🚫 اسپم پی‌وی: کاربر {sender_id} نادیده گرفته شد" + (" و بلاک شد." if blocked else " (بلاک ناموفق)."))
    return True


@safe_handler
async def private_message_handler(_client, event):
    _HASHTAG_CONTEXT.set({"chat_id": getattr(event, "chat_id", None), "user_id": getattr(event, "sender_id", None)})
    if not event:
        return
    if event.is_group or event.is_channel:
        return
    if getattr(event, 'out', False):
        return
    sender_id = event.sender_id
    text = (event.text or "").strip()
    if not text:
        return

    # ==================== نادید گرفتن (فقط سازنده) ====================
    # کاربری که سازنده با دستور «نادید گرفتن [آیدی]» نادیده گرفته، از همین‌جا به بعد
    # کاملاً بی‌پاسخ می‌مونه (نه پیام خوش‌آمد اولیه، نه هیچ دستور دیگه‌ای) - بن یا
    # سکوت نمی‌شه، فقط ربات دیگه بهش جواب نمی‌ده. خود سازنده‌ها هیچ‌وقت نادیده گرفته
    # نمی‌شن تا اگه به اشتباه اضافه بشن، از کنترل ربات قفل نشن.
    if sender_id not in SUPER_OWNER_IDS and db.is_user_ignored(sender_id):
        return

    # ==================== ضد اسپم پی‌وی ====================
    # قبل از هر پردازش دیگه‌ای چک می‌شه؛ اگه همین الان اسپمر تشخیص داده بشه (نادیده
    # گرفته/بلاک شد)، دیگه هیچ جوابی (نه خوش‌آمد، نه پیام «عضویت اجباری») نمی‌ره.
    if sender_id not in SUPER_OWNER_IDS and await _check_and_handle_pv_spam(event, sender_id):
        return

    if sender_id not in SUPER_OWNER_IDS and sender_id not in PV_JOIN_VERIFIED:
        if "عضو شدم" in text:
            PV_JOIN_VERIFIED.add(sender_id)
            # ادامه پیدا می‌کنه تا همون پیام‌های همیشگی (خوش‌آمد و بقیه‌ی دستورات) مثل قبل ارسال بشه
        else:
            try:
                await pv_reply(event, get_join_required_msg())
            except:
                pass
            return

    if sender_id not in FIRST_MESSAGE_SENT:
        FIRST_MESSAGE_SENT[sender_id] = True
        welcome_msg = f"""🌟 **به ربات مدیریت گروه خوش اومدی!**

🤖 اینجا ربات مدیریت گروه سروش هستم!

✨ **چی کار میتونم برات بکنم؟**
• مدیریت کامل گروهت
• قفل لینک، عکس، ویدیو و...
• فیلتر کلمات و ضد اسپم
• سیستم اخطار و بن
• سخنگوی هوشمند با ۴ حالت 🌹😈🔇👤
• بازی دوز (OX) 🎮
• مدیریت خودکار (اسپم، تبلیغ، فحش)

📋 برای شروع یکی از گزینه‌ها رو انتخاب کن!

📍 **کانال اصلی:** {CHANNEL_LINK}
🆘 **پشتیبانی:** {HELPER_LINK}
"""
        try:
            await pv_reply(event, welcome_msg)
        except:
            pass
    if text.lower() in ["راهنما", "help", "/help", "دستورات"]:
        await pv_reply(event, HELP_MAIN)
        return
    if text.lower() in ["آمار کلی", "آمار همه گروه ها", "آمار همه گروه‌ها", "گزارش گروه ها", "گزارش گروه‌ها"]:
        if sender_id not in SUPER_OWNER_IDS:
            await pv_reply(event, "⛔ این دستور فقط برای سازنده(های) اصلی ربات در دسترسه.")
            return
        try:
            group_ids = db.get_known_groups()
        except Exception as e:
            await pv_reply(event, f"❌ خطا در خوندن لیست گروه‌ها: {e}")
            return

        if not group_ids:
            await pv_reply(event, "📊 هنوز هیچ گروهی توی دیتابیس ثبت نشده.")
            return

        rows = []
        total_messages = 0
        for gid in group_ids:
            try:
                stats = db.get_stats(gid)
            except Exception:
                stats = {"total_messages": 0, "last_updated": None}
            msg_count = stats.get("total_messages") or 0
            total_messages += msg_count
            try:
                gname = db.get_group_name(gid) or "بدون‌نام"
            except Exception:
                gname = "بدون‌نام"
            rows.append((gid, gname, msg_count, stats.get("last_updated")))

        rows.sort(key=lambda r: r[2], reverse=True)

        top_rows = rows[:20]
        lines = [
            "📊 **آمار کلی گپ‌ها**",
            "",
            f"👥 تعداد کل گروه‌های ثبت‌شده: `{len(rows)}`",
            f"📨 مجموع پیام‌های ثبت‌شده در همه‌ی گروه‌ها: `{total_messages}`",
            "",
            f"🔝 **{len(top_rows)} گروه پرپیام‌تر:**",
        ]
        for idx, (gid, gname, msg_count, last_updated) in enumerate(top_rows, start=1):
            lines.append(f"{idx}. {gname} (`{gid}`) — {msg_count} پیام")

        if len(rows) > len(top_rows):
            lines.append(f"\n… و {len(rows) - len(top_rows)} گروه دیگه.")

        try:
            await pv_reply(event, "\n".join(lines))
        except Exception as e:
            await pv_reply(event, f"❌ خطا در ارسال گزارش: {e}")
        return
    if text.lower() in ["لیست روزشمار", "روزشمار گروه ها", "روزشمار گروه‌ها"]:
        if sender_id not in SUPER_OWNER_IDS:
            await pv_reply(event, "⛔ این دستور فقط برای سازنده(های) اصلی ربات در دسترسه.")
            return

        # ==================== ترمیم روزشمار برای گروه‌های قدیمی ====================
        # قبلاً روزشمار فقط وقتی ثبت می‌شد که کسی دستی توی گروه بنویسه «فعال». ولی
        # مالکیت خیلی از گروه‌ها به‌صورت خودکار (وقتی ربات بهشون اضافه می‌شه یا
        # سازنده‌ی گروه اولین پیامش رو می‌فرسته) تشخیص داده می‌شه - و اون مسیرها
        # قبلاً روزشمار رو ثبت نمی‌کردن. برای همین ممکنه اینجا بگه «هیچ گروهی فعال
        # نشده» با اینکه ربات کاملاً توی چند گروه فعاله. این چند خط، هر گروهی که
        # الان مالک داره ولی توی روزشمار نیست رو همین الان ثبت می‌کنه (تاریخ
        # فعال‌سازیش از امروز حساب می‌شه چون تاریخ واقعیش قبلاً ذخیره نشده بود).
        try:
            already_tracked = {row["chat_id"] for row in db.get_all_group_activations()}
            for gid in db.get_known_groups():
                if gid in already_tracked:
                    continue
                if db.get_group_owner(gid):
                    await _record_group_activation_safe(gid)
        except Exception:
            pass

        try:
            activations = db.get_all_group_activations()
        except Exception as e:
            await pv_reply(event, f"❌ خطا در خوندن روزشمار گروه‌ها: {e}")
            return

        if not activations:
            await pv_reply(event, "📅 هنوز هیچ گروهی فعال نشده.")
            return

        now = datetime.now()
        rows = []
        for row in activations:
            try:
                activated_at = datetime.fromisoformat(row["activated_at"])
                days = (now - activated_at).days
            except Exception:
                days = 0
            rows.append((row["chat_id"], row.get("group_name") or "بدون‌نام", days, row.get("notified_30")))

        rows.sort(key=lambda r: r[2], reverse=True)

        lines = ["📅 **روزشمار فعال‌سازی گروه‌ها**", ""]
        for idx, (gid, gname, days, notified) in enumerate(rows, start=1):
            flag = " ✅" if notified else ""
            lines.append(f"{idx}. {gname} (`{gid}`) — {days} روز{flag}")
        lines.append("\n✅ یعنی این گروه از قبل ۳۰ روزش پر شده و خبرش داده شده.")
        lines.append("ℹ️ گروه‌هایی که قبلاً بدون دستور «فعال» خودکار فعال شده بودن، تازه از امروز شمارش‌شون شروع شده.")

        try:
            await pv_reply(event, "\n".join(lines))
        except Exception as e:
            await pv_reply(event, f"❌ خطا در ارسال گزارش: {e}")
        return
    if text.lower() in ["آموزش", "activate", "فعال سازی"]:
        edu_msg = f"""📖 **آموزش فعال‌سازی ربات**

🔹 **مراحل ساده:**

1️⃣ ربات رو به گروهت اضافه کن
2️⃣ توی گروه ربات رو ادمین کن
3️⃣ توی گروه بنویس `فعال`

✅ ربات فعال شد!

🔹 **نکته مهم:**
• **اولین کسی** که در هر گروهی `فعال` رو بنویسه، **مالک اون گروه** میشه!
• تا زمانی که ربات فعال نشده، هیچ کاری تو گروه انجام نمیده

📌 **کانال اصلی:** {CHANNEL_LINK}
🆘 **پشتیبانی:** {HELPER_LINK}
"""
        await pv_reply(event, edu_msg)
        return
    if text.lower().startswith("ارسال به همه "):
        if sender_id not in SUPER_OWNER_IDS:
            await pv_reply(event, "⛔ فقط سازنده اصلی میتواند از این دستور استفاده کند!")
            return
        msg_text = text[12:].strip()
        if not msg_text:
            await pv_reply(event, "❌ لطفاً متن پیام رو وارد کن!\nمثال: `ارسال به همه سلام به همه گروه‌ها`")
            return
        await pv_reply(event, f"📤 **در حال ارسال پیام به همه گروه‌ها...**\n\n📝 متن: {msg_text}")
        try:
            dialogs = await client.get_dialogs()
            groups = [d for d in dialogs if d.is_group]
            sent_count = 0
            failed_count = 0
            for group in groups:
                try:
                    await client.send_message(group.id, f"""📢 **پیام از طرف سازنده اصلی**

{msg_text}

━━━━━━━━━━━━━━━━━━━━━
🤖 ربات مدیریت گروه سافت
📌 {CHANNEL_LINK}
""")
                    sent_count += 1
                    safe_print(f"✅ پیام ارسال شد به گروه: {group.name}")
                    await asyncio.sleep(1)
                except Exception as e:
                    failed_count += 1
                    safe_print(f"❌ خطا در ارسال به گروه {group.name}: {e}")
            await pv_reply(event, f"""✅ **ارسال پیام به پایان رسید!**

📤 ارسال موفق: `{sent_count}` گروه
❌ ارسال ناموفق: `{failed_count}` گروه
📝 متن: {msg_text}
""")
        except Exception as e:
            await pv_reply(event, f"❌ خطا در ارسال به گروه‌ها: {e}")
        return
    if text.lower() in ["لفت همه", "leave all", "خروج از همه"]:
        if sender_id not in SUPER_OWNER_IDS:
            await pv_reply(event, "⛔ فقط سازنده اصلی میتواند از این دستور استفاده کند!")
            return
        await pv_reply(event, "⚠️ **هشدار!**\n\nشما در حال خروج ربات از **همه گروه‌ها** هستید!\n\nبرای تایید، دوباره بنویسید:\n`تایید لفت همه`")
        return
    if text.lower() == "تایید لفت همه":
        if sender_id not in SUPER_OWNER_IDS:
            await pv_reply(event, "⛔ فقط سازنده اصلی میتواند از این دستور استفاده کند!")
            return
        await pv_reply(event, "🔄 **در حال خروج از همه گروه‌ها...**")
        try:
            dialogs = await client.get_dialogs()
            groups = [d for d in dialogs if d.is_group]
            left_count = 0
            failed_count = 0
            for group in groups:
                try:
                    await client.leave_chat(group.id)
                    left_count += 1
                    safe_print(f"✅ خروج از گروه: {group.name}")
                    await asyncio.sleep(0.5)
                except Exception as e:
                    failed_count += 1
                    safe_print(f"❌ خطا در خروج از گروه {group.name}: {e}")
            await pv_reply(event, f"""✅ **خروج از همه گروه‌ها به پایان رسید!**

🚪 خروج موفق: `{left_count}` گروه
❌ خروج ناموفق: `{failed_count}` گروه

⚠️ ربات دیگر در هیچ گروهی عضو نیست!
""")
        except Exception as e:
            await pv_reply(event, f"❌ خطا در خروج از گروه‌ها: {e}")
        return

    # ==================== نادید گرفتن کاربر (فقط سازنده) ====================
    if text.lower().startswith("نادید گرفتن "):
        if sender_id not in SUPER_OWNER_IDS:
            await pv_reply(event, "⛔ فقط سازنده اصلی میتواند از این دستور استفاده کند!")
            return
        target_id_str = text[len("نادید گرفتن "):].strip()
        if not target_id_str.isdigit():
            await pv_reply(event, "❌ آیدی کاربر معتبر نیست!\nمثال: `نادید گرفتن 123456789`")
            return
        target_id = int(target_id_str)
        db.add_ignored_user(target_id, sender_id)
        await pv_reply(event, f"""🔇 **نادید گرفته شد**

آیدی `{target_id}` به لیست نادیده‌گرفته‌شده‌ها اضافه شد.
از این به بعد ربات نه توی پی‌وی و نه توی هیچ گروهی به پیام‌های این کاربر جوابی نمی‌ده (بدون بن یا سکوت شدنش؛ خودش و پیام‌هاش دست‌نخورده می‌مونه).

برای لغو: `رفع نادید گرفتن {target_id}`""")
        return

    if text.lower().startswith(("رفع نادید گرفتن ", "حذف نادید گرفتن ")):
        if sender_id not in SUPER_OWNER_IDS:
            await pv_reply(event, "⛔ فقط سازنده اصلی میتواند از این دستور استفاده کند!")
            return
        _ig_parts = text.split(maxsplit=3)
        target_id_str = _ig_parts[3].strip() if len(_ig_parts) > 3 else ""
        if not target_id_str.isdigit():
            await pv_reply(event, "❌ آیدی کاربر معتبر نیست!\nمثال: `رفع نادید گرفتن 123456789`")
            return
        target_id = int(target_id_str)
        removed = db.remove_ignored_user(target_id)
        if removed:
            await pv_reply(event, f"✅ آیدی `{target_id}` از لیست نادیده‌گرفته‌شده‌ها خارج شد؛ ربات دوباره بهش جواب می‌ده.")
        else:
            await pv_reply(event, f"ℹ️ آیدی `{target_id}` توی لیست نادیده‌گرفته‌شده‌ها نبود.")
        return

    if text.lower() in ["لیست نادید گرفته ها", "لیست نادید گرفته‌ها", "لیست نادیده گرفته ها", "لیست نادیده‌ها"]:
        if sender_id not in SUPER_OWNER_IDS:
            await pv_reply(event, "⛔ فقط سازنده اصلی میتواند از این دستور استفاده کند!")
            return
        ignored = db.get_ignored_users()
        if not ignored:
            await pv_reply(event, "📋 لیست نادیده‌گرفته‌شده‌ها خالیه.")
            return
        lines = ["📋 **لیست کاربران نادیده‌گرفته‌شده:**", ""]
        lines += [f"• `{item['user_id']}`" for item in ignored]
        await pv_reply(event, "\n".join(lines))
        return

    # ==================== مدیریت پیام «عضویت اجباری» (فقط سازنده) ====================
    # این متن همونیه که به هر کسی که برای اولین بار به ربات پیام می‌ده نشون داده
    # می‌شه (قفل تا وقتی «عضو شدم» رو بنویسه). قبلاً این متن توی کد هاردکد بود؛
    # الان توی دیتابیس ذخیره می‌شه تا سازنده‌ها هر وقت خواستن، بدون تغییر کد و
    # بدون ری‌استارت ربات، عوضش کنن.
    if text.lower() in ["پیام عضویت", "نمایش پیام عضویت"]:
        if sender_id not in SUPER_OWNER_IDS:
            await pv_reply(event, "⛔ این دستور فقط برای سازنده(های) اصلی ربات در دسترسه.")
            return
        current = get_join_required_msg()
        is_default = (db.get_setting(JOIN_REQUIRED_MSG_SETTING_KEY) is None)
        status = "🔹 پیش‌فرض (هیچ‌وقت دستی تغییر داده نشده)" if is_default else "✏️ دستی تغییر داده شده"
        await pv_reply(event, 
            "📨 **متن فعلی پیام عضویت اجباری**\n"
            f"وضعیت: {status}\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"{current}\n"
            "━━━━━━━━━━━━━━━━━━━━━\n\n"
            "برای تغییرش: `تنظیم پیام عضویت [متن جدید]`\n"
            "برای برگردوندن به پیش‌فرض: `ریست پیام عضویت`"
        )
        return

    if text.lower().startswith("تنظیم پیام عضویت "):
        if sender_id not in SUPER_OWNER_IDS:
            await pv_reply(event, "⛔ این دستور فقط برای سازنده(های) اصلی ربات در دسترسه.")
            return
        new_msg = text[len("تنظیم پیام عضویت "):]
        if not new_msg.strip():
            await pv_reply(event, "❌ متن نمی‌تونه خالی باشه!\nمثال:\n`تنظیم پیام عضویت سلام، اول عضو کانال ما شو ✅`")
            return
        set_join_required_msg(new_msg)
        # کسایی که قبلاً «عضو شدم» فرستادن نیازی به تأیید دوباره ندارن، پس این
        # لیست دست‌نخورده می‌مونه؛ فقط از این به بعد کاربرای جدید این متن جدید رو می‌بینن.
        await pv_reply(event, 
            "✅ **پیام عضویت اجباری به‌روزرسانی شد!**\n\n"
            "📝 پیش‌نمایش:\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"{new_msg}\n"
            "━━━━━━━━━━━━━━━━━━━━━"
        )
        return

    if text.lower() in ["ریست پیام عضویت", "پیش فرض پیام عضویت", "پیش‌فرض پیام عضویت"]:
        if sender_id not in SUPER_OWNER_IDS:
            await pv_reply(event, "⛔ این دستور فقط برای سازنده(های) اصلی ربات در دسترسه.")
            return
        reset_join_required_msg()
        await pv_reply(event, 
            "♻️ پیام عضویت اجباری به حالت پیش‌فرض برگشت:\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"{DEFAULT_JOIN_REQUIRED_MSG}\n"
            "━━━━━━━━━━━━━━━━━━━━━"
        )
        return

    # ==================== پنل سازنده ====================
    if text.lower() in ["پنل سازنده", "پنل مدیریت", "پنل ادمین کل", "پنل"]:
        if sender_id not in SUPER_OWNER_IDS:
            await pv_reply(event, "⛔ این دستور فقط برای سازنده(های) اصلی ربات در دسترسه.")
            return
        try:
            group_count = len(db.get_known_groups())
        except Exception:
            group_count = "?"
        try:
            ignored_count = len(db.get_ignored_users())
        except Exception:
            ignored_count = "?"
        is_default = (db.get_setting(JOIN_REQUIRED_MSG_SETTING_KEY) is None)
        join_status = "پیش‌فرض" if is_default else "سفارشی"
        panel_msg = f"""👑 **پنل سازنده**

سلام {SUPER_OWNER_IDS_DISPLAY}! این پنل کنترل کامل ربات دست توئه.

📊 **وضعیت کلی**
• گروه‌های ثبت‌شده: `{group_count}`
• کاربرهای نادیده‌گرفته‌شده: `{ignored_count}`
• پیام عضویت اجباری: {join_status}

━━━━━━━━━━━━━━━━━━━━━
🔒 **پیام عضویت اجباری** (پیامی که به تازه‌واردهای پی‌وی نشون داده می‌شه)
• `پیام عضویت` — دیدن متن فعلی
• `تنظیم پیام عضویت [متن جدید]` — تغییر متن
• `ریست پیام عضویت` — برگردوندن به پیش‌فرض

📢 **پیام‌رسانی**
• `ارسال به همه [متن]` — پیام به همه گروه‌ها

🚪 **مدیریت گروه‌ها**
• `لفت همه` — خروج ربات از همه گروه‌ها
• `آمار کلی` — آمار پیام‌های همه گروه‌ها
• `لیست روزشمار` — روزشمار فعال‌سازی گروه‌ها

🔇 **مدیریت کاربرها**
• `نادید گرفتن [آیدی]` — بی‌پاسخ کردن یک کاربر
• `رفع نادید گرفتن [آیدی]` — لغوش
• `لیست نادید گرفته‌ها` — دیدن لیست

━━━━━━━━━━━━━━━━━━━━━
📖 برای بقیه‌ی دستورات: `راهنما مالک`"""
        await pv_reply(event, panel_msg)
        return

    if "t.me/" in text or "splus.ir/" in text or "invite" in text:
        schedule_group_join(event, text)
        return
    if text.lower() in ["فعال", "روشن"]:
        await pv_reply(event, f"""🔹 **برای فعال‌سازی ربات در گروه:**

1️⃣ ربات رو به گروه اضافه کن
2️⃣ ربات رو ادمین کن
3️⃣ توی گروه بنویس `فعال`

📖 راهنما: `آموزش`
📌 {CHANNEL_LINK}
""")
        return
    await pv_reply(event, f"""🤖 **ربات مدیریت گروه سافت**

📋 `راهنما` - مشاهده دستورات
📖 `آموزش` - آموزش فعال‌سازی
🔗 لینک گروه بفرست - ورود خودکار
🎮 `دوز` - بازی دوز
🛡️ `وضعیت خودکار` - وضعیت مدیریت خودکار

📌 **کانال اصلی:** {CHANNEL_LINK}
🆘 **پشتیبانی:** {HELPER_LINK}
""")

@safe_handler
async def on_chat_action(_client, event):
    global BOT_ID
    try:
        chat_id = getattr(event, 'chat_id', None)
        if not chat_id:
            return

        if BOT_ID is None:
            try:
                me = await client.get_me()
                BOT_ID = me.id
            except:
                pass

        joined_ids = []
        left_ids = []

        # سازگاری با نام‌های مختلف احتمالی این ایونت (بسته به کتابخانه)
        if getattr(event, 'user_joined', False) or getattr(event, 'user_added', False):
            uid = getattr(event, 'user_id', None)
            if uid:
                joined_ids.append(uid)
        if getattr(event, 'user_left', False) or getattr(event, 'user_kicked', False):
            uid = getattr(event, 'user_id', None)
            if uid:
                left_ids.append(uid)

        for uid in getattr(event, 'user_ids', None) or []:
            if (getattr(event, 'user_joined', False) or getattr(event, 'user_added', False)) and uid not in joined_ids:
                joined_ids.append(uid)
            elif (getattr(event, 'user_left', False) or getattr(event, 'user_kicked', False)) and uid not in left_ids:
                left_ids.append(uid)

        if BOT_ID is not None and BOT_ID in joined_ids:
            # ربات (دستی یا با هر روشی) توی یک گروه اضافه شده: حداقل تعداد عضو رو چک کن.
            asyncio.create_task(_small_group_guard(chat_id, None, None, delays=(5, 60, 240, 900)))

        for uid in joined_ids:
            if uid != BOT_ID and db.is_locked(chat_id, "join_duplicate") and uid in _get_seen_cache(chat_id):
                # «جوین تکراری» رو در همان لحظه‌ای که event ورود می‌آید کنترل می‌کنیم.
                # چون ChatAction پیام متنی ندارد، اینجا رفتار قفل مستقیماً اجرا می‌شود.
                try:
                    action = db.get_lock_action(chat_id, "join_duplicate")
                    settings = db.get_auto_mod_settings(chat_id)
                    silent = settings.get("silent_mode", False)
                    label = "جوین تکراری"
                    if action == "ban" or silent:
                        db.ban_user(chat_id, uid, f"قفل {label}")
                        try:
                            await client.edit_permissions(chat_id, uid, send_messages=False)
                        except:
                            pass
                        try:
                            tag_username = await get_username(client, uid)
                            tag = view_tag(uid, tag_username)
                            await client.send_message(chat_id, format_ban_message("جوین تکراری", tag=tag))
                        except:
                            pass
                    elif action == "mute":
                        db.mute_user(chat_id, uid, "permanent", f"قفل {label}")
                        try:
                            await client.edit_permissions(chat_id, uid, send_messages=False)
                        except:
                            pass
                        try:
                            tag_username = await get_username(client, uid)
                            tag = view_tag(uid, tag_username)
                            await client.send_message(chat_id, format_mute_message("جوین تکراری", tag=tag))
                        except:
                            pass
                    else:
                        count = db.add_user_warning(chat_id, uid, f"قفل {label}")
                        limit = settings.get("warn_limit", 3)
                        try:
                            tag_username = await get_username(client, uid)
                            tag = view_tag(uid, tag_username)
                            await client.send_message(chat_id, format_warn_message("جوین تکراری", count, limit, tag=tag))
                        except:
                            pass
                        if count >= limit:
                            db.ban_user(chat_id, uid, f"رسیدن به {limit} اخطار (قفل {label})")
                            try:
                                await client.edit_permissions(chat_id, uid, send_messages=False)
                            except:
                                pass
                except Exception as e:
                    safe_print(f"[join_duplicate] خطا: {e}")
                continue

            if uid == BOT_ID:
                # وقتی یک کاربر (نه خودمون با لینک) ربات رو به گروه اضافه می‌کنه،
                # امکان «رد کردن» عضویت از قبل وجود نداره (invite همیشه بلافاصله
                # عضو می‌کنه)؛ پس همین لحظه از رجیستری سراسری می‌پرسیم و اگه ربات
                # دیگه‌ای از این مجموعه از قبل اینجا هست، فوراً و بدون اجرای بقیه‌ی
                # مراحل (خوش‌آمد/ادمین‌سازی) خارج می‌شیم.
                if GROUP_REGISTRY_API_URL:
                    occupied, owner_label, _ = await registry_check_group(chat_id)
                    if occupied:
                        try:
                            await client.send_message(chat_id,
                                f"❌ ربات «{owner_label}» از قبل توی این گروه هست؛ طبق تنظیمات هر گروه "
                                f"فقط یک ربات از این مجموعه می‌تونه داشته باشه، پس همین الان خارج می‌شم."
                            )
                        except Exception:
                            pass
                        try:
                            leave_entity = await client.get_entity(chat_id)
                        except Exception:
                            leave_entity = chat_id
                        await _try_leave_chat(leave_entity, chat_id)
                        continue

                # فعال‌سازیِ خودکار (بدون نیاز به نوشتنِ «فعال»): مالکِ واقعی و ادمین‌های
                # گروه از سروش گرفته و ثبت می‌شن؛ اگه همزمان یک جوینِ پنل در حالِ چکِ
                # ادمین باشه، تا پایانِ چک صبر می‌کنه (_on_bot_added_to_group).
                asyncio.create_task(_on_bot_added_to_group(chat_id))
            else:
                await greet_new_member(chat_id, uid)
                mark_member_seen(chat_id, uid)
                asyncio.create_task(check_new_member_bio(chat_id, uid))

        for uid in left_ids:
            if uid != BOT_ID:
                await farewell_member(chat_id, uid)
                unmark_member_seen(chat_id, uid)
            elif GROUP_REGISTRY_API_URL:
                # خودِ ربات از این گروه بن/کیک/حذف شده. قبلاً این حالت اصلاً
                # مدیریت نمی‌شد، پس رجیستری سراسری تا ۷۲ ساعت بعد (تا prune
                # خودکار) همچنان این گروه رو «اشغالِ همین ربات» نشون می‌داد و
                # هیچ ربات جایگزینی نمی‌تونست بیاد. الان همون لحظه رزرو آزاد
                # می‌شه تا یک نمونه‌ی دیگه فوراً بتونه جایگزینش بشه.
                asyncio.create_task(registry_leave_group(chat_id))
    except Exception as e:
        safe_print(f"[on_chat_action] خطا: {e}")

@safe_handler
def _extract_meet_link(text: str):
    if not text:
        return None
    m = re.search(r'https?://splus\.ir/meet/([A-Za-z0-9_-]+)', text, re.IGNORECASE)
    if m:
        return m.group(0)
    m = re.search(r'(?:splus\.ir/meet/)([A-Za-z0-9_-]+)', text, re.IGNORECASE)
    return f"https://splus.ir/meet/{m.group(1)}" if m else None


async def _start_group_call(chat_id, title="Group Call"):
    """Create a conference call for the current group and connect LiveKit audio."""
    async with _CALL_LOCK:
        old = _ACTIVE_CALLS.get(chat_id)
        if old:
            return None, "already"
        created = await client.create_group_call(title=title, chat_id=chat_id)
        if not created or not created.get("meet_link"):
            return None, "create_failed"
        meet_link = created["meet_link"]
        joined = await client.join_group_call(slug=created.get("slug"), meet_link=meet_link, muted=True, video_stopped=True)
        if not joined or not joined.get("url") or not joined.get("token"):
            try:
                await client.leave_group_call(meet_link=meet_link)
            except Exception:
                pass
            return None, "join_failed"
        audio = CallAudioSession()
        try:
            await audio.connect(joined["url"], joined["token"])
        except Exception:
            try:
                await client.leave_group_call(meet_link=meet_link)
            except Exception:
                pass
            raise
        _ACTIVE_CALLS[chat_id] = {"meet_link": meet_link, "slug": joined.get("slug"), "audio": audio}
        return meet_link, "ok"


async def _stop_group_call(chat_id):
    async with _CALL_LOCK:
        state = _ACTIVE_CALLS.pop(chat_id, None)
        if not state:
            return False
        errors_seen = []
        try:
            await state["audio"].disconnect()
        except Exception as e:
            errors_seen.append(e)
        try:
            # Leaving LiveKit/Splus is not enough: it only removes the bot.
            # DiscardConferenceCallRequest is the server-side operation that
            # actually terminates the conference created by this account.
            await client.end_group_call(meet_link=state["meet_link"])
        except Exception as e:
            errors_seen.append(e)
            # Keep a fallback leave attempt for compatibility with servers
            # that reject discard for a conference that is already closing.
            try:
                await client.leave_group_call(meet_link=state["meet_link"])
            except Exception:
                pass
        if errors_seen:
            safe_print(f"[call] stop completed with cleanup warning: {errors_seen[-1]}")
        return True


async def _handle_call_commands(event, chat_id, sender_id, text, is_super_owner, is_group_owner, is_admin, is_only_special_tier=False):
    if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
        return False
    normalized = text.strip()
    lower = normalized.lower()

    if lower in ("کال", "کال گروه", "شروع کال", "ساخت کال", "تماس"):
        try:
            link, status = await _start_group_call(chat_id, title="Group Call")
            if status == "already":
                await event.reply("⚠️ این گروه همین الان داخل یک کال فعال است.")
            elif status == "create_failed":
                await event.reply("❌ ساخت کال ناموفق بود.")
            elif status == "join_failed":
                await event.reply("❌ کال ساخته شد ولی ورود ربات به کال ناموفق بود.")
            else:
                await event.reply(f"📞 کال ساخته و وصل شد!\n🔗 {link}\n\nبرای خروج: `قطع کال`")
        except Exception as e:
            safe_print(f"[call] start error: {e}")
            await event.reply(f"❌ خطا در راه‌اندازی کال: {e}")
        return True

    if lower in ("قطع کال", "قطع‌کال", "خروج از کال", "پایان کال", "بستن کال"):
        try:
            ok = await _stop_group_call(chat_id)
            if ok:
                await event.reply("📞 از کال خارج شدم و اتصال صوتی قطع شد.")
            else:
                await event.reply("ℹ️ کال فعالی که توسط ربات باز شده باشد پیدا نشد.")
        except Exception as e:
            safe_print(f"[call] stop error: {e}")
            await event.reply(f"❌ خطا در قطع کال: {e}")
        return True
    return False


async def _handle_fonts_command(event, text):
    parts = text.strip().split(maxsplit=2)
    if not parts or parts[0].lower() not in ("فونت", "فونت‌ها", "فونتها"):
        return False
    if len(parts) < 2:
        await event.reply("ℹ️ نمونه: `فونت سلام دنیا` یا `فونت shek سلام دنیا`")
        return True
    modes = {"shek", "lash", "kesh", "syriac", "khat", "fonts"}
    mode = None
    body = text.strip().split(maxsplit=1)[1]
    first, *rest = body.split(maxsplit=1)
    if first.lower() in modes and rest:
        mode, body = first.lower(), rest[0]
    answer = await api_fonts(body, mode=mode, restric=False)
    if not answer:
        await event.reply("❌ دریافت نتیجه از API فونت ناموفق بود.")
        return True
    lines = []
    if isinstance(answer, dict):
        for key, value in answer.items():
            lines.append(f"🔹 {key}")
            if isinstance(value, (list, tuple)):
                lines.extend(str(x) for x in value)
            else:
                lines.append(str(value))
    else:
        lines.append(str(answer))
    out = "\n".join(lines)
    if len(out) > 3800:
        out = out[:3800] + "\n…"
    await event.reply(out)
    return True


def _normalize_spam_text(value):
    """متن را برای مقایسه پیام‌های تکراری یکدست می‌کند."""
    if not value:
        return ""
    value = str(value).replace("\u200c", " ").replace("\u200f", "").replace("\u200e", "")
    return re.sub(r"\s+", " ", value).strip().casefold()


def _gif_font(size):
    candidates = [
        os.path.join(BASE_DIR, "fonts", "NotoSansArabic-Regular.ttf"),
        os.path.join(BASE_DIR, "fonts", "Vazirmatn-Regular.ttf"),
        r"C:\Windows\Fonts\tahoma.ttf",
        r"C:\Windows\Fonts\arial.ttf",
        r"C:\Windows\Fonts\segoeui.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansArabic-Regular.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansArabicUI-Regular.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
    ]
    for path in candidates:
        try:
            if os.path.exists(path):
                return ImageFont.truetype(path, size)
        except Exception:
            pass
    try:
        return ImageFont.load_default()
    except Exception:
        return None


def _shape_gif_text(text):
    if arabic_reshaper and bidi_get_display and re.search(r"[\u0600-\u06ff]", text):
        try:
            return bidi_get_display(arabic_reshaper.reshape(text))
        except Exception:
            pass
    return text


def _build_typing_gif(text, output_path):
    """GIF ساده تایپ‌شونده — مقاوم در برابر نسخه‌های مختلف Pillow."""
    text = re.sub(r"\s+", " ", str(text).strip())
    if not text:
        raise ValueError("empty gif text")
    if len(text) > 80:
        text = text[:80]

    W, H = 480, 180
    font = _gif_font(48 if len(text) < 20 else 36)
    small_font = _gif_font(20)
    # اگر فونت پیدا نشد، از پیش‌فرض خیلی ساده استفاده کن
    if font is None:
        font = ImageFont.load_default()
    if small_font is None:
        small_font = ImageFont.load_default()

    # فریم‌ها: حرف‌به‌حرف + چند فریم ثابت در انتها
    steps = list(range(1, len(text) + 1)) + [len(text)] * 3
    rendered = []

    for idx, count in enumerate(steps):
        raw = text[:count]
        shown = _shape_gif_text(raw)
        img = Image.new("RGB", (W, H), (22, 24, 32))
        draw = ImageDraw.Draw(img)

        # کادر ساده (بدون rounded_rectangle برای سازگاری با Pillow قدیمی)
        try:
            draw.rounded_rectangle((40, 40, W - 40, H - 40), radius=24,
                                   fill=(34, 38, 50), outline=(120, 130, 155), width=2)
        except Exception:
            draw.rectangle((40, 40, W - 40, H - 40),
                           fill=(34, 38, 50), outline=(120, 130, 155), width=2)

        try:
            draw.text((60, 52), "typing...", font=small_font, fill=(160, 170, 190))
        except Exception:
            draw.text((60, 52), "typing...", fill=(160, 170, 190))

        # اندازه متن
        try:
            bbox = draw.textbbox((0, 0), shown, font=font)
            tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        except Exception:
            try:
                tw, th = draw.textsize(shown, font=font)
            except Exception:
                tw, th = len(shown) * 12, 20

        x = max(55, (W - tw) // 2)
        y = max(90, (H - th) // 2)
        try:
            draw.text((x, y), shown, font=font, fill=(245, 247, 250))
        except Exception:
            draw.text((x, y), shown, fill=(245, 247, 250))

        # مکان‌نما
        rtl = bool(re.search(r"[\u0600-\u06ff]", raw))
        cursor_x = (x - 10) if rtl else (x + tw + 6)
        if idx % 2 == 0 or idx >= len(steps) - 4:
            cy1, cy2 = y + 2, y + max(th - 2, 14)
            draw.rectangle((cursor_x, cy1, cursor_x + 4, cy2), fill=(240, 240, 245))

        # تبدیل به پالت برای GIF
        try:
            rendered.append(img.convert("P", palette=Image.Palette.ADAPTIVE, colors=128))
        except Exception:
            try:
                rendered.append(img.convert("P", palette=Image.ADAPTIVE, colors=128))
            except Exception:
                rendered.append(img.convert("P"))

    if not rendered:
        raise RuntimeError("no frames rendered")

    # ذخیره با چند روش fallback
    try:
        rendered[0].save(
            output_path,
            save_all=True,
            append_images=rendered[1:],
            duration=120,
            loop=0,
            optimize=True,
            disposal=2,
        )
    except Exception:
        rendered[0].save(
            output_path,
            save_all=True,
            append_images=rendered[1:],
            duration=100,
            loop=0,
        )

    if not os.path.exists(output_path) or os.path.getsize(output_path) < 100:
        raise RuntimeError("gif file empty or missing")


# --- سرور محلی گیف برای تست روی سیستم شخصی ---
_GIF_LOCAL_APP = None
_GIF_LOCAL_RUNNER = None
_GIF_LOCAL_SITE = None
_GIF_TUNNEL_URL = None
_GIF_TUNNEL_PROC = None
_GIF_LOCAL_DIR = None


def _gif_local_dir():
    global _GIF_LOCAL_DIR
    if _GIF_LOCAL_DIR:
        return _GIF_LOCAL_DIR
    try:
        from config import GIF_PUBLIC_DIR
        if GIF_PUBLIC_DIR:
            _GIF_LOCAL_DIR = str(GIF_PUBLIC_DIR)
        else:
            _GIF_LOCAL_DIR = os.path.join(BASE_DIR, "gif_public")
    except Exception:
        _GIF_LOCAL_DIR = os.path.join(BASE_DIR, "gif_public")
    os.makedirs(_GIF_LOCAL_DIR, exist_ok=True)
    return _GIF_LOCAL_DIR


async def _ensure_gif_local_server():
    """سرور استاتیک محلی روی GIF_LOCAL_PORT."""
    global _GIF_LOCAL_APP, _GIF_LOCAL_RUNNER, _GIF_LOCAL_SITE
    if _GIF_LOCAL_RUNNER is not None:
        return
    try:
        from aiohttp import web
    except Exception as e:
        raise RuntimeError(f"aiohttp لازم است: {e}")
    try:
        from config import GIF_LOCAL_PORT
        port = int(GIF_LOCAL_PORT or 8765)
    except Exception:
        port = 8765
    directory = _gif_local_dir()

    async def handle_file(request):
        name = request.match_info.get("name", "")
        # فقط نام فایل ساده
        name = os.path.basename(name)
        path = os.path.join(directory, name)
        if not os.path.isfile(path):
            return web.Response(status=404, text="not found")
        return web.FileResponse(path)

    app = web.Application()
    app.router.add_get("/gifs/{name}", handle_file)
    app.router.add_get("/{name}", handle_file)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    _GIF_LOCAL_APP = app
    _GIF_LOCAL_RUNNER = runner
    _GIF_LOCAL_SITE = site
    safe_print(f"[gif] local static server on http://127.0.0.1:{port}/gifs/...")


async def _ensure_gif_tunnel():
    """لینک عمومی موقت با cloudflared یا localtunnel."""
    global _GIF_TUNNEL_URL, _GIF_TUNNEL_PROC
    if _GIF_TUNNEL_URL:
        return _GIF_TUNNEL_URL
    try:
        from config import GIF_LOCAL_PORT
        port = int(GIF_LOCAL_PORT or 8765)
    except Exception:
        port = 8765

    await _ensure_gif_local_server()

    # 1) cloudflared
    cloudflared = None
    for cand in ("cloudflared", r"C:\Program Files (x86)\cloudflared\cloudflared.exe",
                 r"C:\Program Files\cloudflared\cloudflared.exe"):
        if cand == "cloudflared":
            import shutil
            cloudflared = shutil.which("cloudflared")
            if cloudflared:
                break
        elif os.path.isfile(cand):
            cloudflared = cand
            break

    if cloudflared:
        import subprocess
        _GIF_TUNNEL_PROC = subprocess.Popen(
            [cloudflared, "tunnel", "--url", f"http://127.0.0.1:{port}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        # چند ثانیه برای گرفتن URL
        deadline = time.time() + 25
        url = None
        while time.time() < deadline and _GIF_TUNNEL_PROC.poll() is None:
            line = _GIF_TUNNEL_PROC.stdout.readline()
            if not line:
                await asyncio.sleep(0.2)
                continue
            safe_print(f"[gif/tunnel] {line.strip()}")
            import re as _re
            m = _re.search(r"https://[a-zA-Z0-9-]+\.trycloudflare\.com", line)
            if m:
                url = m.group(0)
                break
        if url:
            _GIF_TUNNEL_URL = url.rstrip("/") + "/gifs"
            safe_print(f"[gif] tunnel ready: {_GIF_TUNNEL_URL}")
            return _GIF_TUNNEL_URL

    # 2) npx localtunnel
    import shutil
    npx = shutil.which("npx")
    if npx:
        import subprocess
        _GIF_TUNNEL_PROC = subprocess.Popen(
            [npx, "--yes", "localtunnel", "--port", str(port)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        deadline = time.time() + 30
        while time.time() < deadline and _GIF_TUNNEL_PROC.poll() is None:
            line = _GIF_TUNNEL_PROC.stdout.readline()
            if not line:
                await asyncio.sleep(0.2)
                continue
            safe_print(f"[gif/lt] {line.strip()}")
            import re as _re
            m = _re.search(r"https://[a-zA-Z0-9.-]+\.loca\.lt", line)
            if m:
                _GIF_TUNNEL_URL = m.group(0).rstrip("/") + "/gifs"
                safe_print(f"[gif] localtunnel ready: {_GIF_TUNNEL_URL}")
                return _GIF_TUNNEL_URL

    raise RuntimeError(
        "برای تست روی سیستم شخصی یکی از این‌ها را نصب کن:\n"
        "• cloudflared  (پیشنهادی)\n"
        "  https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/install-and-setup/installation/\n"
        "• یا Node.js + npx localtunnel"
    )


async def _host_temp_file(file_path, filename="typing.gif"):
    """آپلود موقت فایل روی هاست عمومی یا پوشه عمومی دامنه .ir خودت."""
    # --- مسیر ۱: دامنه/پوشه عمومی خودت (بهترین گزینه برای ایران) ---
    try:
        from config import GIF_PUBLIC_BASE_URL
    except Exception:
        GIF_PUBLIC_BASE_URL = None
    try:
        from config import GIF_PUBLIC_DIR
    except Exception:
        GIF_PUBLIC_DIR = None
    # دامنه عمومی یا تونل محلی برای تست روی PC
    base = None
    if GIF_PUBLIC_BASE_URL:
        base = str(GIF_PUBLIC_BASE_URL).rstrip("/")
    else:
        try:
            from config import GIF_LOCAL_TUNNEL
        except Exception:
            GIF_LOCAL_TUNNEL = True
        if GIF_LOCAL_TUNNEL:
            base = await _ensure_gif_tunnel()

    if base:
        if GIF_PUBLIC_DIR:
            pub_dir = str(GIF_PUBLIC_DIR)
        else:
            pub_dir = _gif_local_dir()
        os.makedirs(pub_dir, exist_ok=True)
        import uuid
        name = f"{uuid.uuid4().hex[:12]}_{filename}"
        dest = os.path.join(pub_dir, name)
        with open(file_path, "rb") as src, open(dest, "wb") as dst:
            dst.write(src.read())
        # اگر base قبلاً /gifs دارد تکرار نکن
        if base.endswith("/gifs"):
            url = f"{base}/{name}"
        else:
            url = f"{base}/gifs/{name}"
        safe_print(f"[gif] public url: {url}")
        return url

    timeout = aiohttp.ClientTimeout(total=35, connect=10)

    async def _post_form(url, form):
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, data=form) as resp:
                body = await resp.text()
                return resp.status, body.strip()

    last_err = None

    # tmpfiles.org
    try:
        form = aiohttp.FormData()
        form.add_field("file", open(file_path, "rb"), filename=filename, content_type="image/gif")
        status, body = await _post_form("https://tmpfiles.org/api/v1/upload", form)
        if status == 200:
            import json as _json
            data = _json.loads(body)
            u = (data.get("data") or {}).get("url") or ""
            # tmpfiles returns http://tmpfiles.org/123/name — need download link
            if u:
                # convert to direct: https://tmpfiles.org/dl/123/name
                u2 = u.replace("http://", "https://").replace("tmpfiles.org/", "tmpfiles.org/dl/")
                safe_print(f"[gif] hosted tmpfiles: {u2}")
                return u2
        last_err = RuntimeError(f"tmpfiles {status}: {body[:100]}")
    except Exception as e:
        last_err = e
        safe_print(f"[gif] tmpfiles failed: {e}")

    # gofile.io
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get("https://api.gofile.io/servers") as r:
                sj = await r.json()
            server = ((sj.get("data") or {}).get("servers") or [{"name": "store1"}])[0]["name"]
            form = aiohttp.FormData()
            form.add_field("file", open(file_path, "rb"), filename=filename, content_type="image/gif")
            async with session.post(f"https://{server}.gofile.io/uploadFile", data=form) as r2:
                j = await r2.json()
                link = ((j.get("data") or {}).get("downloadPage")
                        or (j.get("data") or {}).get("directLink"))
                if link:
                    safe_print(f"[gif] hosted gofile: {link}")
                    return link
            last_err = RuntimeError(f"gofile: {j}")
    except Exception as e:
        last_err = e
        safe_print(f"[gif] gofile failed: {e}")

    # pixeldrain
    try:
        form = aiohttp.FormData()
        form.add_field("file", open(file_path, "rb"), filename=filename, content_type="image/gif")
        status, body = await _post_form("https://pixeldrain.com/api/file", form)
        if status in (200, 201):
            import json as _json
            data = _json.loads(body)
            fid = data.get("id")
            if fid:
                url = f"https://pixeldrain.com/api/file/{fid}"
                safe_print(f"[gif] hosted pixeldrain: {url}")
                return url
        last_err = RuntimeError(f"pixeldrain {status}: {body[:100]}")
    except Exception as e:
        last_err = e
        safe_print(f"[gif] pixeldrain failed: {e}")

    # litterbox / catbox / 0x0 (ممکن است در ایران بسته باشند)
    for name, url, extra in (
        ("litterbox", "https://litterbox.catbox.moe/resources/internals/api.php",
         [("reqtype", "fileupload"), ("time", "1h")]),
        ("catbox", "https://catbox.moe/user/api.php",
         [("reqtype", "fileupload")]),
        ("0x0", "https://0x0.st", []),
    ):
        try:
            form = aiohttp.FormData()
            for k, v in extra:
                form.add_field(k, v)
            field = "fileToUpload" if name != "0x0" else "file"
            form.add_field(field, open(file_path, "rb"), filename=filename, content_type="image/gif")
            status, body = await _post_form(url, form)
            if status == 200 and body.startswith("http"):
                safe_print(f"[gif] hosted {name}: {body}")
                return body
            last_err = RuntimeError(f"{name} {status}: {body[:100]}")
        except Exception as e:
            last_err = e
            safe_print(f"[gif] {name} failed: {e}")

    # telegra.ph (معمولاً در ایران باز است — گاهی فقط عکس استاتیک)
    try:
        form = aiohttp.FormData()
        form.add_field("file", open(file_path, "rb"), filename=filename, content_type="image/gif")
        status, body = await _post_form("https://telegra.ph/upload", form)
        if status == 200:
            import json as _json
            data = _json.loads(body)
            if isinstance(data, list) and data and data[0].get("src"):
                src = data[0]["src"]
                url = "https://telegra.ph" + src if src.startswith("/") else src
                safe_print(f"[gif] hosted telegraph: {url}")
                return url
        last_err = RuntimeError(f"telegraph {status}: {body[:100]}")
    except Exception as e:
        last_err = e
        safe_print(f"[gif] telegraph failed: {e}")

    raise RuntimeError(f"hosting failed: {last_err}")


async def _try_mtproto_upload(chat_id, tmp_path, caption):
    """آخرین تلاش: آپلود مستقیم MTProto با part کوچک و بدون progress."""
    try:
        raw = getattr(client, "_client", None)
        if raw is None:
            return False
        # آپلود دستی با part_size کوچک
        uploaded = await raw.upload_file(tmp_path, part_size_kb=32, file_name="typing.gif")
        entity = await raw.get_input_entity(chat_id)
        await raw.send_file(entity, uploaded, caption=caption, force_document=False)
        return True
    except Exception as e:
        safe_print(f"[gif] mtproto upload failed: {e}")
        return False


async def _handle_gif_command(event, text):
    """دستور `گیف متن` — ساخت گیف تایپی + ارسال از طریق URL یا MTProto."""
    raw = (text or "").strip()
    lower = raw.casefold()
    if not (lower.startswith("گیف ") or lower.startswith("gif ")):
        if lower in ("گیف", "gif"):
            try:
                await event.reply(
                    "📌 نمونه: `گیف سلام`\n\n"
                    "اگر ارسال خطا داد، در `config.py` مقدار بگذار:\n"
                    "`GIF_PUBLIC_BASE_URL = \"https://دامنه-شما/gifs\"`"
                )
            except Exception:
                pass
            return True
        return False

    body = raw.split(maxsplit=1)[1].strip() if len(raw.split(maxsplit=1)) > 1 else ""
    if not body:
        try:
            await event.reply("📌 نمونه: `گیف سلام`")
        except Exception:
            pass
        return True

    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(prefix="splus_typing_", suffix=".gif")
        os.close(fd)
        _build_typing_gif(body, tmp_path)
        safe_print(f"[gif] built size={os.path.getsize(tmp_path)}")
        chat_id = getattr(event, "chat_id", None)
        caption = f"✨ {body}"
        sent = False
        last_err = None

        # ۱) هاست موقت / پوشه عمومی → URL
        try:
            url = await _host_temp_file(tmp_path, filename="typing.gif")
            for force_doc in (False, True):
                try:
                    await client.send_file(
                        chat_id, url, caption=caption,
                        force_document=force_doc, progress=False,
                    )
                    sent = True
                    safe_print(f"[gif] sent via URL force_document={force_doc}")
                    break
                except Exception as e:
                    last_err = e
                    safe_print(f"[gif] URL send failed: {e}")
        except Exception as e:
            last_err = e
            safe_print(f"[gif] hosting failed: {e}")

        # ۲) آپلود مستقیم MTProto
        if not sent:
            sent = await _try_mtproto_upload(chat_id, tmp_path, caption)

        if not sent:
            raise RuntimeError(
                f"{last_err}\n\n"
                "راهکار: در config.py بگذار:\n"
                "GIF_PUBLIC_BASE_URL = \"https://دامنه-عمومی-تو/gifs\"\n"
                "و پوشه gif_public را با nginx/apache سرو کن."
            )
    except Exception as e:
        safe_print(f"[gif] error: {e}")
        try:
            await event.reply(f"❌ ساخت/ارسال گیف ناموفق بود.\n`{type(e).__name__}: {e}`")
        except Exception:
            pass
    finally:
        if tmp_path:
            try:
                os.remove(tmp_path)
            except Exception:
                pass
    return True


@safe_handler
async def message_handler(_client, event):
    _HASHTAG_CONTEXT.set({"chat_id": getattr(event, "chat_id", None), "user_id": getattr(event, "sender_id", None)})
    global BOT_ID
    
    if not event:
        return

    chat_id = getattr(event, "chat_id", None)
    sender_id = getattr(event, "sender_id", None)

    # پیام خصوصی (PV) اصلاً اینجا پردازش نمی‌شود.
    # قبلاً این بلوک هر پیام PV رو (چه لینک گروه، چه هر متن دیگه‌ای) مستقل و
    # موازی با private_message_handler پردازش می‌کرد - یعنی روی *هر* پیام پی‌وی،
    # هر دو هندلر با هم اجرا می‌شدن: هم پیام «عضویت اجباری»/خوش‌آمد/دستورات از
    # private_message_handler، هم یک پیام دومِ اضافه («پیام شما دریافت شد...»)
    # از همینجا - بدون هیچ چک عضویتی. نتیجه‌اش پاسخ‌های تکراری/متناقض توی پی‌وی
    # بود (و چون گیت عضویت اجباری از این مسیر دور زده می‌شد، عملاً بی‌اثر می‌شد).
    # private_message_handler (که با client.add_event_handler جدا و قبل از این
    # هندلر ثبت شده) از قبل *کامل* مسئول پی‌وی است: گیت عضویت اجباری، پیام
    # خوش‌آمد، دستورات، و حتی همین قابلیت «لینک گروه بفرست تا عضو بشه»
    # (join_group_via_link) را بعد از تأیید عضویت خودش صدا می‌زند. پس اینجا فقط
    # باید بی‌سروصدا برگردیم تا private_message_handler تنها مسیر پاسخ‌گویی به
    # پی‌وی بماند.
    is_group_or_channel = bool(
        getattr(event, "is_group", False) or getattr(event, "is_channel", False)
    )
    if not is_group_or_channel:
        return

    # از اینجا به بعد فقط پیام‌های گروه/کانال پردازش می‌شوند.

    if BOT_ID is None:
        try:
            me = await client.get_me()
            BOT_ID = me.id
        except:
            pass

    # جلوگیری از پردازش پیام‌های خود ربات: چون ربات با اکانت خودش پیام می‌فرسته
    # (نه با یک بات جدا)، پیام‌های خودش (هشدارها، پاسخ‌های سخنگو، جواب دستورات و...)
    # هم به‌عنوان یک NewMessage جدید به همین هندلر می‌رسیدن؛ بدون این چک، ربات
    # پیام‌های خودش رو اسپم حساب می‌کرد (و حتی ممکن بود خودش رو بن کنه)، به خودش
    # پاسخ سخنگو می‌داد، و اگه توی متن پیام خودش لقب کسی می‌بود، بهش هم واکنش نشون می‌داد.
    if getattr(event, 'out', False) or (BOT_ID is not None and sender_id == BOT_ID):
        return

    # ==================== پرش از پیام‌های قدیمی/باقی‌مانده از قطعی ====================
    # وقتی اتصال قطع بوده و دوباره وصل می‌شه، سرور همه‌ی پیام‌های ازدست‌رفته رو یک‌جا
    # (getDifference) برای ربات می‌فرسته. بدون این چک، ربات روی تک‌تک همون پیام‌های
    # قدیمی هم مثل پیام تازه واکنش نشون می‌داد (پاسخ سخنگو، اخطار، حذف و ...) - هم
    # باعث یه موج ناگهانی درخواست می‌شد، هم برای اعضای گروه گیج‌کننده بود (ربات به
    # پیام‌های نیم‌ساعت-یک‌ساعت پیش جواب می‌داد انگار همین الان ارسال شدن). اینجا اگه
    # پیام از STALE_MESSAGE_SKIP_SECONDS قدیمی‌تر باشه، بی‌سروصدا (بدون هیچ پاسخ/حذف/
    # اخطاری) رد می‌شه - نه اسپم حساب می‌شه نه فیلتر؛ فقط کاملاً نادیده گرفته می‌شه.
    try:
        msg_date = getattr(event, 'date', None)
        if msg_date is not None:
            now_utc = datetime.now(msg_date.tzinfo) if msg_date.tzinfo else datetime.now(timezone.utc).replace(tzinfo=None)
            age_seconds = (now_utc - msg_date).total_seconds()
            if age_seconds > STALE_MESSAGE_SKIP_SECONDS:
                return
    except Exception:
        pass

    # یک تعویق صفر-ثانیه‌ای (بدون تأخیرِ واقعی) به event loop اجازه می‌ده قبل از
    # شروعِ پردازشِ سنگینِ این پیامِ گروهی، اگه کارِ دیگه‌ای (مثلاً یک پاسخِ پی‌وی
    # که همین الان آماده‌ی ارساله) توی صف باشه، اول نوبتش رو بگیره. بدونِ این،
    # روی گروه‌های پرترافیک، پیام‌های پی‌وی می‌تونن پشتِ سرِ چند پردازشِ گروهیِ
    # پشت‌سرهم معطل بمونن، حتی وقتی خودِ ترافیکِ پی‌وی کم باشه.
    await asyncio.sleep(0)

    # ==================== نادید گرفتن (فقط سازنده) ====================
    # کاربری که سازنده با دستور «نادید گرفتن [آیدی]» در پی‌وی ربات نادیده گرفته،
    # از همین‌جا فیلتر می‌شه: نه کش/آمار پیامش ثبت می‌شه، نه هیچ پاسخ/دستوری پردازش
    # می‌شه - در عین حال بن/سکوت هم نمی‌شه (پیامش توی گروه دست‌نخورده می‌مونه، فقط
    # ربات کاری باهاش نداره). خود سازنده‌ها هیچ‌وقت نادیده گرفته نمی‌شن.
    if sender_id not in SUPER_OWNER_IDS and db.is_user_ignored(sender_id):
        return

    text = (event.text or "").strip()
    lower_text = text.lower()
    # حالت سخنگو مخصوص همین گروه؛ از کش داخلی db خونده می‌شه، پس هزینه‌ی اضافه‌ای نداره.
    SPEAKER_MODE = db.get_speaker_mode(chat_id)

    # اسم اختصاصی ربات مخصوص این گروه: اگه مالک اسمی گذاشته باشه، دستورات هم با اون اسم
    # (مثلاً «ربات من بن») و هم بدون اون (مثلاً فقط «بن») کار می‌کنن.
    try:
        custom_bot_name = db.get_bot_name(chat_id)
    except Exception:
        custom_bot_name = None
    if custom_bot_name:
        stripped, had_prefix = _strip_bot_name_prefix(text, custom_bot_name)
        if had_prefix and stripped:
            text = stripped
            lower_text = stripped.lower()

    # اسم گروه فقط یک‌بار (و بعدش هر چند ساعت) از سروش/تلگرام گرفته می‌شه، نه روی
    # تک‌تک پیام‌ها - چون get_entity یه درخواست شبکه‌ایه و زیر بار زیاد کند می‌کرد.
    now_ts = time.time()
    last_checked = GROUP_INFO_LAST_CHECK.get(chat_id, 0)
    if now_ts - last_checked > GROUP_INFO_REFRESH_SECONDS:
        GROUP_INFO_LAST_CHECK[chat_id] = now_ts
        try:
            group_info = await client.get_entity(chat_id)
            db.add_group(chat_id, group_info.title)
            member_count = getattr(group_info, "participants_count", None)
            if member_count:
                db.set_group_member_count(chat_id, member_count)
        except:
            pass

    # همین‌طور اسمِ نمایشیِ فرستنده - فقط هر ۲۴ ساعت یک‌بار برای هر کاربر، تا
    # لیدربوردِ فعالانِ پنلِ مرکزی اسمِ واقعی داشته باشه بدونِ اینکه پنل خودش
    # هیچ‌وقت مجبور بشه client.get_entity زنده بزنه (همونی که باعثِ هنگ می‌شد).
    last_user_checked = USER_INFO_LAST_CHECK.get(sender_id, 0)
    if now_ts - last_user_checked > USER_INFO_REFRESH_SECONDS:
        USER_INFO_LAST_CHECK[sender_id] = now_ts
        try:
            sender_entity = await client.get_entity(sender_id)
            first = getattr(sender_entity, "first_name", None)
            last = getattr(sender_entity, "last_name", None)
            full_name = f"{first} {last}".strip() if (first and last) else (first or last)
            if full_name:
                db.set_display_name(sender_id, full_name)
        except:
            pass

    cache_message(chat_id, event.id, sender_id, event.text)

    # هویت رسانه‌ی همین پیام (اگه رسانه داشته باشه) برای «فیلتر رسانه»: هم برای چک‌کردنِ
    # پیام‌های بعدی، هم برای اینکه با ریپلای روی این پیام بشه رسانه رو فیلتر کرد.
    _media_info = None
    try:
        _media_info = extract_media_info(event)
        if _media_info:
            cache_media_info(chat_id, event.id, _media_info)
    except Exception:
        _media_info = None

    # آمار پیام‌ها فقط در حافظه جمع می‌شه؛ نوشتن واقعی روی دیتابیس توسط
    # flush_stats_loop به‌صورت دسته‌جمعی و کم‌تعداد انجام می‌شه (نگاه کنید به تعریف تابع).
    _pending_chat_stats[chat_id] = _pending_chat_stats.get(chat_id, 0) + 1
    user_stats_key = (sender_id, chat_id)
    _pending_user_stats[user_stats_key] = _pending_user_stats.get(user_stats_key, 0) + 1
    daily_key = (sender_id, chat_id, datetime.now(IRAN_TZ).strftime("%Y-%m-%d"))
    _pending_daily_activity[daily_key] = _pending_daily_activity.get(daily_key, 0) + 1
    _hour_key = (chat_id, int(time.time() // 3600) * 3600)
    _pending_hourly_activity[_hour_key] = _pending_hourly_activity.get(_hour_key, 0) + 1

    if not db.is_bot_enabled():
        if lower_text in ["فعال", "/start", "start", "روشن"]:
            # ==================== دسترسی «فعال»: فقط ادمین‌های واقعیِ گروه ====================
            # کاربر عادی اصلاً حق نداره «فعال» بزنه؛ فقط ادمین/سازنده‌ی واقعیِ
            # گروه (طبق سروش) یا سازنده‌ی ربات می‌تونن این دستور رو اجرا کنن.
            _activation_permission = await check_user_permission(chat_id, sender_id)
            sender_is_admin_level = _activation_permission in (
                "super_owner", "creator", "admin", "group_owner", "db_admin"
            )
            if not sender_is_admin_level:
                return

            owner_id = db.get_group_owner(chat_id)

            if not owner_id:
                # مالکِ گروه هنوز ثبت نشده: اگه سازنده‌ی واقعیِ گروه قابل تشخیص
                # باشه، همون به‌عنوان مالک ثبت می‌شه (حتی اگه خودِ سازنده الان
                # پیام رو نداده باشه)؛ فقط اگه تشخیص ندادیم، همون ادمینی که
                # دستورِ «فعال» رو زده به‌عنوان مالک ثبت می‌شه.
                real_creator_id = await _get_real_group_creator(chat_id)
                owner_id_to_set = real_creator_id if real_creator_id else sender_id
                result = db.set_group_owner(chat_id, owner_id_to_set)
                if result:
                    db.add_group_admin(chat_id, owner_id_to_set)
                    db.set_bot_enabled(True)
                    try:
                        group_title = (await client.get_entity(chat_id)).title
                        db.set_group_name(chat_id, group_title)
                    except Exception:
                        group_title = await resolve_group_name(chat_id)
                    try:
                        db.record_group_activation(chat_id, group_title)
                    except Exception:
                        pass
                    try:
                        apply_default_group_locks(chat_id)
                    except:
                        pass
                    GROUP_OWNER_CACHE[chat_id] = owner_id_to_set
                    asyncio.create_task(announce_and_register_group_admins(chat_id))
                    try:
                        owner_username = await get_username(client, owner_id_to_set)
                        owner_view = view_tag(owner_id_to_set, owner_username)
                        await event.reply(f"""✅ **ربات با موفقیت فعال شد!** 🎉
    
    👑 **مالک گروه ثبت شد**
    مالک گروه: {owner_view}
    
    📋 **دستورات مدیریتی در اختیار مالک و ادمین‌هاست:**
    • `راهنما` - مشاهده همه دستورات
    • `بن` - بن کردن کاربران
    • `سکوت` - سکوت کردن کاربران
    • `قفل` - قفل کردن امکانات گروه
    • `پاک` - پاکسازی پیام‌ها
    • `حالت مؤدب` - حالت رسمی
    • `حالت بی ادب` - حالت خودمونی
    • `حالت خاموش` - غیرفعال کردن سخنگو
    • `حالت کم حرف` - فقط روی ریپلای جواب می‌دهد
    • `حالت پر حرف` - روی پیام‌های عادی هم جواب می‌دهد
    • `دوز` - بازی دوز
    • `لیست قفل` - مشاهده وضعیت قفل‌ها
    
    📌 **کانال اصلی:** {CHANNEL_LINK}
    """)
                    except:
                        pass
                    try:
                        owner_username = await get_username(client, owner_id_to_set)
                        owner_view = view_tag(owner_id_to_set, owner_username)
                        await client.send_message(chat_id, f"""👑 **ربات فعال شد!**

مالک گروه: {owner_view}

⚠️ **فقط مالک گروه می‌تواند ربات را مدیریت کند!**
📌 {CHANNEL_LINK}
""")
                    except:
                        pass
                else:
                    try:
                        await event.reply("❌ خطا در ثبت مالک! لطفاً دوباره تلاش کنید.")
                    except:
                        pass
                return            
            elif sender_id == owner_id or sender_id in SUPER_OWNER_IDS:
                db.set_bot_enabled(True)
                GROUP_OWNER_CACHE[chat_id] = owner_id
                try:
                    group_title = (await client.get_entity(chat_id)).title
                    db.set_group_name(chat_id, group_title)
                except Exception:
                    group_title = await resolve_group_name(chat_id)
                try:
                    db.record_group_activation(chat_id, group_title)
                except Exception:
                    pass
                asyncio.create_task(announce_and_register_group_admins(chat_id))
                try:
                    owner_username = await get_username(client, owner_id)
                    owner_view = view_tag(owner_id, owner_username)
                    await event.reply(f"""✅ **ربات فعال شد!** 🎉

👑 مالک گروه: {owner_view}
📌 {CHANNEL_LINK}
""")
                except:
                    pass
                return
            
            else:
                # ادمینِ دیگری غیر از مالکِ ثبت‌شده «فعال» زده؛ فقط خودِ مالک
                # یا سازنده‌ی ربات می‌تونه ربات رو دوباره روشن کنه.
                return
        
        return

    is_super_owner = (sender_id in SUPER_OWNER_IDS)
    is_group_owner = is_group_owner_check(chat_id, sender_id)
    
    if not is_group_owner and db.is_group_owner(chat_id, sender_id):
        is_group_owner = True
        GROUP_OWNER_CACHE[chat_id] = sender_id
    
    user_permission = await check_user_permission(chat_id, sender_id)
    is_creator = (user_permission == "creator")
    is_group_admin = (user_permission in ["admin", "creator"])
    is_group_special_admin = db.is_group_special_admin(chat_id, sender_id)
    is_admin = (is_group_owner or is_creator or is_group_admin or is_super_owner
                or db.is_admin(sender_id) or db.is_group_admin(chat_id, sender_id)
                or is_group_special_admin)

    # ==================== مقام‌بندی: معاف / ویژه / ادمین ====================
    # عادی < معاف < ویژه < ادمین < مالک گروه < سازنده.
    # «معاف»: هیچ دستور مدیریتی نداره، فقط از قفل‌ها/فیلترها/Auto Mod آزاده.
    # «ویژه» (جدول group_admins - قبلاً بهش «ادمین ربات» می‌گفتیم): از قفل‌ها
    # آزاده و is_admin براش True می‌شه، ولی فقط اجازه‌ی این دستورها رو داره:
    # بن، رفع بن، سکوت، رفع سکوت، اخطار، حذف اخطار، سنجاق. بقیه‌ی دستورهای
    # مدیریتی (پاکسازی، معاف‌کردن، قفل‌ها/بازکردن‌ها، فیلترها، تایمر، کال،
    # لیست‌ها، فعال/روشن، تنظیم لقب، تنظیم‌ها و...) با فلگ is_only_special_tier
    # مسدود شده‌اند.
    # «ادمین» (جدول group_special_admins - قبلاً «ادمین‌ویژه») مقامش از ویژه
    # بالاتره و هیچ محدودیتی نداره.
    is_group_special = db.is_group_special(chat_id, sender_id)
    is_group_exempt = db.is_group_exempt(chat_id, sender_id)
    is_filter_free = is_admin or is_group_special or is_group_exempt
    # فقط «ویژه»ست، نه ادمین/ادمین‌ویژه/مالک/سازنده/ادمینِ واقعی سروش، نه
    # مدیر سراسری ربات - برای محدودکردنِ پاکسازی/تنظیم‌معاف طبق مقام‌بندی جدید.
    is_only_special_tier = bool(
        db.is_group_admin(chat_id, sender_id)
        and not (
            is_group_owner or is_creator or is_group_admin or is_super_owner
            or db.is_admin(sender_id) or is_group_special_admin
        )
    )

    # ==================== Group Calls ====================
    if await _handle_call_commands(
        event, chat_id, sender_id, text, is_super_owner, is_group_owner, is_admin, is_only_special_tier
    ):
        return

    # ==================== Font API ====================
    if await _handle_fonts_command(event, text):
        return

    # ==================== Timer / Scheduler commands ====================
    # مدیریت تایمر قبل از قفل‌ها انجام می‌شود؛ چون کاربر مجاز قبلاً توسط همان
    # Permission فعلی پروژه تشخیص داده شده و تایمر نیز در زمان اجرا دوباره همین
    # Permission را بررسی می‌کند.
    if await handle_timer_commands(
        event, chat_id, sender_id, text,
        is_super_owner, is_group_owner, is_admin, is_only_special_tier
    ):
        return

    # ==================== انتخاب اسم تصادفی ====================
    if await handle_random_name_commands(
        event, chat_id, is_super_owner, is_group_owner, text
    ):
        return

    # ==================== خوشامد/خداحافظ/پیام ورود ربات ====================
    # این بخش به‌صورت جدا و با نوع ایونت درست (ChatAction) پیاده‌سازی شده -
    # به تابع on_chat_action در پایین همین فایل نگاه کنید.

    if db.is_locked(chat_id, "group"):
        if not is_filter_free:
            try:
                await event.delete()
                cache_key = f"{sender_id}_group_lock"
                if cache_key not in FILTER_WARN_CACHE:
                    await event.respond("🔒 **گروه قفل است!** فقط مدیران می‌توانند پیام بفرستند.")
                    FILTER_WARN_CACHE[cache_key] = True
                    async def clear_cache():
                        await asyncio.sleep(300)
                        FILTER_WARN_CACHE.pop(cache_key, None)
                    asyncio.create_task(clear_cache())
            except:
                pass
            return

    # ==================== GIF ساز تایپ‌شونده ====================
    # قبلاً این دستور داخل بلاک «if not is_filter_free» بود، یعنی فقط برای
    # اعضای عادی اجرا می‌شد؛ چون is_filter_free برای ادمین‌ها/مالک/گروه‌های
    # مستثنی True هست، خودِ ادمین‌ها (که معمولاً دستورات رو تست می‌کنن) اصلاً
    # به این کد نمی‌رسیدن و «گیف سلام» هیچ پاسخی نمی‌گرفت. حالا مستقل از
    # is_filter_free و قبل از Auto Mod اجرا می‌شه تا برای همه (ازجمله ادمین‌ها) کار کنه.
    # گیف‌ساز موقتاً غیرفعال است
    # if text and await _handle_gif_command(event, text):
    #     return

    # ==================== فیلتر رسانه ====================
    # روی عضو عادی، «معاف» و «ویژه» اعمال می‌شه (این سه حق ارسال رسانه‌ی فیلترشده رو
    # ندارن و اخطار می‌گیرن). «ادمین» (ربات/سروش)، مالک گروه، سازنده‌ی گروه و
    # سازنده‌های ربات مستثنی‌ان. is_only_special_tier یعنی «ویژه‌ی محض»، پس
    # (is_admin and not is_only_special_tier) یعنی ادمین یا بالاتر.
    if _media_info:
        _media_filter_immune = bool(is_admin and not is_only_special_tier)
        if not _media_filter_immune:
            _mf_hit = db.find_media_filter(chat_id, _media_info["key"], _media_info["fp"])
            if _mf_hit:
                await handle_media_filter_violation(
                    event, chat_id, sender_id, _mf_hit.get("action", "warn"),
                    _media_info["label"],
                )
                return

    # ==================== قفل بیو ====================
    # روی عضو عادی، معاف و ویژه اعمال می‌شه؛ ادمین و بالاتر مستثنی‌ان. تا وقتی بیوِ کاربر
    # لینک داره، هر پیامش پاک می‌شه (بدون هیچ مهلتی) و اخطار/بن اعمال می‌شه.
    if (not (is_admin and not is_only_special_tier)) and sender_id != BOT_ID and db.is_locked(chat_id, "bio_link"):
        if await _bio_lock_handle(event, chat_id, sender_id):
            return

    # ==================== Auto Mod ====================
    if not is_filter_free:
        auto_mod = db.get_auto_mod_settings(chat_id)
        
        # تشخیص اسپم
        if auto_mod["spam_enabled"]:
            if chat_id not in MESSAGE_HISTORY:
                MESSAGE_HISTORY[chat_id] = {}
            if sender_id not in MESSAGE_HISTORY[chat_id]:
                MESSAGE_HISTORY[chat_id][sender_id] = []
            
            now = time.time()
            # تاریخچه: (زمان، فارسی‌بودن)
            # اسپم سریع: ۴ پیام فارسی در ۵ ثانیه (پیام‌های غیر فارسی شمرده نمی‌شوند)
            hist = MESSAGE_HISTORY[chat_id][sender_id]
            cleaned = []
            for item in hist:
                t = item[0] if isinstance(item, tuple) else item
                if now - t < 5:
                    cleaned.append(item if isinstance(item, tuple) else (t, True))
            hist[:] = cleaned
            is_persian_msg = bool(text and re.search(r"[\u0600-\u06FF]", str(text)))
            hist.append((now, is_persian_msg))
            hist[:] = hist[-20:]
            persian_rapid_count = sum(1 for _t, fa in hist if fa)
            rapid_spam = is_persian_msg and persian_rapid_count >= 4

            # منطق تکرار: ۴ بار همان جمله در ۱۰ ثانیه
            normalized_text = _normalize_spam_text(text)
            if chat_id not in MESSAGE_TEXT_HISTORY:
                MESSAGE_TEXT_HISTORY[chat_id] = {}
            if sender_id not in MESSAGE_TEXT_HISTORY[chat_id]:
                MESSAGE_TEXT_HISTORY[chat_id][sender_id] = []
            text_history = MESSAGE_TEXT_HISTORY[chat_id][sender_id]
            text_history[:] = [item for item in text_history
                               if now - item[0] < REPEATED_SPAM_WINDOW_SECONDS]
            if normalized_text:
                text_history.append((now, normalized_text))
            text_history[:] = text_history[-12:]
            repeated_spam = (
                bool(normalized_text)
                and sum(1 for _, msg_text in text_history if msg_text == normalized_text)
                >= REPEATED_SPAM_COUNT
            )

            if rapid_spam or repeated_spam:
                try:
                    await event.delete()
                    try:
                        prev_ids = get_recent_sender_message_ids(chat_id, sender_id, exclude_msg_id=event.id, limit=3)
                        if prev_ids:
                            await client.delete_messages(chat_id, prev_ids)
                    except Exception as e:
                        safe_print(f"❌ خطا در پاکسازی پیام‌های قبلی اسپم‌دهنده: {e}")

                    if auto_mod.get("silent_mode"):
                        # حالت سایلنت: بدون اخطار، سریع بن
                        db.ban_user(chat_id, sender_id, "سایلنت: اسپم")
                        try:
                            await client.edit_permissions(chat_id, sender_id, send_messages=False)
                            _tag = await build_view_tag(sender_id, chat_id)
                            ban_msg = await send_moderation_message(event, chat_id, format_ban_message("ارسال اسپم", tag=_tag), sender_id=sender_id)
                            if ban_msg:
                                asyncio.create_task(auto_delete_warning(client, chat_id, ban_msg.id))
                            asyncio.create_task(clear_user_messages(client, chat_id, sender_id, 100))
                        except:
                            pass
                        return

                    count = db.add_user_warning(chat_id, sender_id, "اسپم")
                    
                    warn_key = f"spam_{sender_id}_{int(time.time()/60)}"
                    if warn_key not in WARN_MESSAGE_CACHE:
                        _tag = await build_view_tag(sender_id, chat_id)
                        warn_msg = await send_moderation_message(event, chat_id, format_warn_message("ارسال پیام‌های پی‌درپی مجاز نیست", count, auto_mod['warn_limit'], tag=_tag), sender_id=sender_id)
                        WARN_MESSAGE_CACHE[warn_key] = True
                        if warn_msg:
                            asyncio.create_task(auto_delete_warning(client, chat_id, warn_msg.id, delay=get_warning_delete_delay(chat_id, sender_id)))
                    
                    if count >= auto_mod["warn_limit"]:
                        db.ban_user(chat_id, sender_id, f"اسپم بیش از حد ({count} اخطار)")
                        try:
                            await client.edit_permissions(chat_id, sender_id, send_messages=False)
                            _tag = await build_view_tag(sender_id, chat_id)
                            ban_msg = await send_moderation_message(event, chat_id, format_ban_message("ارسال اسپم", tag=_tag), sender_id=sender_id)
                            if ban_msg:
                                asyncio.create_task(auto_delete_warning(client, chat_id, ban_msg.id))
                            asyncio.create_task(clear_user_messages(client, chat_id, sender_id, 100))
                        except:
                            pass
                except:
                    pass
                return
        
        
        # تشخیص تبلیغات
        # «قفل تبلیغ» و «تنظیم خودکار تبلیغ» دقیقاً همون یک تنظیم (ad_enabled) رو
        # کنترل می‌کنن - نگاه کنید به هندلر «قفل »/«باز کردن » که هر دو رو یکی کرده.
        if auto_mod["ad_enabled"] and text:
            if is_advertisement(text):
                try:
                    await event.delete()
                    try:
                        prev_ids = get_recent_sender_message_ids(chat_id, sender_id, exclude_msg_id=event.id, limit=3)
                        if prev_ids:
                            await client.delete_messages(chat_id, prev_ids)
                    except Exception as e:
                        safe_print(f"❌ خطا در پاکسازی پیام‌های قبلی تبلیغ‌چی: {e}")

                    if auto_mod.get("silent_mode"):
                        db.ban_user(chat_id, sender_id, "سایلنت: تبلیغات")
                        try:
                            await client.edit_permissions(chat_id, sender_id, send_messages=False)
                            _tag = await build_view_tag(sender_id, chat_id)
                            ban_msg = await send_moderation_message(event, chat_id, format_ban_message("ارسال تبلیغات", tag=_tag), sender_id=sender_id)
                            if ban_msg:
                                asyncio.create_task(auto_delete_warning(client, chat_id, ban_msg.id))
                            asyncio.create_task(clear_user_messages(client, chat_id, sender_id, 100))
                        except:
                            pass
                        return

                    count = db.add_user_warning(chat_id, sender_id, "تبلیغات")
                    
                    warn_key = f"ad_{sender_id}_{int(time.time()/60)}"
                    if warn_key not in WARN_MESSAGE_CACHE:
                        _tag = await build_view_tag(sender_id, chat_id)
                        warn_msg = await send_moderation_message(event, chat_id, format_warn_message("ارسال تبلیغات در گروه مجاز نیست", count, auto_mod['warn_limit'], tag=_tag), sender_id=sender_id)
                        WARN_MESSAGE_CACHE[warn_key] = True
                        if warn_msg:
                            asyncio.create_task(auto_delete_warning(client, chat_id, warn_msg.id, delay=get_warning_delete_delay(chat_id, sender_id)))
                    
                    if count >= auto_mod["warn_limit"]:
                        db.ban_user(chat_id, sender_id, f"تبلیغات بیش از حد ({count} اخطار)")
                        try:
                            await client.edit_permissions(chat_id, sender_id, send_messages=False)
                            _tag = await build_view_tag(sender_id, chat_id)
                            ban_msg = await send_moderation_message(event, chat_id, format_ban_message("ارسال تبلیغات", tag=_tag), sender_id=sender_id)
                            if ban_msg:
                                asyncio.create_task(auto_delete_warning(client, chat_id, ban_msg.id))
                            asyncio.create_task(clear_user_messages(client, chat_id, sender_id, 100))
                        except:
                            pass
                except:
                    pass
                return

    # ==================== قفل فحش ====================
    # برخلاف بقیه‌ی Auto Mod (اسپم/تبلیغ) که «معاف» و «ویژه» ازشون آزادن، قفل فحش روی
    # عضو عادی، «معاف» و «ویژه» هم اعمال می‌شه (پیام پاک + اخطار/بن). فقط «ادمین» و
    # بالاتر (ادمین، مالک گروه، سازنده، مدیر ربات) آزادن. is_only_special_tier یعنی «ویژه‌ی
    # محض»؛ پس (is_admin and not is_only_special_tier) یعنی ادمین یا بالاتر.
    _badword_immune = bool(is_admin and not is_only_special_tier)
    if (not _badword_immune) and sender_id != BOT_ID:
        auto_mod = db.get_auto_mod_settings(chat_id)
        # تشخیص فحاشی - یک پاس رجکسِ از پیش کامپایل‌شده به‌جای حلقه زدن روی
        # تک‌تک کلمات (سریع‌تر، حتی زیر بار زیاد/گروه‌های پرترافیک).
        if auto_mod["badword_enabled"] and text:
            bad = db.find_bad_word(lower_text)
            if bad:
                try:
                    await event.delete()

                    if auto_mod.get("silent_mode"):
                        db.ban_user(chat_id, sender_id, f"سایلنت: فحش ({bad})")
                        try:
                            await client.edit_permissions(chat_id, sender_id, send_messages=False)
                            _tag = await build_view_tag(sender_id, chat_id)
                            ban_msg = await send_moderation_message(event, chat_id, format_ban_message("استفاده از الفاظ نامناسب", tag=_tag), sender_id=sender_id)
                            if ban_msg:
                                asyncio.create_task(auto_delete_warning(client, chat_id, ban_msg.id))
                            asyncio.create_task(clear_user_messages(client, chat_id, sender_id, 100))
                        except:
                            pass
                        return

                    count = db.add_user_warning(chat_id, sender_id, f"فحش: {bad}")
                        
                    warn_key = f"badword_{sender_id}_{int(time.time()/60)}"
                    if warn_key not in WARN_MESSAGE_CACHE:
                        reason_text = f"استفاده از الفاظ نامناسب در گروه مجاز نیست (کلمه: `{bad}`)"
                        _tag = await build_view_tag(sender_id, chat_id)
                        warn_msg = await send_moderation_message(event, chat_id, format_warn_message(reason_text, count, auto_mod['warn_limit'], tag=_tag), sender_id=sender_id)
                        WARN_MESSAGE_CACHE[warn_key] = True
                        if warn_msg:
                            asyncio.create_task(auto_delete_warning(client, chat_id, warn_msg.id, delay=get_warning_delete_delay(chat_id, sender_id)))
                        
                    if count >= auto_mod["warn_limit"]:
                        db.ban_user(chat_id, sender_id, f"فحش بیش از حد ({count} اخطار)")
                        try:
                            await client.edit_permissions(chat_id, sender_id, send_messages=False)
                            _tag = await build_view_tag(sender_id, chat_id)
                            ban_msg = await send_moderation_message(event, chat_id, format_ban_message("استفاده از الفاظ نامناسب", tag=_tag), sender_id=sender_id)
                            if ban_msg:
                                asyncio.create_task(auto_delete_warning(client, chat_id, ban_msg.id))
                            asyncio.create_task(clear_user_messages(client, chat_id, sender_id, 100))
                        except:
                            pass
                except:
                    pass
                return

    # --- محتوا/قفل‌های نوع پیام ---
    if not is_filter_free:
        # SPlusthon بر پایه API مشابه Telethon است و فیلدهای رسانه‌ای را روی event/message
        # در اختیار می‌گذارد؛ برای نسخه‌های مختلف کتابخانه چند نام جایگزین هم بررسی می‌کنیم.
        media_lock_type = None
        media_lock_label = None
        if getattr(event, 'photo', None):
            media_lock_type, media_lock_label = "photo", "عکس"
        elif getattr(event, 'video', None):
            media_lock_type, media_lock_label = "video", "فیلم"
        elif getattr(event, 'voice', None) or getattr(event, 'audio', None):
            media_lock_type, media_lock_label = "voice", "ویس"
        elif getattr(event, 'gif', None):
            media_lock_type, media_lock_label = "gif", "گیف"
        elif getattr(event, 'sticker', None):
            media_lock_type, media_lock_label = "sticker", "استیکر"
        elif getattr(event, 'document', None) or getattr(event, 'file', None):
            media_lock_type, media_lock_label = "file", "فایل"

        if media_lock_type and db.is_locked(chat_id, media_lock_type):
            await handle_lock_violation(event, chat_id, sender_id, media_lock_label, lock_type=media_lock_type)
            return

        # متن
        if db.is_locked(chat_id, "text") and text:
            await handle_lock_violation(event, chat_id, sender_id, "متن", lock_type="text")
            return

        # اموجی: هم پیام کاملاً اموجی و هم متنی که داخلش اموجی دارد.
        if db.is_locked(chat_id, "emoji") and text and contains_emoji(text):
            await handle_lock_violation(event, chat_id, sender_id, "اموجی", lock_type="emoji")
            return

        # آیدی/یوزرنیم
        if db.is_locked(chat_id, "id") and text and re.search(r'@\w{4,}', text):
            await handle_lock_violation(event, chat_id, sender_id, "ایدی", lock_type="id")
            return

        # لینک (هم لینک‌های t.me/splus.ir و هم http/https عمومی)
        if db.is_locked(chat_id, "link") and text and (
            LINK_PATTERN.search(text)
            or "t.me/" in text.lower()
            or "splus.ir/" in text.lower()
            or "http://" in text.lower()
            or "https://" in text.lower()
            or "://" in text.lower()
        ):
            await handle_lock_violation(event, chat_id, sender_id, "لینک", lock_type="link")
            return

        # فوروارد / ریپلای
        # نکته: عمداً فقط fwd_from چک می‌شه، نه event.forward. کتابخانه‌ی spluspy
        # روی کلاس Message هم یک @property به اسم forward داره (اطلاعات فوروارد،
        # None اگه فوروارد نباشه) و هم - پایین‌تر توی همون کلاس - یک متد async
        # دیگه دقیقاً با همون اسم forward (برای فوروارد کردن پیام به یک چت دیگه).
        # چون متد دوم پایین‌تر تعریف شده، تعریف property رو silent override
        # می‌کنه؛ در نتیجه event.forward همیشه یک bound method (همیشه truthy) هست
        # و هیچ‌وقت None نمی‌شه - چه پیام واقعاً فوروارد باشه چه نباشه. همین باعث
        # می‌شد با فعال بودن قفل فوروارد، هر پیام معمولی هم فوروارد تشخیص داده
        # بشه. fwd_from یک اتریبیوت ساده‌ی داده‌ایه و از این تداخل اسم بی‌تأثیره،
        # پس تنها منبع قابل‌اعتماد برای تشخیص فوروارد واقعی همینه.
        if db.is_locked(chat_id, "forward") and getattr(event, 'fwd_from', None):
            await handle_lock_violation(event, chat_id, sender_id, "فوروارد", lock_type="forward")
            return

        if db.is_locked(chat_id, "reply") and (getattr(event, 'reply_to_msg_id', None) or getattr(event, 'reply_to', None)):
            await handle_lock_violation(event, chat_id, sender_id, "ریپلای", lock_type="reply")
            return

        # کد هنگی
        if db.is_locked(chat_id, "hangcode") and text and is_hang_code(text):
            await handle_lock_violation(event, chat_id, sender_id, "کد هنگی", lock_type="hangcode")
            return

        # لوکیشن / نظرسنجی / پست / استوری / لایو
        special_types = [
            ("location", "لوکیشن", ("location", "geo", "venue")),
            ("poll", "نظرسنجی", ("poll", "poll_results")),
            ("post", "پست", ("post", "channel_post")),
            ("story", "استوری", ("story", "story_item")),
            ("live", "لایو", ("live", "live_location", "live_stream")),
        ]
        for lock_type, label, attrs in special_types:
            if db.is_locked(chat_id, lock_type) and any(getattr(event, attr, None) for attr in attrs):
                await handle_lock_violation(event, chat_id, sender_id, label, lock_type=lock_type)
                return

        # متن انگلیسی
        if db.is_locked(chat_id, "english") and text and re.search(r'[A-Za-z]', text):
            await handle_lock_violation(event, chat_id, sender_id, "انگلیسی", lock_type="english")
            return

        # فحش/متن نامناسب و اسپم توسط Auto Mod کنترل می‌شوند؛ وضعیتشان در «لیست قفل»
        # همان وضعیت Auto Mod است تا دو منبع تنظیم متناقض ایجاد نشود.
    # ==================== غیرفعال‌سازی امنیتی: مترجم ====================
    # این دستور یک متن دلخواه از هر کاربر (بدون نیاز به ادمین بودن) می‌گرفت و
    # عیناً همون متن رو زیر عنوان «متن اصلی» توی پیام ربات تکرار می‌کرد؛ برای
    # کلمه‌هایی که توی دیکشنری کوچیک ترجمه هم نبودن، خودِ «ترجمه» هم چیزی جز
    # همون متن ورودی نبود. یعنی هر کاربری می‌تونست هر متنی (از جمله متن سیاسی)
    # رو وادار کنه ربات عیناً همون رو به عنوان پیامِ خودش بفرسته. به همین دلیل
    # کل دستور غیرفعال شده.
    if lower_text.startswith("ترجمه ") or lower_text.startswith("translate "):
        try:
            await event.reply("ℹ️ قابلیت مترجم به دلایل امنیتی (جلوگیری از تکرار متن دلخواه کاربر توسط ربات) غیرفعال شده است.")
        except:
            pass
        return

    # ==================== تغییر حالت سخنگو (مخصوص همین گروه) ====================
    # فقط ادمین/مالک/سازنده می‌تونه حالت سخنگو رو عوض کنه؛ کاربر عادی نه دستور
    # رو اجرا می‌کنه و نه پیامی می‌گیره (ساکت نادیده گرفته می‌شه).
    _SPEAKER_MODE_COMMANDS = (
        ["حالت مؤدب", "حالت مودب", "مودب", "مؤدب"]
        + ["حالت بی ادب", "حالت بی‌ادب", "بی ادب", "بی‌ادب", "خودمونی"]
        + ["حالت کم حرف", "حالت کم‌حرف", "کم حرف", "کم‌حرف"]
        + ["حالت پر حرف", "حالت پرحرف", "حالت پر‌حرف", "پر حرف", "پرحرف", "پر‌حرف"]
        + ["حالت خاموش", "خاموش"]
    )
    if lower_text in _SPEAKER_MODE_COMMANDS and (not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier):
        return

    if lower_text in ["حالت مؤدب", "حالت مودب", "مودب", "مؤدب"]:
        db.set_speaker_mode(chat_id, "polite")
        try:
            await event.reply("🌹 **حالت سخنگو به مؤدب تغییر کرد!**")
        except:
            pass
        return

    if lower_text in ["حالت بی ادب", "حالت بی‌ادب", "بی ادب", "بی‌ادب", "خودمونی"]:
        db.set_speaker_mode(chat_id, "rude")
        try:
            await event.reply("😈 **حالت سخنگو به بی‌ادب تغییر کرد!**")
        except:
            pass
        return

    if lower_text in ["حالت کم حرف", "حالت کم‌حرف", "کم حرف", "کم‌حرف"]:
        db.set_speaker_mode(chat_id, "short")
        try:
            await event.reply("🤫 **حالت سخنگو به کم‌حرف تغییر کرد!**\nربات فقط وقتی به یک پیام ریپلای بشه پاسخ می‌ده.")
        except:
            pass
        return

    if lower_text in ["حالت پر حرف", "حالت پرحرف", "حالت پر‌حرف", "پر حرف", "پرحرف", "پر‌حرف"]:
        db.set_speaker_mode(chat_id, "long")
        try:
            await event.reply("🗣️ **حالت سخنگو به پرحرف تغییر کرد!**\nربات به پیام‌های عادی هم پاسخ می‌ده.")
        except:
            pass
        return

    if lower_text in ["حالت خاموش", "خاموش"]:
        db.set_speaker_mode(chat_id, "off")
        try:
            await event.reply("🔇 **حالت سخنگو خاموش شد!**")
        except:
            pass
        return

    if lower_text in ["حالت کنونی", "حالت فعلی"]:
        mode_map = {
            "polite": "🌹 مؤدب",
            "rude": "😈 بی‌ادب",
            "short": "🤫 کم‌حرف",
            "long": "🗣️ پرحرف",
            "off": "🔇 خاموش"
        }
        mode_text = mode_map.get(SPEAKER_MODE, "🔇 خاموش")
        try:
            await event.reply(f"📌 **حالت فعلی سخنگو:** {mode_text}")
        except:
            pass
        return

    # ==================== غیرفعال‌سازی امنیتی: تکرار/گفتنِ متن دلخواه کاربر ====================
    # همه‌ی دستورهایی که اجازه می‌دادن یک کاربر (حتی ادمین/مالک/سازنده) متن دلخواه
    # خودش رو به ربات بده و ربات همون رو عیناً به عنوان پیامِ خودش بفرسته یا برای
    # همیشه یاد بگیره و بعداً تکرار کنه («بگو»، «یاد بده»، «حذف از سخنگو»،
    # «لیست سخنگو»، «پاکسازی سخنگو») کاملاً غیرفعال شده‌اند. صرفِ فیلتر کردن کلمات
    # غیرمجاز کافی نبود، چون بقیه‌ی متنِ دلخواه کاربر همچنان تکرار می‌شد؛ به همین
    # خاطر کل مسیر «کاربر یک متن می‌ده و ربات عیناً همون رو می‌گه» حذف شده، نه فقط
    # بخش فیلترشده‌اش. از این به بعد ربات دیگر پاسخ ثابت محلی ندارد و پاسخ سخنگو فقط از API دریافت می‌شود.
    #
    # if lower_text.startswith("بگو "):
    #     parts = text.split(maxsplit=1)
    #     if len(parts) > 1:
    #         hit = find_filtered_word(parts[1])
    #         if hit:
    #             try:
    #                 await event.reply("🚫 این متن حاوی کلمه‌ی غیرمجاز است و طبق قوانین تکرار نمی‌شود.")
    #             except:
    #                 pass
    #             return
    #         try:
    #             await event.reply(parts[1])
    #         except:
    #             pass
    #     return
    #
    # if lower_text.startswith("یاد بده "):
    #     if not (is_super_owner or is_group_owner or is_admin):
    #         return
    #     try:
    #         parts = text.split("=", 1)
    #         if len(parts) == 2:
    #             word = parts[0].replace("یاد بده", "").strip().lower()
    #             response = parts[1].strip()
    #             if word and response:
    #                 hit = find_filtered_word(word) or find_filtered_word(response)
    #                 if hit:
    #                     try:
    #                         await event.reply("🚫 این مورد به سخنگو یاد داده نمی‌شود چون شامل یک کلمه‌ی غیرمجاز است.")
    #                     except:
    #                         pass
    #                     return
    #                 db.add_speaker_response(chat_id, word, response)
    #                 try:
    #                     await event.reply(f"✅ کلمه `{word}` به سخنگو اضافه شد!")
    #                 except:
    #                     pass
    #     except:
    #         pass
    #     return
    #
    # if lower_text.startswith("حذف از سخنگو "):
    #     if not (is_super_owner or is_group_owner or is_admin):
    #         return
    #     word = text.replace("حذف از سخنگو", "").strip().lower()
    #     if word in db.get_speaker_data(chat_id):
    #         db.remove_speaker_response(chat_id, word)
    #         try:
    #             display_word = mask_filtered_words(word) if find_filtered_word(word) else word
    #             await event.reply(f"✅ کلمه `{display_word}` از سخنگو حذف شد.")
    #         except:
    #             pass
    #     return
    #
    # if lower_text == "لیست سخنگو":
    #     speaker_data = db.get_speaker_data(chat_id)
    #     if speaker_data:
    #         msg = "📋 **لیست کلمات سخنگو (این گروه):**\n"
    #         for word, response in list(speaker_data.items())[:20]:
    #             msg += f"• `{word}` → {response}\n"
    #         try:
    #             await event.reply(msg)
    #         except:
    #             pass
    #     return
    #
    # if lower_text == "پاکسازی سخنگو" or lower_text == "حذف سخنگو":
    #     if not (is_super_owner or is_group_owner or is_admin):
    #         return
    #     db.clear_speaker_data(chat_id)
    #     try:
    #         await event.reply("🗑️ تمام کلمات سخنگوی این گروه پاک شد!")
    #     except:
    #         pass
    #     return

    if db.is_banned(chat_id, sender_id):
        if not is_filter_free:
            try:
                await event.delete()
            except:
                pass
            return

    if db.is_muted(chat_id, sender_id):
        if not is_filter_free:
            try:
                await event.delete()
            except:
                pass
            return

    # محروم کامل (سکوت سراسری): این با سکوت معمولی گروه فرق دارد.
    # سکوت معمولی، ادمین/مالک همون گروه رو مستثنی می‌کنه، ولی محروم کامل
    # توسط سازنده‌ی اصلی ثبت شده و باید در همه‌ی گروه‌ها اعمال بشه، حتی اگه
    # همون کاربر در این گروه خاص ادمین یا مالک باشه. فقط خود سازنده اصلی مستثناست.
    if not is_super_owner and db.is_globally_muted(sender_id):
        try:
            await event.delete()
        except:
            pass
        return

    # نکته مهم: فیلتر کلمات فقط باید روی پیام کاربران عادی اعمال شود.
    # قبلاً حتی برای مالک/ادمین هم اگر پیامشان شامل کلمه‌ی فیلترشده بود، این بخش
    # با "return" کل ادامه‌ی پردازش پیام (از جمله دستورهای مدیریتی مثل
    # "حذف فیلتر ...") را متوقف می‌کرد؛ یعنی اگر خود کلمه‌ی فیلترشده در متن دستور
    # "حذف فیلتر" وجود داشت، دستور اصلاً به هندلر مربوطه نمی‌رسید.
    # با قرار دادن کل این بلوک زیر شرط "کاربر عادی بودن"، برای مالک/ادمین اصلاً
    # وارد این منطق نمی‌شویم و پردازش پیام آن‌ها بدون وقفه ادامه پیدا می‌کند.
    if not is_filter_free:
        filters = db.get_word_filters(chat_id)
        for word, action in filters.items():
            if word in lower_text:
                cache_key = f"{sender_id}_{word}"
                if cache_key in FILTER_WARN_CACHE:
                    try:
                        await event.delete()
                    except:
                        pass
                    return
                try:
                    await event.delete()

                    warn_key = f"filter_{sender_id}_{word}_{int(time.time()/60)}"
                    if warn_key not in FILTER_WARN_CACHE:
                        if action == "ban":
                            db.ban_user(chat_id, sender_id, f"فیلتر کلمه: {word}")
                            try:
                                await client.edit_permissions(chat_id, sender_id, send_messages=False)
                                asyncio.create_task(clear_user_messages(client, chat_id, sender_id, 100))
                            except:
                                pass
                        elif action == "mute":
                            db.mute_user(chat_id, sender_id, "permanent", f"فیلتر کلمه: {word}")
                            try:
                                await client.edit_permissions(chat_id, sender_id, send_messages=False)
                            except:
                                pass

                        # کلمه فیلترشده کامل نمایش داده می‌شود (بدون سانسور)
                        warn_msg = await event.respond(f"⛔ ارسال کلمه ممنوعه غیرمجاز است! (کلمه: `{word}`)")
                        FILTER_WARN_CACHE[cache_key] = True
                        FILTER_WARN_CACHE[warn_key] = True
                        asyncio.create_task(auto_delete_warning(client, chat_id, warn_msg.id, delay=get_warning_delete_delay(chat_id, sender_id)))
                except:
                    pass
                return

    if db.is_locked(chat_id, "link") and not is_filter_free:
        if "http" in lower_text or "t.me/" in lower_text or "splus.ir/" in lower_text or "://" in lower_text:
            link_count = 0
            urls = re.findall(r'(https?://[^\s]+|t\.me/[^\s]+|splus\.ir/[^\s]+)', lower_text)
            link_count = len(urls)
            if link_count > 0:
                try:
                    await event.delete()
                except:
                    pass
                _ltag = await build_view_tag(sender_id, chat_id)

                # عمل «بن»/«سکوت» صریح روی همین قفل (با «قفل لینک بن» یا «قفل لینک
                # سکوت» تنظیم شده): بدون هیچ شمارش/اخطاری، همون لینک اول مستقیم اجرا می‌شه.
                lock_action = db.get_lock_action(chat_id, "link")
                if lock_action == "ban":
                    try:
                        await client.edit_permissions(chat_id, sender_id, send_messages=False)
                        db.ban_user(chat_id, sender_id, "ارسال لینک (عمل: بن)")
                        LINK_COUNT_CACHE.pop(sender_id, None)
                        ban_msg = await send_moderation_message(event, chat_id, format_ban_message("ارسال لینک", tag=_ltag), sender_id=sender_id)
                        if ban_msg:
                            asyncio.create_task(auto_delete_warning(client, chat_id, ban_msg.id))
                        asyncio.create_task(clear_user_messages(client, chat_id, sender_id, 100))
                    except Exception as e:
                        await event.reply(f"⛔ کاربر لینک ارسال کرد و دسترسی چت بسته شد (خطا: {e})")
                    return
                if lock_action == "mute":
                    try:
                        db.mute_user(chat_id, sender_id, "permanent", "ارسال لینک (عمل: سکوت)")
                        await client.edit_permissions(chat_id, sender_id, send_messages=False)
                        LINK_COUNT_CACHE.pop(sender_id, None)
                        mute_msg = await send_moderation_message(event, chat_id, format_mute_message("ارسال لینک", tag=_ltag), sender_id=sender_id)
                        if mute_msg:
                            asyncio.create_task(auto_delete_warning(client, chat_id, mute_msg.id))
                    except Exception as e:
                        await event.reply(_user_action_message("mute", f"ارسال لینک (خطا: {e})"))
                    return

                # حالت سایلنت: این قفل (لینک) قبلاً کاملاً جدا از تنظیمات auto_mod
                # (silent_mode) عمل می‌کرد و همیشه اول اخطار می‌داد و فقط بعد از
                # چند بار بن می‌کرد - یعنی وقتی سایلنت روشن بود هم بی‌تفاوت به اون
                # تنظیم، طبق رفتار قدیمی خودش (اخطار پشت اخطار) پیش می‌رفت. حالا
                # این بخش هم دقیقاً مثل بقیه‌ی قفل‌ها به silent_mode احترام می‌ذاره:
                # همون لینک اول، بدون هیچ پیام اخطاری، مستقیم بن می‌کنه.
                auto_mod = db.get_auto_mod_settings(chat_id)
                if auto_mod.get("silent_mode"):
                    try:
                        await client.edit_permissions(chat_id, sender_id, send_messages=False)
                        db.ban_user(chat_id, sender_id, "سایلنت: ارسال لینک")
                        LINK_COUNT_CACHE.pop(sender_id, None)
                        ban_msg = await send_moderation_message(event, chat_id, format_ban_message("ارسال لینک", tag=_ltag), sender_id=sender_id)
                        if ban_msg:
                            asyncio.create_task(auto_delete_warning(client, chat_id, ban_msg.id))
                        asyncio.create_task(clear_user_messages(client, chat_id, sender_id, 100))
                    except Exception as e:
                        await event.reply(f"⛔ کاربر لینک ارسال کرد و دسترسی چت بسته شد (خطا: {e})")
                    return

                if sender_id not in LINK_COUNT_CACHE:
                    LINK_COUNT_CACHE[sender_id] = 0
                LINK_COUNT_CACHE[sender_id] += link_count
                link_limit = int(db.get_setting("link_limit", "3"))

                if LINK_COUNT_CACHE[sender_id] >= link_limit:
                    try:
                        await client.edit_permissions(chat_id, sender_id, send_messages=False)
                        db.ban_user(chat_id, sender_id, f"ارسال {link_limit} لینک")
                        LINK_COUNT_CACHE.pop(sender_id, None)
                        
                        warn_key = f"link_ban_{sender_id}_{int(time.time()/60)}"
                        if warn_key not in WARN_MESSAGE_CACHE:
                            ban_msg = await send_moderation_message(event, chat_id, format_ban_message(f"ارسال {link_limit} لینک", tag=_ltag), sender_id=sender_id)
                            WARN_MESSAGE_CACHE[warn_key] = True
                            if ban_msg:
                                asyncio.create_task(auto_delete_warning(client, chat_id, ban_msg.id))
                            asyncio.create_task(clear_user_messages(client, chat_id, sender_id, 100))
                    except Exception as e:
                        await event.reply(f"⛔ کاربر لینک ارسال کرد و دسترسی چت بسته شد (خطا: {e})")
                    return
                
                remaining = link_limit - LINK_COUNT_CACHE[sender_id]
                if remaining > 0:
                    warn_key = f"link_warn_{sender_id}_{int(time.time()/60)}"
                    if warn_key not in WARN_MESSAGE_CACHE:
                        try:
                            warn_text = format_warn_message(
                                "ارسال لینک در این گروه قفل است",
                                LINK_COUNT_CACHE[sender_id],
                                link_limit,
                                tag=_ltag,
                            )
                            warn_msg = await send_moderation_message(event, chat_id, warn_text, sender_id=sender_id)
                            WARN_MESSAGE_CACHE[warn_key] = True
                            if warn_msg:
                                asyncio.create_task(auto_delete_warning(client, chat_id, warn_msg.id, delay=get_warning_delete_delay(chat_id, sender_id)))
                            safe_print(f"✅ اخطار لینک ارسال شد ({LINK_COUNT_CACHE[sender_id]}/{link_limit}) chat={chat_id} user={sender_id}")
                        except Exception as e:
                            safe_print(f"❌ خطا در ارسال پیام اخطار لینک: {e}")
                    return
                else:
                    return

    # ==================== واژه‌کلیدهای ثابت (حتی با سخنگو خاموش) ====================
    # اسم ربات (با شکل‌های مختلف حروف) اول جمله / تنها / با خطاب → جانم
    _kw_raw = (event.text or "").strip()
    _kw_norm = _normalize_fa_name(_kw_raw)
    try:
        _bot_nm = (db.get_bot_name(chat_id) or "").strip()
    except Exception:
        _bot_nm = ""
    _builtin_kw = {
        _normalize_fa_name("سافت"): "بله",
        _normalize_fa_name("ربات اومد"): "جانم",
        _normalize_fa_name("ربات اومده"): "جانم",
        _normalize_fa_name("ربات"): "جانم",
    }
    _kw_reply = None
    # اگر پیام دستور مدیریتی با اسم ربات است (مثل «آنیتا بن») اینجا جواب جانم نده
    _stripped_cmd, _had_name = _strip_bot_name_prefix(_kw_raw, _bot_nm) if _bot_nm else (_kw_raw, False)
    if _bot_nm and _message_addresses_bot(_kw_raw, _bot_nm) and not (_had_name and _stripped_cmd):
        _kw_reply = "جانم"
    elif _kw_norm in _builtin_kw:
        _kw_reply = _builtin_kw[_kw_norm]
    if _kw_reply:
        try:
            await event.reply(_kw_reply)
        except Exception:
            try:
                await client.send_message(chat_id, _kw_reply)
            except Exception:
                pass
        return

    # API سخنگو خارج از مسیر بحرانی مدیریت اجرا می‌شود.

    # در حالت «کم‌حرف» فقط وقتی پاسخ بده که پیام فعلی مستقیماً روی
    # یکی از پیام‌های خودِ ربات ریپلای شده باشد؛ ریپلای یک کاربر روی
    # پیام کاربر دیگر نباید باعث پاسخ سخنگو شود.
    is_reply_to_bot = False
    if SPEAKER_MODE == "short":
        try:
            reply_msg = await event.get_reply_message()
            if reply_msg is not None:
                reply_sender_id = getattr(reply_msg, "sender_id", None)
                is_reply_to_bot = (
                    BOT_ID is not None and reply_sender_id == BOT_ID
                )
        except Exception:
            is_reply_to_bot = False

    speaker_should_reply = (
        SPEAKER_MODE == "long"
        or (SPEAKER_MODE == "short" and is_reply_to_bot)
        or SPEAKER_MODE in ("polite", "rude")
    )
    looks_like_command = lower_text.startswith(("بن ", "سکوت ", "قفل ", "پاک ", "باز ", "تنظیم ", "حذف ", "لیست ", "راهنما", "آموزش", "دوز", "فعال", "روشن", "لفت", "ارسال", "وضعیت", "فونت ", "فونتها ", "فونت‌ها ")) or lower_text in ("کال", "کال گروه", "شروع کال", "ساخت کال", "تماس", "قطع کال", "قطع‌کال", "خروج از کال", "پایان کال", "بستن کال")
    if SPEAKER_MODE != "off" and speaker_should_reply and not looks_like_command and text:
        async def _answer_speaker(ev=event, txt=text, mode=SPEAKER_MODE):
            async with _SPEAKER_SEMAPHORE:
                try:
                    api_mode = mode if mode in ("polite", "rude") else None
                    answer = await api_speaker(txt, mode=api_mode, restric=(api_mode == "rude"))
                    if answer: await ev.reply(answer)
                except Exception:
                    pass
        _track_background_task(_answer_speaker())

    if lower_text in ["فعال", "/start", "start", "روشن"]:
        # ==================== دسترسی «فعال»: فقط ادمین‌های واقعیِ گروه ====================
        # کاربر عادی حق نداره «فعال»/«روشن» بزنه؛ فقط ادمین/مالک/سازنده (is_admin
        # از قبل شاملِ ادمینِ واقعیِ سروش هم می‌شه، چون check_user_permission
        # قبل از اینجا صدا زده شده و is_group_admin رو پر کرده).
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return

        owner_id = db.get_group_owner(chat_id)
        
        if db.is_bot_enabled():
            if owner_id:
                # قبلاً این حالت (گروهی که از قبل فعال بوده) فقط همین پیامِ وضعیت رو
                # می‌فرستاد و هیچ‌وقت شناسایی/تگ/ثبتِ ادمین‌ویژه اجرا نمی‌شد - چون
                # announce_and_register_group_admins فقط توی مسیرهای «فعال‌سازیِ اولِ
                # گروه» صدا زده می‌شد، نه اینجا. با این تغییر، هر بار که کسی توی یک
                # گروهِ از‌قبل‌فعال دوباره «فعال» بنویسه، همین اسکن/تگ/ثبتِ ادمین‌ویژه
                # هم (دوباره) اجرا می‌شه - هم برای گروه‌های قدیمی که قبل از اضافه‌شدنِ
                # این قابلیت فعال شده بودن، هم برای همگام‌سازیِ ادمین‌های تازه‌اضافه‌شده.
                asyncio.create_task(announce_and_register_group_admins(chat_id))
                try:
                    owner_username = await get_username(client, owner_id)
                    owner_view = view_tag(owner_id, owner_username)
                    await event.reply(f"""✅ **ربات فعال است**

👑 **مالک گروه:** {owner_view}

🔹 فقط مالک گروه میتواند ربات را مدیریت کند.
🔹 برای مشاهده دستورات، `راهنما` را بنویسید.
📌 {CHANNEL_LINK}
""")
                except:
                    pass
            else:
                # مالک هنوز ثبت نشده: اول سعی می‌کنیم سازنده‌ی واقعیِ گروه رو
                # شناسایی کنیم؛ اگه نشد، همون ادمینی که «فعال» رو زده مالک می‌شه.
                real_creator_id = await _get_real_group_creator(chat_id)
                owner_id_to_set = real_creator_id if real_creator_id else sender_id
                result = db.set_group_owner(chat_id, owner_id_to_set)
                if result:
                    db.add_group_admin(chat_id, owner_id_to_set)
                    GROUP_OWNER_CACHE[chat_id] = owner_id_to_set
                    await _record_group_activation_safe(chat_id)
                    # تنظیمات کارخانه هنگام اولین «فعال»:
                    # اسپم = اخطار، لینک = اخطار، کد هنگی = بن.
                    try:
                        db.set_lock(chat_id, "link", True, action="warn")
                        db.set_lock(chat_id, "spam", True, action="warn")
                        db.set_lock(chat_id, "hangcode", True, action="ban")
                        db.set_auto_mod_settings(chat_id, spam_enabled=True, silent_mode=False)
                    except Exception as e:
                        safe_print(f"⚠️ اعمال تنظیمات پیش‌فرض قفل‌ها ناموفق بود: {e}")
                    admin_msg = await announce_and_register_group_admins(chat_id, send_message=False)
                    owner_username = await get_username(client, owner_id_to_set)
                    owner_view = view_tag(owner_id_to_set, owner_username)
                    owner_msg = f"""[✅](https://web.splus.ir/img-apple-64/2705.png) **مالک گروه ثبت شد**

[👑](https://web.splus.ir/img-apple-64/1f451.png) **مالک گروه:** {owner_view}"""
                    if isinstance(admin_msg, str) and admin_msg:
                        owner_msg += "\n\n" + admin_msg.replace("🛡 **ادمین‌های این گروه شناسایی و تگ شدند:**", "[🛡](https://web.splus.ir/img-apple-64/1f6e1.png) **ادمین‌های این گروه شناسایی و تگ شدند:**").replace("👤 ", "[👤](https://web.splus.ir/img-apple-64/1f464.png) ").replace("\n\n✅ همه‌شون", "\n\n[✅](https://web.splus.ir/img-apple-64/2705.png) همه‌شون")
                    try:
                        await event.reply(owner_msg)
                    except:
                        pass
            return
        
        if owner_id:
            if sender_id == owner_id or sender_id in SUPER_OWNER_IDS:
                db.set_bot_enabled(True)
                GROUP_OWNER_CACHE[chat_id] = owner_id
                asyncio.create_task(announce_and_register_group_admins(chat_id))
                try:
                    owner_username = await get_username(client, owner_id)
                    owner_view = view_tag(owner_id, owner_username)
                    await event.reply(f"""✅ **ربات فعال شد!** 🎉

👑 مالک گروه: {owner_view}
📌 {CHANNEL_LINK}
""")
                except:
                    pass
                return
            else:
                try:
                    owner_username = await get_username(client, owner_id)
                    owner_view = view_tag(owner_id, owner_username)
                    await event.reply(f"""⛔ **ربات غیرفعال است!**

👑 **مالک گروه:** {owner_view}

🔹 فقط مالک گروه میتواند ربات را فعال کند.
""")
                except:
                    pass
                return
        
        real_creator_id = await _get_real_group_creator(chat_id)
        owner_id_to_set = real_creator_id if real_creator_id else sender_id
        result = db.set_group_owner(chat_id, owner_id_to_set)
        
        if result:
            db.add_group_admin(chat_id, owner_id_to_set)
            db.set_bot_enabled(True)
            GROUP_OWNER_CACHE[chat_id] = owner_id_to_set
            await _record_group_activation_safe(chat_id)
            asyncio.create_task(announce_and_register_group_admins(chat_id))
            try:
                db.set_auto_mod_settings(chat_id, spam_enabled=True)
            except:
                pass
            
            try:
                owner_username = await get_username(client, owner_id_to_set)
                owner_view = view_tag(owner_id_to_set, owner_username)
                await event.reply(f"""✅ **ربات با موفقیت فعال شد!** 🎉

👑 **مالک گروه ثبت شد**
مالک گروه: {owner_view}

📋 **دستورات مدیریتی در اختیار مالک و ادمین‌هاست:**
• `راهنما` - مشاهده همه دستورات
• `بن` - بن کردن کاربران
• `سکوت` - سکوت کردن کاربران
• `قفل` - قفل کردن امکانات گروه
• `پاک` - پاکسازی پیام‌ها
• `حالت مؤدب` - حالت رسمی
• `حالت بی ادب` - حالت خودمونی
• `حالت خاموش` - غیرفعال کردن سخنگو
• `حالت کم حرف` - فقط روی ریپلای جواب می‌دهد
• `حالت پر حرف` - روی پیام‌های عادی هم جواب می‌دهد
• `دوز` - بازی دوز

📌 **کانال اصلی:** {CHANNEL_LINK}
""")
            except:
                pass
            
            try:
                owner_username = await get_username(client, owner_id_to_set)
                owner_view = view_tag(owner_id_to_set, owner_username)
                await client.send_message(chat_id, f"""👑 **ربات فعال شد!**

مالک گروه: {owner_view}

⚠️ **فقط مالک گروه است و هیچکس دیگری نمیتواند مالک شود!**
📌 {CHANNEL_LINK}
""")
            except:
                pass
        else:
            try:
                await event.reply("❌ خطا در ثبت مالک! لطفاً دوباره تلاش کنید.")
            except:
                pass
        
        return

    if lower_text in ["غیرفعال", "/stop", "stop", "خاموش"]:
        if is_super_owner or is_group_owner:
            db.set_bot_enabled(False)
            try:
                await event.reply("⛔ ربات غیرفعال شد.")
            except:
                pass
        return

    if lower_text.startswith("تغییر مالک "):
        if sender_id not in SUPER_OWNER_IDS:
            return
        
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            try:
                await event.reply("📌 **نحوه استفاده:**\n`تغییر مالک [آیدی کاربر یا یوزرنیم]`\n\nمثال: `تغییر مالک 123456789` یا `تغییر مالک @username`")
            except:
                pass
            return
        
        owner_token = parts[1].strip().split()[0] if parts[1].strip() else ""
        new_owner_id = await resolve_id_or_username(client, owner_token)
        if not new_owner_id:
            try:
                await event.reply("❌ آیدی یا یوزرنیم کاربر معتبر نیست!\nمثال: `تغییر مالک 123456789` یا `تغییر مالک @username`")
            except:
                pass
            return
        
        try:
            user = await client.get_entity(new_owner_id)
        except:
            try:
                await event.reply(f"❌ کاربر با آیدی `{new_owner_id}` پیدا نشد!")
            except:
                pass
            return
        
        old_owner_id = db.get_group_owner(chat_id)
        old_owner_name = await get_user_name(client, old_owner_id, chat_id) if old_owner_id else "ندارد"
        
        db.remove_group_owner(chat_id)
        if old_owner_id:
            db.remove_group_admin(chat_id, old_owner_id)
        result = db.set_group_owner(chat_id, new_owner_id)
        
        if result:
            db.add_group_admin(chat_id, new_owner_id)
            GROUP_OWNER_CACHE[chat_id] = new_owner_id
            new_owner_name = await get_user_name(client, new_owner_id, chat_id)
            
            try:
                await event.reply(f"""✅ **مالک گروه با موفقیت تغییر کرد!**

👑 **مالک قبلی:** {old_owner_name}
👑 **مالک جدید:** {new_owner_name} (آیدی: `{new_owner_id}`)

🔹 این تغییر توسط سازنده اصلی اعمال شد.
""")
            except:
                pass
            
            try:
                await client.send_message(chat_id, f"""👑 **تغییر مالک گروه!**

🔹 مالک گروه توسط سازنده اصلی تغییر کرد.

👑 **مالک جدید:** {new_owner_name} (آیدی: `{new_owner_id}`)
""")
            except:
                pass
        else:
            try:
                await event.reply("❌ خطا در تغییر مالک! لطفاً دوباره تلاش کنید.")
            except:
                pass
        
        return

    # ==================== انتقال مالکیت (توسط خود مالک گروه) ====================
    # بر خلاف «تغییر مالک»/«تنظیم مالک اصلی» که مخصوص سازنده‌ی اصلی ربات‌ان، این
    # دستور به خودِ مالک فعلی گروه اجازه می‌ده مالکیتش رو (با ریپلای یا آیدی عددی)
    # به یک عضو دیگه‌ی همون گروه واگذار کنه - یعنی کاربرها بتونن همدیگه رو مالک کنن.
    if is_bare_admin_command(lower_text, "انتقال مالکیت", "واگذاری مالکیت"):
        if not (is_super_owner or is_group_owner):
            try:
                await event.reply("⛔ فقط مالک فعلی گروه می‌تواند مالکیت را انتقال دهد.")
            except:
                pass
            return

        target_id, _ = await get_target_user(event, client)
        if not target_id:
            try:
                await event.reply("⚠️ روی پیام کاربری که می‌خوای مالکیت رو بهش بدی **ریپلای** کن و «انتقال مالکیت» رو بزن.\n(یا آیدی عددی‌ش رو جلوی همین دستور بنویس)")
            except:
                pass
            return

        if target_id == sender_id:
            try:
                await event.reply("❌ شما همین الان مالک این گروه هستید.")
            except:
                pass
            return

        old_owner_id = db.get_group_owner(chat_id)
        old_owner_name = await get_user_name(client, old_owner_id, chat_id) if old_owner_id else "ندارد"

        db.remove_group_owner(chat_id)
        if old_owner_id:
            db.remove_group_admin(chat_id, old_owner_id)

        result = db.set_group_owner(chat_id, target_id)
        if result:
            db.add_group_admin(chat_id, target_id)
            GROUP_OWNER_CACHE[chat_id] = target_id
            new_owner_name = await get_user_name(client, target_id, chat_id)
            try:
                await event.reply(f"""✅ **مالکیت گروه منتقل شد!**

👑 **مالک قبلی:** {old_owner_name}
👑 **مالک جدید:** {new_owner_name} (آیدی: `{target_id}`)
""")
            except:
                pass
        else:
            try:
                await event.reply("❌ خطا در انتقال مالکیت! لطفاً دوباره تلاش کنید.")
            except:
                pass
        return

    # ==================== محروم کامل (سکوت سراسری در همه‌ی گروه‌ها) ====================
    # فقط سازنده‌ی اصلی می‌تواند از این دستور استفاده کند. کاربر موردنظر صرف‌نظر
    # از این‌که در هر گروه ادمین یا مالک باشد، در همه‌ی گروه‌های ربات سکوت می‌شود.
    if lower_text.startswith("محروم کامل "):
        if sender_id not in SUPER_OWNER_IDS:
            return

        parts = text.split(maxsplit=2)
        if len(parts) < 3:
            try:
                await event.reply("📌 **نحوه استفاده:**\n`محروم کامل [آیدی عددی یا یوزرنیم] [دلیل اختیاری]`\n\nمثال: `محروم کامل 123456789 اسپم مکرر` یا `محروم کامل @username اسپم مکرر`")
            except:
                pass
            return

        target_username = extract_username(parts[2])
        if target_username:
            target_id = await resolve_id_or_username(client, target_username)
            reason = re.sub(r'@[A-Za-z][A-Za-z0-9_]{3,31}\b', '', parts[2], count=1).strip() or "بدون دلیل"
        else:
            target_id = extract_user_id(parts[2])
            reason = re.sub(r'\d{5,}', '', parts[2], count=1).strip() or "بدون دلیل"
        if not target_id:
            try:
                await event.reply("❌ آیدی یا یوزرنیم کاربر معتبر نیست!\nمثال: `محروم کامل 123456789 اسپم مکرر` یا `محروم کامل @username اسپم مکرر`")
            except:
                pass
            return

        if target_id in SUPER_OWNER_IDS:
            try:
                await event.reply("❌ نمی‌توان سازنده‌ی اصلی را محروم کرد!")
            except:
                pass
            return

        db.global_mute_user(target_id, reason)

        applied_count = 0
        failed_count = 0
        try:
            dialogs = await client.get_dialogs()
            groups = [d for d in dialogs if d.is_group]
            for group in groups:
                try:
                    await client.edit_permissions(group.id, target_id, send_messages=False)
                    applied_count += 1
                except:
                    failed_count += 1
        except Exception as e:
            safe_print(f"❌ خطا در دریافت لیست گروه‌ها برای محروم کامل: {e}")

        try:
            await event.reply(f"""🔇 **محروم کامل ثبت شد!**

🆔 آیدی کاربر: `{target_id}`
📝 دلیل: {reason}

این کاربر از این پس در **همه‌ی گروه‌های ربات** سکوت خواهد بود، حتی اگر در
یک گروه خاص ادمین یا مالک آن گروه باشد.

🔧 اعمال فوری روی گروه‌های فعلی: `{applied_count}` موفق / `{failed_count}` ناموفق
(گروه‌های ناموفق معمولاً به این دلیله که ربات در اون گروه ادمین نیست؛
با این حال چون این محرومیت سراسری در دیتابیس ثبت شده، پیام‌های این کاربر
در همه‌ی گروه‌ها توسط خود ربات هم حذف خواهد شد.)
""")
        except:
            pass
        return

    if lower_text.startswith(("رفع محروم کامل ", "حذف محروم کامل ")):
        if sender_id not in SUPER_OWNER_IDS:
            return

        parts = text.split(maxsplit=2)
        if len(parts) < 3:
            try:
                await event.reply("📌 **نحوه استفاده:**\n`رفع محروم کامل [آیدی عددی یا یوزرنیم]` (یا `حذف محروم کامل`)\n\nمثال: `رفع محروم کامل 123456789` یا `رفع محروم کامل @username`")
            except:
                pass
            return

        target_token = parts[2].strip().split()[0] if parts[2].strip() else ""
        target_id = await resolve_id_or_username(client, target_token)
        if not target_id:
            try:
                await event.reply("❌ آیدی یا یوزرنیم کاربر معتبر نیست!")
            except:
                pass
            return

        removed = db.global_unmute_user(target_id)
        if not removed:
            try:
                await event.reply(f"⚠️ کاربر با آیدی `{target_id}` در لیست محروم‌های کامل نبود.")
            except:
                pass
            return

        applied_count = 0
        failed_count = 0
        try:
            dialogs = await client.get_dialogs()
            groups = [d for d in dialogs if d.is_group]
            for group in groups:
                try:
                    await client.edit_permissions(group.id, target_id, send_messages=True)
                    applied_count += 1
                except:
                    failed_count += 1
        except Exception as e:
            safe_print(f"❌ خطا در دریافت لیست گروه‌ها برای رفع محروم کامل: {e}")

        try:
            await event.reply(f"""✅ **محرومیت کامل کاربر `{target_id}` لغو شد.**

🔧 اعمال فوری روی گروه‌های فعلی: `{applied_count}` موفق / `{failed_count}` ناموفق
""")
        except:
            pass
        return

    if lower_text in ["لیست محروم کامل", "لیست محرومیت کامل"]:
        if sender_id not in SUPER_OWNER_IDS:
            return

        muted = db.get_globally_muted_users()
        if not muted:
            try:
                await event.reply("هیچ کاربری به‌صورت کامل محروم نشده.")
            except:
                pass
            return

        msg = "🔇 **لیست محروم‌های کامل:**\n"
        for m in muted:
            reason_txt = f" — {m['reason']}" if m["reason"] else ""
            msg += f"• `{m['user_id']}`{reason_txt}\n"
        try:
            await event.reply(msg)
        except:
            pass
        return

    if lower_text in ["تنظیم مالک اصلی", "بازیابی مالکیت"]:
        if sender_id not in SUPER_OWNER_IDS:
            return

        target_id, _ = await get_target_user(event, client)
        if not target_id:
            try:
                await event.reply("⚠️ روی پیام شخصی که می‌خوای مالک گروه بشه **ریپلای** کن و «تنظیم مالک اصلی» رو بزن.\n(یا آیدی عددی‌ش رو جلوی همین دستور بنویس)")
            except:
                pass
            return

        old_owner_id = db.get_group_owner(chat_id)
        old_owner_name = await get_user_name(client, old_owner_id, chat_id) if old_owner_id else "ندارد"

        db.remove_group_owner(chat_id)
        if old_owner_id and old_owner_id != target_id:
            db.remove_group_admin(chat_id, old_owner_id)

        result = db.set_group_owner(chat_id, target_id)
        if result:
            db.add_group_admin(chat_id, target_id)
            db.set_bot_enabled(True)
            GROUP_OWNER_CACHE[chat_id] = target_id
            name = await get_user_name(client, target_id, chat_id)
            try:
                await event.reply(f"""✅ **مالکیت گروه تنظیم شد!**

👑 **مالک قبلی (اشتباهی):** {old_owner_name}
👑 **مالک جدید:** {name} (آیدی: `{target_id}`)

🔹 این تغییر توسط سازنده اصلی ربات اعمال شد.
""")
            except:
                pass
        else:
            try:
                await event.reply("❌ خطا در تنظیم مالکیت! لطفاً دوباره تلاش کنید.")
            except:
                pass
        return

    if lower_text in ["help", "/help", "راهنما", "دستورات"]:
        try:
            await event.reply(HELP_MAIN)
        except:
            pass
        return

    if lower_text in ["راهنما مالک", "راهنمای مالک", "راهنما مالک گروه", "راهنمای مالک گروه"]:
        try:
            await event.reply(HELP_OWNER)
        except:
            pass
        return

    if lower_text in ["راهنما مالک فعالسازی", "راهنما مالک فعال سازی", "راهنمای مالک فعالسازی"]:
        try:
            await event.reply(HELP_OWNER)
        except:
            pass
        return

    if lower_text in ["راهنما مالک سازنده", "راهنمای مالک سازنده"]:
        try:
            await event.reply(HELP_OWNER_SUPER)
        except:
            pass
        return

    if lower_text in ["راهنما تایمر", "راهنمای تایمر", "راهنما زمانبندی", "راهنمای زمانبندی"]:
        try:
            await event.reply(HELP_TIMER)
        except:
            pass
        return

    if lower_text in ["راهنما ادمین", "راهنمای ادمین"]:
        try:
            await event.reply(HELP_ADMIN)
        except:
            pass
        return

    if lower_text in ["راهنما ادمین اعضا", "راهنمای ادمین اعضا", "راهنما اعضا", "راهنمای اعضا"]:
        try:
            await event.reply(HELP_ADMIN_MEMBERS)
        except:
            pass
        return

    if lower_text in ["راهنما ادمین قفل", "راهنمای ادمین قفل", "راهنما قفل", "راهنمای قفل"]:
        try:
            await event.reply(HELP_ADMIN_LOCKS)
        except:
            pass
        return

    if lower_text in ["راهنما ادمین فیلتر", "راهنمای ادمین فیلتر", "راهنما فیلتر", "راهنمای فیلتر"]:
        try:
            await event.reply(HELP_ADMIN_FILTERS)
        except:
            pass
        return

    if lower_text in ["راهنما ادمین خودکار", "راهنمای ادمین خودکار"]:
        try:
            await event.reply(HELP_ADMIN_AUTOMOD)
        except:
            pass
        return

    if lower_text in ["راهنما ادمین خوشامد", "راهنمای ادمین خوشامد", "راهنما خوشامد", "راهنمای خوشامد"]:
        try:
            await event.reply(HELP_ADMIN_WELCOME)
        except:
            pass
        return

    if lower_text in ["راهنما ادمین مقام", "راهنمای ادمین مقام", "راهنما مقام", "راهنمای مقام"]:
        try:
            await event.reply(HELP_ADMIN_RANKS)
        except:
            pass
        return

    if lower_text in ["راهنما سخنگو", "راهنمای سخنگو"]:
        try:
            await event.reply(HELP_SPEAKER)
        except:
            pass
        return

    if lower_text in ["راهنما فان", "راهنمای فان", "راهنما سرگرمی", "سرگرمی", "لیست سرگرمی"]:
        try:
            await event.reply(HELP_FUN)
        except:
            pass
        return

    # ==================== نمایش محدودیت‌های کاربر ====================
    if lower_text in ["محدودیت", "محدودیت من"]:
        try:
            # نمایش فقط از وضعیت واقعی قفل‌ها/Auto Mod می‌خواند؛ منطق اعمال قفل‌ها دست‌نخورده است.
            all_limitations = []
            seen_lock_keys = set()
            for lock_name, lock_key in LOCK_MAP.items():
                if lock_key not in seen_lock_keys:
                    all_limitations.append((lock_name, lock_key))
                    seen_lock_keys.add(lock_key)
            auto_mod = db.get_auto_mod_settings(chat_id) or {}
            action_labels = {"ban": "بن", "mute": "سکوت", "warn": "اخطار"}

            rows = []
            for label, key in all_limitations:
                if key in ("badword", "spam", "ad"):
                    locked = bool(auto_mod.get({
                        "badword": "badword_enabled",
                        "spam": "spam_enabled",
                        "ad": "ad_enabled",
                    }[key]))
                    action = "بن" if auto_mod.get("silent_mode") else "اخطار"
                else:
                    locked = bool(db.is_locked(chat_id, key))
                    action = action_labels.get(db.get_lock_action(chat_id, key), "اخطار") if locked else "آزاد"

                rows.append(f"{'⬢' if locked else '⬡'} {label} «{action}»")

            try:
                warn_limit = int(auto_mod.get("warn_limit", 3) or 3)
            except Exception:
                warn_limit = 3
            try:
                warning_count = int(db.get_user_warnings(chat_id, sender_id) or 0)
            except Exception:
                warning_count = 0

            is_privileged_for_limit_view = bool(
                is_group_special or is_group_special_admin or is_creator
                or is_group_owner or is_super_owner
            )

            if is_privileged_for_limit_view:
                limitation_text = "**محدودیت**\n\n" + "\n".join(rows)
            else:
                # کاربر عادی فقط مواردی را می‌بیند که واقعاً روی او اعمال می‌شوند.
                active_rows = []
                for row in rows:
                    if row.startswith("■"):
                        active_rows.append(row)
                limitation_text = "**محدودیت**\n\n" + ("\n".join(active_rows) if active_rows else "⬡ موردی قفل نیست")

            limitation_text += f"\n\nاخطار: {warning_count} از {warn_limit}"
            await event.reply(limitation_text)
        except Exception as e:
            safe_print(f"❌ خطا در نمایش محدودیت‌ها: {e}")
            try:
                await event.reply("❌ خطا در نمایش محدودیت‌های این گروه.")
            except Exception:
                pass
        return

    # ==================== خوشامدگویی ثابت ====================
    # متن خوشامد و روشن/خاموش بودن آن عمداً قابل تنظیم نیست.
    # خوشامد همیشه با DEFAULT_WELCOME_TEMPLATE ارسال می‌شود.
        db.set_goodbye_settings(chat_id, enabled=False)
        try:
            await event.reply("❌ **خداحافظی خاموش شد!**")
        except:
            pass
        return

    # ==================== تنظیم اخطار (مخصوص قفل‌ها و مدیریت خودکار) ====================
    if lower_text.startswith("تنظیم اخطار "):
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        parts = text.split()
        if len(parts) >= 3 and parts[2].isdigit():
            limit = int(parts[2])
            if limit < 1 or limit > 10:
                try:
                    await event.reply("❌ تعداد اخطار باید بین 1 تا 10 باشد!")
                except:
                    pass
                return
            db.set_auto_mod_settings(chat_id, warn_limit=limit)
            try:
                await event.reply(f"✅ سقف اخطار روی {limit} تنظیم شد!\n\nاین سقف هم برای قفل‌ها (لینک، عکس، ویدیو، فحش و...) و هم برای مدیریت خودکار اعمال می‌شود؛ کاربر بعد از {limit} اخطار در این گروه به‌صورت خودکار بن می‌شود.")
            except:
                pass
        else:
            try:
                await event.reply("مثال: `تنظیم اخطار 3`")
            except:
                pass
        return

    # ==================== تنظیمات Auto Mod ====================
    if lower_text.startswith("تنظیم خودکار "):
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        parts = text.split()
        if len(parts) < 4:
            try:
                await event.reply("📌 **راهنمای تنظیم خودکار:**\n\n`تنظیم خودکار اسپم [روشن/خاموش]`\n`تنظیم خودکار تبلیغ [روشن/خاموش]`\n`تنظیم خودکار فحش [روشن/خاموش]`\n`تنظیم خودکار اخطار [تعداد]`\n`تنظیم خودکار سکوت [ثانیه]`\n`تنظیم خودکار سایلنت [روشن/خاموش]` - بدون هیچ پیام اخطاری، همون بار اول تخلف مستقیم بن کن")
            except:
                pass
            return
        
        option = parts[2].lower()
        value = parts[3].lower()
        
        settings = db.get_auto_mod_settings(chat_id)
        
        if option in ["اسپم", "spam"]:
            settings["spam_enabled"] = value in ["روشن", "on", "true", "1"]
            db.set_auto_mod_settings(chat_id, spam_enabled=settings["spam_enabled"])
            try:
                await event.reply(f"✅ **تشخیص اسپم {'روشن' if settings['spam_enabled'] else 'خاموش'} شد!**")
            except:
                pass
        elif option in ["تبلیغ", "ad", "تبلیغات"]:
            settings["ad_enabled"] = value in ["روشن", "on", "true", "1"]
            db.set_auto_mod_settings(chat_id, ad_enabled=settings["ad_enabled"])
            try:
                await event.reply(f"✅ **تشخیص تبلیغات {'روشن' if settings['ad_enabled'] else 'خاموش'} شد!**")
            except:
                pass
        elif option in ["فحش", "badword", "فحاشی"]:
            settings["badword_enabled"] = value in ["روشن", "on", "true", "1"]
            db.set_auto_mod_settings(chat_id, badword_enabled=settings["badword_enabled"])
            try:
                await event.reply(f"✅ **تشخیص فحاشی {'روشن' if settings['badword_enabled'] else 'خاموش'} شد!**")
            except:
                pass
        elif option in ["اخطار", "warn"]:
            try:
                limit = int(value)
                if limit < 1 or limit > 10:
                    await event.reply("❌ تعداد اخطار باید بین 1 تا 10 باشد!")
                    return
                settings["warn_limit"] = limit
                db.set_auto_mod_settings(chat_id, warn_limit=limit)
                await event.reply(f"✅ **تعداد اخطار برای بن به {limit} تنظیم شد!**")
            except ValueError:
                await event.reply("❌ تعداد اخطار باید عدد باشد!")
            except:
                pass
        elif option in ["سکوت", "mute"]:
            try:
                duration = int(value)
                if duration < 30 or duration > 3600:
                    await event.reply("❌ مدت سکوت باید بین 30 تا 3600 ثانیه باشد!")
                    return
                settings["mute_duration"] = duration
                db.set_auto_mod_settings(chat_id, mute_duration=duration)
                await event.reply(f"✅ **مدت سکوت به {duration} ثانیه تنظیم شد!**")
            except ValueError:
                await event.reply("❌ مدت سکوت باید عدد باشد!")
            except:
                pass
        elif option in ["سایلنت", "silent"]:
            settings["silent_mode"] = value in ["روشن", "on", "true", "1"]
            db.set_auto_mod_settings(chat_id, silent_mode=settings["silent_mode"])
            try:
                if settings["silent_mode"]:
                    await event.reply("✅ **حالت سایلنت روشن شد!**\nاز این به بعد برای اسپم/فحش/تبلیغات و قفل‌ها، هیچ پیام اخطاری داده نمی‌شه و همون بار اول تخلف، کاربر مستقیم بن می‌شه.")
                else:
                    await event.reply("✅ **حالت سایلنت خاموش شد!**\nسیستم دوباره طبق سقف اخطار عادی عمل می‌کنه.")
            except:
                pass
        else:
            try:
                await event.reply("❌ گزینه نامعتبر!\nگزینه‌های موجود: اسپم، تبلیغ، فحش، اخطار، سکوت، سایلنت")
            except:
                pass
        return

    if lower_text in ["وضعیت خودکار", "وضعیت اتومات"]:
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        settings = db.get_auto_mod_settings(chat_id)
        goodbye = db.get_goodbye_settings(chat_id)
        
        msg = f"""
📊 **وضعیت مدیریت خودکار گروه**

🔹 **خوشامدگویی:** ✅ روشن (ثابت)
🔹 **خداحافظی:** ✅ روشن (ثابت)

🛡️ **Auto Mod:**
   • تشخیص اسپم (پیش‌فرض: روشن): {'✅ روشن' if settings['spam_enabled'] else '❌ خاموش'}
   • تشخیص تبلیغات: {'✅' if settings['ad_enabled'] else '❌'}
   • تشخیص فحاشی: {'✅' if settings['badword_enabled'] else '❌'}
   • تعداد اخطار برای بن: {settings['warn_limit']}
   • مدت سکوت: {settings['mute_duration']} ثانیه
   • حالت سایلنت (بدون اخطار، بن سریع): {'✅ روشن' if settings.get('silent_mode') else '❌ خاموش'}

🔹 برای تغییر: `تنظیم خودکار اسپم/تبلیغ/فحش/سایلنت [روشن/خاموش]`
"""
        try:
            await event.reply(msg)
        except:
            pass
        return

    if lower_text in ["امار من", "امارم", "my stats", "کارنامه"]:
        try:
            info = db.get_full_user_info(sender_id, chat_id)
            name = await get_user_name(client, sender_id, chat_id)
            rank = info["rank"]
            if not rank:
                if info["message_count"] >= 1000:
                    rank = "👑 سلطان گروه"
                elif info["message_count"] >= 500:
                    rank = "⭐ فوق‌العاده"
                elif info["message_count"] >= 200:
                    rank = "💎 کاربر فعال"
                elif info["message_count"] >= 100:
                    rank = "🌟 کاربر خوب"
                elif info["message_count"] >= 50:
                    rank = "📌 کاربر معمولی"
                else:
                    rank = "🆕 کاربر جدید"
            if is_super_owner:
                status = "🌠 سازنده اصلی"
            elif is_group_owner:
                status = "👑 مالک گروه"
            elif is_group_special_admin:
                status = "🎖 ادمین"
            elif is_creator:
                status = "👑 سازنده گروه (سروش)"
            elif is_group_admin:
                status = "🛡️ ادمین گروه (سروش)"
            elif is_only_special_tier:
                status = "🌟 ویژه"
            elif is_admin:
                status = "🛡️ مدیر ربات"
            elif is_group_special:
                status = "👤 عضو عادی"
            elif is_group_exempt:
                status = "🕊 معاف"
            else:
                status = "👤 عضو عادی"
            if info['is_banned']:
                lock_status = "🚫 بن شده"
            elif info['is_muted']:
                lock_status = "🔇 سکوت شده"
            else:
                lock_status = "✅ آزاد"
            
            mode_map = {
                "polite": "🌹 مؤدب",
                "rude": "😈 بی‌ادب",
                "short": "🤫 کم‌حرف",
                "long": "🗣️ پرحرف",
                "off": "🔇 خاموش"
            }
            mode_text = mode_map.get(SPEAKER_MODE, "نامشخص")
            
            msg = f"""
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃     📊 **آمار کاربری**           ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

👤 **نام:** {name}
🏅 **مقام:** {rank}

📨 **تعداد پیام‌ها:** `{info['message_count']}`

👤 **لقب:** {info['title'] or 'ندارد'}

⚠️ **اخطار:** `{info['warns']}`

🛡️ **وضعیت:** {status}

🔒 **وضعیت قفل:** {lock_status}

🗣️ **حالت سخنگو:** {mode_text}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
            await event.reply(msg)
        except:
            pass
        return

    # ==================== روزشمار فعالیت ربات ====================
    # از روی فایل جداگانه‌ی ACTIVATION_FILE خونده می‌شه که فقط یک‌بار (اولین لاگین
    # موفق) نوشته شده؛ با ری‌استارت ربات هیچ‌وقت صفر نمی‌شه.
    if lower_text in ["روزشمار", "چند روزه فعاله", "چند روز فعاله", "مدت فعالیت"]:
        days = get_days_active()
        activation_date = get_activation_date()
        date_str = activation_date.strftime("%Y-%m-%d") if activation_date else "نامشخص"
        try:
            await event.reply(f"📅 **روزشمار فعالیت ربات**\n\n🗓 تاریخ فعال‌سازی: {date_str}\n⏳ مدت فعالیت: **{days} روز**")
        except:
            pass
        return

    # ==================== لقب ====================
    # فقط ادمین/مالک/سازنده می‌تونه روی بقیه کاربرها لقب بذاره. لقب توی جدول جداگانه‌ی
    # user_titles و بر اساس (chat_id, user_id) ذخیره می‌شه، یعنی مخصوص همین گروهه و
    # روی گروه‌های دیگه‌ای که همین کاربر عضوشونه اثر نداره. چون get_user_name همه‌جا
    # برای نمایش اسم کاربر استفاده می‌شه (با پاس دادن chat_id همین گروه)، این لقب
    # خودکار توی همه‌ی جواب‌های ربات در همین گروه (بن، سکوت، آمار، خوش‌آمدگویی، تگ
    # لقب: هر عضو می‌تواند برای خودش یا با ریپلای برای دیگران تنظیم کند
    if lower_text.startswith("تنظیم لقب "):
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        target_id, _ = await get_target_user(event, client)
        raw_title = text[len("تنظیم لقب "):].strip()
        title_text = re.sub(r'^\d{5,}\s*', '', raw_title).strip()
        if not target_id:
            try:
                await event.reply("⚠️ کاربر هدف پیدا نشد.\nروی پیام کاربر **ریپلای** کنید و بنویسید: `تنظیم لقب متن لقب`")
            except Exception:
                pass
            return
        if not title_text or len(title_text) > 40:
            try:
                await event.reply("⚠️ متن لقب باید بین ۱ تا ۴۰ کاراکتر باشد.\nمثال: `تنظیم لقب قهرمان گروه`")
            except Exception:
                pass
            return
        db.set_user_title(chat_id, target_id, title_text)
        try:
            clear_user_cache(target_id, chat_id)
        except Exception:
            pass
        tag_username = await get_username(client, target_id)
        tag = view_tag(target_id, tag_username)
        try:
            await event.reply(f"✅ لقب «{title_text}» برای {tag} ثبت شد.")
        except Exception:
            pass
        return

    if lower_text in ["لقب", "لقب چیه", "لقب چیست"]:
        target_id, _ = await get_target_user(event, client)
        if not target_id:
            try:
                await event.reply("⚠️ کاربر هدف پیدا نشد.\nروی پیام کاربر **ریپلای** کنید و بنویسید: `لقب`")
            except:
                pass
            return
        title = db.get_user_title(chat_id, target_id)
        tag_username = await get_username(client, target_id)
        tag = view_tag(target_id, tag_username)
        try:
            if title:
                await event.reply(f"🏷 لقب {tag}: «{title}»")
            else:
                await event.reply(f"🏷 {tag} لقبی ثبت‌شده نداره.")
        except:
            pass
        return

    if is_bare_admin_command(lower_text, "حذف لقب"):
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        target_id, _ = await get_target_user(event, client)
        if not target_id:
            try:
                await event.reply("⚠️ کاربر هدف پیدا نشد.\nروی پیام کاربر **ریپلای** کنید.")
            except:
                pass
            return
        db.remove_user_title(chat_id, target_id)
        clear_user_cache(target_id, chat_id)
        tag_username = await get_username(client, target_id)
        tag = view_tag(target_id, tag_username)
        try:
            await event.reply(f"✅ لقب {tag} حذف شد.")
        except:
            pass
        return


    # ==================== بن ====================
    # فقط وقتی اجرا می‌شه که دستور کاملاً «خالی» نوشته شده باشه (با ریپلای روی کاربر)
    # یا دقیقاً «بن + آیدی عددی» باشه؛ هر جمله‌ی دیگه‌ای که فقط با «بن» شروع بشه اصلاً
    # به‌عنوان دستور در نظر گرفته نمی‌شه.
    if is_bare_admin_command(lower_text, "بن"):
        if not (is_super_owner or is_group_owner or is_admin):
            return
        target_id, _ = await get_target_user(event, client)
        if target_id:
            if not db.can_moderate(chat_id, sender_id, target_id):
                try:
                    await event.reply(f"» مقام شما ({db.get_mod_rank_label(chat_id, sender_id)}) برای بن‌کردن {db.get_mod_rank_label(chat_id, target_id)} کافی نیست ↺")
                except:
                    pass
                return
            db.ban_user(chat_id, target_id, banned_by=sender_id)
            tag_username = await get_username(client, target_id)
            tag = view_tag(target_id, tag_username)

            # چکِ ارزون‌قیمتِ قبل از تلاش: به‌جای اینکه فقط بریم سراغِ edit_permissions
            # و هر خطایی رو خام نشون بدیم، اول دلیلِ شناخته‌شده‌ی احتمالی رو پیدا کنیم.
            issue_kind = await _diagnose_restrict_issue_kind(chat_id, target_id)
            if issue_kind:
                try:
                    await event.reply(format_restrict_notice(issue_kind, tag, "ban"))
                except:
                    pass
                return

            try:
                await client.edit_permissions(chat_id, target_id, send_messages=False)
                try:
                    await event.reply(_user_action_message("ban", "دستور ادمین", tag=tag))
                except:
                    pass
            except Exception:
                try:
                    await event.reply(_user_action_message("ban", "دستور ادمین (دسترسی واقعی بسته نشد)", tag=tag))
                except:
                    pass
        else:
            try:
                await event.reply("» کاربر هدف پیدا نشد؛ روی پیامش ریپلای کن یا آیدی/یوزرنیمش رو بنویس ↺")
            except:
                pass
        return

    # ==================== ban (بن واقعی: بیرون‌انداختن از گروه) ====================
    # فرق با «بن»: «بن» فقط دسترسی پیام‌دادن رو می‌بنده و پیام‌های کاربر رو پاک می‌کنه،
    # ولی «ban» کاربر رو واقعاً از گروه بیرون می‌ندازه و اجازه‌ی برگشتن هم نمی‌ده.
    # آزادکردن: «رفع بن» یا «حذف بن» (بعدش کاربر باید دوباره با لینک عضو بشه).
    if is_bare_admin_command(lower_text, "ban"):
        if not (is_super_owner or is_group_owner or is_admin):
            return
        target_id, _ = await get_target_user(event, client)
        if target_id:
            if not db.can_moderate(chat_id, sender_id, target_id):
                try:
                    await event.reply(f"» مقام شما ({db.get_mod_rank_label(chat_id, sender_id)}) برای بن‌کردن {db.get_mod_rank_label(chat_id, target_id)} کافی نیست ↺")
                except:
                    pass
                return
            tag_username = await get_username(client, target_id)
            tag = view_tag(target_id, tag_username)

            issue_kind = await _diagnose_restrict_issue_kind(chat_id, target_id)
            if issue_kind:
                # بن واقعی وقتی ربات ادمین نیست/دسترسی نداره یا هدف ادمینه، روی سروش ممکن نیست؛
                # اینجا (برخلاف «بن») هیچ چیزی ثبت نمی‌شه تا الکی وانمود نشه که کاربر بیرون رفته.
                try:
                    if issue_kind == "own":
                        await event.reply("**» ربات توی این گروه دسترسی «محدودکردن اعضا» رو نداره؛ بن واقعی انجام نشد ↺**")
                    else:
                        await event.reply("**» این شخص ادمین/سازنده‌ی گروهه و سروش اجازه‌ی بیرون‌انداختنش رو نمی‌ده ↺**")
                except:
                    pass
                return

            kicked = False
            fallback_used = False
            try:
                # view_messages=False یعنی بن کامل (حق دیدن گروه هم گرفته می‌شه) = اخراج + جلوگیری از بازگشت
                await client.edit_permissions(chat_id, target_id, view_messages=False)
                kicked = True
            except Exception as e1:
                safe_print(f"⚠️ ban واقعی با edit_permissions ناموفق: {e1}")
                try:
                    await client.kick_participant(chat_id, target_id)
                    kicked = True
                    fallback_used = True
                except Exception as e2:
                    safe_print(f"⚠️ ban واقعی با kick_participant هم ناموفق: {e2}")

            if kicked:
                db.ban_user(chat_id, target_id, "ban واقعی (اخراج از گروه)", banned_by=sender_id)
                try:
                    note = "به دلیل دستور ادمین از گروه بیرون انداخته شد"
                    if fallback_used:
                        note += " (امکان بازگشت با لینک هست)"
                    await event.reply(_user_action_message("kick", note, tag=tag))
                except:
                    pass
            else:
                try:
                    await event.reply(f"**» بن واقعی {tag} انجام نشد (سروش درخواست رو رد کرد) ↺**")
                except:
                    pass
        else:
            try:
                await event.reply("» کاربر هدف پیدا نشد؛ روی پیامش ریپلای کن یا آیدی/یوزرنیمش رو بنویس ↺")
            except:
                pass
        return

    # ==================== رفع بن ====================
    if is_bare_admin_command(lower_text, "رفع بن", "حذف بن"):
        if not (is_super_owner or is_group_owner or is_admin):
            return
        target_id, _ = await get_target_user(event, client)
        if target_id:
            db.unban_user(chat_id, target_id)
            tag_username = await get_username(client, target_id)
            tag = view_tag(target_id, tag_username)

            issue_kind = await _diagnose_restrict_issue_kind(chat_id, target_id)
            if issue_kind:
                try:
                    await event.reply(format_restrict_notice(issue_kind, tag, "unban"))
                except:
                    pass
                return

            try:
                await client.edit_permissions(chat_id, target_id, send_messages=True)
                try:
                    await event.reply(f"» بن {tag} برداشته شد ↺")
                except:
                    pass
            except Exception:
                try:
                    await event.reply(f"» بن {tag} از دیتابیس ربات برداشته شد؛ ولی امکان باز کردن دسترسی واقعی نبود ↺")
                except:
                    pass
        else:
            try:
                await event.reply("» کاربر هدف پیدا نشد ↺")
            except:
                pass
        return

    # ==================== لیست بن ====================
    if lower_text == "لیست بن":
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        
        banned = db.get_banned_users(chat_id)
        if banned:
            group_name = await resolve_group_name(chat_id)
            msg = f"🚫 **لیست بن‌های «{group_name}»** ({len(banned)} نفر):\n\n"
            for item in banned:
                name = await get_user_name(client, item["user_id"], chat_id)
                date_str = item["timestamp"][:16].replace("T", " - ") if item.get("timestamp") else "-"
                reason = item.get("reason") or "بدون دلیل"
                msg += f"• {name} (`{item['user_id']}`)\n   📌 دلیل: {reason}\n   🕒 {date_str}\n\n"
            msg += "🧹 برای پاک‌کردن کامل این لیست: `پاکسازی لیست بن`"
            try:
                await event.reply(msg)
            except:
                pass
        else:
            try:
                await event.reply("📋 هیچ کاربری در لیست بن این گروه نیست!")
            except:
                pass
        return

    # ==================== پاکسازی لیست بن (مخصوص همین گروه) ====================
    if lower_text in ["پاکسازی لیست بن", "پاک کردن لیست بن", "خالی کردن لیست بن"]:
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        count = db.clear_banned_users(chat_id)
        try:
            await event.reply(f"🧹 لیست بن همین گروه پاکسازی شد! ({count} نفر حذف شدند)\n\nℹ️ توجه: این کار روی سایر گروه‌ها اثری ندارد و دسترسی چت کاربران در سروش به‌صورت خودکار باز نمی‌شود؛ در صورت نیاز با «رفع بن» جداگانه دسترسی چت را باز کنید.")
        except:
            pass
        return

    # ==================== سکوت ====================
    # فقط وقتی اجرا می‌شه که پیام دقیقاً «سکوت» (به‌همراه ریپلای) یا «سکوت + آیدی
    # عددی» باشه. قبلاً هر پیامی که فقط با «سکوت » شروع می‌شد (حتی یک جمله‌ی عادی
    # مثل «سکوت بود انگار...») وارد این بخش می‌شد و امکان داشت به‌اشتباه کسی رو
    # سکوت کنه؛ الان چنین جمله‌ای اصلاً به‌عنوان دستور شناخته نمی‌شه.
    #
    # سکوت زمان‌دار: اگه روی پیام کسی ریپلای بشه و بعد از «سکوت» یک عدد کوچیک
    # (بین ۱ تا ۱۰۰۰۰۰) نوشته بشه، اون عدد به‌عنوان «دقیقه» تفسیر می‌شه، نه آیدی
    # هدف (چون وقتی ریپلای وجود داره، هدف از خودِ ریپلای مشخصه و دیگه نیازی به
    # آیدی عددی نیست). وقتی ریپلای نباشه، همون رفتار قدیمی (عدد = آیدی عددی هدف،
    # سکوت دائمی) دست‌نخورده می‌مونه.
    if is_bare_admin_command(lower_text, "سکوت"):
        if not (is_super_owner or is_group_owner or is_admin):
            return
        _reply_probe = (
            getattr(event, 'reply_to_msg_id', None)
            or getattr(event, 'reply_to_message_id', None)
            or getattr(event, 'reply_msg_id', None)
            or getattr(getattr(event, 'reply_to', None), 'reply_to_msg_id', None)
            or getattr(getattr(event, 'message', None), 'reply_to_msg_id', None)
        )
        is_reply_message = bool(_reply_probe)
        mute_minutes = None
        if is_reply_message:
            _dur_match = re.match(r'^سکوت\s+(\d+)$', lower_text)
            if _dur_match:
                _candidate = int(_dur_match.group(1))
                if 0 < _candidate <= 100000:
                    mute_minutes = _candidate
        target_id, _ = await get_target_user(event, client)
        if target_id:
            if not db.can_moderate(chat_id, sender_id, target_id):
                try:
                    await event.reply(f"» مقام شما ({db.get_mod_rank_label(chat_id, sender_id)}) برای سکوت‌کردن {db.get_mod_rank_label(chat_id, target_id)} کافی نیست ↺")
                except:
                    pass
                return
            until_str = None
            if mute_minutes:
                until_dt = datetime.now(IRAN_TZ) + timedelta(minutes=mute_minutes)
                until_str = until_dt.strftime("%Y-%m-%d %H:%M")
            db.mute_user(chat_id, target_id, until=until_str, muted_by=sender_id)
            tag_username = await get_username(client, target_id)
            tag = view_tag(target_id, tag_username)

            issue_kind = await _diagnose_restrict_issue_kind(chat_id, target_id)
            if issue_kind:
                try:
                    await event.reply(format_restrict_notice(issue_kind, tag, "mute"))
                except:
                    pass
                return

            reason_label = f"دستور ادمین (سکوت {mute_minutes} دقیقه‌ای)" if mute_minutes else "دستور ادمین"
            try:
                await client.edit_permissions(chat_id, target_id, send_messages=False)
                try:
                    await event.reply(_user_action_message("mute", reason_label, tag=tag))
                except:
                    pass
            except:
                try:
                    await event.reply(_user_action_message("mute", f"{reason_label} (دسترسی واقعی بسته نشد)", tag=tag))
                except:
                    pass

            if mute_minutes:
                async def _auto_unmute(_chat_id=chat_id, _target_id=target_id, _minutes=mute_minutes):
                    await asyncio.sleep(_minutes * 60)
                    try:
                        if db.is_muted(_chat_id, _target_id):
                            db.unmute_user(_chat_id, _target_id)
                            try:
                                await client.edit_permissions(_chat_id, _target_id, send_messages=True)
                            except:
                                pass
                    except Exception:
                        pass
                _track_background_task(_auto_unmute())
        else:
            try:
                await event.reply("» کاربر هدف پیدا نشد ↺")
            except:
                pass
        return

    # ==================== رفع سکوت ====================
    if is_bare_admin_command(lower_text, "رفع سکوت", "حذف سکوت"):
        if not (is_super_owner or is_group_owner or is_admin):
            return
        target_id, _ = await get_target_user(event, client)
        if target_id:
            db.unmute_user(chat_id, target_id)
            tag_username = await get_username(client, target_id)
            tag = view_tag(target_id, tag_username)

            issue_kind = await _diagnose_restrict_issue_kind(chat_id, target_id)
            if issue_kind:
                try:
                    await event.reply(format_restrict_notice(issue_kind, tag, "unmute"))
                except:
                    pass
                return

            try:
                await client.edit_permissions(chat_id, target_id, send_messages=True)
                try:
                    await event.reply(f"» سکوت {tag} برداشته شد ↺")
                except:
                    pass
            except:
                try:
                    await event.reply(f"» سکوت {tag} از دیتابیس برداشته شد؛ ولی امکان باز کردن دسترسی واقعی نبود ↺")
                except:
                    pass
        else:
            try:
                await event.reply("» کاربر هدف پیدا نشد ↺")
            except:
                pass
        return

    # ==================== اخطار ====================
    if is_bare_admin_command(lower_text, "اخطار"):
        if not (is_super_owner or is_group_owner or is_admin):
            return
        target_id, _ = await get_target_user(event, client)
        if target_id:
            if not db.can_moderate(chat_id, sender_id, target_id):
                try:
                    await event.reply(f"⛔ مقام شما ({db.get_mod_rank_label(chat_id, sender_id)}) برای اخطاردادن به {db.get_mod_rank_label(chat_id, target_id)} کافی نیست!")
                except:
                    pass
                return
            # نکته: قبلاً این دستور از سیستم قدیمی و سراسری db.add_warn (بدون
            # chat_id) استفاده می‌کرد که کاملاً از سیستم اخطار واقعیِ هر گروه
            # (همونی که فیلتر/فحش/اسپم ازش استفاده می‌کنن: user_warnings بر اساس
            # chat_id+user_id) جدا بود. حالا از همون سیستم مشترک استفاده می‌شه تا
            # اخطار دستی ادمین هم با بقیه‌ی اخطارها جمع بشه و «حذف اخطار» هم واقعاً
            # روی همین شمارنده اثر بذاره.
            count = db.add_user_warning(chat_id, target_id, "اخطار دستی توسط ادمین")
            tag_username = await get_username(client, target_id)
            tag = view_tag(target_id, tag_username)
            auto_mod = db.get_auto_mod_settings(chat_id)
            warn_limit = auto_mod["warn_limit"]
            try:
                warn_msg = await event.reply(format_warn_message("اخطار دستی توسط ادمین", count, warn_limit, tag=tag))
                asyncio.create_task(auto_delete_warning(client, chat_id, warn_msg.id, delay=get_warning_delete_delay(chat_id, target_id)))
            except:
                pass
            if count >= warn_limit:
                db.ban_user(chat_id, target_id, f"{count} اخطار")
                try:
                    await client.edit_permissions(chat_id, target_id, send_messages=False)
                    try:
                        ban_msg = await event.reply(format_ban_message(f"رسیدن به {warn_limit} اخطار", tag=tag))
                        asyncio.create_task(auto_delete_warning(client, chat_id, ban_msg.id))
                    except:
                        pass
                except Exception as e:
                    try:
                        ban_msg = await event.reply(format_ban_message(f"رسیدن به {warn_limit} اخطار (خطا در بستن دسترسی: {e})", tag=tag))
                        asyncio.create_task(auto_delete_warning(client, chat_id, ban_msg.id))
                    except:
                        pass
        else:
            try:
                await event.reply("⚠️ کاربر هدف پیدا نشد.")
            except:
                pass
        return

    # ==================== حذف اخطار ====================
    # هر بار «حذف اخطار» فقط «یک» اخطار از کاربر کم می‌کنه (نه همه‌ی اخطارها).
    if is_bare_admin_command(lower_text, "حذف اخطار", "رفع اخطار"):
        if not (is_super_owner or is_group_owner or is_admin):
            return
        target_id, _ = await get_target_user(event, client)
        if target_id:
            tag_username = await get_username(client, target_id)
            tag = view_tag(target_id, tag_username)
            warn_limit = db.get_auto_mod_settings(chat_id)["warn_limit"]
            before = db.get_user_warnings(chat_id, target_id)
            if before <= 0:
                try:
                    await event.reply(f"⚠️ {tag} هیچ اخطاری نداره.")
                except:
                    pass
            else:
                left = db.remove_user_warning(chat_id, target_id)
                try:
                    await event.reply(f"✅ یک اخطار از {tag} حذف شد. (اخطارهای فعلی: {left} از {warn_limit})")
                except:
                    pass
        else:
            try:
                await event.reply("⚠️ کاربر هدف پیدا نشد.")
            except:
                pass
        return

    # ==================== پاکسازی ====================
    # نکته: چون این شرط با startswith چک می‌شه، باید دستورات چندکلمه‌ای مثل
    # «پاکسازی لیست سکوت» / «پاک کردن لیست سکوت» / «پاکسازی فیلتر» رو صریحاً
    # استثنا کنیم؛ وگرنه همینجا (چون parts[1] عدد نیست) جواب «مثال: پاک ۵۰»
    # می‌گیرن و هیچوقت به هندلر واقعی خودشون (پایین‌تر) نمی‌رسن.
    _PAKSAZI_EXCLUDED_COMMANDS = [
        "پاکسازی لیست سکوت", "پاک کردن لیست سکوت", "خالی کردن لیست سکوت",
        "پاکسازی لیست بن", "پاک کردن لیست بن", "خالی کردن لیست بن",
        "پاکسازی فیلتر", "پاکسازی فیلتر رسانه", "پاکسازی سخنگو",
    ]
    if (lower_text.startswith("پاک ") or lower_text.startswith("پاکسازی ")) and lower_text not in _PAKSAZI_EXCLUDED_COMMANDS:
        if not (is_super_owner or is_group_owner or is_admin):
            return
        if is_only_special_tier:
            try:
                await event.reply("⛔ مقام «ویژه» اجازه‌ی پاکسازی (پاک‌کردن دسته‌ای پیام‌ها) رو نداره.")
            except:
                pass
            return
        parts = text.split()
        if len(parts) > 1 and parts[1].isdigit():
            requested = int(parts[1])
            # فقط سازنده‌ها (دو سازنده‌ی اصلی) محدودیت ندارن و می‌تونن هر تعدادی
            # بخوان پاک کنن؛ بقیه (مالک گروه/ادمین) همون سقف قبلی ۱۰۰ رو دارن.
            count = requested if is_super_owner else min(requested, 100)
            try:
                messages = await client.get_messages(chat_id, limit=count + 1)
                msg_ids = [m.id for m in messages if m.id != event.id]
                deleted_total = 0
                # اکثر پلتفرم‌های سازگار با تلگرام (از جمله سروش) روی هر
                # درخواست delete_messages سقفی حدود ۱۰۰ آیدی دارن؛ برای اینکه
                # سازنده بتونه واقعاً هزار تا هم پاک کنه، به دسته‌های ۱۰۰تایی
                # می‌شکنیمش تا هیچ بخشی به خاطر همین سقف بی‌صدا رد نشه.
                CHUNK = 100
                for i in range(0, len(msg_ids), CHUNK):
                    batch = msg_ids[i:i + CHUNK]
                    try:
                        await client.delete_messages(chat_id, batch)
                        deleted_total += len(batch)
                    except Exception as batch_err:
                        safe_print(f"❌ خطا در پاک کردن دسته‌ی پیام‌ها: {batch_err}")
                try:
                    await event.reply(f"🧹 {deleted_total} پیام پاک شد.")
                except:
                    pass
            except:
                try:
                    await event.reply("خطا در پاک کردن")
                except:
                    pass
        else:
            try:
                await event.reply("مثال: `پاک ۵۰`")
            except:
                pass
        return

    if lower_text.startswith("محدودیت لینک "):
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        parts = text.split()
        if len(parts) > 2 and parts[2].isdigit():
            limit = int(parts[2])
            if limit < 1 or limit > 20:
                try:
                    await event.reply("» محدودیت باید بین ۱ تا ۲۰ باشد. ↺")
                except:
                    pass
                return
            db.set_setting("link_limit", str(limit))
            try:
                await event.reply(f"» محدودیت لینک روی {limit} تنظیم شد. ↺")
            except:
                pass
        else:
            try:
                await event.reply("» تنظیم محدودیت لینک\nمثال: محدودیت لینک 3 ↺")
            except:
                pass
        return

    if lower_text == "قفل گروه":
        if is_super_owner or is_group_owner:
            db.set_lock(chat_id, "group", True)
            try:
                await event.reply(format_lock_message("گروه", opened=False))
            except:
                pass
        return

    if lower_text == "باز کردن گروه" or lower_text == "بازکردن گروه":
        if is_super_owner or is_group_owner:
            db.set_lock(chat_id, "group", False)
            try:
                await event.reply(format_lock_message("گروه", opened=True))
            except:
                pass
        return

    lock_map = LOCK_MAP

    # کلمه‌ی عمل (اختیاری) که می‌تونه بعد از اسم قفل بیاد؛ همه‌ی این‌ها به یکی از
    # سه اکشن database (warn/mute/ban) نگاشت می‌شن. «بن»/«بند» هر دو یعنی بن.
    LOCK_ACTION_WORDS = {
        "بن": "ban",
        "بند": "ban",
        "سکوت": "mute",
        "اخطار": "warn",
    }
    LOCK_ACTION_LABELS = {"ban": "بن", "mute": "سکوت", "warn": "اخطار (پیش‌فرض)"}

    if lower_text.startswith("قفل "):
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        raw = text[4:].strip().lower()

        # اگه آخرین کلمه‌ی متن یکی از کلمه‌های عمل بود، جداش کن؛ مثلاً «قفل عکس
        # سکوت» یعنی قفل «عکس» + عمل «سکوت»، و «قفل لینک بن» یعنی قفل «لینک» +
        # عمل «بن». بقیه‌ی متن (بدون کلمه‌ی عمل) باید دقیقاً با یکی از اسم‌های
        # LOCK_MAP یکی باشه، وگرنه اصلاً به‌عنوان دستور معتبر قبول نمی‌شه.
        requested_action = None
        lock_type = raw
        raw_parts = raw.rsplit(" ", 1)
        if len(raw_parts) == 2 and raw_parts[1] in LOCK_ACTION_WORDS:
            requested_action = LOCK_ACTION_WORDS[raw_parts[1]]
            lock_type = raw_parts[0].strip()

        matched = False
        for name, key in lock_map.items():
            if lock_type == name:
                matched = True
                # «اسپم»/«فحش»/«تبلیغ»: این‌ها دیگه سیستم قفل جدا ندارن، دقیقاً همون
                # تنظیمات Auto Mod (spam_enabled/badword_enabled/ad_enabled) رو روشن
                # می‌کنن - تا با دستور «تنظیم خودکار ...» و لیست «وضعیت خودکار» یکی
                # باشه و آدم گیج نشه که کدومش واقعاً روشن/خاموشه.
                if key in ("spam", "badword", "ad"):
                    field = {"spam": "spam_enabled", "badword": "badword_enabled", "ad": "ad_enabled"}[key]
                    if requested_action == "warn":
                        db.set_auto_mod_settings(chat_id, **{field: True}, silent_mode=False)
                    elif requested_action == "ban":
                        db.set_auto_mod_settings(chat_id, **{field: True}, silent_mode=True)
                    else:
                        db.set_auto_mod_settings(chat_id, **{field: True})
                    try:
                        auto_mod = db.get_auto_mod_settings(chat_id) or {}
                        action_label = "بن" if auto_mod.get("silent_mode") else "اخطار"
                        await event.reply(format_lock_message(name, opened=False, action_label=action_label))
                    except:
                        pass
                    break
                db.set_lock(chat_id, key, True, action=requested_action)
                current_action = db.get_lock_action(chat_id, key)
                try:
                    action_label = LOCK_ACTION_LABELS[current_action].replace(" (پیش‌فرض)", "")
                    await event.reply(format_lock_message(name, opened=False, action_label=action_label))
                except:
                    pass
                break
        if not matched:
            try:
                await event.reply("» نوع قفل نامعتبر است. ↺")
            except:
                pass
        return

    if lower_text.startswith("بازکردن ") or lower_text.startswith("باز کردن "):
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        lock_type = text.replace("بازکردن", "").replace("باز کردن", "").strip().lower()
        matched = False
        for name, key in lock_map.items():
            if lock_type == name:
                matched = True
                if key in ("spam", "badword", "ad"):
                    field = {"spam": "spam_enabled", "badword": "badword_enabled", "ad": "ad_enabled"}[key]
                    db.set_auto_mod_settings(chat_id, **{field: False})
                    try:
                        await event.reply(format_lock_message(name, opened=True))
                    except:
                        pass
                    break
                db.set_lock(chat_id, key, False)
                try:
                    await event.reply(format_lock_message(name, opened=True))
                except:
                    pass
                break
        if not matched:
            try:
                await event.reply("» نوع قفل نامعتبر است. ↺")
            except:
                pass
        return

    # ==================== بررسی بیوی عضو تازه‌وارد (اخطار بیو / بیو خاموش / بیو قفل) ====================
    if lower_text == "اخطار بیو":
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        db.set_bio_mode(chat_id, "warn")
        try:
            await event.reply("⚠️ حالت بررسی بیو روی **اخطار** تنظیم شد؛ از این به بعد اگه بیوی عضو تازه‌وارد لینک داشته باشه، بهش اخطار داده می‌شه.")
        except:
            pass
        return

    if lower_text == "بیو خاموش":
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        db.set_bio_mode(chat_id, "off")
        try:
            await event.reply("🔕 بررسی بیوی عضو تازه‌وارد **خاموش** شد.")
        except:
            pass
        return

    if lower_text == "بیو قفل":
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        db.set_bio_mode(chat_id, "lock")
        try:
            await event.reply("🔒 حالت بررسی بیو روی **قفل** تنظیم شد؛ از این به بعد اگه بیوی عضو تازه‌وارد لینک داشته باشه، بند می‌شه.")
        except:
            pass
        return

    # ==================== اسم اختصاصی ربات مخصوص این گروه ====================
    if lower_text.startswith("تنظیم اسم ربات "):
        if not (is_super_owner or is_group_owner):
            return
        new_name = text[len("تنظیم اسم ربات "):].strip()
        if not new_name or len(new_name) > 30:
            try:
                await event.reply("❌ اسم باید بین ۱ تا ۳۰ کاراکتر باشد.\nمثال: `تنظیم اسم ربات آنیتا`")
            except Exception:
                pass
            return
        db.set_bot_name(chat_id, new_name)
        try:
            await event.reply(
                f"✅ اسم ربات این گروه: **{new_name}**\n"
                f"• صدا زدن: `{new_name}` → جواب می‌ده: جانم\n"
                f"• دستور با اسم: `{new_name} بن` یا فقط `بن`"
            )
        except Exception:
            pass
        return

    if lower_text in ["حذف اسم ربات", "پاک کردن اسم ربات"]:

        if not (is_super_owner or is_group_owner):
            return
        db.remove_bot_name(chat_id)
        try:
            await event.reply("✅ اسم اختصاصی ربات برای این گروه حذف شد؛ دستورات مثل قبل بدون اسم کار می‌کنند.")
        except:
            pass
        return

    if lower_text in ["اسم ربات", "اسم ربات چیه", "اسم ربات چیست"]:
        current_name = db.get_bot_name(chat_id)
        try:
            if current_name:
                await event.reply(f"🤖 اسم اختصاصی ربات در این گروه: **{current_name}**\n\n(دستورات هم با این اسم و هم بدون آن کار می‌کنند.)")
            else:
                await event.reply("🤖 این گروه اسم اختصاصی ندارد.\nبرای تنظیم: `تنظیم اسم ربات آنیتا`")
        except:
            pass
        return

    # ==================== لیست قفل ====================
    if lower_text == "لیست قفل":
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        try:
            lock_items = []
            seen_lock_keys = set()
            for lock_name, lock_key in LOCK_MAP.items():
                if lock_key not in seen_lock_keys:
                    lock_items.append((lock_name, lock_key))
                    seen_lock_keys.add(lock_key)

            auto_mod = db.get_auto_mod_settings(chat_id) or {}
            action_labels = {"ban": "بن", "mute": "سکوت", "warn": "اخطار"}
            active_locks = db.get_active_locks(chat_id)

            def lock_state(label, key):
                # «لیست قفل» باید دقیقاً وضعیت واقعی قفل را نشان بدهد.
                # اسپم/تبلیغ/متن نامناسب تنظیمات Auto-Mod هستند، اما سه قفل اصلی
                # (اسپم، لینک، کد هنگی) باید وضعیت فعال خودشان را هم درست نمایش دهند.
                if key in ("badword", "spam", "ad"):
                    enabled_key = {
                        "badword": "badword_enabled",
                        "spam": "spam_enabled",
                        "ad": "ad_enabled",
                    }[key]
                    locked = bool(auto_mod.get(enabled_key)) or key in active_locks
                    if key in active_locks:
                        action = action_labels.get(active_locks[key], "اخطار")
                    else:
                        action = "بن" if auto_mod.get("silent_mode") else "اخطار"
                else:
                    locked = key in active_locks
                    action = action_labels.get(active_locks.get(key), "اخطار") if locked else "آزاد"
                return locked, action

            # یک‌ستونه و عمودی: هر قفل فقط یک خط؛ اکشن داخل « ».
            rows = []
            for label, key in lock_items:
                locked, action = lock_state(label, key)
                icon = "⬢" if locked else "⬡"
                rows.append(f"│  {icon} **{label}**  |  **{action}**")

            msg = (
                "🔐 **لیست قفل‌ها**\n\n"
                + "\n".join(rows)
                + "\n\n"
                "⬢ فعال   ⬡ آزاد"
            )
            await event.reply(msg)
        except Exception as e:
            safe_print(f"❌ خطا در نمایش لیست قفل‌ها: {e}")
            try:
                await event.reply("❌ خطا در نمایش لیست قفل‌ها.")
            except Exception:
                pass
        return

    if lower_text in ["سنجاق", "pin"]:
        if not (is_super_owner or is_group_owner or is_admin):
            return
        reply_msg_id = getattr(event, 'reply_to_msg_id', None)
        if reply_msg_id:
            try:
                await client.pin_message(chat_id, reply_msg_id)
                try:
                    await event.reply("📌 پیام سنجاق شد.")
                except:
                    pass
            except:
                try:
                    await event.reply("خطا در سنجاق کردن")
                except:
                    pass
        else:
            try:
                await event.reply("روی پیام ریپلای کن.")
            except:
                pass
        return

    if lower_text in ["لینک", "link"]:
        # Public groups can use their username directly. For private groups,
        # use SplusLib 2.1.0's documented get_chat_invite_link() API.
        link = None
        last_error = None

        try:
            entity = await event.get_chat() if hasattr(event, 'get_chat') else await client.get_entity(chat_id)
        except Exception as e:
            entity = None
            last_error = e

        if entity:
            username = getattr(entity, 'username', None)
            if not username:
                usernames = getattr(entity, 'usernames', None)
                if usernames:
                    first = usernames[0]
                    username = getattr(first, 'username', None) or (first if isinstance(first, str) else None)
            if username:
                link = f"https://splus.ir/{username}"

        # Private/no-username chat: create a real invite link through the
        # library API instead of invoking a TL *type* object by mistake.
        if not link:
            try:
                link = await client.get_chat_invite_link(entity if entity is not None else chat_id)
            except Exception as e:
                last_error = e

        if link:
            try:
                await event.reply(f"🔗 لینک گروه:\n{link}")
            except Exception:
                pass
        else:
            detail = repr(last_error) if last_error else "Unknown error"
            try:
                await event.reply(
                    "❌ ساخت لینک دعوت انجام نشد.\n\n"
                    f"🔹 خطای API: `{detail}`\n\n"
                    "اگر گروه خصوصی است، ربات باید دسترسی لازم برای ساخت لینک دعوت را داشته باشد."
                )
            except Exception:
                pass
        return

    if lower_text in ["آیدی", "id", "شناسه"]:
        target_id, _ = await get_target_user(event, client)
        if not target_id:
            target_id = sender_id
        try:
            await event.reply(f"🆔 آیدی: `{target_id}`")
        except:
            pass
        return

    # ==================== فیلتر رسانه (ریپلای‌محور) ====================
    # روی یک گیف/ویدیو/آهنگ/استیکر/عکس/فایل ریپلای بزن و بنویس «فیلتر رسانه» (اخطار)،
    # «فیلتر رسانه بن» یا «فیلتر رسانه سکوت». از اون به بعد هر بار همون رسانه (حتی فوروارد
    # یا دست‌به‌دست‌شده) ارسال بشه، پیام پاک می‌شه و فرستنده طبق اکشن اخطار/بن/سکوت می‌گیره.
    if lower_text in ("فیلتر رسانه", "فیلتر رسانه اخطار", "فیلتر رسانه بن", "فیلتر رسانه سکوت"):
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        mf_action = "warn"
        if lower_text.endswith("بن"):
            mf_action = "ban"
        elif lower_text.endswith("سکوت"):
            mf_action = "mute"
        info = await get_replied_media_info(event, client)
        if not info:
            try:
                await event.reply(
                    "❌ اول روی همون رسانه‌ای که می‌خوای فیلتر بشه (گیف، ویدیو، آهنگ، استیکر، عکس یا فایل) "
                    "ریپلای بزن، بعد بنویس: `فیلتر رسانه` یا `فیلتر رسانه بن` یا `فیلتر رسانه سکوت`"
                )
            except:
                pass
            return
        db.add_media_filter(chat_id, info["key"], info["kind"], info["fp"], mf_action)
        try:
            await event.reply(
                f"✅ {info['label']} فیلتر شد (اکشن: {MEDIA_FILTER_ACTION_LABELS[mf_action]}). "
                "از این به بعد ارسالش پاک می‌شه."
            )
        except:
            pass
        return

    if lower_text == "حذف فیلتر رسانه" or re.fullmatch(r"حذف فیلتر رسانه\s+\d+", lower_text or ""):
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        removed = False
        num_match = re.fullmatch(r"حذف فیلتر رسانه\s+(\d+)", lower_text)
        if num_match:
            # حذف با شماره‌ی «لیست فیلتر رسانه» (وقتی دیگه دسترسی به پیام اصلی نیست)
            idx = int(num_match.group(1))
            keys = list(db.get_media_filters(chat_id).keys())
            if 1 <= idx <= len(keys):
                removed = db.remove_media_filter(chat_id, keys[idx - 1])
        else:
            info = await get_replied_media_info(event, client)
            if not info:
                try:
                    await event.reply(
                        "❌ اول روی همون رسانه‌ای که فیلترش رو می‌خوای حذف کنی ریپلای بزن، بعد بنویس: `حذف فیلتر رسانه`\n"
                        "یا شماره‌ی لیست رو بنویس: `حذف فیلتر رسانه 2`"
                    )
                except:
                    pass
                return
            removed = db.remove_media_filter(chat_id, info["key"], info["fp"])
        try:
            await event.reply("✅ فیلتر رسانه حذف شد." if removed else "⚠️ این رسانه در لیست فیلترها وجود نداشت.")
        except:
            pass
        return

    if lower_text == "لیست فیلتر رسانه":
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        mfs = db.get_media_filters(chat_id)
        kind_labels = {
            "gif": "گیف", "video": "ویدیو", "video_note": "ویدیو مسیج", "voice": "ویس",
            "audio": "آهنگ", "sticker": "استیکر", "photo": "عکس", "file": "فایل",
        }
        if mfs:
            lines_m = [
                f"{i:02d} {kind_labels.get(item['type'], 'رسانه')} — {MEDIA_FILTER_ACTION_LABELS.get(item['action'], item['action'])}"
                for i, item in enumerate(mfs.values(), start=1)
            ]
            msg = (
                "✦ ─── SOFT MEDIA FILTER ─── ✦\n\n"
                f"تعداد رسانه‌های فیلترشده: {len(mfs)}\n\n" + "\n".join(lines_m)
                + "\n\nحذف: ریپلای روی رسانه + `حذف فیلتر رسانه` یا `حذف فیلتر رسانه [شماره]`"
            )
        else:
            msg = "✦ ─── SOFT MEDIA FILTER ─── ✦\n\nهیچ رسانه‌ای فیلتر نشده."
        try:
            await event.reply(msg[:3900])
        except:
            pass
        return

    if lower_text == "پاکسازی فیلتر رسانه":
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        db.clear_media_filters(chat_id)
        try:
            await event.reply("✅ همه‌ی فیلترهای رسانه‌ی این گروه پاک شدند.")
        except:
            pass
        return

    # ==================== فیلتر (ریپلای‌محور) ====================
    # فرمت جدید: اول همون کلمه یا جمله‌ای که می‌خوای فیلتر بشه رو به‌صورت یک پیام
    # عادی بفرست، بعد رو همون پیام ریپلای بزن و دقیقاً بنویس «فیلتر» (یا «فیلتر
    # بن» / «فیلتر سکوت»). دیگه نیازی نیست همه‌چیز سرهم توی یک پیام نوشته بشه، و
    # چون کل متن پیام ریپلای‌شده (نه فقط یک کلمه‌ی جدا با فاصله) به‌عنوان فیلتر
    # ثبت می‌شه، فیلتر کردن جمله هم دقیقاً مثل فیلتر کردن یک کلمه ممکنه.
    if lower_text in ("فیلتر", "فیلتر بن", "فیلتر سکوت"):
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        action = "delete"
        if lower_text == "فیلتر بن":
            action = "ban"
        elif lower_text == "فیلتر سکوت":
            action = "mute"

        _, replied_text = await get_replied_message_info(event, client)
        if not replied_text or not replied_text.strip():
            try:
                await event.reply(
                    "❌ اول رو همون پیامی که می‌خوای فیلتر بشه (کلمه یا جمله) ریپلای بزن، "
                    "بعد بنویس: `فیلتر` یا `فیلتر بن` یا `فیلتر سکوت`"
                )
            except:
                pass
            return

        word = replied_text.strip().lower()
        db.add_word_filter(chat_id, word, action)
        # نکته‌ی امنیتی: پیام تأیید دیگه هیچ‌وقت خودِ متنِ فیلترشده (که می‌تونه
        # هر متن دلخواهی از جمله متن سیاسی باشه، چون فیلتر روی هر پیامی که
        # ریپلای بشه کار می‌کنه) رو عیناً نشون نمی‌ده؛ چون قبلاً همین تکرارِ متنِ
        # کاربر توی پیامِ خودِ ربات باعث گزارش‌شدن ربات می‌شد. فقط طول متن نشون
        # داده می‌شه تا ادمین مطمئن بشه چیزی ثبت شده، بدون اینکه خودِ محتوا
        # دوباره توسط ربات گفته بشه.
        try:
            await event.reply(f"✅ فیلتر ثبت شد (اکشن: {action}، طول متن: {len(word)} کاراکتر).")
        except:
            pass
        return

    if lower_text == "حذف فیلتر":
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return

        _, replied_text = await get_replied_message_info(event, client)
        if not replied_text or not replied_text.strip():
            try:
                await event.reply(
                    "❌ اول رو همون پیامی که فیلترش رو می‌خوای حذف کنی ریپلای بزن، بعد بنویس: `حذف فیلتر`"
                )
            except:
                pass
            return

        word = replied_text.strip().lower()
        removed = db.remove_word_filter(chat_id, word)
        # نکته‌ی امنیتی: مثل ثبتِ فیلتر، اینجا هم دیگه خودِ متن عیناً تکرار
        # نمی‌شه؛ فقط نتیجه‌ی عملیات اعلام می‌شه.
        if removed:
            await event.reply("✅ فیلتر حذف شد.")
        else:
            await event.reply("⚠️ این متن در لیست فیلترها وجود نداشت.")
        return


    
    if lower_text in ["لیست فیلتر", "فیلترها"]:
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        filters = db.get_word_filters(chat_id)
        if filters:
            header = (
                "✦ ─── SOFT FILTER LIST ─── ✦\n\n"
                f"تعداد فیلترها: {len(filters)}\n"
            )
            lines_f = [f"{i:02d} {word}" for i, (word, _action) in enumerate(filters.items(), start=1)]
            msg = header + "\n".join(lines_f)
            if len(msg) <= 3500:
                try:
                    await event.reply(msg)
                except Exception:
                    pass
            else:
                chunk = header
                for line_f in lines_f:
                    if len(chunk) + len(line_f) + 1 > 3500:
                        try:
                            await event.reply(chunk)
                        except Exception:
                            pass
                        chunk = line_f + "\n"
                    else:
                        chunk += line_f + "\n"
                if chunk.strip():
                    try:
                        await event.reply(chunk)
                    except Exception:
                        pass
        else:
            try:
                await event.reply(
                    "✦ ─── SOFT FILTER LIST ─── ✦\n\n"
                    "تعداد فیلترها: 0\n\n"
                    "هیچ فیلتری ثبت نشده."
                )
            except Exception:
                pass
        return

    if lower_text == "پاکسازی فیلتر":
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        db.clear_word_filters(chat_id)
        try:
            await event.reply("✅ همه‌ی فیلترهای این گروه پاک شدند.")
        except:
            pass
        return

    # ==================== سرگرمی ====================
    fun_map = {
        "جوک": "jock",
        "حدیث": "hadis",
        "فال": "fal",
        "فکت": "fact",
        "دعا": "doa",
        "دانستنی": "dnstni",
        "دقت": "deghat",
        "داستان": "dastan",
        "چیستان": "chistan",
        "بیو": "bio",
        "انگیزشی": "angizeshi",
        "خاطره": "khatereh",
        "خطبه": "khotbeh",
        "پ ن پ": "pnp",
        "تکست": "text",
        "چالش": "chalesh",
    }

    # سرگرمی‌ها فقط وقتی اجرا شوند که پیام واقعاً با قصدِ همان سرگرمی نوشته شده باشد.
    # صرفِ وجود کلمه در وسط یک جمله کافی نیست.
    # نمونه‌های مجاز: «داستان»، «داستان من این بود»، «یه داستان بگو»، «لطفا جوک بگو»
    # نمونه‌های غیرمجاز: «امروز درباره داستان صحبت کردیم»، «این یک جوک خوب بود»
    fun_prefixes = (
        "یه ", "یک ", "یه‌", "یک‌", "لطفا ", "لطفاً ", "لطفا‌",
        "برام ", "برام‌", "برای من ", "میشه ", "میشه‌", "می‌شه ",
        "بگو ", "بفرست ", "بفرستش ", "بگو یه ", "بگو یک ",
    )

    def _fun_command_requested(text, cmd):
        text = (text or "").strip()
        if not text:
            return False

        # خودِ دستور
        if text == cmd:
            return True

        # دستور در ابتدای پیام؛ مثل «داستان من این بود...»
        if text.startswith(cmd + " ") or text.startswith(cmd + "‌"):
            return True

        # شکل طبیعی درخواست؛ مثل «یه داستان بگو» یا «لطفا جوک بفرست»
        for prefix in fun_prefixes:
            if text.startswith(prefix + cmd):
                return True

        # برای «داستانی ...» هم فقط وقتی ابتدای پیام باشد قبول کن، نه وسط جمله.
        if cmd == "داستان" and (text.startswith("داستانی ") or text.startswith("داستانی‌")):
            return True

        return False

    for cmd, category in fun_map.items():
        if _fun_command_requested(lower_text, cmd):
            content = await get_random_content(category)
            try:
                await event.reply(content)
            except:
                pass
            return

    if lower_text == "لیست مالک":
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        owner_id = db.get_group_owner(chat_id)
        if owner_id:
            name = await get_user_name(client, owner_id, chat_id)
            msg = f"👑 **مالک گروه:**\n\n• {name} (آیدی: `{owner_id}`)"
            try:
                await event.reply(msg)
            except:
                pass
        else:
            try:
                await event.reply("📋 این گروه مالک ندارد! لطفاً `فعال` را بزنید تا مالک ثبت شود.")
            except:
                pass
        return

    if lower_text == "لیست مدیر":
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        admins_data = await get_group_admins(chat_id)
        admin_ids = admins_data.get("admins", [])
        if admin_ids:
            msg = "🛡️ **لیست ادمین‌های گروه (سروش):**\n\n"
            for admin_id in admin_ids:
                name = await get_user_name(client, admin_id, chat_id)
                is_creator = "👑 " if admin_id == admins_data.get("creator") else ""
                msg += f"{is_creator}• {name} (`{admin_id}`)\n"
            try:
                await event.reply(msg)
            except:
                pass
        else:
            try:
                await event.reply("📋 هیچ ادمینی در گروه وجود ندارد!")
            except:
                pass
        return

    # ==================== تنظیم ویژه (ادمین رباتِ مخصوص این گروه) ====================
    # قبلاً اسمش «تنظیم ادمین» بود؛ همون جدول/دسترسی قبلیه (group_admins)، فقط
    # اسمش عوض شده و طبق مقام‌بندی جدید سه محدودیت هم داره: نمی‌تونه پاکسازی
    # کنه (چک is_only_special_tier توی پاکسازی)، نمی‌تونه کسی رو معاف کنه (چک
    # is_only_special_tier توی «تنظیم معاف»)، و چون خودِ همین دستور به
    # مالک/سازنده محدوده، خودش هم نمی‌تونه یه ویژه‌ی دیگه بسازه.
    # ==================== ویژه پاک (فقط سازنده) ====================
    # همه‌ی «ویژه»های همین گروه رو پاک می‌کنه. به «ادمین‌ها» (group_special_admins)
    # و خودِ مالک گروه کاری نداره (رکوردِ مالک توی جدول ویژه دست‌نخورده می‌مونه).
    if lower_text == "ویژه پاک":
        if not is_super_owner:
            return
        try:
            _owner_keep = db.get_group_owner(chat_id)
            _removed = 0
            for _vid in list(db.get_group_admins(chat_id)):
                if _owner_keep and _vid == _owner_keep:
                    continue
                db.remove_group_admin(chat_id, _vid)
                _removed += 1
            await event.reply(f"🧹 همه‌ی ویژه‌های این گروه پاک شدند. ({_removed} نفر حذف شد)")
        except Exception as e:
            try:
                await event.reply(f"⚠️ پاک‌کردن ویژه‌ها ناموفق بود: {e}")
            except:
                pass
        return

    if is_bare_admin_command(lower_text, "تنظیم ویژه"):
        if not (is_super_owner or is_group_owner):
            return
        target_id, _ = await get_target_user(event, client)
        if target_id:
            db.add_group_admin(chat_id, target_id)
            name = await get_user_name(client, target_id, chat_id)
            try:
                await event.reply(f"🌟 **{name}** (`{target_id}`) ویژه‌ی **این گروه** شد.\n🔹 این دسترسی فقط در همین گروه معتبره؛ دستورهای مدیریتی رو داره ولی نمی‌تونه پاکسازی کنه، کسی رو معاف کنه یا کسی رو ویژه کنه.")
            except:
                pass
        else:
            try:
                await event.reply("⚠️ کاربر هدف پیدا نشد.\nروی پیام کاربر **ریپلای** کنید یا آیدی بدید.")
            except:
                pass
        return

    if is_bare_admin_command(lower_text, "حذف ویژه"):
        if not (is_super_owner or is_group_owner):
            return
        target_id, _ = await get_target_user(event, client)
        if target_id:
            db.remove_group_admin(chat_id, target_id)
            name = await get_user_name(client, target_id, chat_id)
            try:
                await event.reply(f"✅ مقام ویژه‌ی **{name}** (`{target_id}`) در این گروه حذف شد.")
            except:
                pass
        else:
            try:
                await event.reply("⚠️ کاربر هدف پیدا نشد.\nروی پیام کاربر **ریپلای** کنید یا آیدی بدید.")
            except:
                pass
        return

    if lower_text in ["لیست ویژه", "لیست ویژه‌ها"]:
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        admin_ids = db.get_group_admins(chat_id)
        if admin_ids:
            msg = "🌟 **ویژه‌های این گروه:**\n\n"
            for admin_id in admin_ids:
                name = await get_user_name(client, admin_id, chat_id)
                msg += f"• {name} (`{admin_id}`)\n"
        else:
            msg = "📋 هیچ ویژه‌ای برای این گروه تعیین نشده."
        try:
            await event.reply(msg)
        except:
            pass
        return

    # ==================== مقام‌بندی: معاف / ویژه / ادمین ====================
    # عادی < معاف < ویژه < ادمین < مالک گروه < سازنده.
    # معاف: هیچ دستور مدیریتی نداره، فقط از قفل‌ها/فیلترها/Auto Mod آزاده.
    # ویژه: دستورهای مدیریتی معمولی رو داره ولی نمی‌تونه پاکسازی کنه یا کسی
    # رو معاف/ویژه کنه (is_only_special_tier پایین‌تر). ادمین: همه‌ی دستورهای
    # ادمین رو داره + ویژه‌ها نمی‌تونن روش بن/سکوت/اخطار بزنن (چون مقامش بالاتره).
    if is_bare_admin_command(lower_text, "تنظیم معاف"):
        if not (is_super_owner or is_group_owner or is_admin):
            return
        if is_only_special_tier:
            try:
                await event.reply("⛔ مقام «ویژه» اجازه‌ی معاف‌کردن کسی رو نداره.")
            except:
                pass
            return
        target_id, _ = await get_target_user(event, client)
        if target_id:
            db.add_group_exempt(chat_id, target_id)
            name = await get_user_name(client, target_id, chat_id)
            try:
                await event.reply(f"🕊 **{name}** (`{target_id}`) معاف شد؛ از قفل‌ها و فیلترهای این گروه آزاده.")
            except:
                pass
        else:
            try:
                await event.reply("⚠️ کاربر هدف پیدا نشد.\nروی پیام کاربر **ریپلای** کنید یا آیدی بدید.")
            except:
                pass
        return

    if is_bare_admin_command(lower_text, "حذف معاف"):
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        target_id, _ = await get_target_user(event, client)
        if target_id:
            db.remove_group_exempt(chat_id, target_id)
            name = await get_user_name(client, target_id, chat_id)
            try:
                await event.reply(f"✅ معافیت **{name}** (`{target_id}`) در این گروه حذف شد.")
            except:
                pass
        else:
            try:
                await event.reply("⚠️ کاربر هدف پیدا نشد.\nروی پیام کاربر **ریپلای** کنید یا آیدی بدید.")
            except:
                pass
        return

    if lower_text in ["لیست معاف", "لیست معافین"]:
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        ids = db.get_group_exempt(chat_id)
        if ids:
            msg = "🕊 **لیست معاف‌های این گروه:**\n\n"
            for uid in ids:
                name = await get_user_name(client, uid, chat_id)
                msg += f"• {name} (`{uid}`)\n"
        else:
            msg = "📋 هیچ کاربر معافی در این گروه ثبت نشده."
        try:
            await event.reply(msg)
        except:
            pass
        return

    # ==================== تنظیم ادمین (قبلاً «ادمین ویژه») ====================
    # همون جدول/دسترسی قبلی (group_special_admins)، فقط اسمش عوض شده به «ادمین».
    if is_bare_admin_command(lower_text, "تنظیم ادمین"):
        if not (is_super_owner or is_group_owner):
            return
        target_id, _ = await get_target_user(event, client)
        if target_id:
            db.add_group_special_admin(chat_id, target_id)
            name = await get_user_name(client, target_id, chat_id)
            try:
                await event.reply(f"🎖 **{name}** (`{target_id}`) ادمینِ این گروه شد.\n🔹 مقامش از ویژه بالاتره؛ ویژه‌ها نمی‌تونن روش بن/سکوت/اخطار بزنن.")
            except:
                pass
        else:
            try:
                await event.reply("⚠️ کاربر هدف پیدا نشد.\nروی پیام کاربر **ریپلای** کنید یا آیدی بدید.")
            except:
                pass
        return

    if is_bare_admin_command(lower_text, "حذف ادمین"):
        if not (is_super_owner or is_group_owner):
            return
        target_id, _ = await get_target_user(event, client)
        if target_id:
            db.remove_group_special_admin(chat_id, target_id)
            name = await get_user_name(client, target_id, chat_id)
            try:
                await event.reply(f"✅ مقام ادمینِ **{name}** (`{target_id}`) در این گروه حذف شد.")
            except:
                pass
        else:
            try:
                await event.reply("⚠️ کاربر هدف پیدا نشد.\nروی پیام کاربر **ریپلای** کنید یا آیدی بدید.")
            except:
                pass
        return

    if lower_text in ["لیست ادمین", "لیست ادمین‌ها"]:
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        ids = db.get_group_special_admins(chat_id)
        if ids:
            msg = "🎖 **لیست ادمین‌های این گروه:**\n\n"
            for uid in ids:
                name = await get_user_name(client, uid, chat_id)
                msg += f"• {name} (`{uid}`)\n"
        else:
            msg = "📋 هیچ ادمینی در این گروه ثبت نشده."
        try:
            await event.reply(msg)
        except:
            pass
        return

    if lower_text == "مقام":
        target_id, _ = await get_target_user(event, client)
        if not target_id:
            target_id = sender_id
        name = await get_user_name(client, target_id, chat_id)
        label = db.get_mod_rank_label(chat_id, target_id)
        try:
            await event.reply(f"🏅 مقام {name} در این گروه: {label}")
        except:
            pass
        return

    if lower_text == "لیست سکوت":
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        muted = db.get_muted_users(chat_id)
        if muted:
            group_name = await resolve_group_name(chat_id)
            msg = f"🔇 **لیست سکوت‌های «{group_name}»** ({len(muted)} نفر):\n\n"
            for item in muted:
                name = await get_user_name(client, item["user_id"], chat_id)
                reason = item.get("reason") or "بدون دلیل"
                msg += f"• {name} (`{item['user_id']}`)\n   📌 دلیل: {reason}\n\n"
            msg += "🧹 برای پاک‌کردن کامل این لیست: `پاکسازی لیست سکوت`"
            try:
                await event.reply(msg)
            except:
                pass
        else:
            try:
                await event.reply("📋 هیچ کاربری در سکوت این گروه نیست!")
            except:
                pass
        return

    # ==================== پاکسازی لیست سکوت (مخصوص همین گروه) ====================
    if lower_text in ["پاکسازی لیست سکوت", "پاک کردن لیست سکوت", "خالی کردن لیست سکوت"]:
        if not (is_super_owner or is_group_owner or is_admin) or is_only_special_tier:
            return
        count = db.clear_muted_users(chat_id)
        try:
            await event.reply(f"🧹 لیست سکوت همین گروه پاکسازی شد! ({count} نفر حذف شدند)\n\nℹ️ توجه: این کار روی سایر گروه‌ها اثری ندارد و دسترسی چت کاربران در سروش به‌صورت خودکار باز نمی‌شود؛ در صورت نیاز با «رفع سکوت» جداگانه دسترسی چت را باز کنید.")
        except:
            pass
        return

    if lower_text in ["برترین", "top", "best"]:
        try:
            conn = sqlite3.connect(db.db_path)
            c = conn.cursor()
            c.execute(
                "SELECT user_id, message_count FROM user_stats WHERE chat_id = ? ORDER BY message_count DESC LIMIT 10",
                (chat_id,)
            )
            rows = c.fetchall()
            conn.close()
            if rows:
                # قبلاً اسم هر ۱۰ نفر یکی‌یکی و پشت‌سرهم (سنکرون) از API سروش
                # گرفته می‌شد؛ یعنی اگه اسم کسی کش نبود، کل دستور باید منتظر
                # ۱۰ تا رفت‌وبرگشت جداگانه به سروش می‌موند. حالا همه‌شون با
                # asyncio.gather همزمان درخواست داده می‌شن (چند برابر سریع‌تر،
                # به‌خصوص وقتی چند نفر از این ۱۰ نفر هنوز کش نشدن).
                names = await asyncio.gather(
                    *[get_user_name(client, user_id, chat_id) for user_id, _ in rows],
                    return_exceptions=True
                )
                msg = "🏆 **۱۰ کاربر برتر گروه:**\n\n"
                medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
                for i, ((user_id, count), name) in enumerate(zip(rows, names), 1):
                    if isinstance(name, Exception) or not name:
                        name = str(user_id)
                    msg += f"{medals[i-1]} {name} → `{count}` پیام\n"
                await event.reply(msg)
            else:
                await event.reply("📊 هنوز آماری ثبت نشده!")
        except:
            pass
        return

    # دستورِ آمارِ آنی (جدا از «برترین» بالا - دست‌نخورده می‌مونه): همون
    # چیزی که گزارشِ شبانه هر شب خودکار می‌فرسته (امروز/دیروز/این‌هفته/
    # این‌ماه، ۱۰ نفرِ برتر هر بازه)، ولی همین لحظه که کسی دستور رو بزنه.
    if lower_text in ["گزارش فعالیت", "آمار فعالیت", "آمار امروز", "گزارش امروز"]:
        try:
            report_text = await build_nightly_group_report(chat_id)
            await event.reply(report_text)
        except Exception as e:
            safe_print(f"[گزارش فعالیت] خطا: {e}")
            try:
                await event.reply("⚠️ مشکلی توی ساختِ گزارش پیش اومد.")
            except Exception:
                pass
        return

    if lower_text in ["آمار گروه", "group stats"]:
        if is_super_owner or is_group_owner or is_admin:
            stats = db.get_stats(chat_id)
            owner_id = db.get_group_owner(chat_id)
            owner_name = await get_user_name(client, owner_id, chat_id) if owner_id else "نامشخص"
            admins_data = await get_group_admins(chat_id)
            creator_id = admins_data.get("creator")
            creator_name = await get_user_name(client, creator_id, chat_id) if creator_id else "نامشخص"
            admin_count = len(admins_data.get("admins", []))
            msg = f"""📊 **آمار گروه**

👑 مالک گروه: {owner_name}
👤 سازنده گروه (سروش): {creator_name}
🛡️ تعداد ادمین‌ها: {admin_count}
📨 تعداد پیام‌ها: `{stats['total_messages']}`
🕐 آخرین بروزرسانی: `{stats.get('last_updated', 'نامشخص')}`
"""
            try:
                await event.reply(msg)
            except:
                pass
        return

    if lower_text in ["ساعت", "time"]:
        now = datetime.now(IRAN_TZ).strftime("%H:%M:%S")
        try:
            await event.reply(f"🕒 ساعت فعلی (به وقت ایران): `{now}`")
        except:
            pass
        return

    if lower_text in ["تاریخ", "date"]:
        now = jalali_today_str()
        try:
            await event.reply(f"📅 تاریخ: `{now}`")
        except:
            pass
        return

    # ==================== تگ خودکار با لقب ====================
    # این آخرین بررسی توی کل زنجیره‌ی دستورات هست: اگه متن پیام دقیقاً (بدون هیچ
    # کلمه‌ی اضافه) برابر لقب یکی از اعضای همین گروه باشه (لقبی که قبلاً با «تنظیم
    # لقب» روی اون کاربر ثبت شده)، ربات همون کاربر رو با تگ/لینک پروفایلش (نه آیدی
    # خام) نشون می‌ده. چون فقط وقتی به اینجا می‌رسیم که هیچ‌کدوم از دستورات بالا مچ
    # نشده، هیچ دستور دیگه‌ای رو خراب نمی‌کنه.
    if text:
        title_hit = db.find_user_by_title(chat_id, text)
        if title_hit:
            tag_username = await get_username(client, title_hit)
            tag = view_tag(title_hit, tag_username)
            try:
                await event.reply(tag)
            except:
                pass
            return


# ==================== سیستم بازی OX (دوز) ====================
GAMES = {}
GAME_INVITES = {}

class OXGame:
    def __init__(self, chat_id, player1_id, player2_id=None, is_solo=False):
        self.chat_id = chat_id
        self.player1 = player1_id
        self.player2 = player2_id
        self.is_solo = is_solo
        self.board = [' '] * 9
        self.current_turn = player1_id
        self.winner = None
        self.is_finished = False
        self.last_move = None
        self.move_count = 0
        self.player1_name = None
        self.player2_name = None
    
    def get_board_display(self):
        b = self.board
        symbols = {'X': '❌', 'O': '⭕', ' ': '⬜'}
        return f"""
{symbols.get(b[0], '⬜')} {symbols.get(b[1], '⬜')} {symbols.get(b[2], '⬜')}
{symbols.get(b[3], '⬜')} {symbols.get(b[4], '⬜')} {symbols.get(b[5], '⬜')}
{symbols.get(b[6], '⬜')} {symbols.get(b[7], '⬜')} {symbols.get(b[8], '⬜')}
"""
    
    def make_move(self, position, player_id):
        if self.is_finished:
            return False, "بازی به پایان رسیده است!"
        if player_id != self.current_turn:
            return False, "نوبت شما نیست!"
        if position < 1 or position > 9:
            return False, "عدد باید بین 1 تا 9 باشد!"
        if self.board[position - 1] != ' ':
            return False, "این خانه قبلاً پر شده است!"
        symbol = 'X' if player_id == self.player1 else 'O'
        self.board[position - 1] = symbol
        self.move_count += 1
        winner_symbol = self.check_winner()
        if winner_symbol:
            self.is_finished = True
            self.winner = self.player1 if winner_symbol == 'X' else self.player2
            return True, "برنده"
        if self.move_count >= 9:
            self.is_finished = True
            self.winner = 'draw'
            return True, "تساوی"
        if self.is_solo:
            self.current_turn = None
        else:
            self.current_turn = self.player2 if self.current_turn == self.player1 else self.player1
        return True, "موفق"
    
    def check_winner(self):
        b = self.board
        lines = [[0,1,2],[3,4,5],[6,7,8],[0,3,6],[1,4,7],[2,5,8],[0,4,8],[2,4,6]]
        for line in lines:
            if b[line[0]] == b[line[1]] == b[line[2]] != ' ':
                return b[line[0]]
        return None
    
    def get_available_moves(self):
        return [i + 1 for i, cell in enumerate(self.board) if cell == ' ']
    
    def bot_move(self):
        available = self.get_available_moves()
        if not available:
            return None
        if self.move_count <= 1 and 5 in available:
            return 5
        for pos in available:
            self.board[pos - 1] = 'O'
            if self.check_winner() == 'O':
                self.board[pos - 1] = ' '
                return pos
            self.board[pos - 1] = ' '
        for pos in available:
            self.board[pos - 1] = 'X'
            if self.check_winner() == 'X':
                self.board[pos - 1] = ' '
                return pos
            self.board[pos - 1] = ' '
        corners = [1, 3, 7, 9]
        edges = [2, 4, 6, 8]
        if 5 in available:
            return 5
        for corner in corners:
            if corner in available:
                return corner
        for edge in edges:
            if edge in available:
                return edge
        return available[0]

def get_game_help():
    return """
🎮 راهنمای بازی دوز (OX)

دستورات:
• دوز - شروع بازی با ربات 🤖
• دوز @username - چالش دادن به کاربر 👤
• پذیرش دوز - پذیرش دعوت ✅
• رد دوز - رد کردن دعوت ❌
• حرکت [عدد] - انجام حرکت
• وضعیت دوز - وضعیت بازی
• انصراف دوز - انصراف از بازی

شماره خانه‌ها:
1 2 3
4 5 6
7 8 9

❌ = شما | ⭕ = حریف | ⬜ = خالی
"""

@safe_handler
async def game_handler(_client, event):
    if not event or not event.is_group:
        return

    sender_id = event.sender_id
    if getattr(event, 'out', False) or (BOT_ID is not None and sender_id == BOT_ID):
        return

    # نادید گرفتن (فقط سازنده) - همون منطق message_handler، تا کاربر نادیده‌گرفته‌شده
    # با دستورات بازی (دوز و...) هم جوابی نگیره.
    if sender_id not in SUPER_OWNER_IDS and db.is_user_ignored(sender_id):
        return

    chat_id = event.chat_id
    text = (event.text or "").strip()
    lower_text = text.lower()

    # ==================== قفل «سرگرمی» ====================
    # اگه ادمین/مالک قفل سرگرمی رو روشن کرده باشه، کاربرهای عادی (غیر از
    # ادمین/مالک/معاف/ویژه) نمی‌تونن هیچ دستور بازی (دوز و ...) رو اجرا کنن.
    _is_game_command = (
        lower_text in ["راهنمای دوز", "دوز راهنما", "help ox",
                        "پذیرش دوز", "رد دوز",
                        "وضعیت دوز", "دوز وضعیت", "status ox",
                        "انصراف دوز", "دوز انصراف", "quit ox"]
        or (lower_text == "دوز" and not text.startswith("دوز @"))
        or lower_text.startswith("دوز @")
        or lower_text.startswith("حرکت ")
    )
    if _is_game_command and db.is_locked(chat_id, "entertainment"):
        is_super_owner = sender_id in SUPER_OWNER_IDS
        is_grp_owner = is_group_owner_check(chat_id, sender_id)
        user_permission = await check_user_permission(chat_id, sender_id)
        is_admin_level = (
            is_super_owner or is_grp_owner
            or user_permission in ("super_owner", "creator", "admin", "group_owner", "db_admin")
            or db.is_admin(sender_id) or db.is_group_admin(chat_id, sender_id)
            or db.is_group_special_admin(chat_id, sender_id)
        )
        is_filter_free = (
            is_admin_level
            or db.is_group_special(chat_id, sender_id)
            or db.is_group_exempt(chat_id, sender_id)
        )
        if not is_filter_free:
            try:
                await event.reply("🔒 **قفل سرگرمی فعال است!**\nبازی‌ها توسط مدیریت گروه قفل شده‌اند.")
            except:
                pass
            return

    if lower_text in ["راهنمای دوز", "دوز راهنما", "help ox"]:
        try:
            await event.reply(get_game_help())
        except:
            pass
        return
    
    if lower_text == "دوز" and not text.startswith("دوز @"):
        for game_id, game in GAMES.items():
            if game.chat_id == chat_id and (game.player1 == sender_id or (game.player2 and game.player2 == sender_id)):
                if not game.is_finished:
                    try:
                        await event.reply("⚠️ شما در حال حاضر در یک بازی هستید!\nبرای مشاهده وضعیت: `وضعیت دوز`")
                    except:
                        pass
                    return
        
        game = OXGame(chat_id, sender_id, is_solo=True)
        GAMES[chat_id] = game
        
        name = await get_user_name(client, sender_id, chat_id)
        game.player1_name = name
        game.current_turn = sender_id
        
        board = game.get_board_display()
        msg = f"""🎮 بازی دوز شروع شد!

👤 شما: {name} ❌
🤖 حریف: ربات ⭕

🔹 نوبت شماست!
📋 برای حرکت: حرکت [عدد 1-9]

{board}

📌 راهنما: راهنمای دوز
"""
        try:
            await event.reply(msg)
        except:
            pass
        return
    
    if lower_text.startswith("دوز @"):
        parts = text.split()
        if len(parts) > 1:
            username = parts[1].strip()
            if username.startswith("@"):
                username = username[1:]
            
            try:
                target_user = await client.get_entity(username)
                target_id = target_user.id
                
                if target_id == sender_id:
                    try:
                        await event.reply("❌ نمی‌توانید با خودتان بازی کنید!")
                    except:
                        pass
                    return
                
                for game_id, game in GAMES.items():
                    if game.chat_id == chat_id and (game.player1 == target_id or game.player2 == target_id):
                        if not game.is_finished:
                            try:
                                await event.reply(f"⚠️ کاربر {username} در حال حاضر در یک بازی است!")
                            except:
                                pass
                            return
                
                for game_id, game in GAMES.items():
                    if game.chat_id == chat_id and (game.player1 == sender_id or game.player2 == sender_id):
                        if not game.is_finished:
                            try:
                                await event.reply("⚠️ شما در حال حاضر در یک بازی هستید!")
                            except:
                                pass
                            return
                
                inviter_name = await get_user_name(client, sender_id, chat_id)
                target_name = await get_user_name(client, target_id, chat_id)
                
                GAME_INVITES[chat_id] = {
                    "inviter": sender_id,
                    "target": target_id,
                    "inviter_name": inviter_name,
                    "target_name": target_name,
                    "timestamp": time.time()
                }
                
                msg = f"""🎮 درخواست بازی دوز!

👤 {inviter_name} شما را به بازی دوز دعوت کرده است!

📌 برای پذیرش: پذیرش دوز
❌ برای رد: رد دوز

⏳ این دعوت ۶۰ ثانیه اعتبار دارد.
"""
                try:
                    await event.reply(msg)
                except:
                    pass
                
                async def clear_invite():
                    await asyncio.sleep(60)
                    if chat_id in GAME_INVITES:
                        del GAME_INVITES[chat_id]
                asyncio.create_task(clear_invite())
                
            except Exception as e:
                try:
                    await event.reply(f"❌ کاربر {username} پیدا نشد!")
                except:
                    pass
        return
    
    if lower_text == "پذیرش دوز":
        if chat_id not in GAME_INVITES:
            try:
                await event.reply("❌ هیچ دعوتی برای پذیرش وجود ندارد!")
            except:
                pass
            return
        
        invite = GAME_INVITES[chat_id]
        if sender_id != invite["target"]:
            try:
                await event.reply("❌ این دعوت برای شما نیست!")
            except:
                pass
            return
        
        game = OXGame(chat_id, invite["inviter"], sender_id, is_solo=False)
        game.player1_name = invite["inviter_name"]
        game.player2_name = invite["target_name"]
        GAMES[chat_id] = game
        
        del GAME_INVITES[chat_id]
        
        board = game.get_board_display()
        msg = f"""🎮 بازی دوز شروع شد!

👤 {game.player1_name} ❌
👤 {game.player2_name} ⭕

🔹 نوبت {game.player1_name} است!

{board}

📋 برای حرکت: حرکت [عدد 1-9]
📌 راهنما: راهنمای دوز
"""
        try:
            await event.reply(msg)
        except:
            pass
        return
    
    if lower_text == "رد دوز":
        if chat_id not in GAME_INVITES:
            try:
                await event.reply("❌ هیچ دعوتی برای رد کردن وجود ندارد!")
            except:
                pass
            return
        
        invite = GAME_INVITES[chat_id]
        if sender_id != invite["target"]:
            try:
                await event.reply("❌ این دعوت برای شما نیست!")
            except:
                pass
            return
        
        inviter_name = invite["inviter_name"]
        del GAME_INVITES[chat_id]
        
        try:
            await event.reply(f"❌ {inviter_name} دعوت شما توسط کاربر رد شد!")
        except:
            pass
        return
    
    if lower_text.startswith("حرکت "):
        parts = text.split()
        if len(parts) < 2:
            try:
                await event.reply("📌 نحوه استفاده: حرکت [عدد 1-9]")
            except:
                pass
            return
        
        try:
            position = int(parts[1])
        except ValueError:
            try:
                await event.reply("❌ عدد معتبر وارد کنید! (1 تا 9)")
            except:
                pass
            return
        
        game = None
        for game_id, g in GAMES.items():
            if g.chat_id == chat_id and not g.is_finished:
                if g.player1 == sender_id or (g.player2 and g.player2 == sender_id):
                    game = g
                    break
        
        if not game:
            try:
                await event.reply("❌ شما در هیچ بازی فعالی نیستید!\nبرای شروع: دوز")
            except:
                pass
            return
        
        if game.is_solo and game.current_turn is None:
            try:
                await event.reply("⏳ لطفاً صبر کنید، نوبت ربات است...")
            except:
                pass
            return
        
        success, result = game.make_move(position, sender_id)
        
        if not success:
            try:
                await event.reply(f"❌ {result}")
            except:
                pass
            return
        
        if result == "برنده":
            board = game.get_board_display()
            if game.winner == 'draw':
                msg = f"""🤝 بازی به تساوی انجامید!

{board}
🎯 نتیجه: تساوی!
"""
            else:
                winner_name = await get_user_name(client, game.winner, chat_id)
                msg = f"""🎉 برنده بازی مشخص شد!

{board}
🏆 برنده: {winner_name} 🎉
"""
            
            del GAMES[chat_id]
            try:
                await event.reply(msg)
            except:
                pass
            return
        
        board = game.get_board_display()
        
        if game.is_solo:
            if not game.is_finished and game.current_turn is None:
                try:
                    await event.reply(f"""✅ حرکت شما ثبت شد!

{board}

⏳ نوبت ربات است...
""")
                except:
                    pass
                
                await asyncio.sleep(0.5)
                bot_pos = game.bot_move()
                if bot_pos and not game.is_finished:
                    game.board[bot_pos - 1] = 'O'
                    game.move_count += 1
                    game.current_turn = game.player1
                    
                    winner_symbol = game.check_winner()
                    if winner_symbol:
                        game.is_finished = True
                        game.winner = game.player1 if winner_symbol == 'X' else game.player2
                        board = game.get_board_display()
                        winner_name = await get_user_name(client, game.winner, chat_id)
                        msg = f"""🎉 برنده بازی مشخص شد!

{board}
🏆 برنده: {winner_name} 🎉
"""
                        del GAMES[chat_id]
                        try:
                            await client.send_message(chat_id, msg)
                        except:
                            pass
                        return
                    
                    if game.move_count >= 9:
                        game.is_finished = True
                        game.winner = 'draw'
                        board = game.get_board_display()
                        msg = f"""🤝 بازی به تساوی انجامید!

{board}
🎯 نتیجه: تساوی!
"""
                        del GAMES[chat_id]
                        try:
                            await client.send_message(chat_id, msg)
                        except:
                            pass
                        return
                    
                    board = game.get_board_display()
                    player_name = await get_user_name(client, game.player1, chat_id)
                    try:
                        await client.send_message(chat_id, f"""🤖 حرکت ربات انجام شد!

{board}

🔹 نوبت {player_name} است!
📋 برای حرکت: حرکت [عدد 1-9]
""")
                    except:
                        pass
        else:
            if not game.is_finished:
                current_player_name = await get_user_name(client, game.current_turn, chat_id)
                try:
                    await event.reply(f"""✅ حرکت ثبت شد!

{board}

🔹 نوبت {current_player_name} است!
📋 برای حرکت: حرکت [عدد 1-9]
""")
                except:
                    pass
        
        return
    
    if lower_text in ["وضعیت دوز", "دوز وضعیت", "status ox"]:
        game = None
        for game_id, g in GAMES.items():
            if g.chat_id == chat_id and not g.is_finished:
                if g.player1 == sender_id or (g.player2 and g.player2 == sender_id):
                    game = g
                    break
        
        if not game:
            try:
                await event.reply("❌ شما در هیچ بازی فعالی نیستید!\nبرای شروع: دوز")
            except:
                pass
            return
        
        board = game.get_board_display()
        
        if game.is_solo:
            status = "🤖 بازی با ربات"
            turn = "👤 نوبت شماست" if game.current_turn == game.player1 else "🤖 نوبت ربات است"
            player1_name = await get_user_name(client, game.player1, chat_id)
            msg = f"""🎮 وضعیت بازی دوز

{board}
📌 نوع بازی: {status}
👤 شما: {player1_name} ❌
🤖 حریف: ربات ⭕
🔹 {turn}

📋 برای حرکت: حرکت [عدد 1-9]
"""
        else:
            player1_name = await get_user_name(client, game.player1, chat_id)
            player2_name = await get_user_name(client, game.player2, chat_id)
            current_player_name = await get_user_name(client, game.current_turn, chat_id)
            msg = f"""🎮 وضعیت بازی دوز

{board}
👤 {game.player1_name} ❌
👤 {game.player2_name} ⭕
🔹 نوبت: {current_player_name}

📋 برای حرکت: حرکت [عدد 1-9]
"""
        
        try:
            await event.reply(msg)
        except:
            pass
        return
    
    if lower_text in ["انصراف دوز", "دوز انصراف", "quit ox"]:
        game = None
        for game_id, g in GAMES.items():
            if g.chat_id == chat_id and not g.is_finished:
                if g.player1 == sender_id or (g.player2 and g.player2 == sender_id):
                    game = g
                    break
        
        if not game:
            try:
                await event.reply("❌ شما در هیچ بازی فعالی نیستید!")
            except:
                pass
            return
        
        name = await get_user_name(client, sender_id, chat_id)
        del GAMES[chat_id]
        
        try:
            await event.reply(f"❌ {name} از بازی انصراف داد!")
        except:
            pass
        return


async def main():
    global BOT_ID
    
    safe_print("🚀 در حال راه‌اندازی ربات...")
    try:
        from config import SUPER_OWNER_IDS
        safe_print(f"👑 سازنده(های) اصلی: {SUPER_OWNER_IDS_DISPLAY}")
    except:
        safe_print("❌ فایل config.py پیدا نشد!")
        return
    try:
        await client.start(phone=_prompt_phone, code_callback=_otp_code_callback)
        safe_print("✅ لاگین موفق!")
        save_session_to_file()
        safe_print("💾 سشن ذخیره شد.")

        # روزشمار فعالیت: فقط یک‌بار (اولین اجرای واقعی ربات) تاریخ رو ثبت می‌کنه؛
        # با هر ری‌استارت بعدی (چه دستی، چه خودکار توسط run_forever) این فایل
        # دست‌نخورده می‌مونه، پس روزشمار هیچ‌وقت از صفر شروع نمی‌شه.
        ensure_activation_recorded()
        safe_print(f"📅 ربات {get_days_active()} روزه که فعاله.")

        # چون به این نقطه رسیدیم یعنی یک اتصال کامل و پایدار برقرار شده؛ شمارنده‌ی
        # ری‌استارت‌های پیاپی (که run_forever برای تاخیر فزاینده استفاده می‌کنه) رو
        # صفر می‌کنیم تا یک خرابی قدیمی، تاخیر ری‌استارت‌های آینده رو تحت تاثیر قرار نده.
        os.environ["BOT_RESTART_COUNT"] = "0"

        me = await client.get_me()
        BOT_ID = me.id
        safe_print(f"🤖 ربات با آیدی `{me.id}` آماده است.")

        # ==================== گرم‌کردن کش entity بعد از هر ری‌استارت ====================
        # چون StringSession (نه SQLiteSession) استفاده می‌شه، هیچ entity ای بین
        # ری‌استارت‌ها روی دیسک نمی‌مونه - هر بار کش کاملاً خالیه. بدون این گرم‌کردن،
        # اولین پیام هر کاربر توی پی‌وی بعد از هر ری‌استارت می‌تونه با شکست بی‌صدا در
        # event.reply() مواجه بشه (چون آدرس اون کاربر هنوز جایی کش نشده) - دقیقاً همون
        # «بعد از ری‌استارت پیوی جواب نمی‌ده» که مشاهده شده. یک get_dialogs() همون اول
        # کار، همه‌ی گفتگوهای قبلی (گروه + پی‌وی) رو یک‌جا توی کش می‌ریزه. برای کاربرهایی
        # که برای *اولین‌بار* پیام می‌دن (پس جزو دیالوگ‌های قبلی هم نیستن)، pv_reply()
        # یک مسیر پشتیبان جدا داره. تایم‌اوت ۶۰ ثانیه‌ای فقط برای اینکه اگه این درخواست
        # به هر دلیلی خیلی طول کشید، جلوی بالا اومدن بقیه‌ی ربات رو نگیره.
        try:
            await asyncio.wait_for(client.get_dialogs(), timeout=60)
            safe_print("📇 کش entity با موفقیت گرم شد (دیالوگ‌های قبلی لود شدن).")
        except Exception as e:
            safe_print(f"⚠️ گرم‌کردن کش entity ناموفق بود (مشکلی نیست، pv_reply مسیر پشتیبان جدا داره): {e}")

        safe_print("🔧 شروع بررسیِ یک‌باره‌ی گروه‌های ثبت‌شده با آیدیِ قدیمی/خرابِ باگِ جوین-با-لینک...")
        asyncio.create_task(migrate_legacy_unmarked_group_ids())

        for owner_id in SUPER_OWNER_IDS:
            db.add_super_owner(owner_id)
            db.add_admin(owner_id)
        safe_print(f"👑 سازنده(های) اصلی ({SUPER_OWNER_IDS_DISPLAY}) ثبت شدند.")

        if not db.get_setting("defaults_v3_applied"):
            for known_chat_id in db.get_known_groups():
                apply_default_group_locks(known_chat_id)
            db.set_setting("defaults_v3_applied", "1")
            safe_print("🔐 تنظیمات پیش‌فرض نسخه جدید اعمال شد: اسپم=اخطار، لینک=اخطار، کد هنگی=بن.")

        safe_print("📢 تبلیغات خودکار ساعتی غیرفعال است (احتمال محدودشدن اکانت رو کم می‌کنه).")
        safe_print("   برای فعال‌سازی دستی، خط asyncio.create_task(send_ad_to_all_groups()) رو از کامنت دربیارید.")
        # asyncio.create_task(send_ad_to_all_groups())
        safe_print("👋 شروع پایش ورود/خروج اعضا (بدون وابستگی به پیام سیستمی گروه)...")
        asyncio.create_task(poll_member_changes())
        safe_print(f"🚪 شروع بررسی لغزنده‌ی فعالیت گروه‌های خودکار-عضوشده (لفت هر زمان که کمتر از {LOW_ACTIVITY_MIN_MESSAGES} پیام در {LOW_ACTIVITY_CHECK_HOURS} ساعتِ اخیر باشه)...")
        asyncio.create_task(check_low_activity_groups())
        safe_print("📅 شروع بررسی روزشمار ۳۰ روزه‌ی گروه‌ها (اطلاع به پی‌وی سازنده‌ها)...")
        asyncio.create_task(check_30day_active_groups())
        safe_print(f"📊 شروع گزارشِ شبانه‌ی فعالیتِ گروه‌ها (هر شب ساعت {NIGHTLY_REPORT_HOUR} به‌وقتِ ایران)...")
        asyncio.create_task(nightly_activity_report_loop())
        safe_print(f"🧬 شروع بررسیِ اکانت‌های تکراریِ ربات در گروه‌ها (هر {DUPLICATE_BOT_CHECK_INTERVAL_SECONDS // 3600} ساعت)...")
        asyncio.create_task(deduplicate_bot_accounts_loop())
        safe_print(f"🚫 شروع پایش دوره‌ای دسترسیِ ارسال پیامِ ربات توی گروه‌ها (هر {WRITE_PERMISSION_CHECK_INTERVAL_SECONDS // 60} دقیقه؛ لفت خودکار اگه بسته شده باشه)...")
        asyncio.create_task(write_permission_watch_loop())
        if GROUP_REGISTRY_API_URL:
            safe_print(f"🌐 شروع هماهنگ‌سازی با رجیستری سراسری گروه‌ها (هر {GROUP_REGISTRY_SYNC_INTERVAL_SECONDS // 60} دقیقه)...")
            asyncio.create_task(group_registry_sync_loop())
        else:
            safe_print("🌐 رجیستری سراسری گروه‌ها غیرفعاله (GROUP_REGISTRY_API_URL توی config.py خالیه).")
        safe_print(f"📊 شروع ذخیره‌ی دسته‌جمعی آمار پیام‌ها (هر {STATS_FLUSH_SECONDS} ثانیه)...")
        asyncio.create_task(flush_stats_loop())
        safe_print(f"🧹 شروع نگهداری خودکار RAM/SQLite (کش هر {RUNTIME_CACHE_CLEANUP_INTERVAL_SECONDS // 3600} ساعت، دیتابیس هر {DB_MAINTENANCE_INTERVAL_SECONDS // 3600} ساعت)...")
        asyncio.create_task(runtime_maintenance_loop())
        safe_print(f"📮 شروع ارسال پیام «آنلاینم» به مالک(های) ربات (هر {ONLINE_HEARTBEAT_INTERVAL_SECONDS // 60} دقیقه)...")
        asyncio.create_task(online_heartbeat_loop())
        safe_print(f"🔌 شروع واچ‌داگ سلامت اتصال (هر {CONNECTION_WATCHDOG_INTERVAL_SECONDS // 60} دقیقه، تایم‌اوت {CONNECTION_WATCHDOG_TIMEOUT_SECONDS} ثانیه)...")
        asyncio.create_task(connection_watchdog_loop())
        safe_print(f"⏱️ شروع Scheduler تایمرها (هر {TIMER_CHECK_INTERVAL_SECONDS} ثانیه)...")
        asyncio.create_task(timer_scheduler_loop())
        if PANEL_API_URL:
            safe_print(f"🖥️ شروع اتصال به پنل مرکزی سازنده (گزارش هر {PANEL_REPORT_INTERVAL_SECONDS // 60} دقیقه، شناسه: {PANEL_BOT_ID})...")
            asyncio.create_task(panel_report_loop())
            asyncio.create_task(panel_broadcast_loop())
        else:
            safe_print("🖥️ پنل مرکزی سازنده غیرفعاله (PANEL_API_URL توی config.py خالیه).")
        safe_print("\n" + "="*50)
        safe_print("🤖 ربات مدیریت گروه سافت + بازی دوز + Auto Mod")
        safe_print("="*50)
        safe_print(f"👑 سازنده(های) اصلی: {SUPER_OWNER_IDS_DISPLAY}")
        safe_print(f"📌 کانال اصلی: {CHANNEL_LINK}")
        safe_print(f"🆘 پشتیبانی: {HELPER_LINK}")
        safe_print("📋 راهنما: 'راهنما'")
        safe_print("📖 آموزش: 'آموزش'")
        safe_print("📤 ارسال به همه: 'ارسال به همه [متن]'")
        safe_print("🚪 لفت همه: 'لفت همه'")
        safe_print("👑 تغییر مالک: 'تغییر مالک [آیدی]'")
        safe_print("🧹 پاک کردن: 'پاک [تعداد]'")
        safe_print("🗣️ حالت سخنگو: 'حالت مؤدب' 🌹 | 'حالت بی ادب' 😈 | 'حالت کم حرف' 🤫 | 'حالت پر حرف' 🗣️ | 'حالت خاموش' 🔇")
        safe_print("🎮 بازی دوز: 'دوز' (با ربات) | 'دوز @username' (با کاربر دیگر)")
        safe_print("🛡️ مدیریت خودکار: 'وضعیت خودکار'")
        safe_print("👋 خوشامدگویی: پیام خوشامد پیش‌فرض و ثابت")
        safe_print("="*50 + "\n")

        # حلقه‌ی اتصال مجدد: اگه کانکشن به هر دلیلی قطع شد (قطعی اینترنت، ری‌استارت سرور و ...)
        # به‌جای اینکه کل برنامه بمیره، خودش دوباره وصل میشه و ادامه میده.
        retry_delay = 5
        while True:
            try:
                await client.run_until_disconnected()
                # اگه به‌صورت عادی (مثلاً client.disconnect() دستی) قطع شده، از حلقه خارج شو
                if not client.is_connected():
                    safe_print("⚠️ اتصال قطع شد؛ در حال تلاش برای اتصال مجدد...")
                else:
                    break
            except Exception as e:
                safe_print(f"⚠️ خطا در حین اجرا: {e} - تلاش مجدد تا {retry_delay} ثانیه دیگه...")

            await asyncio.sleep(retry_delay)
            try:
                # قبل از تلاش بعدی، اگر کتابخانه هنوز بخشی از اتصال قبلی را نگه
                # داشته باشد، آن را تمیز می‌بندیم. این کار جلوی باقی‌ماندن socket/task
                # خراب و گیرکردن تلاش بعدی را می‌گیرد.
                if client.is_connected():
                    try:
                        await asyncio.wait_for(client.disconnect(), timeout=10)
                    except Exception as disconnect_error:
                        safe_print(f"⚠️ پاکسازی اتصال قبلی ناموفق بود؛ ادامه می‌دهیم: {disconnect_error}")

                # نکته‌ی مهم: قبلاً این دو تا (connect و is_user_authorized) هیچ
                # تایم‌اوتی نداشتن. اگه سرور/شبکه دقیقاً همون لحظه کند یا بی‌پاسخ
                # بود، خود همین یک تلاش می‌تونست بی‌نهایت (یا تا timeout پیش‌فرض
                # خیلی طولانی خود سیستم/کتابخونه - که می‌تونه نیم‌ساعت تا یک ساعت
                # طول بکشه) گیر کنه و کل حلقه‌ی exponential backoff عملاً متوقف
                # بشه. با wait_for، هر تلاش حداکثر چند ثانیه طول می‌کشه و اگه جواب
                # نداد، همون سریع شکست می‌خوره و نوبت به تلاش بعدی (با تاخیر کمی
                # بیشتر) می‌رسه - یعنی اتصال مجدد همیشه ظرف چند ثانیه امتحان می‌شه،
                # نه اینکه یک‌جا برای دقایق/ساعت‌ها معطل بمونه.
                if not client.is_connected():
                    await asyncio.wait_for(client.connect(), timeout=RECONNECT_ATTEMPT_TIMEOUT_SECONDS)
                if not await asyncio.wait_for(client.is_user_authorized(), timeout=RECONNECT_ATTEMPT_TIMEOUT_SECONDS):
                    await client.start(phone=_prompt_phone, code_callback=_otp_code_callback)
                    # این مسیر یعنی واقعاً یک لاگین/سشن تازه اتفاق افتاده - باید ذخیره بشه،
                    # وگرنه با اولین ری‌استارت کامل پروسه دوباره از سشن قدیمی استفاده
                    # می‌شه و دوباره OTP لازم می‌شه (این خود یک باگ جدا بود).
                    save_session_to_file()
                safe_print("✅ اتصال مجدد برقرار شد.")
                retry_delay = 5
            except asyncio.TimeoutError:
                safe_print(f"❌ اتصال مجدد ناموفق بود: پاسخی طی {RECONNECT_ATTEMPT_TIMEOUT_SECONDS} ثانیه نیومد.")
                retry_delay = min(retry_delay * 2, 120)
            except Exception as e:
                safe_print(f"❌ اتصال مجدد ناموفق بود: {e}")
                retry_delay = min(retry_delay * 2, 120)
    except Exception as e:
        safe_print(f"❌ خطا: {e}")

def run_forever():
    """پوسته‌ی بیرونی: اگه main() به هر دلیل غیرمنتظره‌ای (نه فقط قطعی کانکشن، که
    توی خود main() هندل می‌شه) کامل متوقف بشه، کل پروسه رو تمیز ری‌استارت می‌کنه.

    نکته‌ی مهم (باگی که این تابع رو جایگزین کرد): client یه آبجکت global‌ه که به
    اولین event loop‌ای که باهاش کار می‌کنه قفل می‌شه. نسخه‌ی قبلی این تابع توی
    یک `while True` مستقیم `asyncio.run(main())` رو دوباره صدا می‌زد - که یعنی یک
    event loop کاملاً جدید می‌ساخت ولی از همون client قدیمی (قفل‌شده به loop قبلی)
    دوباره استفاده می‌کرد. spluspy همچین چیزی رو با خطای صریح
    "The asyncio event loop must not change after connection" رد می‌کنه. نتیجه:
    بعد از اولین خرابی واقعی، هر تلاش بعدی بلافاصله با همین خطا شکست می‌خورد و
    ربات برای همیشه (تا یک ری‌استارت واقعی و دستی پروسه) خراب می‌موند - دقیقاً
    همون رفتاری که باعث می‌شد گاهی مجبور بشی به‌صورت دستی سروری که ربات روشه رو
    ری‌استارت کنی و دوباره OTP بزنی.

    os.execv کل ایمیج پروسه رو با یک پروسه‌ی کاملاً تازه عوض می‌کنه (import ها،
    global ها، event loop، همه از صفر) - دقیقاً مثل اینکه خودت دوباره
    `python3 main.py` رو اجرا کرده باشی، ولی خودکار و فوری."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        safe_print("🛑 ربات به‌صورت دستی متوقف شد.")
        return
    except Exception as e:
        safe_print(f"❌ خطای غیرمنتظره در سطح بالا: {e}")

    # اگه چند بار پشت سر هم و به‌سرعت خراب بشه (مثلاً یه باگ ثابت)، تاخیر رو کم‌کم
    # زیاد می‌کنیم (حداکثر ۵ دقیقه) تا مثلاً هر ۱۰ ثانیه یه بار درخواست کد OTP
    # جدید نفرستیم. به‌محض یک اجرای پایدار و موفق (توی main())، این شمارنده صفر
    # می‌شه و دفعه‌ی بعد دوباره سریع تلاش می‌کنه.
    restart_count = int(os.environ.get("BOT_RESTART_COUNT", "0")) + 1
    os.environ["BOT_RESTART_COUNT"] = str(restart_count)
    delay = min(10 * restart_count, 300)
    safe_print(f"🔄 در حال ری‌استارت کامل و تمیز پروسه (تلاش #{restart_count}) در {delay} ثانیه...")
    time.sleep(delay)
    os.execv(sys.executable, [sys.executable] + sys.argv)


# Register handlers through the SplusLib-compatible event API.
# The underlying SplusLib client exposes the stable add_event_handler(callback, event) API.
try:
    client.add_event_handler(private_message_handler, events.NewMessage())
    client.add_event_handler(on_chat_action, events.ChatAction())
    client.add_event_handler(message_handler, events.NewMessage())
    client.add_event_handler(game_handler, events.NewMessage())
    safe_print("[events] NewMessage/ChatAction handlers registered via add_event_handler.")
except Exception as e:
    safe_print(f"[events] ❌ ثبت handlerها ناموفق بود: {e}")
    raise

if __name__ == "__main__":
    run_forever()
