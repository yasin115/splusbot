import re
import asyncio
import time
from collections import defaultdict
import random
import gc

import aiohttp

import os
from datetime import datetime

from config import WELCOME_TEXT, ACTIVATION_FILE
from database import db


def safe_print(*args, **kwargs):
    """جایگزین امن برای print(): روی خیلی از هاست‌ها (اجرا با nohup/systemd/screen و ...)
    وقتی stdout به هر دلیلی قطع/بسته بشه، خود print() یک OSError با متن
    '[Errno 5] Input/output error' پرت می‌کنه. چون بخش زیادی از کد (ارسال به همه،
    لفت همه، تبلیغ خودکار و ...) از print فقط برای گزارش وضعیت توی کنسول استفاده
    می‌کنه، یک خطای چاپ نباید کل عملیات واقعی (ارسال/خروج از گروه) رو با یک پیام
    خطای گمراه‌کننده متوقف کنه. این تابع همون کار print رو انجام می‌ده ولی هر
    خطایی حین چاپ رو می‌بلعه (و در صورت امکان توی یک فایل لاگ ذخیره می‌کنه)."""
    try:
        print(*args, **kwargs)
    except Exception:
        try:
            with open("bot_log.txt", "a", encoding="utf-8") as f:
                f.write(" ".join(str(a) for a in args) + "\n")
        except Exception:
            pass

MESSAGE_CACHE = defaultdict(dict)
USERNAME_CACHE = {}
spam_tracker = defaultdict(list)

# حداکثر تعداد پیام کش‌شده به ازای هر گروه؛ قبلاً این کش هیچ سقفی نداشت و برای
# همیشه رشد می‌کرد (هر پیام هر گروه، تا ابد توی حافظه می‌موند) که با گذشت زمان
# باعث مصرف فزاینده‌ی رم و در نهایت کرش/خاموش‌شدن ربات روی هاست می‌شد. حالا با
# رسیدن به این سقف، قدیمی‌ترین پیام‌های همون گروه از حافظه حذف می‌شن.
MESSAGE_CACHE_MAX_PER_CHAT = 300
USERNAME_CACHE_MAX = 20000
SPAM_TRACKER_MAX_USERS = 10000

def cache_message(chat_id, msg_id, sender_id, text=None):
    if chat_id and msg_id and sender_id:
        chat_cache = MESSAGE_CACHE[chat_id]
        chat_cache[msg_id] = (sender_id, text)
        if len(chat_cache) > MESSAGE_CACHE_MAX_PER_CHAT:
            # قدیمی‌ترین ورودی‌ها رو حذف کن تا اندازه کش این گروه از سقف بیشتر نشه
            for old_id in list(chat_cache.keys())[:len(chat_cache) - MESSAGE_CACHE_MAX_PER_CHAT]:
                del chat_cache[old_id]

def get_cached_sender(chat_id, msg_id):
    cached = MESSAGE_CACHE.get(chat_id, {}).get(msg_id)
    return cached[0] if cached else None

def get_recent_sender_message_ids(chat_id, sender_id, exclude_msg_id=None, limit=3):
    """آخرین `limit` آیدی پیامِ همین کاربر توی همین گروه رو از کش برمی‌گردونه (جدیدترین اول)،
    برای پاکسازی خودکار پیام‌های قبلی یک اسپم‌دهنده/تبلیغ‌چی (متن، عکس یا گیف - فرقی نداره،
    چون کش بر اساس sender_id کار می‌کنه نه نوع پیام). exclude_msg_id معمولاً همون پیامی‌ه
    که خودش جداگانه با event.delete() پاک می‌شه، پس اینجا لازم نیست دوباره برگردونده بشه."""
    chat_cache = MESSAGE_CACHE.get(chat_id, {})
    ids = [mid for mid, (sid, _) in chat_cache.items() if sid == sender_id and mid != exclude_msg_id]
    ids.sort(reverse=True)
    return ids[:limit]

# ==================== شناسه‌ی رسانه (برای «فیلتر رسانه») ====================
# کش سبک: شناسه‌ی رسانه‌ی هر پیام (فقط پیام‌های رسانه‌ای) تا وقتی ادمین روی یک گیف/ویدیو/...
# ریپلای می‌کنه، بدون نیاز به RPC (که روی سروش گاهی NOT_SUPPORTED می‌ده) شناسه‌اش رو داشته باشیم.
MEDIA_INFO_CACHE = defaultdict(dict)
MEDIA_INFO_CACHE_MAX_PER_CHAT = 300


def _safe_attr(obj, name):
    try:
        return getattr(obj, name, None)
    except Exception:
        return None


def extract_media_info(msg):
    """از یک پیام/ایونت، هویت رسانه‌اش رو درمیاره؛ اگه پیام رسانه نداشت None.

    خروجی: {"kind": "gif"|"video"|..., "label": "گیف"|..., "key": "d<document_id>" یا
    "p<photo_id>", "fp": اثر انگشت (حجم|نوع|مدت|عرض|ارتفاع) یا ""}.
    ترتیب چک مهمه: گیف‌ها روی تلگرام/سروش در واقع mp4 هستن و ممکنه property «video» هم
    براشون truthy باشه؛ پس استیکر و گیف قبل از ویدیو بررسی می‌شن.
    """
    if msg is None:
        return None
    kind = label = None
    doc = None
    for attr, k, lb in (
        ("sticker", "sticker", "استیکر"),
        ("gif", "gif", "گیف"),
        ("video_note", "video_note", "ویدیو مسیج"),
        ("video", "video", "ویدیو"),
        ("voice", "voice", "ویس"),
        ("audio", "audio", "آهنگ"),
    ):
        obj = _safe_attr(msg, attr)
        if obj:
            kind, label = k, lb
            doc = obj if _safe_attr(obj, "id") else _safe_attr(msg, "document")
            break

    photo = None
    if doc is None:
        photo = _safe_attr(msg, "photo")
        if photo and _safe_attr(photo, "id"):
            kind, label = "photo", "عکس"
        else:
            photo = None
            doc = _safe_attr(msg, "document")
            if doc and _safe_attr(doc, "id"):
                kind, label = "file", "فایل"
            else:
                return None

    try:
        if doc is not None:
            doc_id = _safe_attr(doc, "id")
            if not doc_id:
                return None
            key = f"d{doc_id}"
            size = int(_safe_attr(doc, "size") or 0)
            mime = str(_safe_attr(doc, "mime_type") or "")
            dur = w = h = 0
            for a in (_safe_attr(doc, "attributes") or []):
                dur = dur or (_safe_attr(a, "duration") or 0)
                w = w or (_safe_attr(a, "w") or 0)
                h = h or (_safe_attr(a, "h") or 0)
            # حجم دقیق به بایت + نوع + مدت + ابعاد؛ زیر ۱ کیلوبایت به‌خاطر احتمال برخورد اشتباه استفاده نمی‌شه.
            fp = f"{size}|{mime}|{int(round(float(dur)))}|{int(w)}|{int(h)}" if size >= 1024 else ""
        else:
            key = f"p{_safe_attr(photo, 'id')}"
            fp = ""
    except Exception:
        return None
    return {"kind": kind, "label": label, "key": key, "fp": fp}


def cache_media_info(chat_id, msg_id, info):
    if chat_id and msg_id and info:
        chat_cache = MEDIA_INFO_CACHE[chat_id]
        chat_cache[msg_id] = info
        if len(chat_cache) > MEDIA_INFO_CACHE_MAX_PER_CHAT:
            for old_id in list(chat_cache.keys())[:len(chat_cache) - MEDIA_INFO_CACHE_MAX_PER_CHAT]:
                del chat_cache[old_id]


async def get_replied_media_info(event, client):
    """هویت رسانه‌ی پیامِ ریپلای‌شده؛ اول از کش داخلی، بعد از خود کتابخانه/API. یا None."""
    chat_id = getattr(event, 'chat_id', None)
    reply_msg_id = (
        getattr(event, 'reply_to_msg_id', None)
        or getattr(event, 'reply_to_message_id', None)
        or getattr(event, 'reply_msg_id', None)
        or getattr(getattr(event, 'reply_to', None), 'reply_to_msg_id', None)
        or getattr(getattr(event, 'message', None), 'reply_to_msg_id', None)
    )
    if reply_msg_id and chat_id:
        cached = MEDIA_INFO_CACHE.get(chat_id, {}).get(reply_msg_id)
        if cached:
            return cached
    if hasattr(event, 'get_reply_message'):
        try:
            reply = await event.get_reply_message()
            info = extract_media_info(reply)
            if info:
                if chat_id and getattr(reply, 'id', None):
                    cache_media_info(chat_id, reply.id, info)
                return info
        except Exception:
            pass
    if reply_msg_id and chat_id:
        try:
            msgs = await client.get_messages(chat_id, ids=reply_msg_id)
            if isinstance(msgs, (list, tuple)):
                msgs = msgs[0] if msgs else None
            info = extract_media_info(msgs)
            if info:
                cache_media_info(chat_id, reply_msg_id, info)
                return info
        except Exception:
            pass
    return None


def get_cached_text(chat_id, msg_id):
    """متن پیامی که قبلاً کش شده رو برمی‌گردونه (یا None). چون این پلتفرم (سروش) گاهی
    درخواست GetMessagesRequest (برای گرفتن یه پیام قدیمی با آیدیش) رو ساپورت نمی‌کنه
    و با خطای RPC مواجه می‌شه، برای فیچرهایی مثل «ثبت اصل» که به متن پیام ریپلای‌شده
    نیاز دارن، به‌جای رفتن سراغ API از همین کش داخلی (که موقع رسیدن هر پیام پر می‌شه)
    استفاده می‌کنیم که همیشه در دسترسه و به RPC نیاز نداره."""
    cached = MESSAGE_CACHE.get(chat_id, {}).get(msg_id)
    return cached[1] if cached else None

def extract_user_id(text):
    if not text:
        return None
    match = re.search(r'(\d{5,})', text)
    return int(match.group(1)) if match else None

# الگوی یوزرنیم: با یا بدون @ جلوش، حرف اول باید حرف باشه (مطابق قانون یوزرنیم
# تلگرام/سروش)، طول کل بین ۴ تا ۳۲ کاراکتر (بدون احتساب @).
_USERNAME_TOKEN_RE = re.compile(r'@([A-Za-z][A-Za-z0-9_]{3,31})\b')
_BARE_USERNAME_RE = re.compile(r'^[A-Za-z][A-Za-z0-9_]{3,31}$')

def extract_username(text):
    """اولین یوزرنیمِ باقاعده (با @ جلوش) که توی متن دستور اومده رو برمی‌گردونه،
    بدون علامت @. اگه چیزی پیدا نشد None برمی‌گردونه."""
    if not text:
        return None
    match = _USERNAME_TOKEN_RE.search(text)
    return match.group(1) if match else None

def is_username_token(token):
    """چک می‌کنه که یک تکه‌متنِ تنها (بدون فاصله) یوزرنیمِ باقاعده‌ست یا نه؛
    چه با @ جلوش نوشته شده باشه چه بدون @."""
    if not token:
        return False
    token = token.strip()
    if token.startswith('@'):
        token = token[1:]
    return bool(_BARE_USERNAME_RE.match(token))

async def resolve_id_or_username(client, token):
    """یک تکه‌متن (آیدی عددی، یا یوزرنیم با/بدون @) رو می‌گیره و آیدی عددیِ واقعیِ
    کاربر رو برمی‌گردونه؛ برای یوزرنیم، از client.get_entity برای رزولوکردن
    استفاده می‌کنه. اگه چیزی پیدا/رزولو نشد None برمی‌گردونه."""
    if not token:
        return None
    token = token.strip()
    if token.isdigit():
        return int(token)
    if not is_username_token(token):
        return None
    username = token[1:] if token.startswith('@') else token
    try:
        entity = await client.get_entity(username)
        return getattr(entity, 'id', None)
    except Exception:
        return None

async def get_replied_message_info(event, client):
    """برخلاف مصرف مستقیم event.get_reply_message()، این تابع هم آیدی فرستنده و هم
    متنِ پیامِ ریپلای‌شده رو با همون منطق مقاوم get_target_user برمی‌گردونه: چون این
    پلتفرم (سروش) بعضی وقتا درخواست GetMessagesRequest رو با خطای RPC (مثلاً
    NOT_SUPPORTED) رد می‌کنه و get_reply_message ممکنه به همین خاطر همیشه شکست
    بخوره، اول کش داخلی (که موقع رسیدن هر پیام پر می‌شه) رو امتحان می‌کنه که به هیچ
    RPC ای نیاز نداره؛ اگه اونجا هم چیزی نبود، به‌عنوان آخرین تلاش سراغ خود کتابخانه
    و API می‌ره. خروجی: (sender_id, text) یا (None, None)."""
    chat_id = getattr(event, 'chat_id', None)
    reply_msg_id = (
        getattr(event, 'reply_to_msg_id', None)
        or getattr(event, 'reply_to_message_id', None)
        or getattr(event, 'reply_msg_id', None)
        or getattr(getattr(event, 'reply_to', None), 'reply_to_msg_id', None)
        or getattr(getattr(event, 'message', None), 'reply_to_msg_id', None)
    )

    # اول کش داخلی (بدون نیاز به RPC)
    cached_sender, cached_text = None, None
    if reply_msg_id and chat_id:
        cached_sender = get_cached_sender(chat_id, reply_msg_id)
        cached_text = get_cached_text(chat_id, reply_msg_id)
        if cached_sender and cached_text:
            return cached_sender, cached_text

    # بعد خود کتابخانه (ممکنه روی این پلتفرم کار نکنه، ولی امتحانش ضرری نداره)
    if hasattr(event, 'get_reply_message'):
        try:
            reply = await event.get_reply_message()
            if reply:
                sender_id = getattr(reply, 'sender_id', None) or getattr(getattr(reply, 'sender', None), 'id', None)
                reply_text = getattr(reply, 'text', None)
                reply_id = getattr(reply, 'id', None)
                if chat_id and reply_id and sender_id:
                    cache_message(chat_id, reply_id, sender_id, reply_text)
                if sender_id and reply_text:
                    return sender_id, reply_text
                # شاید حداقل فرستنده پیدا شده باشه ولی متن نه (یا برعکس)؛ با هرچی از کش داریم ترکیبش کن
                if sender_id or reply_text:
                    return sender_id or cached_sender, reply_text or cached_text
        except:
            pass

    # آخرین تلاش: یه کوئری مستقیم (ممکنه NOT_SUPPORTED بده، ولی وقتی کار می‌کنه مفیده)
    if reply_msg_id and chat_id:
        try:
            messages = await client.get_messages(chat_id, ids=reply_msg_id)
            reply = messages[0] if messages else None
            if reply:
                sender_id = getattr(reply, 'sender_id', None) or getattr(getattr(reply, 'sender', None), 'id', None)
                reply_text = getattr(reply, 'text', None)
                if sender_id and reply_text:
                    cache_message(chat_id, reply_msg_id, sender_id, reply_text)
                    return sender_id, reply_text
        except:
            pass

    return None, None

def is_bare_admin_command(lower_text, *commands):
    """تشخیص می‌ده که آیا متن پیام (لاوِرکِیس‌شده) دقیقاً یکی از این دستورهای مدیریتیه
    یا نه - نه اینکه فقط با اون *شروع* بشه. برای دستورهایی که هدفشون (کاربر مقصد) از
    طریق ریپلای یا آیدی عددی صریح مشخص می‌شه (بن، سکوت، اخطار و مشابه اونا) استفاده
    می‌شه، تا یک جمله‌ی معمولی که فقط کلمه‌ی اول‌ش تصادفاً با اسم یک دستور یکیه
    (مثلاً «سکوت بود انگار توی خونه») هیچ‌وقت به‌عنوان دستور اجرا نشه.

    سه حالت قبول می‌شه:
    1) خود متن دقیقاً برابر یکی از commands باشه (دستور «خالی»، هدف از ریپلای میاد).
    2) متن برابر «دستور + یک فاصله + فقط رقم» باشه (آیدی عددی صریح، مثل «سکوت 123456789»).
    3) متن برابر «دستور + یک فاصله + یک یوزرنیم» باشه، با یا بدون @ جلوش
       (مثل «سکوت @username» یا «سکوت username»).
    هر حالت دیگه‌ای (هر متن یا کلمه‌ی اضافه‌ی دیگه بعد از دستور) رد می‌شه."""
    lower_text = (lower_text or "").strip()
    for cmd in commands:
        cmd = cmd.strip()
        if lower_text == cmd:
            return True
        prefix = cmd + " "
        if lower_text.startswith(prefix):
            rest = lower_text[len(prefix):].strip()
            if rest.isdigit():
                return True
            if is_username_token(rest):
                return True
    return False


async def get_target_user(event, client):
    text = getattr(event, 'text', '') or ''
    chat_id = getattr(event, 'chat_id', None)

    # روش اول (مطمئن‌ترین): مستقیم از خود کتابخانه بخوایم پیام ریپلای‌شده رو بده،
    # صرف‌نظر از اینکه بتونیم اسم درست attribute آیدی ریپلای رو حدس بزنیم یا نه.
    # قبلاً این تابع اول سعی می‌کرد با حدس زدن اسم‌های مختلف (reply_to_msg_id و...)
    # تشخیص بده پیام ریپلایه یا نه، و فقط اگه تشخیص می‌داد، get_reply_message رو صدا
    # می‌زد؛ همین حدس اشتباه باعث می‌شد خیلی وقت‌ها با اینکه پیام واقعاً ریپلای بود،
    # اصلاً سراغ get_reply_message نره و کاربر رو پیدا نکنه.
    if hasattr(event, 'get_reply_message'):
        try:
            reply = await event.get_reply_message()
            if reply:
                sender_id = getattr(reply, 'sender_id', None) or getattr(getattr(reply, 'sender', None), 'id', None)
                if sender_id:
                    reply_msg_id = getattr(reply, 'id', None)
                    if chat_id and reply_msg_id:
                        cache_message(chat_id, reply_msg_id, sender_id)
                    return sender_id, getattr(getattr(reply, 'sender', None), 'username', None)
        except:
            pass

    # روش دوم: یوزرنیم صریحی که توی متن دستور نوشته شده (مثل «بن @username»).
    # عمداً قبل از استخراج آیدی عددی چک می‌شه، چون ممکنه خودِ یوزرنیم شامل رقم
    # باشه (مثل user12345) و نباید اون رقم‌ها به‌اشتباه به‌عنوان آیدی عددی گرفته بشن.
    username = extract_username(text)
    if username:
        try:
            entity = await client.get_entity(username)
            target_id = getattr(entity, 'id', None)
            if target_id:
                return target_id, getattr(entity, 'username', username)
        except Exception:
            pass

    # روش سوم: آیدی عددی که مستقیم توی متن دستور نوشته شده (مثل «بن 123456789»)
    user_id = extract_user_id(text)
    if user_id:
        return user_id, None

    # روش چهارم (پشتیبان): شناسه‌ی پیام ریپلای‌شده رو با چند اسم محتمل پیدا کن و از کش
    # داخلی یا با یک کوئری مستقیم بگیر؛ برای مواقعی که get_reply_message در دسترس
    # نبود یا خودش هم نتیجه‌ای نداد.
    reply_msg_id = (
        getattr(event, 'reply_to_msg_id', None)
        or getattr(event, 'reply_to_message_id', None)
        or getattr(event, 'reply_msg_id', None)
        or getattr(getattr(event, 'reply_to', None), 'reply_to_msg_id', None)
        or getattr(getattr(event, 'message', None), 'reply_to_msg_id', None)
    )

    if reply_msg_id and chat_id:
        cached = get_cached_sender(chat_id, reply_msg_id)
        if cached:
            return cached, None
        try:
            messages = await client.get_messages(chat_id, ids=reply_msg_id)
            reply = messages[0] if messages else None
            if reply:
                sender_id = getattr(reply, 'sender_id', None) or getattr(getattr(reply, 'sender', None), 'id', None)
                if sender_id:
                    cache_message(chat_id, reply_msg_id, sender_id)
                    return sender_id, getattr(getattr(reply, 'sender', None), 'username', None)
        except:
            pass

    # عمداً هیچ تلاش دیگه‌ای نمی‌کنیم. قبلاً اینجا یک حدس آخر می‌زدیم: هر چند کلمه از
    # ابتدای متن رو حذف می‌کردیم و باقیمانده رو با لقب‌های ثبت‌شده‌ی اعضای گروه مقایسه
    # می‌کردیم (مثلاً برای «اخطار قهرمان»). مشکل این بود که این حدس روی هر پیام عادی
    # هم اجرا می‌شد؛ اگه کسی توی گروه یک جمله‌ی معمولی می‌نوشت که با کلمه‌ی یک دستور
    # (مثل «سکوت») شروع می‌شد و بقیه‌ی جمله هم تصادفاً با لقب ثبت‌شده‌ی یکی از اعضا
    # یکی می‌شد، همون عضو بدون قصد کسی سکوت/بن/اخطار می‌شد. برای جلوگیری از این خطای
    # ناخواسته، حالا هدف فقط از سه راه مطمئن پیدا می‌شه: ریپلای روی پیام کاربر، یا
    # یوزرنیم صریح (با @)، یا آیدی عددی صریحی که خود ادمین عمداً جلوی دستور نوشته.
    # اگه هیچ‌کدوم نبود، دستور اصلاً هدفی پیدا نمی‌کنه (و در عمل، چون شرط ورود به
    # هر دستور مدیریتی هم فقط حالت «خالی» یا «خالی + آیدی عددی/یوزرنیم» رو قبول
    # می‌کنه، این تابع اصلاً برای یک جمله‌ی معمولی صدا زده نمی‌شه).
    return None, None

async def has_permission(user_id, require_owner=False):
    if db.is_super_owner(user_id):
        return True
    if require_owner:
        return False
    return db.is_admin(user_id)

RANDOM_FANTASY_NAMES = [
    "آریا", "رایان", "کیان", "سورن", "آدرین", "دانیار", "پارسا", "نیما",
    "آوین", "رها", "نیلا", "روشا", "آترین", "مهسا", "هلیا", "باران",
    "نوا", "سایا", "آریانا", "رونیا", "ویستا", "دارین", "آرون", "سام",
    "شایان", "ماهان", "هیراد", "کایان", "آریو", "رادین", "یونا", "سها",
    "آیلین", "روژان", "کیانا", "الین", "آرمین", "نیوشا", "رهام", "مانا",
]

def _looks_like_missing_display_name(name):
    value = str(name or "").strip()
    if not value:
        return True
    return bool(re.fullmatch(r"[-+]?\d+", value))

async def _get_or_create_random_name(chat_id, user_id):
    if chat_id is None:
        return None
    try:
        if not db.get_random_name_enabled(chat_id):
            return None
        existing = db.get_random_name(chat_id, user_id)
        if existing:
            return existing
    except Exception:
        return None

    chosen = random.choice(RANDOM_FANTASY_NAMES)
    try:
        conn = db._get_connection()
        cur = conn.cursor()
        cur.execute("SELECT name FROM random_names WHERE chat_id = ?", (int(chat_id),))
        used = {row[0] for row in cur.fetchall() if row and row[0]}
        conn.close()
        available = [name for name in RANDOM_FANTASY_NAMES if name not in used]
        if available:
            chosen = random.choice(available)
    except Exception:
        pass

    try:
        db.set_random_name(chat_id, user_id, chosen)
    except Exception:
        pass
    return chosen

async def get_user_name(client, user_id, chat_id=None):
    """اسم نمایشی کاربر رو برمی‌گردونه، همراه با لقبش (اگه داشته باشه). چون لقب حالا
    مخصوص هر گروهه، برای دیدن لقب درست باید chat_id همون گروهی که پیام توش رد و بدل
    می‌شه رو پاس بدی؛ بدون chat_id (مثلاً برای پیام‌های پی‌وی به مالک ربات) لقبی نشون
    داده نمی‌شه. کش هم بر اساس (chat_id, user_id) جداگانه نگه داشته می‌شه تا لقب یک
    گروه با گروه دیگه قاطی نشه."""
    cache_key = (chat_id, user_id)
    if cache_key in USERNAME_CACHE:
        return USERNAME_CACHE[cache_key]
    
    try:
        profile = db.get_user_profile(user_id)
        title = db.get_user_title(chat_id, user_id) if chat_id is not None else None

        if profile and profile.get("custom_name"):
            name = profile["custom_name"]
        elif profile and profile.get("display_name"):
            # کش دائمی (روی دیتابیس، نه فقط حافظه): اسمی که یک‌بار از API سروش
            # گرفته و توی ستون display_name ذخیره شده. چون این کش با ری‌استارت
            # ربات پاک نمی‌شه، بعد از هر ری‌استارت دیگه لازم نیست دوباره برای
            # هر کاربر یک درخواست کند get_entity به سروش زده بشه - فقط همون
            # بار اولی که کاربر دیده می‌شه یک تماس API لازمه.
            name = profile["display_name"]
        else:
            user = await client.get_entity(user_id)
            first_name = getattr(user, 'first_name', '')
            last_name = getattr(user, 'last_name', '')
            username = getattr(user, 'username', '')

            if first_name:
                name = first_name
                if last_name:
                    name += " " + last_name
            elif username:
                name = "@" + username
            else:
                name = str(user_id)

            # اسم تازه‌گرفته‌شده رو دائمی ذخیره کن تا دفعه‌ی بعد (حتی بعد از
            # ری‌استارت ربات) نیازی به تماس دوباره با API نباشه.
            try:
                db.set_display_name(user_id, name)
            except Exception:
                pass

        # اگر کاربر اسم نمایشی واقعی و لقب ندارد، در صورت فعال بودن قابلیت
        # برای همان گروه یک نام فانتزیِ پایدار انتخاب می‌شود.
        if not title and _looks_like_missing_display_name(name):
            random_name = await _get_or_create_random_name(chat_id, user_id)
            if random_name:
                name = random_name

        # لقبی که ادمین/مالک روی کاربر گذاشته، همیشه همراه نامش در جواب‌های ربات نمایش داده می‌شود.
        full_name = apply_user_title(name, title)
        USERNAME_CACHE[cache_key] = full_name
        if len(USERNAME_CACHE) > USERNAME_CACHE_MAX:
            for old_key in list(USERNAME_CACHE)[:max(1, USERNAME_CACHE_MAX // 10)]:
                USERNAME_CACHE.pop(old_key, None)
        return full_name
    except:
        return str(user_id)


def apply_user_title(name, title):
    """اگه کاربر لقبی (که توسط ادمین ثبت شده) داشته باشه، همون‌جا کنار اسمش نشون بده."""
    if title:
        return f"{name}『🏷 {title}』"
    return name

async def get_username(client, user_id):
    if user_id in USERNAME_CACHE:
        return USERNAME_CACHE[user_id]
    try:
        user = await client.get_entity(user_id)
        username = getattr(user, 'username', None) or str(user_id)
        USERNAME_CACHE[user_id] = username
        if len(USERNAME_CACHE) > USERNAME_CACHE_MAX:
            for old_key in list(USERNAME_CACHE)[:max(1, USERNAME_CACHE_MAX // 10)]:
                USERNAME_CACHE.pop(old_key, None)
        return username
    except:
        return str(user_id)

async def send_welcome(client, chat_id, user_id):
    try:
        name = await get_user_name(client, user_id)
        welcome = WELCOME_TEXT.format(name=name)
        await client.send_message(chat_id, welcome)
        print(f"[WELCOME] پیام خوش‌آمدگویی برای {name} ارسال شد")
    except Exception as e:
        print(f"[send_welcome] خطا: {e}")

def check_anti_spam(user_id, max_msg=5, window=10):
    now = time.time()
    user_times = spam_tracker[user_id]
    user_times[:] = [t for t in user_times if now - t < window]
    if len(user_times) >= max_msg:
        return True
    user_times.append(now)
    return False

def truncate_display_name(name, max_len=20):
    """اسمِ نمایشیِ کاربر رو تا حدِ max_len کاراکتر نگه می‌داره؛ اگه بلندتر بود
    کامل کات می‌شه و یک «…» ته‌ش می‌ذاره - تا یک اسمِ خیلی بلند لِی‌آوتِ تگ‌ها/
    لیدربوردها رو به‌هم نریزه، ولی اسم‌های عادی دست‌نخورده می‌مونن."""
    name = str(name or "").strip()
    if len(name) <= max_len:
        return name
    return name[:max_len].rstrip() + "…"


def mention_link(name, user_id, username=None):
    """یک تگ خوانا برای کاربر می‌سازه. توجه مهم: فرمت لینک بر پایه‌ی آیدی عددی
    (https://web.splus.ir/#USER_ID) که قبلاً امتحان شده بود، توسط سروش قبول نمی‌شه
    و باعث می‌شه کل پیام با خطای RPC (400 BAD_REQUEST روی SendMessageRequest) اصلاً
    ارسال نشه! تنها فرمت لینکی که سروش قبول می‌کنه، بر پایه‌ی یوزرنیمه
    (https://splus.ir/username). پس: اگه کاربر یوزرنیم داشته باشه، اسمش (همراه لقبش)
    به‌صورت لینک قابل‌کلیک به پروفایلش نشون داده می‌شه؛ اگه یوزرنیم نداشته باشه (چون
    سروش امکان تگ/لینک واقعی بر پایه‌ی آیدی رو نداره)، فقط اسمش پررنگ نمایش داده می‌شه
    - بدون لینک - تا پیام همیشه با موفقیت ارسال بشه.
    اسمِ خیلی بلند (مثلاً یک بیوگرافیِ کاملِ ۱۰۰ کاراکتری به‌جای اسم) هم اینجا
    کوتاه می‌شه تا لِی‌آوتِ پیام‌های اخطار/بن به‌هم نریزه."""
    safe_name = truncate_display_name((name or str(user_id)).replace("[", "").replace("]", "").replace("(", "").replace(")", ""))
    if username:
        return f"[{safe_name}](https://splus.ir/{username})"
    return f"**{safe_name}**"

def view_tag(user_id, username=None):
    """یک تگِ قابل‌کلیک برای کاربر می‌سازه که واقعاً مشخص می‌کنه «کی» هست.
    قبلاً این تابع فقط یک لینکِ گنگِ «مشاهده» می‌ساخت که هیچ‌جا نمی‌گفت پشتِ
    این لینک کدوم کاربره - دقیقاً همون چیزی که باعث می‌شد توی پیام‌های
    اخطار/بن معلوم نباشه چه کسی تخلف کرده. الان به‌جاش خودِ یوزرنیم (تنها
    روشِ تگِ واقعیِ قابل‌کلیک که سروش پشتیبانی می‌کنه) نشون داده می‌شه؛ اگه
    یوزرنیم نداشته باشه، حداقل آیدیِ عددیش نشون داده می‌شه (نه یک متنِ مبهم)."""
    if username:
        return f"[@{username}](https://splus.ir/{username})"
    return f"`{user_id}`"

def mention_user(name, user_id, username=None):
    """اگر یوزرنیم داشته باشه، با @یوزرنیم تگ می‌کنه (همون روشی که کاربرا عادی همدیگه رو تگ می‌کنن).
    اگر یوزرنیم نداشته باشه، امکان تگ واقعی/پینگ نیست (چون بدون یوزرنیم، تگ بر پایه آیدی
    نیاز به پشتیبانی خاص کلاینت داره که سروش ساپورت نمی‌کنه) - فقط اسم پررنگ نمایش داده میشه."""
    if username:
        return f"@{username}"
    safe_name = truncate_display_name((name or str(user_id)).replace("[", "").replace("]", ""))
    return f"**{safe_name}**"

def _extract_text_from_payload(data):
    """از جواب یک API (که می‌تونه رشته، دیکشنری یا لیست باشه) یه متن قابل‌نمایش دربیار.
    چون هر وب‌سرویس رایگان یه شکل جواب متفاوت داره (result/data/text/joke/...),
    این تابع چند تا از رایج‌ترین حالت‌ها رو امتحان می‌کنه."""
    if isinstance(data, str):
        text = data.strip()
        return text or None

    if isinstance(data, list):
        if not data:
            return None
        return _extract_text_from_payload(random.choice(data))

    if isinstance(data, dict):
        for key in ("result", "data", "text", "joke", "hadith", "hadis", "content",
                    "message", "matn", "rhyme", "poem"):
            if key in data:
                inner = _extract_text_from_payload(data[key])
                if inner:
                    return inner
        # اگه هیچ‌کدوم از کلیدهای رایج پیدا نشد، اولین مقدار متنی معنی‌دار رو بردار
        for value in data.values():
            inner = _extract_text_from_payload(value)
            if inner and len(inner) > 3:
                return inner
        return None

    return None


async def send_external_alert(text, timeout=10):
    """یه پیام هشدار فوری از یک کانال کاملاً جدا از خود ربات می‌فرسته - مهم‌ترین
    کاربردش وقتیه که سشن ربات باطل شده و نیاز به OTP داره؛ توی اون حالت خود ربات
    اصلاً نمی‌تونه پیام بده (چون لاگین نیست)، پس نمی‌شه از طریق خودش خبر داد.
    از سرویس رایگان و بدون-ثبت‌نام ntfy.sh استفاده می‌کنه (کافیه اپ ntfy رو نصب
    کنی و همین Topic رو Subscribe کنی). اگه NTFY_TOPIC توی config.py تنظیم نشده
    باشه (None)، فقط توی لاگ چاپ می‌شه. هر خطایی (قطعی اینترنت و ...) رو می‌بلعه
    تا هیچ‌وقت باعث کرش ربات نشه."""
    from config import NTFY_TOPIC
    safe_print(f"🚨 [هشدار خارجی] {text}")
    if not NTFY_TOPIC:
        return
    try:
        client_timeout = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(timeout=client_timeout) as session:
            await session.post(
                f"https://ntfy.sh/{NTFY_TOPIC}",
                data=text.encode("utf-8"),
                headers={"Title": "SoroushBot"}
            )
    except Exception as e:
        safe_print(f"[send_external_alert] ارسال هشدار خارجی ناموفق بود: {e}")


# ==================== بهینه‌سازی API سرگرمی ====================
# یک Session مشترک به‌جای ساختن ClientSession برای هر درخواست.
# این کار TCP/SSL handshake و سربار اتصال را حذف می‌کند و برای چندصد گروه
# هم‌زمان بسیار سبک‌تر است.
_FUN_API_URL = "https://l8pStudio.ir/apis-loop/api-fun.php"
_FUN_API_TIMEOUT = aiohttp.ClientTimeout(total=8, connect=3, sock_read=6)
_FUN_API_SESSION = None
_FUN_API_SESSION_LOOP = None
_FUN_API_SEMAPHORE = None
_FUN_INFLIGHT = {}
_FUN_INFLIGHT_LOCK = None

async def _get_fun_api_session():
    global _FUN_API_SESSION, _FUN_API_SESSION_LOOP, _FUN_API_SEMAPHORE, _FUN_INFLIGHT_LOCK
    loop = asyncio.get_running_loop()
    if _FUN_API_SESSION is None or _FUN_API_SESSION.closed or _FUN_API_SESSION_LOOP is not loop:
        if _FUN_API_SESSION is not None and not _FUN_API_SESSION.closed:
            try:
                await _FUN_API_SESSION.close()
            except Exception:
                pass
        _FUN_API_SESSION = aiohttp.ClientSession(
            timeout=_FUN_API_TIMEOUT,
            connector=aiohttp.TCPConnector(
                limit=80,
                limit_per_host=80,
                ttl_dns_cache=300,
                enable_cleanup_closed=True,
            ),
        )
        _FUN_API_SESSION_LOOP = loop
        _FUN_API_SEMAPHORE = asyncio.Semaphore(80)
        _FUN_INFLIGHT_LOCK = asyncio.Lock()
        _FUN_INFLIGHT.clear()
    return _FUN_API_SESSION

async def get_random_content(category):
    """محتوای سرگرمی فقط از API تنظیم‌شده؛ بدون دیتای محلی.
    اتصال HTTP مشترک و درخواست‌های هم‌زمان کنترل‌شده‌اند تا در بار چندصد گروه
    ربات سریع بماند و برای هر پیام Session جدید ساخته نشود."""
    if not category:
        return "خطایی رخ داد."

    session = await _get_fun_api_session()
    payload = {"category": category}

    # اگر چند درخواست دقیقاً هم‌زمان برای یک دسته رسید، فقط یک درخواست به API
    # می‌رود و بقیه همان نتیجه را تحویل می‌گیرند؛ این فقط in-flight است و کش دائمی نیست.
    async with _FUN_INFLIGHT_LOCK:
        task = _FUN_INFLIGHT.get(category)
        if task is None or task.done():
            task = asyncio.create_task(_fetch_fun_api(session, payload, category))
            _FUN_INFLIGHT[category] = task

    try:
        return await task
    finally:
        if task.done():
            async with _FUN_INFLIGHT_LOCK:
                if _FUN_INFLIGHT.get(category) is task:
                    _FUN_INFLIGHT.pop(category, None)

async def _fetch_fun_api(session, payload, category):
    try:
        async with _FUN_API_SEMAPHORE:
            async with session.post(_FUN_API_URL, json=payload) as response:
                if response.status != 200:
                    return "خطایی رخ داد."
                data = await response.json(content_type=None)
                if isinstance(data, dict) and data.get("status"):
                    content = data.get("data", {}).get("content")
                    if content:
                        return str(content).strip()
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError):
        pass
    except Exception:
        pass
    return "خطایی رخ داد."


def ensure_activation_recorded():
    """اولین باری که ربات با موفقیت لاگین می‌کنه، تاریخ همون لحظه رو (فقط یک‌بار)
    داخل ACTIVATION_FILE ذخیره می‌کنه. اگه فایل از قبل وجود داشته باشه (یعنی ربات
    قبلاً فعال شده بوده)، هیچ‌وقت دوباره نوشته نمی‌شه - این یعنی حتی با ری‌استارت
    مکرر پروسه، روزشمار فعالیت هیچ‌وقت از صفر شروع نمی‌شه. توجه: پاک کردن این
    فایل یعنی روزشمار از اول شروع می‌شه، پس نباید حذفش کنید."""
    try:
        if not os.path.exists(ACTIVATION_FILE):
            with open(ACTIVATION_FILE, "w", encoding="utf-8") as f:
                f.write(datetime.now().isoformat())
            safe_print(f"📅 تاریخ اولین فعال‌سازی ربات ثبت شد: {datetime.now().isoformat()}")
    except Exception as e:
        safe_print(f"[ensure_activation_recorded] خطا در ثبت تاریخ فعال‌سازی: {e}")


def get_activation_date():
    """تاریخ اولین فعال‌سازی ربات رو (به‌صورت datetime) برمی‌گردونه، یا None اگه
    هنوز ثبت نشده (مثلاً فایل بین راه پاک شده)."""
    try:
        if os.path.exists(ACTIVATION_FILE):
            with open(ACTIVATION_FILE, "r", encoding="utf-8") as f:
                raw = f.read().strip()
            if raw:
                return datetime.fromisoformat(raw)
    except Exception as e:
        safe_print(f"[get_activation_date] خطا در خواندن تاریخ فعال‌سازی: {e}")
    return None


def get_days_active():
    """تعداد روزهایی که از اولین فعال‌سازی ربات می‌گذره رو برمی‌گردونه (عدد صحیح،
    حداقل ۱). اگه تاریخ فعال‌سازی هنوز ثبت نشده، 0 برمی‌گردونه."""
    activation_date = get_activation_date()
    if not activation_date:
        return 0
    delta = datetime.now() - activation_date
    return max(delta.days + 1, 1)


def is_group_owner(chat_id, user_id):
    return db.is_group_owner(chat_id, user_id)

def is_super_owner(user_id):
    return db.is_super_owner(user_id)

def clear_user_cache(user_id, chat_id=None):
    """کش اسم/لقب کاربر رو پاک می‌کنه. اگه chat_id داده بشه، فقط کش همون گروه پاک
    می‌شه؛ در غیر این صورت، کش این کاربر در همه‌ی گروه‌ها (چون لقب مخصوص هر گروهه)
    به‌علاوه‌ی کش یوزرنیم خامش پاک می‌شه."""
    if chat_id is not None:
        USERNAME_CACHE.pop((chat_id, user_id), None)
    else:
        for key in [k for k in USERNAME_CACHE if isinstance(k, tuple) and k[1] == user_id]:
            del USERNAME_CACHE[key]
    if user_id in USERNAME_CACHE:
        del USERNAME_CACHE[user_id]


def clear_chat_name_cache(chat_id):
    """کش نام/لقب تمام کاربران یک گروه را پاک می‌کند."""
    for key in [k for k in list(USERNAME_CACHE) if isinstance(k, tuple) and k[0] == chat_id]:
        USERNAME_CACHE.pop(key, None)


def clear_all_cache():
    """پاکسازی کامل کش‌های این ماژول؛ فقط RAM پاک می‌شود و هیچ دیتای دائمی حذف نمی‌شود."""
    USERNAME_CACHE.clear()
    MESSAGE_CACHE.clear()
    MEDIA_INFO_CACHE.clear()
    spam_tracker.clear()
    try:
        gc.collect()
    except Exception:
        pass

def censor_word(word):
    """یک کلمه رو سانسور می‌کنه: فقط حرف اول و آخرش نشون داده می‌شه و بقیه با
    نقطه پوشونده می‌شه (مثلاً «کیری» -> «ک..ی»). هیچ‌وقت کلمه‌ی کامل رو برنمی‌گردونه."""
    if not word:
        return ""
    if len(word) <= 2:
        return word[0] + "."
    return word[0] + ("." * (len(word) - 2)) + word[-1]



def trim_runtime_memory():
    """پاکسازی سبک کش‌های کوتاه‌عمر؛ اطلاعات دائمی DB دست‌نخورده می‌ماند."""
    now = time.time()
    for user_id in list(spam_tracker.keys()):
        times = spam_tracker.get(user_id)
        if not times:
            spam_tracker.pop(user_id, None)
            continue
        fresh = [t for t in times if now - t < 60]
        if fresh:
            spam_tracker[user_id] = fresh
        else:
            spam_tracker.pop(user_id, None)

    if len(USERNAME_CACHE) > USERNAME_CACHE_MAX:
        for old_key in list(USERNAME_CACHE)[:len(USERNAME_CACHE) - USERNAME_CACHE_MAX]:
            USERNAME_CACHE.pop(old_key, None)

    try:
        gc.collect()
    except Exception:
        pass
