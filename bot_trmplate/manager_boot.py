import asyncio

# Compatibility bootstrap for fresh Soroush sessions.
# The bundled library uses updates.GetStateRequest() as the first authorization
# probe. On some Soroush servers a brand-new auth key is rejected by that probe
# with "The key is not registered in the system" and the socket becomes unusable.
# Reconnect once before returning False so the unchanged main.py can continue to
# its normal send-code flow.
try:
    from spluslib._base.client.users import UserMethods
    from spluslib import errors as _errors

    _original_is_user_authorized = UserMethods.is_user_authorized

    async def _patched_is_user_authorized(self):
        if self._authorized is not None:
            return self._authorized
        try:
            return await _original_is_user_authorized(self)
        except Exception:
            # Preserve the library's normal behavior for non-RPC failures.
            raise

    # The original method catches RPCError internally, so inspect the resulting
    # state and refresh the transport when the authorization probe failed.
    #
    # قبلاً اینجا یک باگ واقعی بود: اگه اولین probe به هر دلیلی (نه فقط
    # سناریوی خاصِ "کلید تازه رد شد"، بلکه یک هیک‌آپِ گذرای شبکه - مثلاً وقتی
    # چند تا سلف هم‌زمان استارت/ری‌استارت می‌شن و هم‌زمان به سرور سروش وصل
    # می‌شن) False برمی‌گشت، کد بدونِ هیچ تلاشِ دوباره‌ای self._authorized رو
    # برای همیشه (تا آخر عمر همین پروسه) False می‌ذاشت. یعنی حتی با یک
    # session.txt کاملاً معتبر، اگه probeِ اول به هر خاطر گذرایی fail می‌شد،
    # main.py مجبور می‌شد بره سراغِ فلوی لاگینِ کامل (خواستنِ شماره تلفن)،
    # که هیچ‌کس پشتِ stdin منتظرش نیست (چون این یک استارتِ معمولیه، نه ساختِ
    # سلف جدید) - و همون‌جا با EOF گیر می‌کرد.
    #
    # الان بعد از reconnect، probe دوباره (یک‌بار) امتحان می‌شه؛ فقط اگه
    # همون‌جا هم False بود، واقعاً غیرمجاز در نظر گرفته می‌شه.
    async def _safe_is_user_authorized(self):
        if self._authorized is not None:
            return self._authorized
        result = await _original_is_user_authorized(self)
        if result is False:
            try:
                if self.is_connected():
                    await self.disconnect()
            except Exception:
                pass
            try:
                # A complete reconnect creates/registers a fresh transport key.
                await self.connect()
            except Exception:
                pass
            try:
                result = await _original_is_user_authorized(self)
            except Exception:
                result = False
        self._authorized = result
        return result

    UserMethods.is_user_authorized = _safe_is_user_authorized
except Exception:
    pass

import runpy
runpy.run_path("main.py", run_name="__main__")
