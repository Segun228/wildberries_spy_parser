from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import AuthenticationFailed
from django.contrib.auth import get_user_model
import logging
from rest_framework_simplejwt.authentication import JWTAuthentication

class TelegramAuthentication(BaseAuthentication):
    def authenticate(self, request):
        telegram_header = request.headers.get('Authorization')
        if (telegram_header is None) or (not telegram_header):
            logging.error("Error finding auth headero")
            return None
        prefix, telegram_id = telegram_header.split(" ", 1)
        if prefix.strip() != "Bot":
            logging.error("Error finding header prefix")
            return None
        if not telegram_id.strip() or not telegram_id.isdigit():
            logging.error("Telegram id is invalid")
            return None
        telegram_id = int(telegram_id.strip())
        User = get_user_model()
        try:
            user = User.objects.get(telegram_id=telegram_id)
        except User.DoesNotExist:
            logging.error("Пользователь с таким Telegram ID не найден.")
            raise AuthenticationFailed('Пользователь с таким Telegram ID не найден.')
        return (user, None)


class CombinedAuthentication(BaseAuthentication):
    def authenticate(self, request):
        try:
            tg_auth = TelegramAuthentication()
            jwt_auth = JWTAuthentication()
            try:
                tg_auth_result = tg_auth.authenticate(
                    request=request
                )
            except Exception as e:
                logging.error("Telegram not authorized")
                logging.error(e)
            try:
                jwt_auth_result = jwt_auth.authenticate(
                    request=request
                )
            except Exception as e:
                logging.error("Telegram not authorized")
                logging.error(e)
            if tg_auth_result is not None and tg_auth_result:
                return tg_auth_result[0], None
            elif jwt_auth_result is not None and jwt_auth_result:
                return jwt_auth_result[0], None
            else:
                raise AuthenticationFailed('User with corresponding info was not found')
        except Exception as e:
            logging.error(f"Error while authorizing {e}")
            raise AuthenticationFailed('Пользователь с таким Telegram ID не найден.')