"""Компоненты авторизации клиентов Port Tunnel."""

import abc
import hmac
from typing import Mapping


class ABCClientAuthenticator(abc.ABC):
    """Интерфейс проверки учётных данных клиента."""

    @abc.abstractmethod
    def authenticate(self, client_id: str, token: str) -> bool:
        """Вернуть True, если токен разрешён для указанного клиента."""
        raise NotImplementedError


class StaticTokenAuthenticator(ABCClientAuthenticator):
    """Проверяет токены по статическому набору разрешённых значений."""

    def __init__(self, tokens: Mapping[str, str]) -> None:
        self._tokens = dict(tokens)  # client_id -> token

    def authenticate(self, client_id: str, token: str) -> bool:
        """Сравнить переданный токен с токеном зарегистрированного клиента."""
        expected_token = self._tokens.get(client_id)
        if expected_token is None:
            return False

        return hmac.compare_digest(expected_token, token)
