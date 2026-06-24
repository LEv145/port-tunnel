"""Типизированные сообщения управляющего протокола Port Tunnel."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


Port = Annotated[
    int,
    Field(ge=1, le=65535),
]

NonEmptyString = Annotated[
    str,
    Field(min_length=1),
]

SecretToken = Annotated[
    str,
    Field(min_length=1, repr=False),
]


class ControlMessageBase(BaseModel):
    """Базовая модель служебного сообщения."""

    model_config = ConfigDict(
        strict=True,
        frozen=True,
        extra="forbid",
        hide_input_in_errors=True,
    )


class RegisterMessage(ControlMessageBase):
    """Запрос на регистрацию TCP-туннеля."""

    type: Literal["register"] = "register"

    client_id: NonEmptyString
    token: SecretToken
    local_port: Port
    public_port: Port


class RegisteredMessage(ControlMessageBase):
    """Подтверждение успешной регистрации туннеля."""

    type: Literal["registered"] = "registered"

    tunnel_id: NonEmptyString
    data_token: SecretToken
    public_port: Port


class NewConnectionMessage(ControlMessageBase):
    """Уведомление о новом внешнем подключении."""

    type: Literal["new_connection"] = "new_connection"

    tunnel_id: NonEmptyString
    connection_id: NonEmptyString


class DataMessage(ControlMessageBase):
    """Привязка data-соединения к внешнему подключению."""

    type: Literal["data"] = "data"

    tunnel_id: NonEmptyString
    connection_id: NonEmptyString
    data_token: SecretToken


class ErrorMessage(ControlMessageBase):
    """Ошибка обработки управляющего сообщения."""

    type: Literal["error"] = "error"

    message: NonEmptyString
    code: str | None = None


class PingMessage(ControlMessageBase):
    """Запрос проверки доступности control-соединения."""

    type: Literal["ping"] = "ping"

    heartbeat_id: NonEmptyString


class PongMessage(ControlMessageBase):
    """Ответ на сообщение ping."""

    type: Literal["pong"] = "pong"

    heartbeat_id: NonEmptyString
