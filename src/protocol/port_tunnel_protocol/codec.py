"""Проверка и сериализация управляющих сообщений."""

from typing import Annotated, cast

from pydantic import Field, TypeAdapter, ValidationError

from .messages import (
    ControlMessageBase,
    DataMessage,
    ErrorMessage,
    NewConnectionMessage,
    PingMessage,
    PongMessage,
    RegisteredMessage,
    RegisterMessage,
)


type ControlMessage = Annotated[
    RegisterMessage
    | RegisteredMessage
    | NewConnectionMessage
    | DataMessage
    | ErrorMessage
    | PingMessage
    | PongMessage,
    Field(discriminator="type"),
]


_CONTROL_MESSAGE_ADAPTER = TypeAdapter(ControlMessage)


class InvalidControlMessageError(ValueError):
    """Полученные данные не соответствуют управляющему протоколу."""

    def __init__(self, *, error_count: int) -> None:
        self.error_count = error_count

        super().__init__(
            f"Invalid control message: {error_count} validation error(s)"
        )


def parse_control_message(payload: object) -> ControlMessage:
    """Проверить данные и создать сообщение соответствующего типа."""
    try:
        return _CONTROL_MESSAGE_ADAPTER.validate_python(payload)
    except ValidationError as error:
        # В текст исключения намеренно не помещаются входные данные,
        # поскольку среди них могут находиться токены.
        raise InvalidControlMessageError(
            error_count=error.error_count(),
        ) from error


def serialize_control_message(
    message: ControlMessageBase,
) -> dict[str, object]:
    """Преобразовать сообщение в JSON-совместимый словарь."""
    return cast(
        dict[str, object],
        message.model_dump(mode="json"),
    )
