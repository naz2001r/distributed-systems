from enum import IntEnum

class MessageType(IntEnum):
    REQUEST = 0
    RESPONSE = 1
    HEALTHCHECK = 2

class MessageHeader:
    def __init__(self, type: MessageType, number: int, data_size: int):
        self._type = MessageType(type)
        self._number = int(number)
        self._data_size = int(data_size)

    @property
    def type(self) -> MessageType:
        return self._type

    @property
    def number(self) -> int:
        return self._number

    @property
    def data_size(self) -> int:
        return self._data_size

class Message:
    def __init__(self, header: MessageHeader, data: str):
        self._header = header
        self._data = data

    @property
    def header(self) -> MessageHeader:
        return self._header

    @property
    def data(self) -> str:
        return self._data

class MessageFactory:

    @staticmethod
    def _create_message(type, number, data):
        header = MessageHeader(type, number, len(data))
        return Message(header, data)

    @staticmethod
    def create_request_message(number: int, data: str) -> Message:       
        return MessageFactory._create_message(MessageType.REQUEST, number, data)

    @staticmethod
    def create_response_message(number: int, data: str = '') -> Message:
        return MessageFactory._create_message(MessageType.RESPONSE, number, data)

    @staticmethod
    def create_healthcheck_message(number: int = 0, data: str = '') -> Message:
        return MessageFactory._create_message(MessageType.HEALTHCHECK, number, data)

    