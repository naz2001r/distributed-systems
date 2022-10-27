from numpy import frombuffer, uint32

from common.message import Message, MessageHeader, MessageType

class MessageEncoder:
    # Message header encoding consists of 8 bytes (64 bits) and has the following layout:
    #   1) Type          - 1 bit (0 - Request, 1 - Response);
    #   2) Number        - 31 bits (0...2147483647);
    #   3) Data size     - 32 bits (0...4294967295);

    TYPE_BITMASK = uint32(1) << uint32(31)
    HEADER_BYTES_SIZE = 8
    
    @staticmethod
    def decode_message_header(header_buffer: bytes) -> MessageHeader:
        header_parts = frombuffer(header_buffer, dtype=uint32, count=2)
        type_and_name = header_parts[0]
        data_size = header_parts[1]

        type_bit_field = type_and_name & MessageEncoder.TYPE_BITMASK
        type = MessageType.REQUEST if type_bit_field == 0 else MessageType.RESPONSE
        number = type_and_name & ~MessageEncoder.TYPE_BITMASK 
        
        return MessageHeader(type=type, number=number, data_size=data_size)

    @staticmethod
    def encode_message_header(header: MessageHeader) -> bytes:
        type = header.type
        number = uint32(header.number)
        data_size = uint32(header.data_size)
        
        type_and_number = number
        if type == MessageType.RESPONSE:
            type_and_number = type_and_number | MessageEncoder.TYPE_BITMASK
        
        header_buffer_parts = [type_and_number.tobytes(), data_size.tobytes()]
        header_buffer = bytes().join(header_buffer_parts)

        return header_buffer

    @staticmethod
    def encode_message(message: Message) -> bytes:
        header_buffer = MessageEncoder.encode_message_header(message.header)
        data_buffer = message.data.encode('utf-8')
        message_buffer = header_buffer + data_buffer if message.data else header_buffer
        return message_buffer





