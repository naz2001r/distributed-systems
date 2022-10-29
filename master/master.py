import os
import json
import socket
import logging
from fastapi import FastAPI
from threading import Lock

from common.message import Message, MessageFactory, MessageType
from common.message_encoder import MessageEncoder

SECONDARY_INFO = json.loads(os.environ['SECONDARY_INFO'])
logging.info(f"Secondary Info:{str(SECONDARY_INFO)}")
app = FastAPI()

data_storage = []

def get_secondary_answer(host, port, request_message: Message):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))

        request_message_buffer = MessageEncoder.encode_message(request_message)       
        s.sendall(request_message_buffer)
        
        response_message_header_buffer = s.recv(MessageEncoder.HEADER_BYTES_SIZE)
        response_message_header = MessageEncoder.decode_message_header(response_message_header_buffer)

        if response_message_header.type != MessageType.RESPONSE:
            raise ValueError("Unexpected message received! " +  
                            f"Expected {MessageType.RESPONSE}, but received {response_message_header.type}.")

        if response_message_header.number != request_message.header.number:
            raise ValueError("Response message number doesn't correspond to request message number! " +
                            f"Expected {request_message.header.number}, but received {response_message_header.number}.")

        # If response doesn't contain any data, it means success.
        # Otherwise, data represents an error message explaining why it has failed.
        if response_message_header.data_size > 0:
            data_buffer = s.recv(response_message_header.data_size)
            error_message = data_buffer.decode("utf-8")
            raise ValueError(f"Secondary has failed to store data: {error_message}")

    return True

@app.on_event("startup")
def startup_event():
    app.message_number = 0
    app.message_number_locker = Lock()

@app.get("/get_list")
async def root():
    return {"messages": data_storage}

@app.post("/append")
async def root(data):
    with app.message_number_locker:
        app.message_number+=1
        request_message = MessageFactory.create_request_message(app.message_number, data)
        
    status =[]
    for host,port in SECONDARY_INFO.items():
        # TODO: ADD CONCURRENCY
        # We should send data to all secondaries concurrently.
        # This way, if one blocks/hands, we can process others.
        # At the moment, we wait on an answer from each secondary sequentially.
        status.append(get_secondary_answer(
            host=host,
            port=port,
            request_message=request_message
        ))
        logging.info(f"Got answer '{request_message}' from Secondary {host}:{port}")

    data_storage.append(data)
    return {"Status": 200}