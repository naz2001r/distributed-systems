import os
import socket
import logging
from typing import List
from fastapi import FastAPI
from threading import Thread

from common.message import MessageFactory, MessageType
from common.message_encoder import MessageEncoder

logging.basicConfig(level=logging.INFO)


class Secondary:
    HOST = "0.0.0.0" # Standard loopback interface address (localhost)
    # For local run use: os.environ['PORT'] = str(65441)
    PORT = int(os.environ['PORT']) # Port to listen on (non-privileged ports are > 1023)

    def __init__(self) -> None:
        self.data_storage = []
        logging.info(f"Secondary Port: {self.PORT}")

    def get_data(self) -> List[str]:
        return self.data_storage

    def start_server(self,socket, continue_run) -> None:
        try:
            while continue_run:
                conn, addr = socket.accept()
                with conn:
                    try:
                        logging.info(f"Connected by {addr}")
                        request_message_header_buffer = conn.recv(MessageEncoder.HEADER_BYTES_SIZE)
                        request_message_header = MessageEncoder.decode_message_header(request_message_header_buffer)

                        if request_message_header.type != MessageType.REQUEST:
                            raise ValueError("Unexpected message received! " +  
                                            f"Expected {MessageType.REQUEST}, but received {request_message_header.type}.")

                        if request_message_header.number <= 0:
                            raise ValueError("Request message number should be a positive number!")

                        if request_message_header.data_size == 0:         
                            raise ValueError(f"Master didn't send any data to store!")

                        data_buffer = conn.recv(request_message_header.data_size)
                        data = data_buffer.decode("utf-8")
                        self.data_storage.append(data)
                        logging.info(f'Received message:{data}')

                        # If response doesn't contain any data, it means success.
                        # Otherwise, data represents an error message explaining why it has failed.
                        response_message = MessageFactory.create_response_message(request_message_header.number)
                        response_message_buffer = MessageEncoder.encode_message(response_message)
                        conn.sendall(response_message_buffer)

                    except Exception as error:
                        response_message = MessageFactory.create_response_message(request_message_header.number, str(error))
                        response_message_buffer = MessageEncoder.encode_message(response_message)
                        conn.sendall(response_message_buffer)

        except:
            logging.info("Can't start server")


app = FastAPI()

@app.on_event("startup")
def startup_event():
    app.continue_run = True
    app.secondary = Secondary()

    app.socket= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    app.socket.bind((app.secondary.HOST, app.secondary.PORT))
    app.socket.listen()
    app.thread = Thread(target=app.secondary.start_server, args=(app.socket,app.continue_run))
    app.thread.start()

@app.get("/get_data")
async def get_data():
    data_storage = app.secondary.get_data()
    logging.info(f"List of messages at node:\n{data_storage}")
    return {"messages": data_storage}

@app.on_event("shutdown")
def shutdown_event():
    app.continue_run = False
    app.socket.close()
    app.thread.join()