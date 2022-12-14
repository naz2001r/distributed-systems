import os
import random
import socket
import logging
import time
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
        self.continue_run = True

        self.socket = None
        self.thread = None
        logging.info(f"Secondary Port: {self.PORT}")

    def get_data(self) -> List[str]:
        return self.data_storage

    def start_server(self) -> None:
        try:
            while self.continue_run:
                conn, addr = self.socket.accept()
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

                        self._make_artificial_delay()

                        # Sending empty data in response message, in case of successful replication.
                        response_message = MessageFactory.create_response_message(request_message_header.number)
                        response_message_buffer = MessageEncoder.encode_message(response_message)
                        conn.sendall(response_message_buffer)

                    except Exception as error:
                        # Sending error message as data in response message, in case of failed replication.
                        response_message = MessageFactory.create_response_message(request_message_header.number, str(error))
                        response_message_buffer = MessageEncoder.encode_message(response_message)
                        conn.sendall(response_message_buffer)

        except:
            logging.info("Can't start server")

    def start_receiving_data(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.HOST, self.PORT))
        self.socket.listen()
        self.thread = Thread(target=self.start_server)
        self.thread.start()

    def stop_receiving_data(self):
        self.continue_run = False
        self.socket.close()
        self.thread.join()

    def _make_artificial_delay(self):
        delay_probability = random.uniform(0, 1)
        if delay_probability > 0.5:
            time.sleep(5)


app = FastAPI()

@app.on_event("startup")
def start_server():
    app.secondary = Secondary()
    app.secondary.start_receiving_data()

@app.get("/get_data")
async def get_data():
    data_storage = app.secondary.get_data()
    logging.info(f"List of messages at node:\n{data_storage}")
    return {"messages": data_storage}

@app.on_event("shutdown")
def stop_server():
    app.secondary.start_receiving_data()