import os
import time
import socket
import random
import logging
from typing import List
from fastapi import FastAPI
from threading import Thread

from common.message_encoder import MessageEncoder
from common.message import MessageFactory, MessageHeader, MessageType

logging.basicConfig(level=logging.INFO)

class Secondary:
    HOST = "0.0.0.0" # Standard loopback interface address (localhost)
    # For local run use: os.environ['PORT'] = 65441
    PORT = int(os.environ['PORT']) # Port to listen on (non-privileged ports are > 1023)

    def __init__(self) -> None:
        self.data_storage = []
        self.temp_data_storage = {}
        self.continue_run = True
        self.connection_threads = {}
        self.socket = None
        self.thread = None
        logging.info(f"Secondary Port: {self.PORT}")

    def get_data(self) -> List[str]:
        return self.data_storage

    def handle_client_connection(self, client_connection, adddress):
        while True:
            try:
                request_message_header_buffer = client_connection.recv(MessageEncoder.HEADER_BYTES_SIZE)
                if len(request_message_header_buffer) == 0:
                    logging.info(f"End of connection with {adddress}")
                    del self.connection_threads[adddress]
                    client_connection.close()
                    return

                request_message_header = MessageEncoder.decode_message_header(request_message_header_buffer)

                if request_message_header.number == MessageHeader.HEALTHCHECK_MSG_NUM:
                    healthcheck_message = MessageFactory.create_healthcheck_response_message()
                    healthcheck_message_buffer = MessageEncoder.encode_message(healthcheck_message)
                    client_connection.sendall(healthcheck_message_buffer)
                    continue

                if request_message_header.type != MessageType.REQUEST:
                    raise ValueError("Unexpected message received! " +  
                                    f"Expected {MessageType.REQUEST}, but received {request_message_header.type}.")

                if request_message_header.number < 0:
                    raise ValueError("Request message number should be a positive number!")

                if request_message_header.data_size == 0:    
                    raise ValueError(f"Master didn't send any data to store!")

                data_buffer = client_connection.recv(request_message_header.data_size)
                data = data_buffer.decode("utf-8")

                # Make a delay before storing data.
                self._make_artificial_delay()

                self._store_data(data, request_message_header.number)

                # Sending empty data in response message, in case of successful replication.
                response_message = MessageFactory.create_response_message(request_message_header.number)
                response_message_buffer = MessageEncoder.encode_message(response_message)
                client_connection.sendall(response_message_buffer)

            except Exception as error:
                # Sending error message as data in response message, in case of failed replication.
                response_message = MessageFactory.create_response_message(request_message_header.number, str(error))
                response_message_buffer = MessageEncoder.encode_message(response_message)
                client_connection.sendall(response_message_buffer)

    def start_server(self) -> None:
        try:
            while self.continue_run:
                logging.info(f'Accepting connections')
                connection, address = self.socket.accept()
                logging.info(f"Connected by {address}")

                connection_thread = Thread(target=self.handle_client_connection, args=(connection, address))
                connection_thread.daemon = True
                connection_thread.start()

                self.connection_threads[address] = connection_thread

        except:
            logging.info("Can't start server")

    def start_receiving_data(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.HOST, self.PORT))
        self.socket.listen(0)
        self.thread = Thread(target=self.start_server)
        self.thread.start()

    def stop_receiving_data(self):
        self.continue_run = False
        self.socket.close()
        self.thread.join()

    def _store_data(self, data, data_number):
        if len(self.data_storage) + 1 == data_number:
            self.data_storage.append(data)
            logging.info(f'Received message: {data}')
            
        elif len(self.data_storage) >= data_number:
            logging.info(f"Received duplicate message: {data}")

        else:
            self.temp_data_storage[data_number] = data
            logging.info(f"Added message '{data}' to temporary storage")
            
        if len(self.temp_data_storage) > 0:
            num = len(self.data_storage) + 1
            while num in self.temp_data_storage.keys():
                self.data_storage.append(self.temp_data_storage[num])
                del self.temp_data_storage[num]
                num += 1
                logging.info(f"Got message '{data}' from temporary storage")

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
    app.secondary.stop_receiving_data()