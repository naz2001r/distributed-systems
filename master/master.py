import os
import json
import socket
import logging
from typing import List
from fastapi import FastAPI
from threading import Lock

from common.message import Message, MessageFactory, MessageType
from common.message_encoder import MessageEncoder

logging.basicConfig(level=logging.INFO)

class Master:
    def __init__(self) -> None:
        self.replication_number = 0
        self.replication_number_locker = Lock()
        self.data_storage = []

        # For local run use: {"0.0.0.0": 65441, "0.0.0.0": 65442}
        info_json = json.dumps(os.environ['SECONDARY_INFO'])
        self.secondaries_info = json.loads(info_json )
        # self.secondaries_info = json.loads(os.environ['SECONDARY_INFO']) 
        logging.info(f"Secondary info: {self.secondaries_info}")

    def append_data(self, data:str) -> bool:
        has_succeded = self._replicate_data_to_secondaries(data)
        if not has_succeded:
            return False

        self.data_storage.append(data)
        return True

    def get_data(self) -> List[str]:
        return self.data_storage
    
    def _replicate_data_to_secondaries(self, data:str) -> bool:
        try:
            data_replication_message = self._create_message_for_data_replication(data) 

            for host, port in self.secondaries_info.items():
                logging.info(host)
                # TODO: ADD CONCURRENCY
                # We should send data to all secondaries concurrently.
                # This way, if one blocks/hangs, we can process the others.
                # At the moment, we wait on an answer from each secondary sequentially.
                has_data_replication_succeeded = self._replicate_data_to_secondary(
                    host=host,
                    port=port,
                    request_message=data_replication_message
                    )

                logging.info(f"Replication to secondary {host}:{port} finished " + 
                             f"with the status: {has_data_replication_succeeded}.")
                
                if not has_data_replication_succeeded:
                    return False

            return True

        except Exception as error:
            logging.error(f"Replication to secondaries has failed with the error: {error}")
            return False

    def _replicate_data_to_secondary(self, host, port, request_message: Message):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as secondary_client_socket:
            secondary_address = (host, port)
            secondary_client_socket.connect(secondary_address)

            request_message_buffer = MessageEncoder.encode_message(request_message)       
            secondary_client_socket.sendall(request_message_buffer)

            response_message_header_buffer = secondary_client_socket.recv(MessageEncoder.HEADER_BYTES_SIZE)
            response_message_header = MessageEncoder.decode_message_header(response_message_header_buffer)

            if response_message_header.type != MessageType.RESPONSE:
                logging.error("Unexpected message received! " +  
                             f"Expected {MessageType.RESPONSE}, but received {response_message_header.type}.")
                return False

            if response_message_header.number != request_message.header.number:
                logging.error("Response message number doesn't correspond to request message number! " +
                             f"Expected {request_message.header.number}, but received {response_message_header.number}.")
                return False

            # If response doesn't contain any data, it means success.
            # Otherwise, data represents an error message explaining why it has failed.
            if response_message_header.data_size > 0:
                error_message_buffer = secondary_client_socket.recv(response_message_header.data_size)
                error_message = error_message_buffer.decode("utf-8")
                logging.error(f"Secondary has failed to store data: {error_message}")
                return False

        return True

    def _create_message_for_data_replication(self, data:str) -> Message:
        with self.replication_number_locker:
            self.replication_number+=1
            return MessageFactory.create_request_message(self.replication_number, data)

app = FastAPI()

@app.on_event("startup")
def startup_event():
    app.master = Master()

@app.get("/get_data")
async def get_data():
    return {"messages": app.master.get_data()}

@app.post("/append_data")
async def append_data(data):
    if app.master.append_data(data):
        return {"Status": 200}
    else:
        return {"Status": 500}