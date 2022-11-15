import asyncio
import os
import ast
import json
import socket
import logging
from typing import List
from fastapi import FastAPI, HTTPException
from threading import Lock

from common.message import Message, MessageFactory, MessageType
from common.message_encoder import MessageEncoder
from replication_latch import ReplicationLatch

logging.basicConfig(level=logging.INFO)

class Master:
    def __init__(self) -> None:
        self.message_number = 0
        self.message_number_locker = Lock()
        self.data_storage = []
        self.event_loop = asyncio.get_running_loop()

        # For local run use: {"0.0.0.0": 65441, "localhost": 65442}
        info_json = json.dumps(os.environ['SECONDARY_INFO'])
        self.secondaries_info = ast.literal_eval(json.loads(info_json))
        logging.info(f"Secondary info: {self.secondaries_info}")

        # Quantity of secondaries plus a single master defines a maximum value for a write concern.
        self.max_write_concern = len(self.secondaries_info) + 1

    def append_data(self, data:str, write_concern:int) -> bool:
        if write_concern > self.max_write_concern:
            logging.info(f"Write concern of '{write_concern}' is too big for a current application. " +
                         f"Maximum write concern equals to '{self.max_write_concern}'.")
            return False

        try:
            data_replication_awaited = write_concern - 1
            data_replication_latch = ReplicationLatch(data_replication_awaited)
            data_replication_tasks = self._replicate_data_to_secondaries(data, data_replication_latch)
            data_replication_status = data_replication_latch.wait_for_replications(timeout=100.0)

            if not data_replication_status:
                logging.error(f"Appending data '{data}' has failed.")
                return False

            self.data_storage.append(data)
            return True

        except TimeoutError:
            logging.error(f"Appending data '{data}' has timed out.")
            return False

    def get_data(self) -> List[str]:
        return self.data_storage
    
    def _replicate_data_to_secondaries(self, data:str, data_replication_latch: ReplicationLatch) -> list:
        data_replication_tasks = []
        data_replication_message = self._create_message_for_data_replication(data)

        for host, port in self.secondaries_info.items():     
            data_replication_coro = self._replicate_data_to_secondary(
                host=host,
                port=port,
                request_message=data_replication_message,
                replication_latch=data_replication_latch
                )
                 
            data_replication_task = self.event_loop.create_task(data_replication_coro)
            data_replication_tasks.append(data_replication_task)

        return data_replication_tasks

    async def _replicate_data_to_secondary(self, host, port, request_message: Message, replication_latch: ReplicationLatch):
        try:
            logging.info(f"Replication to secondary '{host}:{port}' started...")

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as secondary_client_socket:
                secondary_address = (host, port)
                secondary_client_socket.connect(secondary_address)
                logging.info(f"Connected to '{host}:{port}'.")

                request_message_buffer = MessageEncoder.encode_message(request_message)     
                await self.event_loop.sock_sendall(secondary_client_socket, request_message_buffer)
                logging.info(f"Replication data '{request_message.data}'.")

                response_message_header_buffer = await self.event_loop.sock_recv(secondary_client_socket, MessageEncoder.HEADER_BYTES_SIZE)
                response_message_header = MessageEncoder.decode_message_header(response_message_header_buffer)

                if response_message_header.type != MessageType.RESPONSE:
                    logging.error("Unexpected message received! " +  
                                 f"Expected '{MessageType.RESPONSE}', " +
                                 f"but received '{response_message_header.type}'.")
                    replication_latch.set_replication(False)
                    return

                if response_message_header.number != request_message.header.number:
                    logging.error("Response message number doesn't correspond to request message number! " +
                                 f"Expected '{request_message.header.number}', "
                                 f"but received '{response_message_header.number}'.")
                    replication_latch.set_replication(False)
                    return

                # If response doesn't contain any data, it means success.
                # Otherwise, data represents an error message explaining why it has failed.
                if response_message_header.data_size > 0:
                    error_message_buffer = secondary_client_socket.recv(response_message_header.data_size)
                    error_message = error_message_buffer.decode("utf-8")
                    logging.error(f"Secondary has failed to store data: '{error_message}'.")
                    replication_latch.set_replication(False)
                    return

            logging.info(f"Replication to secondary '{host}:{port}' successfully ended.")
            replication_latch.set_replication(True)
            return
            
        except Exception as error:
            logging.error(f"Replication to secondaries has failed with the error: {error}")
            replication_latch.set_replication(False)

    def _create_message_for_data_replication(self, data:str) -> Message:
        with self.message_number_locker:
            self.message_number+=1
            return MessageFactory.create_request_message(self.message_number, data)

app = FastAPI()

@app.on_event("startup")
def startup_event():
    app.master = Master()

@app.get("/get_data")
async def get_data():
    return {"messages": app.master.get_data()}

@app.post("/append_data")
def append_data(data: str, write_concern: int):
    append_status = app.master.append_data(data, write_concern)

    if not append_status:
        # Possible TODO: Propagate errors to the WebAPI.
        raise HTTPException(500)
        
    return {f"Appending data '{data}' has succeeded."}
        