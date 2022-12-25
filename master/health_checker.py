import socket
import logging

from enum import IntEnum 
from typing import Callable
from threading import Thread,Timer

from common.message_encoder import MessageEncoder  
from common.message import MessageFactory, MessageType

logging.basicConfig(level=logging.INFO)


class HealthStatus(IntEnum):
    HEALTHY = 0
    SUSPECTED = 1
    UNHEALTHY = 2

class HealthChecker(Thread):
    HEALTHCHECK_MSG_NUM=0

    def __init__(self, host: str,port: int):
        super().__init__()
        super().setDaemon(True)
        # configure socket and connect to server  
        self.health_status = HealthStatus.HEALTHY
        self.clientSocket = socket.socket()  
        self.address = ( host,port )
        self.connected = False
        self.connect()

    def __del__(self):
        self.clientSocket.close()

    def get_status(self) -> HealthStatus:
        return self.health_status

    def _connect(self) -> None:
        try:  
            self.clientSocket.connect(self.address)  
            self.clientSocket.settimeout(5.0)
            logging.warning("(re)connection successful.")  
            self.connected = True 
        except socket.error:  
            self.connected = False

    def _wait_before(self,func:Callable, time:float = 0.1)->None:
        timer = Timer(time, func)
        timer.start()
        timer.join()
    
    def _healthcheck(self) -> None:
        # attempt to send and receive wave, otherwise reconnect  
        try:
            request = MessageFactory.create_healthcheck_message(self.HEALTHCHECK_MSG_NUM)
            request_bytes = MessageEncoder.encode_message(request)     
            self.clientSocket.send(request_bytes)  

            response_header_bytes = self.clientSocket.recv(MessageEncoder.HEADER_BYTES_SIZE)
            response_header = MessageEncoder.decode_message_header(response_header_bytes)

            if response_header.type != MessageType.HEALTHCHECK:
                logging.error("Unexpected message received! " +  
                                f"Expected '{MessageType.HEALTHCHECK}', " +
                                f"but received '{response_header.type}'.")
                self.health_status = HealthStatus.UNHEALTHY

            if response_header.number != request.header.number:
                logging.error("Response message number doesn't correspond to request message number! " +
                                f"Expected '{request.header.number}', "
                                f"but received '{response_header.number}'.")
                self.health_status = HealthStatus.UNHEALTHY

            # If response doesn't contain any data, it means success.
            # Otherwise, data represents an error message explaining why it has failed.
            if response_header.data_size > 0:
                error_message_buffer = self.clientSocket.recv(response_header.data_size)
                error_message = error_message_buffer.decode("utf-8")
                logging.error(f"Secondary has health problem : {error_message}.")
                self.health_status = HealthStatus.UNHEALTHY

            self.health_status = HealthStatus.HEALTHY

        except socket.timeout:
            self.health_status = HealthStatus.SUSPECTED 

        except socket.error:  
            # set connection status and recreate socket  
            self.clientSocket = socket.socket()  
            logging.warning("Connection lost... reconnecting")  
            while not self.connected:  
                # attempt to reconnect, otherwise sleep for 0.1 seconds  
                self.health_status = HealthStatus.UNHEALTHY
                self._wait_before(self._connect)
  
    def run(self):
        while True:  
            self._wait_before(self._healthcheck)  