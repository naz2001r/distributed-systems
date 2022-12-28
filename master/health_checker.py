import socket
import logging

from enum import Enum 
from typing import Callable
from threading import Thread,Timer

from common.message_encoder import MessageEncoder  
from common.message import MessageFactory, MessageHeader, MessageType

logging.basicConfig(level=logging.INFO)


class HealthStatus(Enum):
    HEALTHY = 'HEALTHY'
    SUSPECTED = 'SUSPECTED'
    UNHEALTHY = 'UNHEALTHY'

class HealthChecker(Thread):

    def __init__(self, host: str, port: int):
        super().__init__()
        super().setDaemon(True)
        self.health_status = HealthStatus.HEALTHY
        self.client_socket = None
        self.address = (host, port)
        self.connected = False
        self._connect()

    def __del__(self):
        self.client_socket.close()

    def get_status(self) -> HealthStatus:
        return self.health_status

    def _connect(self) -> None:
        try:  
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect(self.address)  
            self.client_socket.settimeout(5.0)
            self.connected = True 

        except socket.error:  
            self.connected = False

    def _wait_before(self, func: Callable, time: float = 0.1) -> None:
        timer = Timer(time, func)
        timer.start()
        timer.join()
    
    def _healthcheck(self) -> None:
        # attempt to send and receive wave, otherwise reconnect  
        try:
            request = MessageFactory.create_healthcheck_request_message()
            request_bytes = MessageEncoder.encode_message(request)     
            self.client_socket.sendall(request_bytes)  

            response_header_bytes = self.client_socket.recv(MessageEncoder.HEADER_BYTES_SIZE)
            response_header = MessageEncoder.decode_message_header(response_header_bytes)

            if response_header.type != MessageType.RESPONSE:
                logging.error("Unexpected message received in health check! " +  
                             f"Expected '{MessageType.RESPONSE}', " +
                             f"but received '{response_header.type}'.")
                self.health_status = HealthStatus.UNHEALTHY

            if response_header.number != MessageHeader.HEALTHCHECK_MSG_NUM:
                logging.error("Response message number doesn't correspond to health check message! " +
                             f"Expected '{request.header.number}', "
                             f"but received '{response_header.number}'.")
                self.health_status = HealthStatus.UNHEALTHY

            # If response doesn't contain any data, it means success.
            # Otherwise, data represents an error message explaining why it has failed.
            if response_header.data_size > 0:
                error_message_buffer = self.client_socket.recv(response_header.data_size)
                error_message = error_message_buffer.decode("utf-8")
                logging.error(f"Secondary has health problem : {error_message}.")
                self.health_status = HealthStatus.UNHEALTHY

            self.health_status = HealthStatus.HEALTHY

        except socket.timeout:
            self.health_status = HealthStatus.SUSPECTED 

        except socket.error:  
            while not self.connected:  
                self.health_status = HealthStatus.UNHEALTHY
                self._wait_before(self._connect)

        except Exception as error:
            logging.error(f'Unexpected error: {error}')
            self.health_status = HealthStatus.UNHEALTHY
  
    def run(self):
        while True:  
            self._wait_before(self._healthcheck)  