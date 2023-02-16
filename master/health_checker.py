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
        self.current_health_status = HealthStatus.HEALTHY
        self.on_health_status_changed_handlers = []
        self.client_socket = None
        self.address = (host, port)
        self.connected = False
        self._connect()

    def __del__(self):
        self.client_socket.close()

    def get_current_health_status(self):
        return self.current_health_status

    def add_health_status_changed_handler(
        self, 
        health_status_change_handler: Callable[[HealthStatus], None]) -> None:
        health_status_change_handler(self.current_health_status)
        self.on_health_status_changed_handlers.append(health_status_change_handler)

    def _health_status_changed(self, health_status) -> None:
        self.current_health_status = health_status
        for health_status_change_handler in self.on_health_status_changed_handlers:
            health_status_change_handler(health_status)

    def add_health_status_changed_handler(
        self, 
        health_status_change_handler: Callable[[HealthStatus], None]) -> None:
        health_status_change_handler(self.current_health_status)
        self.on_health_status_changed_handlers.append(health_status_change_handler)

    def _health_status_changed(self, health_status) -> None:
        self.current_health_status = health_status
        for health_status_change_handler in self.on_health_status_changed_handlers:
            health_status_change_handler(health_status)
            
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
                self._health_status_changed(HealthStatus.UNHEALTHY)

            if response_header.number != MessageHeader.HEALTHCHECK_MSG_NUM:
                logging.error("Response message number doesn't correspond to health check message! " +
                             f"Expected '{request.header.number}', "
                             f"but received '{response_header.number}'.")
                self._health_status_changed(HealthStatus.UNHEALTHY)

            # If response doesn't contain any data, it means success.
            # Otherwise, data represents an error message explaining why it has failed.
            if response_header.data_size > 0:
                error_message_buffer = self.client_socket.recv(response_header.data_size)
                error_message = error_message_buffer.decode("utf-8")
                logging.error(f"Secondary has health problem : {error_message}.")
                self._health_status_changed(HealthStatus.UNHEALTHY)

            self._health_status_changed(HealthStatus.HEALTHY)

        except socket.timeout:
            self._health_status_changed(HealthStatus.SUSPECTED)

        except socket.error:  
            while not self.connected:  
                self._health_status_changed(HealthStatus.UNHEALTHY)
                self._wait_before(self._connect)

        except Exception as error:
            logging.error(f'Unexpected error: {error}')
            self._health_status_changed(HealthStatus.UNHEALTHY)
  
    def run(self):
        while True:  
            self._wait_before(self._healthcheck)  