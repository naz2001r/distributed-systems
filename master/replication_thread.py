import logging
import math
import socket
<<<<<<< HEAD
from threading import Event, Thread
from typing import Callable
from common.message import Message, MessageType
from common.message_encoder import MessageEncoder
=======
import logging

from typing import Callable
from threading import Event, Thread
>>>>>>> 8d37201bb30eb16586dd2aacdcf2a5e25dd5d41a

from replication_latch import ReplicationLatch
from health_checker import HealthChecker,HealthStatus

<<<<<<< HEAD
logging.basicConfig(level=logging.INFO)
=======
from common.message import Message, MessageType
from common.message_encoder import MessageEncoder
>>>>>>> 8d37201bb30eb16586dd2aacdcf2a5e25dd5d41a

class ReplicationThread(Thread):
    def __init__(self,
                replication_message: Message,
                replication_latch: ReplicationLatch, 
                host: str, 
                port: int,
                health_checker: HealthChecker) -> None:
        super().__init__()
        super().setDaemon(True)
        self.replication_latch = replication_latch
        self.replication_message = replication_message
        self.replication_retry_delay_event = Event()
        self.replication_retry_delay_seconds = None
        self.host = host
        self.port = port

        health_checker.add_health_status_changed_handler(
            self._adjust_replication_for_health_status
<<<<<<< HEAD
            )
=======
        )
>>>>>>> 8d37201bb30eb16586dd2aacdcf2a5e25dd5d41a

    def _adjust_replication_for_health_status(self, health_status: HealthStatus):
        if health_status == HealthStatus.UNHEALTHY:
            self.replication_retry_delay_seconds = None         

        elif health_status == HealthStatus.HEALTHY:
            self.replication_retry_delay_seconds = 0.1
<<<<<<< HEAD
            self.replication_retry_delay_event.set()        
=======
            self.replication_retry_delay_event.set()  
>>>>>>> 8d37201bb30eb16586dd2aacdcf2a5e25dd5d41a

    def _wait_before_next_replication_retry(self):
        self.replication_retry_delay_event.wait(self.replication_retry_delay_seconds)
        self.replication_retry_delay_event.clear()
        self.replication_retry_delay_seconds *= 2
<<<<<<< HEAD
=======
        logging.info(f"Next retry will be in {round(self.replication_retry_delay_seconds, 2)} seconds")
>>>>>>> 8d37201bb30eb16586dd2aacdcf2a5e25dd5d41a

    def run(self):        
        try:
            while not self._execute_replication():     
                self._wait_before_next_replication_retry()           

            self.replication_latch.set_replication(True)

        except Exception:
            self.replication_latch.set_replication(False)

<<<<<<< HEAD

=======
>>>>>>> 8d37201bb30eb16586dd2aacdcf2a5e25dd5d41a
    def _execute_replication(self) -> bool:
        try:
            logging.info(f"Replication to secondary '{self.host}:{self.port}' started...")

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as secondary_client_socket:
                secondary_address = (self.host, self.port)
                secondary_client_socket.connect(secondary_address)
                logging.info(f"Connected to '{self.host}:{self.port}'.")
                
                request_message_buffer = MessageEncoder.encode_message(self.replication_message)     
                secondary_client_socket.sendall(request_message_buffer)
                logging.info(f"Replicating data '{self.replication_message.data}'.")

                response_message_header_buffer = secondary_client_socket.recv(MessageEncoder.HEADER_BYTES_SIZE)
                response_message_header = MessageEncoder.decode_message_header(response_message_header_buffer)

                if response_message_header.type != MessageType.RESPONSE:
                    logging.error("Unexpected message received! " +  
                                 f"Expected '{MessageType.RESPONSE}', " +
                                 f"but received '{response_message_header.type}'.")
                    return False

                if response_message_header.number != self.replication_message.header.number:
                    logging.error("Response message number doesn't correspond to request message number! " +
                                 f"Expected '{self.replication_message.header.number}', "
                                 f"but received '{response_message_header.number}'.")
                    return False

                # If response doesn't contain any data, it means success.
                # Otherwise, data represents an error message explaining why it has failed.
                if response_message_header.data_size > 0:
                    error_message_buffer = secondary_client_socket.recv(response_message_header.data_size)
                    error_message = error_message_buffer.decode("utf-8")
                    logging.error(f"Secondary has failed to store data: '{error_message}'.")
                    return False

            logging.info(f"Replication to secondary '{self.host}:{self.port}' successfully ended.")
            return True
            
        except Exception as error:
            logging.error(f"Replication to secondaries has failed with the error: {error}")
<<<<<<< HEAD
            return False
=======
            return False
>>>>>>> 8d37201bb30eb16586dd2aacdcf2a5e25dd5d41a
