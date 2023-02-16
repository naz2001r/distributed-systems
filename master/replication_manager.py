from typing import Callable
from common.message import Message

from health_checker import HealthChecker
from replication_latch import ReplicationLatch
from replication_thread import ReplicationThread

class ReplicationManager():
    def __init__(self,
                 replication_message: Message,
                 replication_awaited: int) -> None:
        self.replication_message = replication_message
        self.replication_latch = ReplicationLatch(replication_awaited)
        self.replication_threads = []

    def start_replication(self, host: str, port: int, health_checker: HealthChecker):
        replication_thread = ReplicationThread(self.replication_message, self.replication_latch, host, port, health_checker)
        replication_thread.start()
        self.replication_threads.append(replication_thread)

    def wait_for_replication(self, timeout: float = None):
        return self.replication_latch.wait_for_replications(timeout=timeout)