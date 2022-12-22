from typing import Callable
from common.message import Message

from master.replication_latch import ReplicationLatch
from master.replication_thread import ReplicationThread

class ReplicationManager():
    def __init__(self,
                 replication_executor: Callable[[str, int, Message], bool],
                 replication_data: Message,
                 replication_awaited: int) -> None:
        self.replication_executor = replication_executor
        self.replication_data = replication_data
        self.replication_latch = ReplicationLatch(replication_awaited)
        self.replication_threads = []

    def start_replication(self, host: str, port: int):
        replication_action = lambda: self.replication_executor(host, port, self.replication_data)
        replication_thread = ReplicationThread(replication_action, self.replication_latch)
        replication_thread.start()
        self.replication_threads.append(replication_thread)

    def stop_all_replication(self):
        for replication_thread in self.replication_threads:
            replication_thread.stop_replication_retry()

    def wait_for_replication(self, timeout: int):
        return self.replication_latch.wait_for_replications(timeout=timeout)