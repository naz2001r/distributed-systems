from threading import Event, Thread
from typing import Callable

from replication_latch import ReplicationLatch

class ReplicationThread(Thread):
    def __init__(self, replication_action: Callable[[], bool], replication_latch: ReplicationLatch) -> None:
        super().__init__()
        super().setDaemon(True)
        self.replication_latch = replication_latch
        self.replication_action = replication_action
        self.replication_retry_stop_event = Event()
        self.replication_retry_delay_seconds = 0.1

    def stop_replication_retry(self):
        self.replication_retry_stop_event.set()

    def _retry_replication(self):
        # TODO: Think of a strategy for smart waiting logic. 
        return self.replication_retry_stop_event.wait(self.replication_retry_delay_seconds)

    def run(self):
        replication_succeeded = False

        while not replication_succeeded:
            replication_succeeded = self.replication_action()
            if replication_succeeded:
                break

            replication_retry_stopped = self._retry_replication()
            if replication_retry_stopped:
                break
        
        self.replication_latch.set_replication(replication_succeeded)