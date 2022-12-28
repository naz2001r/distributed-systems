import math
from threading import Event, Thread
from typing import Callable

from replication_latch import ReplicationLatch
from health_checker import HealthChecker,HealthStatus

class ReplicationThread(Thread):
    def __init__(self, replication_action: Callable[[], bool], replication_latch: ReplicationLatch, host: str, port: int) -> None:
        super().__init__()
        super().setDaemon(True)
        self.replication_latch = replication_latch
        self.replication_action = replication_action
        self.replication_retry_stop_event = Event()
        self.replication_retry_delay_seconds = 0.1
        self.host = host
        self.port = port

    def stop_replication_retry(self):
        self.replication_retry_stop_event.set()

    def _retry_replication(self, retry_num):
        if retry_num == 1:
            return self.replication_retry_stop_event.wait(self.replication_retry_delay_seconds)
        else:
            coeff = 0.25 # coefficient for getting time less then 1 sec for the few first retries
            retry_delay_time = coeff*math.exp(retry_num*self.replication_retry_delay_seconds)
            return self.replication_retry_stop_event.wait(retry_delay_time)

    def run(self):
        replication_succeeded = False
        retry_num = 1

        while not replication_succeeded:
            replication_succeeded = self.replication_action()
            if replication_succeeded:
                break
            
            health_checker = HealthChecker(self.host, self.port)
            status = health_checker.get_status()
            
            if status == HealthStatus.HEALTHY:
                replication_retry_stopped = self._retry_replication(retry_num)
                retry_num += 1
            else:
                continue
            
            if replication_retry_stopped:
                break
        retry_num = 1
        self.replication_latch.set_replication(replication_succeeded)