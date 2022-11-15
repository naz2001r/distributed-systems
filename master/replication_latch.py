from threading import Condition

class ReplicationLatch:

    def __init__(self, replication_awaited) -> None:
        self.replication_awaited = replication_awaited
        self.replication_finished = Condition()
        self.replication_status = True

    def wait_for_replications(self, timeout: float):
        with self.replication_finished:
            if self.replication_awaited <= 0:
                return self.replication_status

            if not self.replication_finished.wait(timeout=timeout):
                raise TimeoutError()
                
            return self.replication_status
        
    def set_replication(self, replication_status):
        with self.replication_finished:
            self.replication_awaited -= 1
            self.replication_status*=replication_status

            if self.replication_awaited <=0:
                self.replication_finished.notify_all()