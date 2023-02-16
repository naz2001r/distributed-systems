import os
import ast
import json
import logging
from typing import List
from threading import Lock
from fastapi import FastAPI, HTTPException

from replication_manager import ReplicationManager
from health_checker import HealthChecker,HealthStatus
from common.message import Message, MessageFactory

logging.basicConfig(level=logging.INFO)

class Master:
    QUORUM = 2

    def __init__(self) -> None:
        self.message_number = 0
        self.message_number_locker = Lock()
        self.data_storage = []

        # For local run use: {"0.0.0.0": 65441, "localhost": 65442}
        info_json = json.dumps(os.environ['SECONDARY_INFO'])
        self.secondaries_info = ast.literal_eval(json.loads(info_json))
        logging.info(f"Secondary info: {self.secondaries_info}")

        # Quantity of secondaries plus a single master defines a maximum value for a write concern.
        self.max_write_concern = len(self.secondaries_info) + 1

        self.health_checkers = {}
        for host, port in self.secondaries_info.items():
            health_checker = HealthChecker(host, port)
            self.health_checkers[host] = health_checker
            health_checker.start()

    def append_data(self, data: str, write_concern: int) -> bool:
        if write_concern > self.max_write_concern:
            logging.error(f"Write concern of '{write_concern}' is too big for a current application. " +
                         f"Maximum write concern equals to '{self.max_write_concern}'.")
            return False
        
        if write_concern <= 0:
            logging.error(f"Write concern of '{write_concern}' is too small for a current application. " +
                         f"Write concern must be higher than '0'.")
            return False

        self.data_storage.append(data)
        
        data_replication_status = self._replicate_data_to_secondaries(data, write_concern)
        if not data_replication_status:
            logging.error(f"Appending data '{data}' has failed.")
            return False

        return True

    def get_data(self) -> List[str]:
        return self.data_storage
    
    def _replicate_data_to_secondaries(self, data: str, write_concern: int) -> list:
        data_replication_message = self._create_message_for_data_replication(data)
        data_replication_manager = ReplicationManager(replication_message=data_replication_message,
                                                      replication_awaited=write_concern-1)

        for host, port in self.secondaries_info.items():
            health_checker = self.health_checkers[host]
            data_replication_manager.start_replication(host, port, health_checker)

        try:
            data_replication_status = data_replication_manager.wait_for_replication()
            return data_replication_status

        except TimeoutError:
            logging.error(f"Replication of data '{data}' has timed out.")
            return False

    def _create_message_for_data_replication(self, data:str) -> Message:
        with self.message_number_locker:
            self.message_number+=1
            return MessageFactory.create_request_message(self.message_number, data)

    def _check_for_replication_quorum(self) -> bool:
        healthy_nodes_count = len([checker for checker in self.health_checkers.values() 
                                   if checker.get_current_health_status() == HealthStatus.HEALTHY ])+1
        return self.QUORUM <= healthy_nodes_count


app = FastAPI()

@app.on_event("startup")
def startup_event():
    app.master = Master()

@app.get("/get_data")
async def get_data():
    return {"messages": app.master.get_data()}

@app.get("/get_health")
async def get_health():
    return { key: value.get_current_health_status() for key,value in app.master.health_checkers.items()}

@app.post("/append_data")
def append_data(data: str, write_concern: int):
    if not app.master._check_for_replication_quorum():
        return {f"Appending data is not possible, " + 
                 "because replication quorum '{app.master.QUORUM}' is not satisfied."}

    append_status = app.master.append_data(data, write_concern)
    if not append_status:
        # Possible TODO: Propagate errors to the WebAPI.
        raise HTTPException(500)
        
    return {f"Appending data '{data}' has succeeded."}
        
        