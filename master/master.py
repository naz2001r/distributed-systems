import os
import json
import socket
from fastapi import FastAPI

SECONDARY_INFO = json.loads(os.environ['SECONDARY_INFO'])
app = FastAPI()

data = []

def get_secondary_answer(host,port,message):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(message.encode('utf-8'))
        status = s.recv(1024)
    return status

@app.get("/get_list")
async def root():
    return {"messages": data}

@app.post("/append")
async def root(message):
    status =[]
    for host,port in SECONDARY_INFO.items():
        status.append(get_secondary_answer(
            host=host,
            port=port,
            message=message
        ))

    data.append(message)
    return {"Status": 200}