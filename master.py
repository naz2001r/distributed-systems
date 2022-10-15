import os
import sys
import socket
from fastapi import FastAPI

HOST = "127.0.0.1"  # The server's hostname or IP address
PORT = 65432  # The port used by the server

app = FastAPI()

data = []

@app.get("/get_list")
async def root():
    return {"messages": data}

@app.post("/append")
async def root(message):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        s.sendall(message.encode('utf-8'))
        #data = s .recv(1024)

    data.append(message)
    return {"Status": 200}