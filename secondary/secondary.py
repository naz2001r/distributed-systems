import os
import socket
from fastapi import FastAPI
from threading import Thread

app = FastAPI()

HOST = "0.0.0.0"  # Standard loopback interface address (localhost)
PORT = int(os.environ['PORT'])  # Port to listen on (non-privileged ports are > 1023)

data = []

@app.get("/get_list")
async def root():
    return {"messages": data}


def start_server(s, continue_run):
    try:
        while continue_run:
            conn, addr = s.accept()
            with conn:
                print(f"Connected by {addr}")
                message = conn.recv(1024)
                if message:
                    data.append(message)
                    conn.sendall(b'Done')
    except:
        print('Close')

@app.on_event("startup")
def startup_event():
    app.continue_run = True
    app.s= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    app.s.bind((HOST, PORT))
    app.s.listen()
    app.thread = Thread(target=start_server, args=(app.s,app.continue_run))
    app.thread.start()

@app.on_event("shutdown")
def shutdown_event():
    app.continue_run = False
    app.s.close()
    app.thread.join()