import socket
from fastapi import FastAPI

HOST = "127.0.0.1"  # Standard loopback interface address (localhost)
PORT = 65432  # Port to listen on (non-privileged ports are > 1023)
data = []

@app.get("/get_list")
async def root():
    return {"messages": data}

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    conn, addr = s.accept()
    with conn:
        print(f"Connected by {addr}")

        message = conn.recv(1024)
        if message:
            data.append(message)

        conn.sendall(b'done')