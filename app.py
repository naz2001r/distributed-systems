import os
import sys
from fastapi import FastAPI


app = FastAPI()

data = []

@app.get("/get_list")
async def root():
    return {"messages": data}

@app.post("/append")
async def root(message):
    data.append(message)
    return {"Status": 200}