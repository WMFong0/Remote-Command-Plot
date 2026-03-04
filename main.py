import os
import time
import paramiko
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# Data model for the input endpoint
class InputData(BaseModel):
    command: str

# Global variables to store the SSH state
client = None
channel = None

@app.get("/health")
def health_check():
    connected = client is not None and client.get_transport() is not None and client.get_transport().is_active()
    return {"status": "healthy", "ssh_connected": connected}

@app.post("/open")
async def open_connection():
    global client, channel
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=os.getenv("host"),
            username=os.getenv("user"),
            password=os.getenv("password")
        )
        
        # Start interactive shell
        channel = client.invoke_shell()
        
        # Initial setup commands
        channel.send(os.getenv("home_dir") + "\n")
        channel.send("bash -l\n")
        time.sleep(1)
        
        initial_output = channel.recv(9999).decode()
        return {"status": "connected", "output": initial_output}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Connection failed: {str(e)}")

@app.post("/input")
async def post_input(data: InputData):
    global channel
    if not channel or channel.closed:
        raise HTTPException(status_code=400, detail="SSH channel is not open. Call /open first.")

    try:
        # Send command received in POST
        channel.send(data.command + '\n')
        time.sleep(1) # Wait for remote processing
        
        # Read available output
        output = ""
        if channel.recv_ready():
            output = channel.recv(9999).decode()
            
        return {"command": data.command, "output": output}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/close")
async def close_connection():
    global client, channel
    try:
        if channel:
            channel.close()
        if client:
            client.close()
        return {"status": "closed"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
