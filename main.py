import paramiko
import time
from dotenv import load_dotenv
import os

load_dotenv()

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(
    hostname = os.getenv("host"),
    username = os.getenv("user"),
    password = os.getenv("password")
)

# Start interactive shell
channel = client.invoke_shell()

# Run your initial command
channel.send(os.getenv("home_dir") + "\n")
channel.send("bash -l\n")
time.sleep(1)

print(channel.recv(9999).decode())
# Now run follow‑up commands
while True:
    input_command: str = input("Input the command you want to send: ")
    if input_command == "quit":
        print("Closing Connection")
        break
    channel.send(input_command + '\n')
    time.sleep(1)
    print(channel.recv(9999).decode())
    
# Read output

# (Optional) More commands later:
# channel.send("ls -l\n")
# print(channel.recv(9999).decode())

channel.close()
client.close()
