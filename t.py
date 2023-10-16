import json
import socket


host = "127.0.0.1"
port = 6379

with  socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((host, port))
    s.sendall(b"GET hello \"world\"\0")
    data: list[bytes] = []
    while (i:=s.recv(1024)):
        data.append(i)
    result = b"".join(data)
json.dump(json.loads(result), open("pyresult.json", "w"), indent=2)