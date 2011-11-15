# Echo server program ipv4
import socket
import json

HOST = ''      # Symbolic name meaning all available interfaces
PORT = 51094   # Arbitrary non-privileged port
MAX_BUFFSIZE = 4096

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((HOST, PORT))
s.listen(1)

try:
    while 1:
        conn, addr = s.accept()
        print 'Connected by', addr
        while 1:
            data = conn.recv(MAX_BUFFSIZE)
            if not data: 
                break
            try:
                obj=json.loads(data)
                print 'got command:',data,'; sending back'
                ret=json.dumps({"response":"OK"})
                conn.send(ret)
            except:
                ret = {"error":"could not parse JSON request: '%s'" % data}
                ret = json.dumps(ret)
                conn.send(ret)
        conn.close()

except KeyboardInterrupt:
    pass
