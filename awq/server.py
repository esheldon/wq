# Echo server program ipv4
import socket

HOST = ''      # Symbolic name meaning all available interfaces
PORT = 51093   # Arbitrary non-privileged port
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
            print 'got command:',data,'; sending back'
            conn.send(data)
        conn.close()

except KeyboardInterrupt:
    pass
