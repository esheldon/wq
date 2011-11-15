"""
    %prog command [other args]
"""
# Echo client program
import socket
import sys

from optparse import OptionParser

HOST = '127.0.0.1'    # The remote host
PORT = 51093              # The same port as used by the server
MAX_BUFFSIZE = 4096

parser=OptionParser(__doc__)
options, args = parser.parse_args(sys.argv[1:])
if len(args) < 1:
    parser.print_help()
    sys.exit(45)

data=' '.join(sys.argv[1:])

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))
s.send(data)
data = s.recv(MAX_BUFFSIZE)
s.close()
print 'Received', repr(data)

