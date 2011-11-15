#!/usr/bin/env python
"""
    %prog command [other args]

    Command can be one of the following:

        ls : lists the queue

        rm : remove an object from the queue 

        submit : submits a new job
        
        Submit syntax:

            ./client.py submit submit_mode -- commandline
            
            submit_mode can be:
                 cores=5 : ask for five cores
                 cores_on_node= 3 : ask for 3 cores on the same node
                 nodes=2 : ask for 2 nodes
                 block=gen2 : ask for the entire block called gen2
                 
            if no submit_mode is specified, cores=1 is assumed
        
    
"""
# Echo client program
import socket
import sys
import json
import os

from optparse import OptionParser

HOST = '127.0.0.1'    # The remote host
PORT = 51093              # The same port as used by the server
MAX_BUFFSIZE = 4096


parser=OptionParser(__doc__)

try:
    kk=sys.argv.index('--')  ## end of my options
    commandline = " ".join(sys.argv[kk+1:])
    myargs=sys.argv[1:kk]
except:
     commandline=None
     myargs=sys.argv[1:]



options, args = parser.parse_args(myargs)

if len(args) < 1:
    parser.print_help()
    sys.exit(0)

commands = ['submit', 'ls', 'rm']
command=args.pop(0)
if (command not in commands):
    print " Bad command, must be on of : ", ','.join(commands)
    sys.exit(1)

dict={}
dict['command']=command

if command == 'submit':
    if (not commandline):
        print "Need to supply command line."
        exit(1)


    if (len(args)<1):
        submit_mode = "cores=1"
        print "No submit mode, assuming: ", submit_mode
    else:
        submit_mode = args.pop(0)

    dict['submit_mode']=submit_mode
    dict['command_line']=commandline

dict['pid'] = os.getpid()
dict['hostname'] = socket.gethostname()



s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))
s.send(json.dumps(dict))
rdict = json.loads(s.recv(MAX_BUFFSIZE))
s.close()

## now parse what we got back

if "error" in rdict:
    print "Error reported by server."
    print rdict['error']
    sys.exit(1)
if ("response"  not in rdict):
    print "Internal error. Expected a response and got screwed."
    sys.exit(1)

print "Servery says :", rdict["response"]


