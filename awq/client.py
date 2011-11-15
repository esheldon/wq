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
import signal 

from optparse import OptionParser

HOST = '127.0.0.1'    # The remote host
PORT = 51093              # The same port as used by the server
MAX_BUFFSIZE = 4096


parser=OptionParser(__doc__)
parser.add_option("-H", "--host-file-to", dest="hostfile",
                  help="write hostfile to")

parser.add_option("-n", "--num-units", dest="N",
                  help="Number of units (cores,nodes)", default=1)

parser.add_option("-g", "--in-group", dest="ingr",
                  help="must be in group", default="")

parser.add_option("-x", "--not-in-group", dest="ningr",
                  help="most not be in group", default="")

parser.add_option("-c", "--min-cores", dest="min_cores",
                  help="min cores when asking bynode", default=1)

parser.add_option("-d", "--node", dest="node",
                  help="node when asking exactnode", default="")



def TalkToServer (dictout):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    s.send(json.dumps(dictout))
    rdict = json.loads(s.recv(MAX_BUFFSIZE))
    s.close()

    if "error" in rdict:
        print "Error reported by server."
        print rdict['error']
        sys.exit(1)

    if ("response"  not in rdict):
        print "Internal error. Expected a response and got screwed."
        sys.exit(1)


    return rdict

def receive_signal(a,b):
    pass

def main():

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
    req={}

    if command == 'submit':
        if (not commandline):
            print "Need to supply command line."
            exit(1)


        if (len(args)<1):
            submit_mode = "bycore"
            print "No submit mode, assuming: ", submit_mode
        else:
            submit_mode = args.pop(0)

        req['submit_mode']=submit_mode
        req['N']=int(options.N)
        req['node']=options.node
        req['in_group']=options.ingr.split(',')
        req['not_in_group']=options.ningr.split(',')
        req['min_cores']=int(options.min_cores)
        
        dict['command_line']=commandline
        dict['require']=req

    dict['pid'] = os.getpid()
    dict['hostname'] = socket.gethostname()



    rdict = TalkToServer(dict)

    ## now parse what we got back


    print "Server says :", rdict["response"]


    if (command=='ls'):
        for job in rdict['response']:
            print job
        sys.exit(0)

    if (command=='rm'):
        print "Done."
        sys.exit(1)


    if (not command=='submit'):
        print " We really shouldn't be here."
        sys.exit(1)

    ### Assuming we work with submit now.


    signal.signal(signal.SIGUSR1, receive_signal)

    while (rdict["response"] == "wait"):
        signal.pause()
        #print "Trying again."
        #rdict = TalkToServer(dict)


    if (rdict["response"] != "run"):
        print "Response from server should be wait or run!"
        sys.exit(1)

    hosts = rdict["hosts"]

    try:
        hostfile=options.hostfile
    except:
        hostfile=None

    if (hostfile):
        f= open(hostfile,'w')
        for host in hosts:
            f.write (host+"\n")
        f.close()


    ### Now execute
    target= hosts[0]
    pwd = os.getcwd()
    ## here we execute
    os.system ('ssh -t '+target+'  "cd '+pwd+'; bash"')


    ## Now notify the server that we are done.

    dict['command'] = 'notify'
    dict['notification'] = 'jobdone'

    rdict = TalkToServer(dict)
    print " Job done, servey says:" ,rdict['response']

        
if __name__=="__main__":
    main()
