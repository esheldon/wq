"""
    %prog [options] cluster_description_fname

The description file is 

    hostname ncores mem labels

The labels are optional.
"""

import socket
import json
import copy

HOST = ''      # Symbolic name meaning all available interfaces
PORT = 51093   # Arbitrary non-privileged port
MAX_BUFFSIZE = 4096

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind((HOST, PORT))
# only listen for this many seconds, then refresh the queue
sock.settimeout(30.0)
sock.listen(1)

from optparse import OptionParser
parser=OptionParser(__doc__)

class Request(dict):
    def __init__(self, request):
        self.request_string = request
    def json_decode(self):
        self.request = json.loads(self.request_string)



class Node:
    def __init__ (self, line):
        ls = line.split()
        host, ncores, mem = ls[0:3]

        if len(ls) > 3:
            self.grps = ls[3].split(',')
        else:
            self.grps = []

        self.host   = host
        self.ncores = int(ncores)
        self.mem    = float(mem)
        self.used   = 0

    def Reserve(self):
        self.used+=1
        if (self.used>self.ncores):
            print "Internal error."
            sys.exit(1)

    def Unreserve(self):
        self.used-=1
        if (self.used<0):
            print "Internal error."
            sys.exit(1)
            
    

class Cluster:
    def __init__(self,filename):
        self.filename=filename
        self.nodes={}

        for line in open(filename):
            nd = Node(line);
            self.nodes[nd.host] = nd
    
    def Reserve(self,hosts):
        for h in hosts:
            self.nodes[h].Reserve()

    def Unreserve(self,hosts):
        for h in hosts:
            self.nodes[h].Unreserve()

    def Status(self):
        res={}
        tot=0
        used=0
        use=[]
        for h in self.nodes:
            tot+=self.nodes[h].ncores
            used+=self.nodes[h].used
            if (self.nodes[h].used>0):
              use.append((h,self.nodes[h].used))  

        res['used']=used
        res['ncores']=tot
        res['nnodes']=len(self.nodes)
        res['use']=use
        return res



class Job(dict):

    def __init__(self, message):
        # make sure pid,require are in message
        # and copy them into self

        for k in message:
            self[k] = message[k]
        self['status'] = 'wait'

    def match(self, cluster):
        if (self['status']!='wait'):
            return

        submit_mode=self['require']['submit_mode']

        # can also add reasons from the match methods
        # later

        reason=None
        if (submit_mode=='bycore'):
            pmatch, match, hosts = self.match_bycore(cluster)
        elif (submit_mode=='bynode'):
            pmatch, match, hosts = self.match_bynode(cluster)
        elif (submit_mode=='byhost'):
            pmatch, match, hosts = self.match_byhost(cluster)
        else:
            pmatch=False ## unknown request never mathces
            reason="bad submit_mode '%s'" % submit_mode

        if pmatch:
            if match:
                self['hosts']=hosts
                cluster.Reserve(hosts)
                self['status']='run'
            else:
                 self['status']='wait'
        else:
            self['status']='nevermatch'
            self['reason']=reason

    def match_bycore(self, cluster):
        reqs = self['require']
        N=reqs['N']
        Np=N

        pmatch=False
        match=False
        hosts=[] # actually matched hosts

        for h in cluster.nodes:
            nd = cluster.nodes[h]

            ## is this node actually what we want
            ing=reqs['in_group']
            if (len(ing)>0): ##any group in any group
                ok=False
                for g in ing:
                    if g in nd.grps:
                        ok=True
                        break
                if (not ok):
                    continue ### not in the group
                    
            ing=reqs['not_in_group']
            if (len(ing)>0): ##any group in any group
                ok=True
                for g in ing:
                    if g in nd.grps:
                        ok=False
                        break
                if (not ok):
                    continue ### not in the group

            if (nd.ncores>Np):
                pmatch=True
            else:
                Np-=nd.ncores

            nfree= nd.ncores-nd.used
            if (nfree>=N):
                for x in xrange(N):
                    hosts.append(h)
                N=0
                match=True
                break #we are done
            else:
                N-=nfree
                for x in xrange(nfree):
                    hosts.append(h)

        return pmatch, match, hosts


    def match_bynode(self, cluster):
        reqs = self['require']
        N=reqs['N']
        Np=N

        pmatch=False
        match=False
        hosts=[] # actually matched hosts


        N=reqs['N']
        Np=N
        for h in cluster.nodes:
            nd = cluster.nodes[h]
            if (nd.ncores<reqs['min_cores']):
                continue
            ## is this node actually what we want
            ing=reqs['in_group']
            if (len(ing)>0): ##any group in any group
                ok=False
                for g in ing:
                    if g in nd.grps:
                        ok=True
                        break
                if (not ok):
                    continue ### not in the group

                    
            ing=reqs['not_in_group']
            if (len(ing)>0): ##any group in any group
                ok=True
                for g in ing:
                    if g in nd.grps:
                        ok=False
                        break
                if (not ok):
                    continue ### not in the group
            
            Np-=1
            if (Np==0):
                pmatch=True
            if (nd.used==0):
                N-=1
                for x in xrange(nd.ncores):
                    hosts.append(h)
                if (N==0):
                    match=True
                    break

        return pmatch, match, hosts

    def match_byhost(self, cluster):
        reqs = self['require']
        h = reqs['host']

        # make sure the node name exists
        if h not in cluster.nodes:
            return False, False, []

        nd = cluster.nodes[h]
        N= reqs['N']
        
        pmatch=False
        match=False
        hosts=[] # actually matched hosts

        if (nd.ncores>=N):
            pmatch=True

        nfree = nd.ncores-nd.used
        if (nfree>=N):
            for x in xrange(N):
                hosts.append(h)
            N=0
            match=True

        return pmatch, match, hosts

    def unmatch(self, cluster):
        if (self['status']=='run'):
            cluster.Unreserve(self['hosts'])
            self['status'] = 'done'


    def asdict(self):
        d={}
        for k in self:
            d[k] = self[k]
        return d

class JobQueue:
    def __init__(self, cluster_file):
        print "Loading cluster"
        self.cluster = Cluster(cluster_file)
        self.queue = []

    def process_message(self, message):
        # we will overwrite this
        self.response = copy.deepcopy(message)

        if not isinstance(message,dict):
            self.response['error'] = "message should be a dictionary"
        elif 'command' not in message:
            self.response['error'] = "message should contain a command"
        else:
            self._process_command(message)

    def _process_command(self, message):
        command = message['command']
        if command[0:3] == 'sub':
            self._process_submit_request(message)
        elif command == 'ls' or command == 'list':
            self._process_listing_request(message)
        elif command[0:4] == 'stat':
            self._process_status_request(message)
        elif command == 'rm' or command == 'remove':
            self._process_remove_request(message)
        elif command == 'notify':
            self._process_notification(message)
        else:
            self.response['error'] = "only support 'sub' and 'list' commands"

    def _process_submit_request(self, message):
        pid = message.get('pid')
        if pid is None:
            self.response['error'] = "submit requests must contain the 'pid' field"
            return

        req = message.get('require',None)
        if req  is None:
            self.response['error'] = "submit requests must contain the 'require' field"
            return

        newjob = Job(pid)
        self.queue.append(newjob)

        self.response['response'] = 'wait'

    def _process_listing_request(self, message):
        listing = []
        for job in self.queue:
            listing.append(job.asdict())
        
        self.response['response'] = listing

    def _process_status_request(self, message):
        self.response['response'] = self.cluster.Status()

    def _process_remove_request(self, message):
        pid = message.get('pid',None)
        if pid is None:
            self.response['error'] = "remove requests must contain the 'pid' field"
            return

        self._remove(pid)

    def _process_notification(self, message):
        notifi = message.get('notification',None)
        if notifi is None:
            self.response['error'] = "notify requests must contain the 'notification' field"
            return

        if notifi == 'done':
            pid = message.get('pid',None)
            if pid is None:
                self.response['error'] = "remove requests must contain the 'pid' field"
                return
            self._remove(pid)
        elif notifi == 'refresh':
            self._refresh()
        else:
            self.response['error'] = "Only support 'done' or 'refresh' notifications for now"
            return

    def _remove(self, pid):
        for i,job in enumerate(self.queue):
            if j['pid'] == pid:
                del self.queue[i]
                self.response['response'] = 'OK'
                break

    def refresh(self):
        """
        refresh the job list

        Loop through the jobs.  Try to match each job against the cluster.  This means 
        
            - is the job runnable on the cluster at all
            - are the requirements met and we can run?
            - Does the pid associated with the job still exits.  If not, remove
            the job

        Need to wait for Anze on this
        """
        pass
    def get_response(self):
        return self.response


    
def main():

    options, args = parser.parse_args(sys.argv[1:])
    if len(args) < 1:
        parser.print_help()
        sys.exit(45)

    cluster_file = args[1]
    queue = JobQueue(cluster_file)

    try:
        while True:
            while True:
                try:
                    conn, addr = sock.accept()
                    print 'Connected by', addr
                    break
                except socket.timeout:
                    # we just reached the timeout, refresh the queue
                    print 'refreshing queue'
                    queue.refresh()

            while True:
                # this is just in case the data are larger than the buffer
                data = conn.recv(MAX_BUFFSIZE)

                print 'data:',data
                if not data: 
                    break
                try:
                    message =json.loads(data)
                    print 'got JSON request:',message
                except:
                    ret = {"error":"could not e JSON request: '%s'" % data}
                    ret = json.dumps(ret)
                    conn.send(ret)
                    break

                queue.process_message(message)
                response = queue.get_response()

                try:
                    json_response = json.dumps(response)
                except:
                    err = {"error":"server error creating JSON response from '%s'" % ret}
                    json_response = json.dumps(err)

                print 'response:',json_response
                conn.send(json_response)

            conn.close()

    except KeyboardInterrupt:
        pass
    finally:
        sock.close()

if __name__=="__main__":
    main()
