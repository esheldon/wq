"""
    %prog [options] cluster_description_file

The description file is 

    hostname ncores mem labels

The labels are optional.
"""

import socket
import signal
import json
import time
import copy
import sys
import os

HOST = ''      # Symbolic name meaning all available interfaces
PORT = 51093   # Arbitrary non-privileged port
MAX_BUFFSIZE = 4096

# only listen for this many seconds, then refresh the queue
SOCK_TIMEOUT = 30.0

from optparse import OptionParser
parser=OptionParser(__doc__)

class Server:
    def __init__(self, cluster_file):
        self.cluster_file = cluster_file
        self.queue = JobQueue(cluster_file)

    def open_socket(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((HOST, PORT))
        self.sock.settimeout(SOCK_TIMEOUT)

    def wait_for_connection(self):
        """
        we want a chance to look for disappearing pids 
        even if we don't get a signal from any clients
        """
        while True:
            try:
                conn, addr = self.sock.accept()
                print 'Connected by', addr
                return conn, addr
            except socket.timeout:
                # we just reached the timeout, refresh the queue
                print 'refreshing queue'
                self.queue.refresh()
                print self.queue.cluster.Status()


    def run(self):
        self.open_socket()
        self.sock.listen(1)
        try:
            while True:

                conn, addr = self.wait_for_connection()
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

                self.queue.process_message(message)
                response = self.queue.get_response()

                try:
                    json_response = json.dumps(response)
                except:
                    err = {"error":"server error creating JSON response from '%s'" % ret}
                    json_response = json.dumps(err)

                print 'response:',json_response
                conn.send(json_response)

                conn.close()
                conn=None

        except KeyboardInterrupt:
            pass
        finally:
            print 'shutdown'
            self.sock.shutdown(socket.SHUT_RDWR)
            print 'close'
            self.sock.close()



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

        if 'require' not in self:
            self['status'] = 'nevermatch'
            self['reason'] = "'require' field not in message"
        elif 'pid' not in self:
            self['status'] = 'nevermatch'
            self['reason'] = "'pid' field not in message"
        elif 'user' not in self:
            self['status'] = 'nevermatch'
            self['reason'] = "'user' field not in message"
        else:
            self['status'] = 'wait'
            self['reason'] = ''


    def match(self, cluster):
        if self['status'] == 'nevermatch':
            return
        if self['status'] != 'wait':
            return

        # default to bycore
        submit_mode = self['require'].get('mode','bycore')

        # can also add reasons from the match methods
        # later

        if (submit_mode=='bycore'):
            pmatch, match, hosts, reason = self._match_bycore(cluster)
        elif (submit_mode=='bycore1'):
            pmatch, match, hosts, reason = self._match_bycore1(cluster)
        elif (submit_mode=='bynode'):
            pmatch, match, hosts, reason = self._match_bynode(cluster)
        elif (submit_mode=='byhost'):
            pmatch, match, hosts,reason = self._match_byhost(cluster)
        elif (submit_mode=='bygrp'):
            pmatch, match, hosts,reason = self._match_bygrp(cluster)
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


    def _get_req_list(self, reqs, key):
        """
        Can either send 'v1,v2,v3' as a string or an actual list [v1,v2,v3]
        Will be converted to a list
        """
        val = reqs.get(key,[])
        if not isinstance(val, list):
            val = val.split(',')
        return val

    def _match_bycore(self, cluster):

        pmatch=False
        match=False
        hosts=[] # actually matched hosts
        reason=''

        reqs = self['require']

        N = reqs.get('N', 1)
        Np=N

        for h in cluster.nodes:
            nd = cluster.nodes[h]
            print "node = ",h

            ## is this node actually what we want
            ing = self._get_req_list(reqs, 'group')
            if len(ing) > 0: ##any group in any group
                ok=False
                for g in ing:
                    if g in nd.grps:
                        ok=True
                        break
                if (not ok):
                    continue ### not in the group
                    
            ing = self._get_req_list(reqs, 'notgroup')
            if len(ing) > 0: ##any group in any group
                ok=True
                for g in ing:
                    if g in nd.grps:
                        ok=False
                        break
                if (not ok):
                    continue ### not in the group

            print "nd.nc=",nd.ncores
            if (nd.ncores>=Np):
                pmatch=True
            else:
                Np-=nd.ncores

            nfree= nd.ncores-nd.used
            print "nfree=",nfree, N
            if (nfree>=N):
                for x in xrange(N):
                    hosts.append(h)
                N=0
                match=True
                break
            else:
                N-=nfree
                for x in xrange(nfree):
                    hosts.append(h)

        if (not pmatch):
            reason = 'Not enough total cores satistifying condition.'
        elif (not match):
            resont = 'Not enough free cores.'
    
        print pmatch, match, hosts, reason
        return pmatch, match, hosts, reason



    def _match_bycore1(self, cluster):

        pmatch=False
        match=False
        hosts=[] # actually matched hosts
        reason=''

        reqs = self['require']

        N = reqs.get('N', 1)
        Np=N

        for h in cluster.nodes:
            nd = cluster.nodes[h]

            ## is this node actually what we want
            ing = self._get_req_list(reqs, 'group')
            if len(ing) > 0: ##any group in any group
                ok=False
                for g in ing:
                    if g in nd.grps:
                        ok=True
                        break
                if (not ok):
                    continue ### not in the group
                    
            ing = self._get_req_list(reqs, 'notgroup')
            if len(ing) > 0: ##any group in any group
                ok=True
                for g in ing:
                    if g in nd.grps:
                        ok=False
                        break
                if (not ok):
                    continue ### not in the group

            if (nd.ncores>=Np):
                pmatch=True
            else:
                pass

            nfree= nd.ncores-nd.used
            if (nfree>=N):
                for x in xrange(N):
                    hosts.append(h)
                N=0
                match=True
                break
            else:
                pass

        if (not pmatch):
            reason = 'Not a node with that many cores.'
        elif (not match):
            resont = 'Not enough free cores on any one node.'
    
        return pmatch, match, hosts, reason




        


    def _match_bynode(self, cluster):
        pmatch=False
        match=False
        hosts=[] # actually matched hosts
        reason=''

        reqs = self['require']

        N = reqs.get('N', 1)
        Np=N

        for h in cluster.nodes:
            nd = cluster.nodes[h]
            min_cores = reqs.get('min_cores',0)
            if nd.ncores < min_cores:
                continue

            ## is this node actually what we want
            ing = self._get_req_list(reqs, 'group')
            if len(ing) > 0: ##any group in any group
                ok=False
                for g in ing:
                    if g in nd.grps:
                        ok=True
                        break
                if (not ok):
                    continue ### not in the group

            ing = self._get_req_list(reqs, 'notgroup')
            if len(ing) > 0: ##any group in any group
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

        if (not pmatch):
            reason = 'Not enough total cores satistifying condition.'
        elif (not match):
            resont = 'Not enough free cores.'
    

        return pmatch, match, hosts, reason

    def _match_byhost(self, cluster):

        pmatch=False
        match=False
        hosts=[] # actually matched hosts
        reason=''

        reqs = self['require']

        h = reqs.get('host',None)
        if h is None:
            reason = "'host' field not in requirements"
            return pmatch, match, hosts, reason

        # make sure the node name exists
        if h not in cluster.nodes:
            reason = "host '%s' does not exist" % h
            return pmatch, match, hosts, reason

        nd = cluster.nodes[h]
        N = reqs.get('N', 1)
        

        if nd.ncores >= N:
            pmatch=True

        nfree = nd.ncores-nd.used
        if (nfree>=N):
            for x in xrange(N):
                hosts.append(h)
            N=0
            match=True
        else:
            reason = "Not enough free cores on "+h

        return pmatch, match, hosts, reason


    def _match_bygrp(self, cluster):

        pmatch=False
        match=False
        hosts=[] # actually matched hosts
        reason=''

        reqs = self['require']
        g=reqs.get('group',None)
        if not g:
            pmatch=False
            reason = 'Need to specify group'
        else:
            ing=ing[0]
            for h in self.cluster.nodes.keys():
                nd = self.cluster.nodes[h]
                if ing in nd.grps:
                    pmatch=True
                    match=True
                    if (nd.use>0):
                        match=False ## we actually demand the entire group
                        reason = 'Host '+h+' not entirely free.'
                        break
                    else:
                        for x in range(nd.ncores):
                            hosts.append(h)
            if (not pmatch):
                reason = 'Not a single node in that group'
        return pmatch, match, hosts, reason



    def unmatch(self, cluster):
        if self['status'] == 'run':
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
        print self.cluster.Status()
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

    def refresh(self, purge=False):
        """
        refresh the job list

        This is the key, as it tells the jobs when they can run.

            - Remove jobs where the pid no longer is valid.
        Otherwise run match(cluster) and
            - are the requirements met and we can run?
            - note we should have no 'nevermatch' status here

        """

        for i,job in enumerate(self.queue):
            if job['status'] == 'run':
                # job was told to run.
                # see if the pid is still running, if not remove the job
                if not self._pid_exists(job['pid']):
                    print 'removing job %s, pid no longer valid'
                    self.queue[i].unmatch(self.cluster)
                    del self.queue[i]
            else:
                # we if we can now run the job
                job.match(self.cluster)
                if job['status'] == 'run':
                    # *now* send a signal to start it
                    self._signal_start(job['pid'])


    def get_response(self):
        return self.response


    def _process_command(self, message):
        command = message['command']
        if command in ['sub']:
            self._process_submit_request(message)
        elif command == 'gethosts':
            self._process_gethosts(message)
        elif command in ['ls']:
            self._process_listing_request(message)
        elif command in ['stat']:
            self._process_status_request(message)
        elif command in ['rm']:
            self._process_remove_request(message)
        elif command == 'notify':
            self._process_notification(message)
        elif command == 'refresh':
            self.refresh()
            self.response['response'] = 'OK'
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

        newjob = Job(message)
        newjob.match(self.cluster)
        if newjob['status'] == 'nevermatch':
            self.response['error'] = newjob['reason']
        else:
            # if the status is 'run', the job will immediately
            # run. Otherwise it will wait and can't run till
            # we do a refresh
            self.queue.append(newjob)
            self.response['response'] = newjob['status']
            if (self.response['response']=='run'):
                    self.response['hosts']=newjob['hosts']

    def _process_gethosts(self, message):
        pid = message.get('pid')
        if pid is None:
            self.response['error'] = "submit requests must contain the 'pid' field"
            return

        for job in self.queue:
            if job['pid']==pid:
                self.response['hosts']=job['hosts']
                self.response['response']='ok'
                return

        self.response['error'] = "we don't have this pid"
        return

        

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

        self._remove(pid,force=True)

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
            self.refresh()
        elif notifi == 'refresh':
            self.refresh()
        else:
            self.response['error'] = "Only support 'done' or 'refresh' notifications for now"
            return

    def _remove(self, pid, force=False):
        for i,job in enumerate(self.queue):
            if job['pid'] == pid:
                if (force):
                    self._signal_terminate(pid)
                job.unmatch(self.cluster)
                del self.queue[i]
                self.response['response'] = 'OK'
                found=True
                break
        if not found:
            self.response['error'] = 'pid %s not found' % pid

    def _signal_terminate(self,pid):
        if self._pid_exists(pid):
            os.kill(pid,signal.SIGTERM)
            sleep (2) ## sleep  a bit
            if (self._pid_exists(pid)):
                os.kill(pid,signal.SIGKILL)

    def _signal_start(self, pid):
        import signal
        os.kill(pid, signal.SIGUSR1)

    def _pid_exists(self, pid):        
        """ Check For the existence of a unix pid. """
        try:
            # this doesn't actually kill the job, it does nothing if the pid
            # exists, if doesn't exist raises OSError
            os.kill(pid, 0)
        except OSError:
            return False
        else:
            return True



    
def main():

    options, args = parser.parse_args(sys.argv[1:])
    if len(args) < 1:
        parser.print_help()
        sys.exit(45)

    cluster_file = args[0]
    srv = Server(cluster_file)
    
    srv.run()


if __name__=="__main__":
    main()
