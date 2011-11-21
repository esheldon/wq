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
import glob
import cPickle

HOST = ''      # Symbolic name meaning all available interfaces
DEFAULT_PORT = 51093   # Arbitrary non-privileged port
MAX_BUFFSIZE = 4096

# only listen for this many seconds, then refresh the queue
SOCK_TIMEOUT = 30.0
WAIT_SLEEP = 10.0
SPOOL_DIR = "/astro/u/anze/wqspool/"

from optparse import OptionParser
parser=OptionParser(__doc__)
parser.add_option("-p", "--port", default=None, help="port for socket")


def print_stat(status):
    """
    input status is the result of cluster.Status
    """
    print
    nodes=status['nodes']
    lines=[]
    lens={}
    for k in ['usage','host','mem','groups']:
        lens[k] = len(k)
    for d in nodes:

        usage = '['+'*'*d['used']+'.'*(d['ncores']-d['used'])+']'
        l={'usage':usage,
           'host':d['hostname'],
           'mem':'%g' % d['mem'],
           'groups':','.join(d['grps'])}
        for n in lens:
            lens[n] = max(lens[n],len(l[n]))
        lines.append(l)

    fmt = '%(usage)-'+str(lens['usage'])+'s  %(host)-'+str(lens['host'])+'s '
    fmt += '%(mem)'+str(lens['mem'])+'s %(groups)-'+str(lens['groups'])+'s'
    hdr={}
    for k in lens:
        hdr[k]=k
    print fmt % hdr
    for l in lines:
        print fmt % l

    perc=100.*status['used']/status['ncores']
    print '\nUsed cores: %i/%i (%3.1f%%)' % (status['used'],status['ncores'],perc)


def socket_send(conn, mess):
    """
    Send a message using a socket or connection, trying until all data is sent.

    hmm... is this going to max the cpu if we can't get through right away?
    """
    reslen=len(mess)
    tnsent=conn.send(mess)
    nsent = tnsent
    if nsent < reslen:
        tnsent=conn.send(mess[nsent:])
        nsent += tnsent

def socket_recieve(conn, buffsize):
    """
    Recieve all data from a socket or connection, dealing with buffers.
    """
    tdata = conn.recv(buffsize)
    data=tdata
    while len(tdata) == buffsize:
        tdata = conn.recv(buffsize)
        data += tdata

    return data
 
class Server:
    def __init__(self, cluster_file, port=None):
        self.cluster_file = cluster_file
        self.queue = JobQueue(cluster_file)
        
        if port is None:
            self.port = DEFAULT_PORT
        else:
            self.port=port

    def open_socket(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((HOST, self.port))
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
                #print self.queue.cluster.Status()
                print_stat(self.queue.cluster.Status())


    def run(self):
        self.open_socket()
        self.sock.listen(1)
        try:
            while True:

                conn, addr = self.wait_for_connection()

                data = socket_recieve(conn, MAX_BUFFSIZE)

                # hmmm.. empty message, should we really dump out?
                if not data: 
                    break

                try:
                    message =json.loads(data)
                    print 'got JSON request:',message
                except:
                    ret = {"error":"could not process JSON request: '%s'" % data}
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

                # timeout mode is non-blocking under the hood, can't use
                # sendall but we wouldn't want the exception possibility anyway
                print 'response:',json_response
                socket_send(conn, json_response)

                print 'closing conn'
                conn.close()
                conn=None

        except KeyboardInterrupt:
            pass
        except:
            es=sys.exc_info()
            print 'caught exception type:', es[0],'details:',es[1]
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
        nds=[]
        nodes=self.nodes.keys()
        nodes.sort()
        for h in nodes:
            nds.append({'hostname':h,'used':self.nodes[h].used,'ncores':self.nodes[h].ncores, \
                       'mem':self.nodes[h].mem, 'grps':self.nodes[h].grps})
            
            tot+=self.nodes[h].ncores
            used+=self.nodes[h].used
            if (self.nodes[h].used>0):
              use.append((h,self.nodes[h].used))  

        res['used']=used
        res['ncores']=tot
        res['nnodes']=len(self.nodes)
        res['nodes']=nds
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
        elif 'commandline' not in self:
            self['status'] = 'nevermatch'
            self['reason'] = "'commandline' field not in message"
        else:
            self['status'] = 'wait'
            self['reason'] = ''


        self['priority'] = self['require'].get('priority','med')
        if self['priority'] not in ['low','med','high']:
            self['status'] = 'nevermatch'
            self['reason']="priority must be on of:  low, med, high."

        self['time_sub'] = time.time()
        self['spool_fname'] = None


    def Spool(self):
        fname = SPOOL_DIR+'/'+str(self['pid'])+'.'+self['status']
        self.UnSpool() ## just remove the old one first
                       ## we could just rename, but things that were waiting
                       ## maybe running now

        self['spool_fname'] = fname
        f=open(fname,'w')
        cPickle.dump(self,f,-1) #highest protocol
        f.close()
        self['spool_wait'] = WAIT_SLEEP


    def UnSpool(self):
        if (self['spool_fname']):
            if os.path.exists(self['spool_fname']):
                os.remove(self['spool_fname'])
            self['spool_fname']=None
    

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
        elif (submit_mode=='bygroup'):
            pmatch, match, hosts,reason = self._match_bygroup(cluster)
        else:
            pmatch=False ## unknown request never mathces
            reason="bad submit_mode '%s'" % submit_mode



        if pmatch:
            if match:
                self['hosts']=hosts
                cluster.Reserve(hosts)
                self['time_run'] = time.time()
                self['status']='run'
                self.Spool()

            else:
                 self['status']='wait'
                 self['reason']=reason
                 self.Spool()

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
            val = [val]
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
                Np-=nd.ncores

            nfree= nd.ncores-nd.used
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
            reason = 'Not enough free cores.'
    
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
            reason = 'Not enough free cores on any one node.'
    
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
            reason = 'Not enough free cores.'
    

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


    def _match_bygroup(self, cluster):

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
            for h in cluster.nodes:
                nd = cluster.nodes[h]
                if g in nd.grps:
                    pmatch=True
                    match=True
                    if (nd.used>0):
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
        self.UnSpool()
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
        #print self.cluster.Status()
        self.queue = []
        print "Loading jobs"
        for fn in glob.glob (SPOOL_DIR+'/*'):
            job = cPickle.load(open(fn))
            if (job['status']=='run'):
                ### here we just need to reserver cluster.
                self.cluster.Reserve(job['hosts'])
            self.queue.append(job)

        print_stat(self.cluster.Status())


    def process_message(self, message):
        # we will overwrite this
        self.response = copy.deepcopy(message)

        if not isinstance(message,dict):
            self.response['error'] = "message should be a dictionary"
        elif 'command' not in message:
            self.response['error'] = "message should contain a command"
        else:
            self._process_command(message)

    def refresh(self):
        """
        refresh the job list

        This is the key, as it tells the jobs when they can run.

            - Remove jobs where the pid no longer is valid.
        Otherwise run match(cluster) and
            - are the requirements met and we can run?
            - note we should have no 'nevermatch' status here

        """

        for priority in ['high','med','low']:
            for i,job in enumerate(self.queue):
                if (job['priority']!=priority):
                    continue
                # job was told to run.
                # see if the pid is still running, if not remove the job
                if not self._pid_exists(job['pid']):
                    print 'removing job %s, pid no longer valid'
                    self.queue[i].unmatch(self.cluster)
                    del self.queue[i]
                elif job['status'] != 'run':
                    # see if we can now run the job
                    job.match(self.cluster)
                    ## signal send automatically by spool.

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
            self.response['spool_fname']=newjob['spool_fname'].replace('wait','run')
            self.response['spool_wait']=newjob['spool_wait']
            if (self.response['response']=='run'):
                self.response['hosts']=newjob['hosts']
            elif self.response['response'] == 'wait':
                self.response['reason'] = newjob['reason']

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
        self.refresh()

        pid = message.get('pid',None)
        user = message.get('user',None)
        if pid is None:
            self.response['error'] = "remove requests must contain the 'pid' field"
            return
        if user is None:
            self.response['error'] = "remove requests must contain the 'user' field"
            return
            
        if pid == 'all':
            self._process_remove_all_request(user)
        else:
            found = False
            for i,job in enumerate(self.queue):
                if job['pid'] == pid:
                    ## we don't actually remove anything, hope refresh will do it.
                    if (job['user']!=user and user!='root'):
                        self.response['error']='PID belongs to user '+job['user']
                        return

                    self.response['response'] = 'OK'
                    self.response['pids_to_kill'] = [pid]
                    found=True
                    break
            if not found:
                self.response['error'] = 'pid %s not found' % pid

    def _process_remove_all_request(self, user):
        pids_to_kill=[]
        for i,job in enumerate(self.queue):
            if job['user'] == user:
                # just drop them from the queue, and append
                # to return list of pids to kill
                pids_to_kill.append(job['pid'])
                job.unmatch(self.cluster)

        if len(pids_to_kill) == 0:
            self.response['error'] = 'No jobs for user:',user,'in queue'
        else:
            # rebuild the queue without these items
            self.queue = [j for j in self.queue if j['pid'] not in pids_to_kill]

            self.response['response'] = 'OK'
            self.response['pids_to_kill'] = pids_to_kill


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
            self._remove_from_notify(pid)
            self.refresh()
        elif notifi == 'refresh':
            self.refresh()
        else:
            self.response['error'] = "Only support 'done' or 'refresh' notifications for now"
            return

    def _remove_from_notify(self, pid):
        """
        this is when the user has notified us the job is done.  we don't
        send a kill message back
        """
        found = False
        for i,job in enumerate(self.queue):
            if job['pid'] == pid:
                job.unmatch(self.cluster)
                del self.queue[i]
                self.response['response'] = 'OK'
                found=True
                break

        if not found:
            self.response['error'] = 'pid %s not found' % pid



    # Not needed anymore
    # def _signal_start(self, pid):
    #     import signal
    #     try:
    #         os.kill(pid, signal.SIGUSR1)
    #     except OSError:
    #         print 'pid %s no longer exists' % pid

    def _pid_exists(self, pid):        
        """ Check For the existence of a unix pid. """
        pid_path =  "/proc/%s" % pid
        if os.path.exists(pid_path):
            return True
        else:
            return False

        """
        try:
            # this doesn't actually kill the job, it does nothing if the pid
            # exists, if doesn't exist raises OSError
            os.kill(pid, 0)
        except OSError:
            return False
        else:
            return True
        """


    
def main():

    options, args = parser.parse_args(sys.argv[1:])
    if len(args) < 1:
        parser.print_help()
        sys.exit(45)

    cluster_file = args[0]
    srv = Server(cluster_file, port=options.port)
    
    srv.run()


if __name__=="__main__":
    main()
