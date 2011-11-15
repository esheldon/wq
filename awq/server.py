# Echo server program ipv4
import socket
import json
import copy

HOST = ''      # Symbolic name meaning all available interfaces
PORT = 51093   # Arbitrary non-privileged port
MAX_BUFFSIZE = 4096

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((HOST, PORT))
s.listen(1)

# this can be configurable
_cluster_description_file='./bnl_desc.txt'


class Request(dict):
    def __init__(self, request):
        self.request_string = request
    def json_decode(self):
        self.request = json.loads(self.request_string)



class Node:
    def __init__ (self, line):
        self.host, self.ncores, self.mem, self.grps = line.split()
        self.ncores=int(self.ncores)
        self.mem=float(self.mem)
        self.grps=self.grps.split(',')
        self.used=0

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
        for line in open(filename).readlines():
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
        for h in self.nodes.keys():
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
    #cluster=None

    def __init__(self, message, cluster):
        #print self.cluster
        #if cluster==None:
        #    print "Loading cluster"
        #    cluster = Cluster(_cluster_description_file)

        # make sure pid,require are in message
        # and copy them into self
        
        self.cluster=cluster

        for k in message:
            self[k] = message[k]
        self['status'] = 'wait'

    def match(self):
        if (self['status']!='wait'):
            return

        reqs=self['require']
        submit_mode=reqs['submit_mode']

        pmatch=False
        match=False
        hosts=[] # actually matched hosts

        if (submit_mode=='bycore'):
            N=reqs['N']
            Np=N
            for h in self.cluster.nodes.keys():
                nd = self.cluster.nodes[h]

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
                    for x in range(N):
                        hosts.append(h)
                    N=0
                    match=True
                    break #we are done
                else:
                    N-=nfree
                    for x in range(nfree):
                        hosts.append(h)

        elif (submit_mode=='bynode'):
            N=reqs['N']
            Np=N
            for h in self.cluster.nodes.keys():
                nd = self.cluster.nodes[h]
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
                    for x in range(nd.ncores):
                        hosts.append(h)
                    if (N==0):
                        match=True
                        break



        elif (submit_mode=='exactnode'):
            h = reqs['node']
            nd = self.cluster.nodes[h]
            N= reqs['N']
            
            if (nd.ncores>=N):
                pmatch=True

            nfree = nd.ncores-nd.used
            if (nfree>=N):
                for x in range(N):
                    hosts.append(h)
                N=0
                match=True
        else:
            pmatch=False ## unknown request never mathces
            pass
                        
        if (pmatch):
            if match:
                self['hosts']=hosts
                self.cluster.Reserve(hosts)
                self['status']='run'
            else:
                 self['status']='wait'
        else:
            self['status']='nevermatch'
                    

    def unmatch(self):
        if (self['status']=='run'):
            self.cluster.Unreserve(self['hosts'])
            self['status'] = 'done'
        


    def asdict(self):
        d={}
        for k in self:
            d[k] = self[k]
        return d

class JobQueue:
    def __init__(self):
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
        elif command[0:4] == 'ls':
            self._process_listing_request(message)
        elif command == 'rm':
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

    def _process_remove_request(self, message):
        pid = message.get('pid',None)
        if pid is None:
            self.response['error'] = "remove requests must contain the 'pid' field"
            return

        for i,job in enumerate(self.queue):
            if j['pid'] == pid:
                del self.queue[i]
                self.response['response'] = 'OK'
                break

    def _process_notification(self, message):
        pass

    def get_response(self):
        return self.response


def main():
    Cluster(__cluster_description_file)
    queue = JobQueue()


    try:
        while 1:
            conn, addr = s.accept()
            print 'Connected by', addr
            while 1:
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
        s.close()

if __name__=="__main__":
    main()
