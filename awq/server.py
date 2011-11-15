# Echo server program ipv4
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

# this can be configurable
_cluster_description_file='./somename.txt'

class Request(dict):
    def __init__(self, request):
        self.request_string = request
    def json_decode(self):
        self.request = json.loads(self.request_string)


class Cluster:
    def __init__(self,filename):
        self.filename=filename

class Job(dict):
    cluster=None
    def __init__(self, message):
        if cluster is None:
            cluster = Cluster(_cluster_description_file)

        # make sure pid,require are in message
        # and copy them into self
        
        for k in message:
            self[k] = message[k]
        self['status'] = 'wait'

    def start(self):
        self['status'] = 'run'

    def stop(self):
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
        elif command == 'ls' or command == 'list':
            self._process_listing_request(message)
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
    queue = JobQueue()

    try:
        while 1:
            while 1:
                try:
                    conn, addr = sock.accept()
                    print 'Connected by', addr
                    break
                except socket.timeout:
                    # we just reached the timeout, refresh the queue
                    print 'refreshing queue'
                    queue.refresh()

            while 1:
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
