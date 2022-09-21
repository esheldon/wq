import sys
from sys import stderr
import socket
import yaml
from yaml import YAMLError
from .defaults import HOST, BUFFSIZE


def send_message(port, message, timeout=None, crash_on_timeout=False):

    if len(message) == 0:
        raise ValueError("message must have len > 0")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # make sure to set timeout *before* calling connect()
    if timeout is not None:
        sock.settimeout(timeout)

    conninfo = (HOST, port)
    socket_connect(sock, conninfo, crash_on_timeout=crash_on_timeout)

    rdict = None
    try:
        jmess = yaml.dump(message)

        socket_send(sock, jmess)
        data = socket_receive(sock, BUFFSIZE)

        sock.close()

        try:
            rdict = yaml_load(data)
        except YAMLError as err:
            print(str(err), file=stderr)
            print(data, file=stderr)
            sys.exit(1)

        if 'error' in rdict:
            raise RuntimeError("Error reported by server: %s" % rdict['error'])

        if 'response' not in rdict:
            raise RuntimeError("Internal error. Expected a response "
                               "and got screwed.")

    finally:
        sock.close()

    if rdict is None:
        sys.exit(1)

    return rdict


def socket_connect(sock, conninfo, crash_on_timeout=False):
    """
    crash will only happen if timeouts have been enabled, otherwise we just
    wait
    """
    if crash_on_timeout:
        sock.connect(conninfo)
    else:
        while True:
            try:
                sock.connect(conninfo)
                break
            except socket.timeout:
                pass


def socket_send(conn, mess):
    """
    Send a message using a socket or connection, trying until all data is sent.

    hmm... is this going to max the cpu if we can't get through right away?
    """

    try:
        # python 3
        mess = bytes(mess, 'utf-8')
    except TypeError:
        # python 2
        mess = bytes(mess)

    conn.sendall(mess)


def socket_receive(conn, buffsize):
    """
    Recieve all data from a socket or connection, dealing with buffers.
    """
    tdata = conn.recv(buffsize)
    data = tdata
    while len(tdata) == buffsize:
        tdata = conn.recv(buffsize)
        data += tdata

    return data


def yaml_load(obj):
    return yaml.safe_load(obj)


def get_time_diff(dt):
    # surprisingly, there is nothing like this in date or time modules
    ds = int(dt)
    nd = int(ds/(24*60*60))
    ds -= nd*24*60*60
    nh = int(ds/(3600))
    ds -= nh*3600
    nm = int(ds/60)
    ds -= nm*60
    ns = ds

    r = ''

    if nd > 0:
        r = '%dd%02dh%02dm' % (nd, nh, nm)
    elif nh > 0:
        r = '%02dh%02dm%02ds' % (nh, nm, ns)
    elif nm > 0:
        r = '%02dm%02ds' % (nm, ns)
    else:
        r = '%02ds' % ns

    return r
