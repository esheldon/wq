from optparse import OptionParser
from .util import send_message


class Status(dict):
    """
    usage: wq stat

    print the status of the queue and compute cluster
    """

    def __init__(self, port, args):
        self.port = port
        parser = OptionParser(Status.__doc__)
        options, args = parser.parse_args(args)

    def execute(self):
        message = {}
        message['command'] = 'stat'

        resp = send_message(self.port, message)

        status = resp['response']

        print_status(status)


def print_status(status):
    """
    print out a status report given the input statistics
    """
    lines = get_statuss_lines(status)
    for line in lines:
        print(line)


def get_statuss(port):
    message = {}
    message['command'] = 'stat'

    resp = send_message(self.port, message)

    status = resp['response']
    return status


def get_status_lines(status):
    """
    input status is the result of cluster.status
    """
    lines = []
    lines.append('')

    nodes = status['nodes']
    stat_lines = []
    lens = {}
    tot_active_cores = status['ncores']
    for k in ['usage', 'host', 'mem', 'groups']:
        lens[k] = len(k)
    for d in nodes:
        if d['online'] is True:
            usage = '['+'*'*d['used']+'.'*(d['ncores']-d['used'])+']'
            line = {
                'usage': usage,
                'host': d['hostname'],
                'mem': '%g' % d['mem'],
                'groups': ','.join(d['grps']),
            }
            for n in lens:
                lens[n] = max(lens[n], len(line[n]))
            stat_lines.append(line)
        elif d['online'] is False:
            usage = '['+'X'*d['ncores']+']'
            line = {
                'usage': usage,
                'host': d['hostname'],
                'mem': '%g' % d['mem'],
                'groups': ','.join(d['grps']),
            }
            for n in lens:
                lens[n] = max(lens[n], len(line[n]))

            stat_lines.append(line)
            tot_active_cores = tot_active_cores-d['ncores']

    fmt = ' %(usage)-'+str(lens['usage'])+'s  %(host)-'+str(lens['host'])+'s '
    fmt += ' %(mem)'+str(lens['mem'])+'s %(groups)-'+str(lens['groups'])+'s'
    hdr = {}
    for k in lens:
        hdr[k] = k.capitalize()

    lines.append(fmt % hdr)
    for line in stat_lines:
        lines.append(fmt % line)

    if tot_active_cores > 0:
        perc = 100.*status['used']/tot_active_cores
    else:
        perc = 00.00

    lines.append('')
    mess = ' Used/avail/active cores: %i/%i/%i (%3.1f%% load, %i are offline)'
    mess = mess % (
        status['used'],
        tot_active_cores-status['used'],
        tot_active_cores,
        perc,
        status['ncores']-tot_active_cores,
    )

    lines.append(mess)
    return lines
