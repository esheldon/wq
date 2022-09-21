import yaml
from .util import get_time_diff, send_message


class JobLister(dict):
    """
    usage: wq ls [options]

    print the job list to stdout.

    If -u/--user is sent, the listing is restricted to that user/users.

    If -f/--full is sent, the full job listing is given.  This is a yaml
    document that can be read and processed to provide a customised
    listing.
    """

    def __init__(self, port, args):
        self.port = port
        self._process_args(args)

    def _process_args(self, args):
        from optparse import OptionParser
        parser = OptionParser(JobLister.__doc__)
        parser.add_option("-u", "--user", default=None,
                          help=("Only list jobs for the user.  can be "
                                "a comma separated list"))
        parser.add_option("-f", "--full", action='store_true',
                          help="Give a full job listing as a YAML stream.")

        options, args = parser.parse_args(args)
        self.user = options.user
        self.full = options.full

        if self.user is not None:
            self.user = self.user.split(',')

    def execute(self):
        res = get_job_listing(port=self.port, full=self.full, user=self.user)

        if self.full:
            if len(res['entries']) > 0:
                print(yaml.dump(res))
        else:

            lines = get_job_lines(res, user=self.user)
            for line in lines:
                print(line)


def get_job_lines(job_listing, user=None):
    lines = []

    if len(job_listing['entries']) > 0:
        lines.append(job_listing['footer'])

        for entry in sorted(job_listing['entries'], key=lambda x: x['time_sub']):  # noqa
            lines.append(job_listing['fmt'] % entry)

    stats = 'Jobs: %s Running: %s Waiting: %s' % (
        job_listing['njobs'], job_listing['nrun'], job_listing['nwait'],
    )
    if user is not None:
        lines.append(' User: %s %s' % (','.join(user), stats))
    else:
        lines.append(' %s' % stats)

    return lines


def get_job_listing(port, full=False, user=None):
    """
    Get the job listing and other info to print stats

    Parameters
    ----------
    port: int
        The port number
    user: str, optional
        If set to True, limit listing to the specified user
    full: boolean, optional
        If set to True, return the full listing
    """
    import time

    message = {}
    message['command'] = 'ls'
    resp = send_message(port, message)

    if full:
        if user is not None:
            full_listing = [
                r for r in resp['response'] if r['user'] in user
            ]
        else:
            full_listing = resp['response']

        return full_listing

    names = ['pid', 'user', 'st', 'pri', 'nc',
             'nh', 'host0', 'Tq', 'Trun', 'cmd']

    lens = {}
    for k in names:
        lens[k] = len(k)

    nrun = 0
    nwait = 0
    entries = []

    timenow = time.time()
    for r in resp['response']:
        if 'user' not in r:
            continue

        if user is None or r['user'] in user:
            if r['status'] == 'run':
                nrun += 1
            else:
                nwait += 1

            this = {}
            this['pid'] = r['pid']
            this['user'] = r['user']
            this['pri'] = r['priority']
            this['st'] = _extract_status(r)
            this['nc'] = _extract_ncores(r)
            # this may replace command with job_name, if it exists
            this['cmd'] = _extract_command(r)
            this['Tq'] = _extract_time_in(r, timenow)
            this['Trun'] = _extract_time_run(r, timenow)

            this['nh'] = _extract_nhosts(r)

            # this is the first host on the list
            this['host0'] = _extract_host0(r)

            # for sorting
            this['time_sub'] = r['time_sub']

            for k in this:
                if k in lens:
                    lens[k] = max(lens[k], len(('%s' % this[k])))

            entries.append(this)

    fmt = []
    for k in names:
        if k in ['Tq', 'Trun']:
            align = ''
        else:
            align = '-'

        fmt.append('%('+k+')'+align+str(lens[k])+'s')

    fmt = ' '.join(fmt)
    fmt = ' '+fmt

    footer = fmt % {
        'pid': 'Pid', 'user': 'User',
        'st': 'St', 'pri': 'Pri', 'nc': 'Nc',
        'nh': 'Nh', 'host0': 'Host0',
        'cmd': 'Cmd', 'Tq': 'Tq', 'Trun': 'Trun',
    }

    return {
        'footer': footer,
        'entries': entries,
        'fmt': fmt,
        'nrun': nrun,
        'nwait': nwait,
        'njobs': nrun + nwait,
    }


def _extract_status(r):
    if r['status'] == 'run':
        return 'R'
    else:
        return 'W'


def _extract_ncores(r):
    if r['status'] == 'run':
        return len(r['hosts'])
    else:
        return '-'


def _extract_command(r):
    c = r['job_name']

    # remove spaces from name so we can use awk on
    # the output of wq ls
    c = '-'.join(str(c).split())
    return c


def _extract_time_in(r, timenow):
    return get_time_diff(timenow-r['time_sub'])


def _extract_time_run(r, timenow):
    if r['status'] == 'run':
        return get_time_diff(timenow-r['time_run'])
    else:
        return '-'


def _extract_nhosts(r):
    if r['status'] == 'run':
        hd = {}
        for host in r['hosts']:
            hd[host] = host
        return len(hd)
    else:
        return '-'


def _extract_host0(r):
    """
    Get the first host in the list
    """
    if r['status'] != 'run':
        return '-'

    if not r['hosts']:
        return '-'
    else:
        return r['hosts'][0]
