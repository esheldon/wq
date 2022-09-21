import sys
from optparse import OptionParser
from .util import send_message


class UsersLister(dict):
    """
    usage: wq users

    print user info for the system
    """

    def __init__(self, port, args):
        self.port = port
        parser = OptionParser(UsersLister.__doc__)
        options, args = parser.parse_args(args)

    def execute(self):
        message = {}
        message['command'] = 'users'

        resp = send_message(self.port, message)

        users = resp['response']

        print_users(users)


class UserLister(dict):
    """
    usage: wq user {username}

    print info for a user in the system
    """

    def __init__(self, port, args):
        self.port = port
        parser = OptionParser(UserLister.__doc__)
        options, args = parser.parse_args(args)
        if len(args) < 1:
            parser.print_help()
            sys.exit(1)

        self.user = args[0]

    def execute(self):
        message = {}
        message['command'] = 'user'
        message['user'] = self.user

        resp = send_message(self.port, message)

        userdata = resp['response']
        print_users(userdata)


def print_users(users):
    """
    print out user info

    input should be a dict.  You can convert a Users instance
    using asdict()
    """

    lines = get_user_lines(users)
    for line in lines:
        print(line)


def get_user_lines(users):
    """
    input should be a dict.  You an convert a Users instance
    using asdict()
    """
    lines = []

    keys = ['user', 'total', 'run', 'cores', 'limits']
    lens = {}
    for k in keys:
        lens[k] = len(k)

    udata = {}
    for uname in users:
        user = users[uname]
        udata[uname] = {}
        udata[uname]['user'] = uname
        udata[uname]['total'] = user['total']
        udata[uname]['run'] = user['run']
        udata[uname]['cores'] = user['cores']
        limits = user['limits']
        limits = (
            '{' + ';'.join(['%s:%s' % (y, limits[y]) for y in limits]) + '}'
        )
        udata[uname]['limits'] = limits

        for k in lens:
            lens[k] = max(lens[k], len(str(udata[uname][k])))

    fmt = ' %(user)-'+str(lens['user'])+'s'
    fmt += '  %(total)-'+str(lens['total'])+'s'
    fmt += '  %(run)-'+str(lens['run'])+'s'
    fmt += '  %(cores)-'+str(lens['cores'])+'s'
    fmt += '  %(limits)-'+str(lens['limits'])+'s'

    hdr = {}
    for k in lens:
        # hdr[k] = k.capitalize()
        hdr[k] = k
    lines.append(fmt % hdr)
    for uname in sorted(udata):
        lines.append(fmt % udata[uname])

    return lines
