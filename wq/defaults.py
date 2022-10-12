HOST = ''      # Symbolic name meaning all available interfaces
DEFAULT_PORT = 51093   # Arbitrary non-privileged port
BUFFSIZE = 4096

# only listen for this many seconds, then refresh the queue
SOCK_TIMEOUT = 30.0
WAIT_SLEEP = 10.0
DEFAULT_SPOOL_DIR = '~/wqspool/'

PRIORITY_LIST = ['block', 'high', 'med', 'low']

# how many seconds to wait before restart
RESTART_DELAY = 60
