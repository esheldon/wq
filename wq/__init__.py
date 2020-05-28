__version__ = '0.2.0'

from . import server

from .server import DEFAULT_HOST
from .server import DEFAULT_PORT
from .server import DEFAULT_MAX_BUFFSIZE

# only listen for this many seconds, then refresh the queue
from .server import DEFAULT_SOCK_TIMEOUT
from .server import DEFAULT_WAIT_SLEEP
from .server import DEFAULT_SPOOL_DIR
from .server import PRIORITY_LIST


