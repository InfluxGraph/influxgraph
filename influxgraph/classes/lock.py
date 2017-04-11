import fcntl
import logging

logger = logging.getLogger('influxgraph.lock')


class FileLock(object):
    __slots__ = ('handle', 'filename')

    def __init__(self, filename):
        self.filename = filename
        try:
            self.handle = open(self.filename, 'w')
        except (IOError, OSError):
            logger.critical("Could not create/open lock file %s",
                            self.filename,)
            raise

    def acquire(self):
        fcntl.flock(self.handle, fcntl.LOCK_EX)

    def release(self):
        fcntl.flock(self.handle, fcntl.LOCK_UN)

    def __del__(self):
        self.release()
        self.handle.close()
