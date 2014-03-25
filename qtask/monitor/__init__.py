import os
import datetime
import time
# import signal


def load_monitor(uri):
    if uri[:7] == 'file://':
        return TextMonitor(os.path.realpath(os.path.expanduser(uri[7:])))
    elif uri[:9] == 'sqlite://':
        import sqlite
        return sqlite.SqliteMonitor(os.path.realpath(os.path.expanduser(uri[9:])))
    return None

class LockAcquireError(Exception):
    pass

# __active_locks = set()
# def __cleanup_handler(signum, frame): 
#     global __active_locks
#     for lock in __active_locks:
#         lock._abort()
#     __active_locks.clear()

# signal.signal(signal.SIGTERM, __cleanup_handler) 
# signal.signal(signal.SIGINT , __cleanup_handler) 


class Lock(object):
    def __init__(self, path):
        self.path = os.path.abspath(path)+'.lock'
        self.__locked = False

    def acquire(self, timeout = 60):
        if self.__locked:
            return

        start = datetime.datetime.now()
        while (datetime.datetime.now() - start).seconds < timeout:
            try:
                os.mkdir(self.path)
                self.__locked = True
                return
            except OSError:
                time.sleep(.1)
        
        raise LockAcquireError

    def release(self):
        if self.__locked:
            self.__locked = False
            os.rmdir(self.path)


class Monitor(object):
    def __init__(self):
        pass
    def close(self):
        pass
    def start_run(self, runcode, project, sample, cluster):
        raise NotImplementedError
    def submit_job(self, job, runcode, src):
        raise NotImplementedError
    def start(self, jobid, hostname):
        raise NotImplementedError
    def stop(self, jobid, retcode, stdout=None, stderr=None):
        raise NotImplementedError
    def abort(self, jobid, by=None):
        raise NotImplementedError
    def failed(self, jobid):
        raise NotImplementedError


class TextMonitor(Monitor):
    def __init__(self, path):
        self.path = path


