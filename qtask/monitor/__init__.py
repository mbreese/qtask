import os
import datetime
import time

def load_monitor(uri):
    if uri[:7] == 'file://':
        return TextMonitor(uri[7:])
    elif uri[:9] == 'sqlite://':
        import sqlite
        return sqlite.SqliteMonitor(uri[9:])
    return None

class LockAcquireError(Exception):
    pass

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
    def submit(self, jobid, jobname, procs=1, deps=[], project=None):
        raise NotImplementedError
    def start(self, jobid, hostname=None):
        raise NotImplementedError
    def stop(self, jobid, return_code, stdout=None, stderr=None):
        raise NotImplementedError
    def stdout(self, jobid, filename):
        raise NotImplementedError
    def stderr(self, jobid, filename):
        raise NotImplementedError
    def view(self, jobid):
        raise NotImplementedError


class TextMonitor(Monitor):
    def __init__(self, path):
        self.path = path


