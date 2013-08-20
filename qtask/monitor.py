import sys
import os
import sqlite3
import calendar
import datetime
import time
import re

def load_monitor(uri):
    if uri[:7] == 'file://':
        return TextMonitor(uri[7:])
    elif uri[:9] == 'sqlite://':
        return SqliteMonitor(uri[9:])
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

def _now_ts():
    return calendar.timegm(time.gmtime())


def _ts_to_datetime(ts):
    return datetime.datetime.fromtimestamp(ts)


class Monitor(object):
    def __init__(self):
        pass
    def submit(self, jobid, jobname, procs, mem, project=None):
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


class SqliteMonitor(Monitor):
    def __init__(self, path):
        self.path = path
        self.lock = Lock(self.path)
        self.conn = None

        if not os.path.exists(self.path):
            conn = sqlite3.connect(self.path)
            conn.execute('''
CREATE TABLE jobs (
    jobid TEXT,
    project TEXT,
    sample TEXT,
    name TEXT,
    procs INTEGER,
    mem TEXT,
    hostname TEXT,
    retcode INTEGER,
    submit INTEGER,
    start INTEGER,
    stop INTEGER,
    src BLOB,
    stdout BLOB,
    stderr BLOB
);''')
            conn.commit()
            conn.close()

    def connect(self):
        if not self.conn:
            self.lock.acquire()
            self.conn = sqlite3.connect(self.path)

    def close(self):
        if self.conn:
            self.conn.close()
            self.lock.release()

    def execute(self, sql, args=None):
        self.connect()
        self.conn.execute(sql, args)
        self.conn.commit()

    def submit(self, jobid, jobname, procs, mem, src, project=None, sample=None):
        self.execute('INSERT INTO jobs (jobid, project, sample, name, procs, mem, submit, src) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', (jobid, project, sample, jobname, procs, mem, _now_ts(), src))

    def start(self, jobid, hostname=None):
        self.execute('UPDATE jobs SET hostname = ?, start = ? WHERE jobid = ?', (hostname, _now_ts(), jobid))

    def stop(self, jobid, retcode, stdout=None, stderr=None):
        self.execute('UPDATE jobs SET retcode = ?, stop = ? WHERE jobid = ?', (retcode, _now_ts(), jobid))

        if stdout:
            self.stdout(jobid, stdout)

        if stderr:
            self.stderr(jobid, stderr)

    def stdout(self, jobid, stdout):
        stdout_s = ''
        if stdout and os.path.exists(stdout):
            with open(stdout) as f:
                stdout_s = f.read()

        self.execute('UPDATE jobs SET stdout = ? WHERE jobid = ?', (stdout_s, jobid))

    def stderr(self, jobid, stderr, conn=None):
        stderr_s = ''
        if stderr and os.path.exists(stderr):
            with open(stderr) as f:
                # the regex removes any progress bars
                stderr_s = re.sub('(.*)\r(.*?)\r','\\2',f.read())

        self.execute('UPDATE jobs SET stderr = ? WHERE jobid = ?', (stderr_s, jobid))
