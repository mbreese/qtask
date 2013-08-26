import os
import sqlite3
import calendar
import datetime
import time
import re

import qtask.monitor

def _now_ts():
    return calendar.timegm(time.gmtime())

def _ts_to_datetime(ts):
    return datetime.datetime.fromtimestamp(ts)


class SqliteMonitor(qtask.monitor.Monitor):
    def __init__(self, path):
        self.path = path
        self.lock = qtask.monitor.Lock(self.path)
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
    deps TEXT,
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

    def submit(self, jobid, jobname, src, procs=1, deps=[], project=None, sample=None):
        self.execute('INSERT INTO jobs (jobid, project, sample, name, procs, deps, submit, src) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', (jobid, project, sample, jobname, procs, ','.join(deps), _now_ts(), src))

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
