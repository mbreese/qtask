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
    run TEXT,
    name TEXT,
    procs INTEGER,
    hostname TEXT,
    retcode INTEGER,
    submit_time INTEGER,
    start_time INTEGER,
    stop_time INTEGER,
    abort_time INTEGER,
    abort_code INTEGER,
    aborted_by TEXT,
    src BLOB,
    stdout BLOB,
    stderr BLOB
);
''')
            conn.execute('''
CREATE TABLE job_deps (
    jobid TEXT,
    parentid TEXT
);
''')
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

    def query(self, sql, args=None):
        self.connect()

        cur = self.conn.cursor()
        cur.execute(sql, args)
        for row in cur:
            yield row
        cur.close()

    def submit(self, jobid, jobname, src, procs=1, deps=[], project=None, sample=None, run=None):
        self.execute('INSERT INTO jobs (jobid, project, sample, run, name, procs, submit_time, src, abort_code) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)', (jobid, project, sample, run, jobname, procs, _now_ts(), src))
        for d in deps:
            self.execute('INSERT INTO job_deps (jobid, parentid) VALUES (?,?)', (jobid, d))

    def start(self, jobid, hostname=None):
        self.execute('UPDATE jobs SET hostname = ?, start_time = ? WHERE jobid = ?', (hostname, _now_ts(), jobid))

    def stop(self, jobid, retcode, stdout=None, stderr=None):
        self.execute('UPDATE jobs SET retcode = ?, stop_time = ? WHERE jobid = ?', (retcode, _now_ts(), jobid))

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

    def signal(self, jobid, sig):
        self.abort(jobid, sig, 2)
        self.killdeps(jobid)

    def killdeps(self, jobid):
        children = set()
        for cid in self._find_children(jobid):
            children.add(cid)

        for cid in children:
            self.abort(cid, jobid, 1)

    def _find_children(self, jobid):
        for row in self.query('SELECT jobid FROM job_deps WHERE parentid = ?', (jobid, )):
            childid = row[0]
            for cid in self._find_children(childid):
                yield cid
            yield childid

    def abort(self, jobid, reason, code):
        '''
        codes:
            0 - error during submission
            1 - error with parent
            2 - got killed by SGE/job scheduler
        '''
        self.execute('UPDATE jobs SET abort_code = ?, aborted_by = ?, abort_time = ? WHERE jobid = ?', (code, reason, _now_ts(), jobid))

    def find(self, project=None, sample=None, jobname=None, jobid=None):
        raise NotImplementedError
