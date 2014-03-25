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
        self.conn = None
        self.lock = qtask.monitor.Lock(self.path)

        if not os.path.exists(self.path):
            self.lock.acquire()
            conn = sqlite3.connect(self.path)
            conn.executescript('''
CREATE TABLE runs (
    runcode TEXT,
    project TEXT,
    sample TEXT,
    cluster TEXT
);

CREATE TABLE jobs (
    jobid TEXT,
    runcode INTEGER,
    name TEXT,
    cmd TEXT,
    exechost TEXT,
    retcode INTEGER,
    submit_time INTEGER,
    start_time INTEGER,
    stop_time INTEGER,
    abort_time INTEGER,
    abort_code INTEGER,
    aborted_by TEXT
);
CREATE TABLE job_resources (
    jobid INTEGER,
    key TEXT,
    value TEXT
);
CREATE TABLE job_output (
    jobid INTEGER,
    script TEXT,
    stdout TEXT,
    stderr TEXT
);
CREATE TABLE job_deps (
    jobid INTEGER,
    parentid INTEGER
);
''')
            conn.commit()
            conn.close()
            self.lock.release()

    def connect(self):
        if not self.conn:
            self.conn = sqlite3.connect(self.path)

    def close(self):
        if self.conn:
            self.conn.close()
            self.lock.release()

    def execute(self, sql, args=None):
        self.connect()
        cur = self.conn.cursor()
        rowid = None
        cur.execute(sql, args)
        if sql.upper()[:7] == 'INSERT':
            rowid = cur.last_insert_rowid()
        cur.close()
        self.conn.commit()
        return rowid

    def query(self, sql, args=None):
        self.connect()

        cur = self.conn.cursor()
        cur.execute(sql, args)
        for row in cur:
            yield row
        cur.close()

    def start_run(self, runcode, project, sample, cluster):
        rowid = self.execute('INSERT INTO runs(runcode, project, sample, cluster) VALUES (?,?,?,?)', (runcode, project, sample, cluster))
        return rowid

    def submit_job(self, job, runcode, src):
        self.execute('INSERT INTO jobs (jobid, runcode, name, cmd, submit_time, abort_code) VALUES (?,?,?,?,?,0)', (job.cluster_jobid, runcode, job.taskname, job.cmd, _now_ts()))

        for d in job.depends_on:
            self.execute('INSERT INTO job_deps (jobid, parentid) VALUES (?, ?)', (job.cluster_jobid, d.cluster_jobid))

        # for k in ['mem', 'procs', 'walltime', 'hold', 'mail', 'queue', 'qos', 'wd', 'stdout', 'stderr', 'env', 'account']:
        for k in job._options:
            if job.option(k):
                self.execute('INSERT INTO job_resources (jobid, key, value) VALUES (?, ?, ?)', (job.cluster_jobid, k, job.option(k)))

        self.execute('INSERT INTO job_output (jobid, script) VALUES (?, ?)', (job.cluster_jobid, src))

    def start(self, jobid, hostname):
        self.execute('UPDATE jobs SET exechost = ?, start_time = ? WHERE jobid = ?', (hostname, _now_ts(), jobid))

    def stop(self, jobid, retcode, stdout=None, stderr=None):
        self.execute('UPDATE jobs SET retcode = ?, stop_time = ? WHERE jobid = ?', (retcode, _now_ts(), jobid))

        if stdout or stderr:
            if stdout:
                self._load_output(jobid, 'stdout', stdout)

            if stderr:
                self._load_output(jobid, 'stdout', stderr)

    def _load_output(self, jobid, output_type, filename):
        src = ''
        if os.path.exists(filename):
            with open(filename) as f:
                if output_type == 'stderr':
                    src = re.sub('(.*)\r(.*?)\r','\\2',f.read())
                else:
                    src = f.read()

            self.execute('UPDATE job_output SET %s = ? WHERE jobid = ?' % output_type, (src, jobid))


    def abort(self, jobid, by=None):
        if by:
            self.execute('UPDATE jobs SET abort_code = ?, aborted_by = ?, abort_time = ? WHERE jobid = ?', (2, by, _now_ts(), jobid))
        else:
            self.execute('UPDATE jobs SET abort_code = ?, aborted_by = ?, abort_time = ? WHERE jobid = ?', (2, jobid, _now_ts(), jobid))
        self._killdeps(jobid)

    def failed(self, jobid):
        self._killdeps(jobid)

    def _killdeps(self, jobid):
        children = set()
        for cid in self._find_children(jobid):
            children.add(cid)

        for cid in children:
            self.execute('UPDATE jobs SET abort_code = ?, aborted_by = ?, abort_time = ? WHERE jobid = ?', (1, jobid, _now_ts(), cid))

    def _find_children(self, jobid):
        for row in self.query('SELECT jobid FROM job_deps WHERE parentid = ?', (jobid, )):
            childid = row[0]
            for cid in self._find_children(childid):
                yield cid
            yield childid

