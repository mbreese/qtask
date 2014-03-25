import sys
import datetime

class Level(object):
    _levels = []
    def __init__(self, val, name):
        self._val = val
        self._name = name
        Level._levels.append(self)

    def __repr__(self):
        return self._name

    def __lt__(self, val):
        return self._val < val

    def __eq__(self, val):
        return self._val == val

    def __isub__(self, v):
        for lev in Level._levels:
            if lev == self._val - v:
                return lev

        return self

    def __iadd__(self, v):
        for lev in Level._levels:
            if lev == self._val + v:
                return lev

        return self


Level.TRACE=Level(1, 'TRACE')
Level.DEBUG=Level(2, 'DEBUG')
Level.INFO=Level(3, 'INFO')
Level.WARN=Level(4, 'WARN')
Level.ERROR=Level(5, 'ERROR')
Level.FATAL=Level(6, 'FATAL')


class RunLogger(object):
    def __init__(self, level=Level.INFO):
        self._file = None
        self._level = level

    def set_level(self, level):
        for lev in Level._levels:
            if lev == level:
                self._level = lev
                return

        self._level = Level.INFO


    def _log(self, level, msg, *args):
        
        # We use lazy initialization so that the config value
        # can be set at runtime if necessary

        if not self._file:
            if 'qtask.log' in qtask.config:
                if qtask.config['qtask.log'] == 'stdout':
                    self._file = sys.stdout
                elif qtask.config['qtask.log'] == 'stderr':
                    self._file = sys.stderr
                else:
                    self._file = open(qtask.config['qtask.log'], 'w')

                self.info('RUN START: %s\n' % datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
            else:
                raise RuntimeError("Unable to find qtask.log path!")

        if level >= self._level:
            self._file.write('[%s] %s\n' % (level, msg % tuple([_sanitize(x) for x in args])))
        if level >= Level.ERROR:
            sys.stderr.write('[%s] %s\n' % (level, msg % tuple([_sanitize(x) for x in args])))


    def trace(self, msg, *args):
        self._log(Level.TRACE, msg, *args)
    def debug(self, msg, *args):
        self._log(Level.DEBUG, msg, *args)
    def info(self, msg, *args):
        self._log(Level.INFO, msg, *args)
    def warn(self, msg, *args):
        self._log(Level.WARN, msg, *args)
    def error(self, msg, *args):
        self._log(Level.ERROR, msg, *args)
    def fatal(self, msg, *args):
        self._log(Level.FATAL, msg, *args)

    def close(self):
        if self._file and self._file != sys.stdout and self._file != sys.stderr:
            self._file.close()

def _sanitize(s):
    new = str(s).replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t').replace('"', '\\"')
    if ' ' in new:
        new = '"%s"' % new

    return new

import qtask
