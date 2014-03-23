'QTask config'

import os

DEFAULT_PATH=os.path.expanduser('~/.qtaskrc')

class QTaskProperties(object):
    def __init__(self, path=None, initial=None):
        self._values = {}
        if initial:
            self._values.update(initial)

        self.load_file(DEFAULT_PATH)

        if path:
            self.load_file(path)

        self.load_env()

    def set(self, k, val, overwrite=False):
        if overwrite or k.lower() not in self._values:
            self._values[k.lower()] = _config_value(val)

    def load_file(self, path):
        if os.path.exists(path):
            for line in open(path):
                k,v = line.strip().split('=')
                self._values[k.lower().strip()] = _config_value(v.strip())

    def load_env(self):
        for env in os.environ:
            if env[:6] == 'QTASK_':
                k = env[6:].replace('_', '.').lower()
                val = os.environ[env]
                self._values[k] = _config_value(val)

    def get_prefix(self, prefix, replace=False):
        vals = {}
        for k in self._values:
            if k.startswith(prefix):
                if replace:
                    vals[k[len(prefix):]] = self._values[k]
                else:
                    vals[k] = self._values[k]
        return vals

    def __contains__(self, k):
        kl = k.lower()
        return kl in self._values

    def get(self, k, default=None):
        kl = k.lower()
        if kl in self._values:
            return self._values[kl]
        return default

    def __getitem__(self, k):
        return self.get(k)


def _config_value(val):
    if type(val) != str:
        return val

    if val.upper() in ['T', 'TRUE', 'Y']:
        return True
    if val.upper() in ['F', 'FALSE', 'N']:
        return False

    try:
        intval = int(val)
        return intval
    except:
        try:
            intval = int(val)
            return intval
        except:
            return val
