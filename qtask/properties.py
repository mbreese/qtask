'QTask config'

import os

DEFAULT_PATH=os.path.expanduser('~/.qtaskrc')

class QTaskProperties(dict):
    def __init__(self, path=None, initial=None):
        self._writeable = True

        if initial:
            self.update(initial)

        self.load_file(DEFAULT_PATH)

        if path:
            self.load_file(path)

        self.load_env()

    def lock(self):
        self._writeable = False

    def log(self):
        for k in self:
            qtask.log.debug('config: %s = %s', k, self[k])

    def set(self, k, val, overwrite=True):
        if not self._writeable:
            raise RuntimeError("Configuration locked!")

        if overwrite or k.lower() not in self:
            self[k.lower()] = _config_value(val)

    def load_file(self, path):
        if not self._writeable:
            raise RuntimeError("Configuration locked!")
        if os.path.exists(path):
            for line in open(path):
                if '=' in line:
                    k,v = line.strip().split('=')
                    self[k.lower().strip()] = _config_value(v.strip())
                else:
                    self[line.strip().lower()] = True

    def load_env(self):
        if not self._writeable:
            raise RuntimeError("Configuration locked!")
        for env in os.environ:
            if env[:6] == 'QTASK_':
                k = env[6:].replace('_', '.').lower()
                val = os.environ[env]
                self[k] = _config_value(val)

    def get_prefix(self, prefix, replace=False):
        vals = {}
        for k in self:
            if k.startswith(prefix):
                if replace:
                    vals[k[len(prefix):]] = self[k]
                else:
                    vals[k] = self[k]
        return vals

    def __contains__(self, k):
        kl = k.lower()
        return dict.__contains__(self, kl)

    def has_key(self, k):
        kl = k.lower()
        return kl in dict.has_key(self, k)

    def get(self, k, default=None):
        kl = k.lower()
        if kl in self:
            return self[kl]
        return default

    def __setitem__(self, k, v):
        if not self._writeable:
            raise RuntimeError("Configuration locked!")

        if type(k) == int:
            dict.__setitem__(self, k, v)

        kl = k.lower()
        dict.__setitem__(self, kl, v)

    def __getitem__(self, k):
        if type(k) == int:
            return dict.__getitem__(self, k)

        kl = k.lower()
        if kl in self:
            return dict.__getitem__(self, kl)
        return None


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

import qtask
