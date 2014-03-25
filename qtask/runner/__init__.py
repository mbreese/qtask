class JobRunner(object):
    def __init__(self, multiplier=1.0):
        self.multiplier = float(multiplier)

    def done(self):
        pass

    def qdel(self, *jobid):
        raise NotImplementedError

    def qrls(self, *jobid):
        raise NotImplementedError

    def qsub(self, task, monitor, cluster, dryrun=False):
        'return a tuple: (jobid, script_src)'
        raise NotImplementedError

    def _calc_time(self, val):
        seconds = 0
        if ':' in val:
            cols = [int(x) for x in val.split(':')]
            if len(cols) == 3:
                h = cols[0]
                m = cols[1]
                s = cols[2]
            elif len(cols) == 2:
                h = 0
                m = cols[0]
                s = cols[1]

            seconds = s + (m * 60) + (h * 60 * 60)
        else:
            seconds = int(val)

        seconds = seconds * self.multiplier

        h = seconds / (60 * 60)
        seconds = seconds % (60 * 60)

        m = seconds / 60
        s = seconds % 60

        return '%d:%02d:%02d' % (h, m, s)

