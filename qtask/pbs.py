import subprocess
import qtask

class PBS(qtask.JobRunner):
    def __init__(self, *args, **kwargs):
        self.dry_run_cur_jobid = 1

        qtask.JobRunner.__init__(self, *args, **kwargs)

    def qdel(self, *jobid):
        subprocess.call(["qdel", ' '.join([str(x) for x in jobid])])

    def qrls(self, *jobid):
        subprocess.call(["qrls", ' '.join([str(x) for x in jobid])])
