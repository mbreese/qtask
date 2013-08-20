import subprocess
import qtask

class PBS(qtask.JobRunner):
    def __init__(self, *args, **kwargs):
        self.dry_run_cur_jobid = 1

    def qdel(self, jobid):
        subprocess.call(["qdel", str(jobid)])

    def qrls(self, jobid):
        subprocess.call(["qrls", str(jobid)])
