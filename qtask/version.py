'''
Tracks the versions of pipelined programs
'''

import subprocess

_program_version_script = {}
_program_version_cache = {}

def register_program(prog, src):
    _program_version_script[prog] = src

def find_version(prog):
    if not prog in _program_version_cache:
        if prog in _program_version_script:
            proc = subprocess.Popen([_program_version_script[prog], ], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
            version = proc.communicate()[0].strip()
            if proc.returncode != 0 or 'command not found' in version:
                raise RuntimeError("Missing required program: %s" % prog)


            _program_version_cache[prog] = version

        else:
            return 'Unknown program - not registered'

    return _program_version_cache[prog]

__path_cache = set()
def check_path(prog):
    if prog in __path_cache:
        return True

    with open('/dev/null', 'w') as devnull:
        if subprocess.call("which %s" % prog, stderr=devnull, stdout=devnull, shell=True) != 0:
            raise RuntimeError("Missing required program from $PATH: %s\n\n" % prog)

    __path_cache.add(prog)
    return True
