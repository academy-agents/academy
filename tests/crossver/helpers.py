import contextlib
import os
import pathlib
import signal
import subprocess
import random
import z3


envs = {}

test_run_root: pathlib.Path | None = None

def create_env(descr: dict) -> pathlib.Path:
    global test_run_root

    if test_run_root is None:
        test_run_root = pathlib.Path('.') / ('crossver-' + str(random.randint(0, 999999999)))

    assert test_run_root is not None

    if (
        str(descr) in envs
    ):  # this is a bit denormalised so will result in false negatives but not false positives?
        print(f'Using cached environment: {descr!s}')
        return envs[str(descr)]

    env_path = test_run_root / (
        'env_' + str(random.randint(0, 999999999))
    )

    print(f'creating env for {descr} at {env_path}')

    dir = env_path.mkdir(parents=True, exist_ok=True)

    here_path = os.getcwd()

    print(install_deps)
    print(descr['academy'])
    print(type(descr['academy']))
    install_target = install_deps[descr['academy']]

    if install_target == 'HERE':
        install_target = here_path

    os.system(
        f'cd {env_path}; virtualenv ./venv; pwd; ls; . ./venv/bin/activate; which python; pip install {install_target}',
    )

    envs[str(descr)] = env_path
    return env_path


@contextlib.contextmanager
def managed_commandline(cmdline: str, *, daemon: bool, env: str):
    """context manager for managed process execution around a block of code.

    This manager will start up the process on entering the block, and
    ensure it is shut down when leaving the block.

    daemon mode specifies the shutdown expectations which will be enforced
    on leaving the block:

    a non-daemon process is expected to exit successfully itself and the
    managed commandline block will wait when leaving the block until that
    process has exited, and will raise an exception if the exit code is
    not 0.

    a daemon process is expected to remain alive up to the point that
    the block is left. the manager will terminate the process tree, and
    will raise an exception is the process exited by some other mechanism.
    """

    p = subprocess.Popen(
        f"set -e; cd {env}; . venv/bin/activate;" + cmdline,
        shell=True,
        process_group=0,
    )

    try:
        yield p
    finally:
        if daemon:
            print('terminating')
            os.killpg(p.pid, signal.SIGTERM)
            print('waiting on process')
            p.wait()
            assert p.returncode == -15, 'process should have been terminated by SIGTERM'
        else:
            p.wait()
            assert p.returncode == 0, 'process should have exited successfully'


Version = z3.Datatype('Version')
Version.declare('SemVer', ('major', z3.IntSort()), ('minor', z3.IntSort()), ('patch', z3.IntSort()))
VersionSort = Version.create()

v010 = VersionSort.SemVer(0,1,0)
v020 = VersionSort.SemVer(0,2,0)
v030 = VersionSort.SemVer(0,3,0)
v031 = VersionSort.SemVer(0,3,1)
v040 = VersionSort.SemVer(0,4,0)
v050 = VersionSort.SemVer(0,5,0)
v060 = VersionSort.SemVer(0,6,0)
v070 = VersionSort.SemVer(0,7,0)
v100 = VersionSort.SemVer(1,0,0)
v_here = VersionSort.SemVer(1,0,1)  # this should dynamically be the latest, incremented by a relevant "next release type" parameter

def valid_academy_version(v):
  return z3.Or(v == v010,
               v == v020,
               v == v030,
               v == v031,
               v == v040,
               v == v050,
               v == v060,
               v == v070,
               v == v100,
               v == v_here)

install_deps = {}

install_deps[v010] = 'academy-py==0.1.0'
install_deps[v020] = 'academy-py==0.2.0'
install_deps[v030] = 'academy-py==0.3.0'
install_deps[v031] = 'academy-py==0.3.1'
install_deps[v040] = 'academy-py==0.4.0'
install_deps[v050] = 'packaging academy-py==0.5.0'
install_deps[v060] = 'packaging git+https://github.com/academy-agents/academy@dff06fc3bdfe1b906cc9adb9490cc2e22d1406b1'  # untagged
install_deps[v070] = 'packaging git+https://github.com/academy-agents/academy@2c2127324aacf5e6402b665876b9e7e548c9506d'  # untagged
install_deps[v100] = 'academy-py==1.0.0'
install_deps[v_here] = 'HERE'

