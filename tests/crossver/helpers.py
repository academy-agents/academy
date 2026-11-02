import contextlib
import os
import pathlib
import signal
import subprocess
import time
import random
import z3


envs = {}

test_run_root: pathlib.Path | None = None

def create_env(descr: dict) -> pathlib.Path:
    global test_run_root

    if test_run_root is None:
        test_run_root = pathlib.Path('.') / ('crossver-' + str(int(time.time())))

    assert test_run_root is not None

    if (
        str(descr) in envs
    ):  # this is a bit denormalised so will result in false negatives but not false positives?
        print(f'Using cached environment: {descr!s}')
        return envs[str(descr)]

    env_path = test_run_root / (
        'env_academy-' + str(random.randint(0, 999999999))
    )

    print(f'creating env for {descr} at {env_path}')

    dir = env_path.mkdir(parents=True, exist_ok=True)

    here_path = os.getcwd()

    print(f"Install deps: {install_deps}")
    print(f"Academy version: {descr['academy']}")
    print(f"Python version: {descr['python']}")
    install_target = install_deps[descr['academy']]
    py_target = py_targets[descr['python']]

    if install_target == 'HERE':
        install_target = here_path

    commandline = f'export PATH=~/.local/bin/:$PATH ; set -e ; uv venv {env_path}/venv --seed --python={py_target} ; . {env_path}/venv/bin/activate ; which python ; python -m pip install {install_target}'
    print(f"Will run command line: {commandline}")
    r = os.system(commandline)
    if r != 0:
        raise RuntimeError(f"System call exited with exit code {r}")

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

PyVersion = z3.Datatype('PythonVersion')
PyVersion.declare ('Python', ('minor', z3.IntSort()))
PyVersionSort = PyVersion.create()

py310 = PyVersionSort.Python(10)
# py312 = PyVersionSort.Python(12)
py314 = PyVersionSort.Python(14)

py_targets = {}
py_targets[py310] = "python3.10"
# py_targets[py312] = "python3.12"
py_targets[py314] = "python3.14"

def valid_python_version(v):
  # TODO: be a bit more exhaustive?
  # but restrict heavily here because this adds in a
  # lot of complexity and could take a few days to
  # test it all. for small scale testing, test far
  # fewer versions: e.g. oldest and newest?
  # return z3.Or(v == py310, v == py312, v == py314)
  return z3.Or(v == py310, v == py314)

Version = z3.Datatype('Version')
Version.declare('SemVer', ('major', z3.IntSort()), ('minor', z3.IntSort()), ('patch', z3.IntSort()))
VersionSort = Version.create()

v010 = VersionSort.SemVer(0,1,0)
v020 = VersionSort.SemVer(0,2,0)
v030 = VersionSort.SemVer(0,3,0)
v031 = VersionSort.SemVer(0,3,1)
v040 = VersionSort.SemVer(0,4,0)
v050 = VersionSort.SemVer(0,5,0)
v100 = VersionSort.SemVer(1,0,0)
v_here = VersionSort.SemVer(1,0,1)  # this should dynamically be the latest, incremented by a relevant "next release type" parameter, or configured manually and checked that it hasn't already been released, a "candidate release" variable

def valid_academy_version(v):
  return z3.Or(v == v010,
               v == v020,
               v == v030,
               v == v031,
               v == v040,
               v == v050,
               v == v100,
               v == v_here)

install_deps = {}

install_deps[v010] = 'academy-py==0.1.0'
install_deps[v020] = 'academy-py==0.2.0'
install_deps[v030] = 'academy-py==0.3.0'
install_deps[v031] = 'academy-py==0.3.1'
install_deps[v040] = 'academy-py==0.4.0'
install_deps[v050] = 'packaging academy-py==0.5.0'
install_deps[v100] = 'academy-py==1.0.0'
install_deps[v_here] = 'HERE'
