import contextlib
import os
import pathlib
import signal
import subprocess
import random


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

    install_target = descr['academy']
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


